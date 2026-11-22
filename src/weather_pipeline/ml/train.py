"""Train temperature models with a chronological split and compare them with naive baselines.

Training years fit the models and the normals, the validation year picks the model, and the
test years only produce the reported numbers: nothing is selected or tuned on them.
"""

import datetime as dt
import json
import time

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import functions as F

from weather_pipeline.batch import aggregates
from weather_pipeline.common import influx
from weather_pipeline.common.config import Settings
from weather_pipeline.common.session import spark_session
from weather_pipeline.ml import features

TRAIN_FROM = "2005-01-01"
VALID_FROM = "2024-01-01"
TEST_FROM = "2025-01-01"


def candidates() -> dict[str, Pipeline]:
    index = StringIndexer(inputCol="station", outputCol="station_index", handleInvalid="keep")
    one_hot = OneHotEncoder(inputCol="station_index", outputCol="station_vector")
    assemble = VectorAssembler(inputCols=[*features.NUMERIC, "station_vector"], outputCol="features")
    scale = StandardScaler(inputCol="features", outputCol="scaled")
    linear = LinearRegression(featuresCol="scaled", regParam=0.001)
    boosted = GBTRegressor(maxIter=50, maxDepth=5, stepSize=0.15, subsamplingRate=0.8, seed=42)
    return {
        "linear_regression": Pipeline(stages=[index, one_hot, assemble, scale, linear]),
        "gradient_boosted_trees": Pipeline(stages=[index, one_hot, assemble, boosted]),
    }


def score(frame, change) -> dict:
    error = F.col("temperature_c") + change - F.col("actual")
    row = frame.select(
        F.avg(F.abs(error)).alias("mae"), F.sqrt(F.avg(error * error)).alias("rmse"), F.count("*").alias("n")
    ).first()
    return row.asDict()


def main() -> None:
    settings = Settings()
    spark = spark_session(f"{settings.namespace}-train")
    read = lambda lane: spark.read.format("delta").load(settings.table("silver", lane))  # noqa: E731
    # Silver, not gold: gold QC looks at the following reports, which serving never has.
    observations = aggregates.canonical(read("history"), read("live"))
    hours = features.on_the_hour(observations).where(F.col("time") >= TRAIN_FROM).persist()
    normals = features.hourly_normals(hours.where(F.col("time") < VALID_FROM)).persist()

    report = []
    for horizon in features.HORIZONS:
        data = features.with_label(features.build(hours, normals, horizon), hours, horizon).persist()
        when = F.col("time")
        # Leave a gap of one horizon before each boundary so no label crosses into the next split.
        margin = dt.timedelta(hours=horizon)
        valid_start = dt.datetime.fromisoformat(VALID_FROM)
        test_start = dt.datetime.fromisoformat(TEST_FROM)
        train = data.where(when < F.lit(valid_start - margin))
        valid = data.where((when >= F.lit(valid_start)) & (when < F.lit(test_start - margin)))
        test = data.where(when >= F.lit(test_start))

        fitted, validation, seconds = {}, {}, {}
        for name, pipeline in candidates().items():
            started = time.monotonic()
            fitted[name] = pipeline.fit(train)
            seconds[name] = round(time.monotonic() - started, 1)
            validation[name] = score(fitted[name].transform(valid), F.col("prediction"))
        winner = min(validation, key=lambda name: validation[name]["mae"])

        fitted[winner].write().overwrite().save(f"{settings.model(horizon)}/pipeline")
        result = {
            "horizon": horizon,
            "model": winner,
            "train_rows": train.count(),
            "fit_seconds": seconds,
            "validation": validation,
            "test": {
                "persistence": score(test, F.lit(0.0)),
                "diurnal_baseline": score(test, F.col("normal_change")),
                "selected_model": score(fitted[winner].transform(test), F.col("prediction")),
            },
        }
        report.append(result)
        print(json.dumps(result), flush=True)
        data.unpersist()

    normals.write.format("delta").mode("overwrite").save(settings.table("models", "normals"))
    flat = [
        (r["horizon"], r["model"], method, float(m["mae"]), float(m["rmse"]), int(m["n"]))
        for r in report
        for method, m in r["test"].items()
    ]
    table = spark.createDataFrame(flat, "horizon int, model string, method string, mae double, rmse double, n long")
    table.write.format("delta").mode("overwrite").save(settings.table("gold", "model_metrics"))
    influx.write_frame(
        table.withColumn("time", F.current_timestamp()),
        settings,
        influx.ANALYTICS,
        "model_metrics",
        ["horizon", "method"],
        ["mae", "rmse", "n"],
        "time",
    )
    spark.stop()


if __name__ == "__main__":
    main()
