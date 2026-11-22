"""Score the most recent observation of every station."""

from functools import reduce

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from weather_pipeline.common import influx
from weather_pipeline.common.config import Settings
from weather_pipeline.common.session import spark_session
from weather_pipeline.ml import features


def model_exists(spark, path: str) -> bool:
    target = spark._jvm.org.apache.hadoop.fs.Path(path)
    return target.getFileSystem(spark._jsc.hadoopConfiguration()).exists(target)


def main() -> None:
    settings = Settings()
    spark = spark_session(f"{settings.namespace}-forecast")
    if not all(model_exists(spark, f"{settings.model(h)}/pipeline") for h in features.HORIZONS):
        print("no trained model yet; run `make train`", flush=True)
        spark.stop()
        return

    recent = (
        spark.read.format("delta")
        .load(settings.table("silver", "live"))
        .where("source = 'aviationweather'")
        .where(F.col("event_time") > F.current_timestamp() - F.expr("INTERVAL 12 HOURS"))
    )
    hours = features.on_the_hour(recent).persist()
    normals = spark.read.format("delta").load(settings.table("models", "normals"))

    outputs = []
    for horizon in features.HORIZONS:
        ready = features.build(hours, normals, horizon)
        newest = ready.groupBy("station").agg(F.max("time").alias("time"))
        latest = ready.join(newest, ["station", "time"])
        scored = PipelineModel.load(f"{settings.model(horizon)}/pipeline").transform(latest)
        outputs.append(
            scored.select(
                "station",
                F.lit(horizon).alias("horizon"),
                F.col("time").alias("issued_at"),
                (F.col("time") + F.expr(f"INTERVAL {horizon} HOURS")).alias("valid_at"),
                (F.col("temperature_c") + F.col("prediction")).alias("temperature_c"),
            )
        )
    result = reduce(lambda a, b: a.unionByName(b), outputs).persist()
    result.write.format("delta").mode("overwrite").save(settings.table("gold", "forecast_latest"))
    influx.write_frame(result, settings, influx.LIVE, "forecast", ["station", "horizon"], ["temperature_c"], "valid_at")
    result.orderBy("station", "horizon").show(50, False)
    spark.stop()


if __name__ == "__main__":
    main()
