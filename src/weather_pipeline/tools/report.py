"""Print the numbers quoted in docs/RESULTS.md as JSON (read-only)."""

import json

from pyspark.sql import functions as F

from weather_pipeline.common.config import Settings
from weather_pipeline.common.session import spark_session
from weather_pipeline.streaming.transforms import MEASURES


def collect(spark, settings: Settings) -> dict:
    read = lambda layer, name: spark.read.format("delta").load(settings.table(layer, name))  # noqa: E731
    rows = lambda frame: [r.asDict() for r in frame.collect()]  # noqa: E731
    result = {}
    for lane in ("live", "history"):
        bronze, silver, refused = read("bronze", lane), read("silver", lane), read("quarantine", lane)
        counts = {"bronze": bronze.count(), "silver": silver.count(), "quarantine": refused.count()}
        counts["duplicates_merged"] = counts["bronze"] - counts["silver"] - counts["quarantine"]
        result[lane] = {
            **counts,
            "bronze_bytes": spark.sql(f"DESCRIBE DETAIL delta.`{settings.table('bronze', lane)}`").first().sizeInBytes,
            "silver_bytes": spark.sql(f"DESCRIBE DETAIL delta.`{settings.table('silver', lane)}`").first().sizeInBytes,
            "reject_reasons": rows(refused.groupBy("reject_reason").count()),
            "qc_flags": rows(silver.select(F.explode("qc_flags").alias("flag")).groupBy("flag").count()),
            "sums": silver.select(*[F.count(m).alias(m) for m in MEASURES]).first().asDict(),
            "per_station": rows(
                silver.groupBy("station", "source").agg(
                    F.count("*").alias("rows"),
                    F.min("event_time").alias("first"),
                    F.max("event_time").alias("last"),
                    F.sum("temperature_c").alias("temperature_sum"),
                    F.count("temperature_c").alias("temperature_n"),
                )
            ),
        }
    flags = ("is_spike", "is_climate_outlier", "is_wind_error")
    cleaned = read("gold", "observations").select(*[F.count_if(flag).alias(flag) for flag in flags])
    result["offline_qc"] = cleaned.first().asDict()
    result["trends"] = rows(read("gold", "trends"))
    agreement = ("temperature", "humidity", "pressure", "wind")
    result["thingspeak_vs_metar"] = (
        read("gold", "reconciliation")
        .agg(
            F.count("*").alias("rows"),
            F.count("ref_temperature").alias("matched"),
            *[F.avg(f"agree_{name}").alias(name) for name in agreement],
        )
        .first()
        .asDict()
    )
    result["model_metrics"] = rows(read("gold", "model_metrics").orderBy("horizon", "method"))
    return result


def main() -> None:
    settings = Settings()
    spark = spark_session(f"{settings.namespace}-report")
    print("REPORT " + json.dumps(collect(spark, settings), default=str), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
