"""Streaming entry point: five queries in one driver.

One driver is deliberate: Delta on plain S3 only guarantees atomic commits for a single
writing driver, and this app is the only writer of bronze, silver and quarantine.
"""

import logging

from pyspark.sql import DataFrame, SparkSession

from weather_pipeline.common.config import Settings, env
from weather_pipeline.common.session import spark_session
from weather_pipeline.streaming import analytics, monitor, sinks, transforms

LANES = {"live": "10000", "history": "250000"}  # max offsets per micro-batch


def create_tables(spark: SparkSession, settings: Settings) -> None:
    """Downstream streams and MERGE need their tables to exist before the first row."""
    bronze = spark.createDataFrame([], transforms.BRONZE_SCHEMA)
    parsed = transforms.standardize(bronze)
    for lane in LANES:
        for layer, frame, partition in (
            ("bronze", bronze, "ingest_date"),
            ("silver", transforms.accepted(parsed), "station"),
            ("quarantine", transforms.rejected(parsed), "reject_reason"),
        ):
            frame.write.format("delta").mode("ignore").partitionBy(partition).save(settings.table(layer, lane))


def kafka_stream(spark: SparkSession, settings: Settings, lane: str) -> DataFrame:
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka)
        .option("subscribe", settings.topic(f"raw.{lane}"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", LANES[lane])
        .option("failOnDataLoss", "true")
        .load()
    )
    return transforms.from_kafka(stream)


def start_queries(spark: SparkSession, settings: Settings) -> None:
    trigger = env("TRIGGER_INTERVAL", "5 seconds")

    def start(writer, name: str):
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", name)
        return (
            writer.queryName(name)
            .option("checkpointLocation", settings.checkpoint(name))
            .trigger(processingTime=trigger)
            .start()
        )

    for lane in LANES:
        bronze = settings.table("bronze", lane)
        start(
            kafka_stream(spark, settings, lane)
            .writeStream.format("delta")
            .partitionBy("ingest_date")
            .option("path", bronze),
            f"bronze_{lane}",
        )
        start(
            spark.readStream.format("delta")
            .option("maxBytesPerTrigger", "64m")
            .load(bronze)
            .writeStream.foreachBatch(sinks.silver_writer(settings, lane)),
            f"silver_{lane}",
        )

    live = transforms.accepted(transforms.standardize(kafka_stream(spark, settings, "live")))
    start(
        analytics.rolling_windows(live).writeStream.outputMode("update").foreachBatch(sinks.window_writer(settings)),
        "windows_live",
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = Settings()
    spark = spark_session(f"{settings.namespace}-streaming")
    create_tables(spark, settings)
    spark.streams.addListener(monitor.ProgressListener(settings))
    start_queries(spark, settings)
    # If any query dies the driver exits; Compose restarts it and it resumes from checkpoints.
    spark.streams.awaitAnyTermination()
    raise SystemExit("a streaming query stopped")


if __name__ == "__main__":
    main()
