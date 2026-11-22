"""foreachBatch sinks. Each one is safe to repeat when Spark retries a batch."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weather_pipeline.common import influx
from weather_pipeline.common.config import Settings
from weather_pipeline.streaming import analytics, transforms


def to_kafka(frame: DataFrame, settings: Settings, topic: str, key: str) -> None:
    (
        frame.select(F.col(key).cast("string").alias("key"), F.to_json(F.struct("*")).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka)
        .option("topic", settings.topic(topic))
        .save()
    )


def merge_silver(events: DataFrame, path: str) -> None:
    spark = events.sparkSession
    # Station names are literals in the condition so that Delta can prune partitions.
    stations = [row.station for row in events.select("station").distinct().collect()]
    if not stations:
        return
    quoted = ",".join("'" + s.replace("\\", "").replace("'", "") + "'" for s in stations)
    events.createOrReplaceTempView("incoming")
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` t USING incoming s
        ON t.station IN ({quoted}) AND t.station = s.station AND t.event_id = s.event_id
        WHEN MATCHED AND s.ingested_at >= t.ingested_at AND s.payload_hash <> t.payload_hash
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    ).collect()


def silver_writer(settings: Settings, lane: str):
    def write(batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        parsed = transforms.standardize(batch).persist()
        events = transforms.latest_per_event(transforms.accepted(parsed)).persist()
        try:
            merge_silver(events, settings.table("silver", lane))

            bad = transforms.rejected(parsed).persist()
            if not bad.isEmpty():
                # (txnAppId, txnVersion) makes this append idempotent. The query id lives in the checkpoint.
                query_id = spark.sparkContext.getLocalProperty("sql.streaming.queryId")
                (
                    bad.write.format("delta")
                    .mode("append")
                    .option("txnAppId", f"quarantine-{query_id}")
                    .option("txnVersion", batch_id)
                    .save(settings.table("quarantine", lane))
                )
                to_kafka(bad, settings, "dlq", "reject_reason")
            bad.unpersist()

            if lane == "live":
                publish_live(events, settings)
        finally:
            events.unpersist()
            parsed.unpersist()

    return write


def publish_live(events: DataFrame, settings: Settings) -> None:
    now = F.current_timestamp()
    recent = events.where(F.col("event_time") > now - F.expr(f"INTERVAL {influx.LIVE_RETENTION_DAYS - 1} DAYS"))
    points = recent.withColumns(
        {
            "source_lag_s": F.unix_timestamp("ingested_at") - F.unix_timestamp("event_time"),
            "pipeline_lag_s": F.unix_timestamp(now) - F.unix_timestamp("ingested_at"),
            "qc_issues": F.size("qc_flags"),
        }
    )
    influx.write_frame(
        points,
        settings,
        influx.LIVE,
        "observation",
        ["station", "source"],
        [*transforms.MEASURES, "weather", "source_lag_s", "pipeline_lag_s", "qc_issues"],
        "event_time",
    )
    fired = analytics.alerts(events.where(F.col("event_time") > now - F.expr("INTERVAL 48 HOURS"))).persist()
    if not fired.isEmpty():
        to_kafka(fired, settings, "alerts", "station")
        influx.write_frame(
            fired, settings, influx.LIVE, "alert", ["station", "kind", "severity"], ["value"], "event_time"
        )
    fired.unpersist()


def window_writer(settings: Settings):
    def write(batch: DataFrame, _batch_id: int) -> None:
        influx.write_frame(
            analytics.up_to_date_windows(batch),
            settings,
            influx.LIVE,
            "window_3h",
            ["station", "source"],
            [
                "temperature_min",
                "temperature_max",
                "temperature_mean",
                "wind_max",
                "pressure_min",
                "pressure_max",
                "pressure_range",
                "observations",
            ],
            "last_event",
        )

    return write
