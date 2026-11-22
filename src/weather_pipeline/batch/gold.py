"""Rebuild the gold layer from silver and publish serving tables to InfluxDB."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weather_pipeline.batch import aggregates
from weather_pipeline.common import influx
from weather_pipeline.common.config import Settings
from weather_pipeline.common.session import spark_session


def save(frame: DataFrame, settings: Settings, name: str, *partitions: str) -> DataFrame:
    """Overwrite atomically, then hand back the stored copy to avoid recomputing the lineage."""
    path = settings.table("gold", name)
    writer = frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)
    stored = frame.sparkSession.read.format("delta").load(path)
    print(f"gold/{name}: {stored.count()} rows", flush=True)
    return stored


def publish(settings: Settings, tables: dict[str, DataFrame]) -> None:
    def write(frame, measurement, tags, fields, time_column="time"):
        # A plain rewrite cannot clear a field that became null or a row that disappeared.
        influx.delete_measurement(settings, influx.ANALYTICS, measurement)
        influx.write_frame(frame, settings, influx.ANALYTICS, measurement, tags, fields, time_column)

    days = tables["daily"]
    write(
        days.withColumn("time", F.col("local_date").cast("timestamp")),
        "daily",
        ["station"],
        [c for c in days.columns if c not in ("station", "local_date")],
    )
    write(
        tables["monthly"].withColumn("time", F.make_date("year", "month", F.lit(1)).cast("timestamp")),
        "monthly",
        ["station"],
        ["temperature_mean"],
    )
    january_first = F.make_date("year", F.lit(1), F.lit(1)).cast("timestamp")
    write(tables["annual"].withColumn("time", january_first), "annual", ["station"], ["temperature_mean"])
    write(
        tables["quality"].withColumn("time", january_first),
        "quality",
        ["station", "source"],
        ["rows", "flagged_rows", "spikes", "climate_outliers", "wind_errors"],
    )
    write(
        tables["trends"].withColumn("time", F.current_timestamp()),
        "trend",
        ["station"],
        ["celsius_per_decade", "r2", "years", "first_year", "last_year"],
    )
    agreement = ["agree_temperature", "agree_humidity", "agree_pressure", "agree_wind"]
    write(
        tables["reconciliation"].withColumn("channel", F.lit("thingspeak")),
        "reconciliation",
        ["channel"],
        agreement,
        "event_time",
    )


def main() -> None:
    settings = Settings()
    spark = spark_session(f"{settings.namespace}-gold")
    read = lambda lane: spark.read.format("delta").load(settings.table("silver", lane))  # noqa: E731
    history, live = read("history"), read("live")

    observations = save(aggregates.clean(aggregates.canonical(history, live)), settings, "observations", "station")
    tables = {}
    tables["hourly"] = save(aggregates.hourly(observations), settings, "hourly", "station")
    tables["daily"] = save(aggregates.daily(tables["hourly"]), settings, "daily")
    tables["monthly"] = save(aggregates.monthly(tables["hourly"]), settings, "monthly")
    tables["annual"] = save(aggregates.annual(tables["monthly"]), settings, "annual")
    tables["trends"] = save(aggregates.trends(tables["annual"]), settings, "trends")
    tables["climatology"] = save(aggregates.climatology(tables["hourly"]), settings, "climatology")
    tables["quality"] = save(aggregates.quality(history.unionByName(live), observations), settings, "quality")
    tables["reconciliation"] = save(aggregates.reconcile(live, observations), settings, "reconciliation")
    publish(settings, tables)
    spark.stop()


if __name__ == "__main__":
    main()
