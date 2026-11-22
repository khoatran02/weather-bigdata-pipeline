"""Features for the short-range temperature forecast."""

import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from weather_pipeline.common.config import LOCAL_UTC_OFFSET_HOURS

HORIZONS = (1, 3, 6)
NUMERIC = [
    "temperature_c",
    "dew_point_c",
    "relative_humidity",
    "pressure_hpa",
    "wind_u",
    "wind_v",
    "temperature_change_1h",
    "temperature_change_3h",
    "pressure_change_3h",
    "hour_sin",
    "hour_cos",
    "season_sin",
    "season_cos",
    "normal_change",
    "anomaly",
]


def on_the_hour(observations: DataFrame) -> DataFrame:
    direction = F.radians(F.coalesce("wind_dir_deg", F.lit(0.0)))
    speed = F.when(F.col("wind_dir_deg").isNull(), 0.0).otherwise(F.coalesce("wind_kmh", F.lit(0.0)))
    return observations.where((F.minute("event_time") == 0) & F.col("temperature_c").isNotNull()).select(
        "station",
        F.col("event_time").alias("time"),
        "temperature_c",
        "dew_point_c",
        "relative_humidity",
        "pressure_hpa",
        (-speed * F.sin(direction)).alias("wind_u"),
        (-speed * F.cos(direction)).alias("wind_v"),
    )


def hourly_normals(hours: DataFrame) -> DataFrame:
    """Must be fitted on training years only, otherwise the evaluation leaks."""
    local = F.col("time") + F.expr(f"INTERVAL {LOCAL_UTC_OFFSET_HOURS} HOURS")
    return hours.groupBy("station", F.month(local).alias("month"), F.hour(local).alias("hour")).agg(
        F.avg("temperature_c").alias("normal")
    )


def build(hours: DataFrame, normals: DataFrame, horizon: int) -> DataFrame:
    """Join on timestamps, not row positions, so gaps can never shift a lag."""

    def earlier(offset: int, **columns: str) -> DataFrame:
        return hours.select(
            "station",
            (F.col("time") + F.expr(f"INTERVAL {offset} HOURS")).alias("time"),
            *[F.col(old).alias(new) for new, old in columns.items()],
        )

    frame = hours.join(earlier(1, t1="temperature_c"), ["station", "time"]).join(
        earlier(3, t3="temperature_c", p3="pressure_hpa"), ["station", "time"]
    )
    local = F.col("time") + F.expr(f"INTERVAL {LOCAL_UTC_OFFSET_HOURS} HOURS")
    target = local + F.expr(f"INTERVAL {horizon} HOURS")
    frame = frame.withColumns(
        {"_month": F.month(local), "_hour": F.hour(local), "_month2": F.month(target), "_hour2": F.hour(target)}
    )
    now = normals.selectExpr("station", "month AS _month", "hour AS _hour", "normal")
    then = normals.selectExpr("station", "month AS _month2", "hour AS _hour2", "normal AS normal_target")
    frame = frame.join(F.broadcast(now), ["station", "_month", "_hour"]).join(
        F.broadcast(then), ["station", "_month2", "_hour2"]
    )
    tau = 2 * math.pi
    day = F.dayofyear(local)
    return frame.withColumns(
        {
            "temperature_change_1h": F.col("temperature_c") - F.col("t1"),
            "temperature_change_3h": F.col("temperature_c") - F.col("t3"),
            "pressure_change_3h": F.col("pressure_hpa") - F.col("p3"),
            "hour_sin": F.sin(F.col("_hour") * tau / 24),
            "hour_cos": F.cos(F.col("_hour") * tau / 24),
            "season_sin": F.sin(day * tau / 365.25),
            "season_cos": F.cos(day * tau / 365.25),
            "normal_change": F.col("normal_target") - F.col("normal"),
            "anomaly": F.col("temperature_c") - F.col("normal"),
        }
    ).dropna(subset=NUMERIC)


def with_label(features: DataFrame, hours: DataFrame, horizon: int) -> DataFrame:
    """The label is the temperature change, so predicting zero equals persistence."""
    future = hours.select(
        "station",
        (F.col("time") - F.expr(f"INTERVAL {horizon} HOURS")).alias("time"),
        F.col("temperature_c").alias("actual"),
    )
    return features.join(future, ["station", "time"]).withColumn("label", F.col("actual") - F.col("temperature_c"))
