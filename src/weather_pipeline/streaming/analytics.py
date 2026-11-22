"""Real-time analytics on the live stream: event-time windows and threshold alerts."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

WATERMARK = "3 hours"
WINDOW = "3 hours"
SLIDE = "30 minutes"


def rolling_windows(observations: DataFrame) -> DataFrame:
    """Sliding windows whose every aggregate is unaffected by duplicate delivery."""
    grouped = (
        observations.withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", WINDOW, SLIDE), "station", "source")
        .agg(
            F.max("event_time").alias("last_event"),
            F.min("temperature_c").alias("temperature_min"),
            F.max("temperature_c").alias("temperature_max"),
            F.max("wind_kmh").alias("wind_max"),
            F.min("pressure_hpa").alias("pressure_min"),
            F.max("pressure_hpa").alias("pressure_max"),
            # A set instead of avg()/count(), which would count a redelivered event twice.
            F.collect_set(F.struct("event_time", "temperature_c")).alias("samples"),
        )
    )
    temperatures = F.filter(F.col("samples.temperature_c"), lambda x: x.isNotNull())
    total = F.aggregate(temperatures, F.lit(0.0), lambda acc, x: acc + x)
    return grouped.select(
        "station",
        "source",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "last_event",
        "temperature_min",
        "temperature_max",
        F.when(F.size(temperatures) > 0, total / F.size(temperatures)).alias("temperature_mean"),
        "wind_max",
        "pressure_min",
        "pressure_max",
        (F.col("pressure_max") - F.col("pressure_min")).alias("pressure_range"),
        F.size("samples").alias("observations"),
    )


def up_to_date_windows(windows: DataFrame) -> DataFrame:
    """Drop windows that extend into the future of the data they contain."""
    return windows.where(F.col("last_event") >= F.col("window_end") - F.expr(f"INTERVAL {SLIDE}"))


def alerts(observations: DataFrame) -> DataFrame:
    t, rh = F.col("temperature_c"), F.col("relative_humidity")
    f = t * 1.8 + 32
    # Rothfusz regression used by the US National Weather Service; valid from about 27 °C.
    heat_index_f = (
        -42.379
        + 2.04901523 * f
        + 10.14333127 * rh
        - 0.22475541 * f * rh
        - 0.00683783 * f * f
        - 0.05481717 * rh * rh
        + 0.00122874 * f * f * rh
        + 0.00085282 * f * rh * rh
        - 0.00000199 * f * f * rh * rh
    )
    heat_index = F.when((t >= 27) & (rh >= 40), (heat_index_f - 32) / 1.8)
    weather = F.col("weather")

    def alert(condition, kind: str, severity: str, value):
        return F.when(
            condition,
            F.struct(
                F.lit(kind).alias("kind"),
                F.lit(severity).alias("severity"),
                value.cast("double").alias("value"),
            ),
        )

    rules = [
        alert(F.col("wind_kmh") >= 39, "strong_wind", "warning", F.col("wind_kmh")),  # Beaufort 6
        alert(F.col("gust_kmh") >= 62, "gale_gust", "critical", F.col("gust_kmh")),  # Beaufort 8
        alert(heat_index >= 41, "dangerous_heat", "critical", heat_index),
        alert(F.col("pressure_hpa") < 1000, "low_pressure", "warning", F.col("pressure_hpa")),
        alert(F.col("visibility_km") < 1.5, "low_visibility", "warning", F.col("visibility_km")),
        alert(weather.rlike("TS"), "thunderstorm", "warning", F.lit(1.0)),
        alert(weather.rlike(r"\+(SH|TS)?RA"), "heavy_rain", "warning", F.lit(1.0)),
    ]
    return (
        observations.where(F.col("source") != "thingspeak")  # a mirror, it would alert twice
        .withColumn("a", F.explode(F.array_compact(F.array(*rules))))
        .select("event_id", "station", "event_time", "a.kind", "a.severity", "a.value")
    )
