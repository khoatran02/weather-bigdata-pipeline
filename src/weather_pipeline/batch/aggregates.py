"""Silver to gold transformations (pure DataFrame functions)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from weather_pipeline.common.config import LOCAL_UTC_OFFSET_HOURS, THINGSPEAK_REFERENCE
from weather_pipeline.streaming.transforms import MEASURES

SPIKE_C = 8.0
WIND_SPIKE_MIN_KMH = 46.3  # 25 kt
WIND_SPIKE_JUMP_KMH = 37.0  # 20 kt
WIND_SPIKE_RATIO = 3
WIND_SQUALL_MAX_KMH = 92.6  # 50 kt: still believable for a thunderstorm squall
WIND_NEIGHBOUR_S = 6 * 3600
OUTLIER_FITTED_BEFORE = 2024
OUTLIER_MARGIN_C = 5.0
OUTLIER_MIN_SAMPLES = 1000
OUTLIER_MIN_YEARS = 3
# Found by profiling: wind was converted from m/s twice (values 8, 14, 21, 27 kt are 4, 7, 11, 14 x 1.944).
BAD_WIND_PERIODS = (("VVTS", "1988-04-01", "1991-01-01"),)
SPIKE_NEIGHBOUR_S = 2 * 3600
SYNOPTIC_HOURS = 8  # 00, 03, ..., 21 UTC
MIN_DAYS_PER_HOUR = 15
TREND_SINCE = 1980
TREND_MIN_YEARS = 15
REFERENCE_YEARS = (2005, 2024)
MIRROR_DELAY_MINUTES = 7  # best alignment between the channel and the METAR, see docs/DATA.md


def canonical(history: DataFrame, live: DataFrame) -> DataFrame:
    """One row per station and observation time. The live report beats the archived copy."""
    reports = live.where(F.col("source") == "aviationweather").unionByName(history)
    priority = F.when(F.col("source") == "aviationweather", 0).otherwise(1)
    order = Window.partitionBy("station", "event_time").orderBy(priority, F.col("ingested_at").desc(), "event_id")
    return reports.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").drop("_rank")


def remove_spikes(observations: DataFrame) -> DataFrame:
    """Null temperatures that jump away from both neighbours in the same direction."""
    order = Window.partitionBy("station").orderBy("event_time")
    value, seconds = F.col("temperature_c"), F.unix_timestamp("event_time")
    before = value - F.lag(value).over(order)
    after = value - F.lead(value).over(order)
    close = (seconds - F.lag(seconds).over(order) <= SPIKE_NEIGHBOUR_S) & (
        F.lead(seconds).over(order) - seconds <= SPIKE_NEIGHBOUR_S
    )
    spike = close & (((before > SPIKE_C) & (after > SPIKE_C)) | ((before < -SPIKE_C) & (after < -SPIKE_C)))
    flagged = observations.withColumn("is_spike", F.coalesce(spike, F.lit(False)))
    good = ~F.col("is_spike")
    return flagged.withColumns(
        {
            "temperature_c": F.when(good, F.col("temperature_c")),
            "relative_humidity": F.when(good, F.col("relative_humidity")),
        }
    )


def remove_climate_outliers(observations: DataFrame) -> DataFrame:
    """Null temperatures that are impossible for this station in this calendar month.

    A global range cannot see that 3 °C is impossible in Ho Chi Minh City in June.
    """
    month = F.month("event_time")
    bounds = (
        observations.where(F.col("temperature_c").isNotNull() & (F.year("event_time") < OUTLIER_FITTED_BEFORE))
        .groupBy("station", month.alias("_month"))
        .agg(
            (F.percentile_approx("temperature_c", 0.001) - OUTLIER_MARGIN_C).alias("_low"),
            (F.percentile_approx("temperature_c", 0.999) + OUTLIER_MARGIN_C).alias("_high"),
            F.count("*").alias("_samples"),
            F.countDistinct(F.year("event_time")).alias("_years"),
        )
        .where((F.col("_samples") >= OUTLIER_MIN_SAMPLES) & (F.col("_years") >= OUTLIER_MIN_YEARS))
        .select("station", "_month", "_low", "_high")
    )
    joined = observations.withColumn("_month", month).join(F.broadcast(bounds), ["station", "_month"], "left")
    outside = F.coalesce(~F.col("temperature_c").between(F.col("_low"), F.col("_high")), F.lit(False))
    flagged = joined.withColumn("is_climate_outlier", outside)
    good = ~F.col("is_climate_outlier")
    return flagged.withColumns(
        {
            "temperature_c": F.when(good, F.col("temperature_c")),
            "relative_humidity": F.when(good, F.col("relative_humidity")),
        }
    ).drop("_month", "_low", "_high")


def remove_wind_errors(observations: DataFrame) -> DataFrame:
    """Null isolated wind typos (30053KT among 3 kt reports) and known bad periods.

    Real storms survive: their neighbours are windy too, or the report carries a gust group
    or a thunderstorm.
    """
    order = Window.partitionBy("station").orderBy("event_time")
    seconds, wind = F.unix_timestamp("event_time"), F.col("wind_kmh")

    def neighbour(offset: int):
        distance = F.abs(F.lag(seconds, offset).over(order) - seconds)
        return F.when(distance <= WIND_NEIGHBOUR_S, F.lag(wind, offset).over(order))

    frame = observations.withColumn("_around", F.array(*[neighbour(i) for i in (2, 1, -1, -2)]))
    frame = frame.withColumn("_around", F.sort_array(F.array_compact("_around")))
    size = F.size("_around")
    median = F.when(size >= 2, F.get("_around", F.floor((size - 1) / 2).cast("int")))
    squall = F.coalesce(F.col("weather"), F.lit("")).rlike("TS") & (wind < WIND_SQUALL_MAX_KMH)
    spike = (
        (wind >= WIND_SPIKE_MIN_KMH)
        & (wind > median * WIND_SPIKE_RATIO)
        & (wind - median >= WIND_SPIKE_JUMP_KMH)
        & F.col("gust_kmh").isNull()
        & ~squall
    )
    known_bad = F.lit(False)
    for station, start, stop in BAD_WIND_PERIODS:
        known_bad |= (
            (F.col("station") == station)
            & (F.col("event_time") >= start)
            & (F.col("event_time") < stop)
            & wind.isNotNull()
        )
    flagged = frame.withColumn("is_wind_error", F.coalesce(spike | known_bad, F.lit(False)))
    good = ~F.col("is_wind_error")
    return flagged.withColumns({"wind_kmh": F.when(good, wind), "gust_kmh": F.when(good, F.col("gust_kmh"))}).drop(
        "_around"
    )


def clean(observations: DataFrame) -> DataFrame:
    """Offline QC. It looks at neighbours in both directions, so it is not for model inputs."""
    return remove_wind_errors(remove_spikes(remove_climate_outliers(observations)))


def hourly(observations: DataFrame) -> DataFrame:
    weather = F.coalesce("weather", F.lit(""))
    result = observations.groupBy("station", F.date_trunc("hour", "event_time").alias("hour_utc")).agg(
        F.avg("temperature_c").alias("temperature_c"),
        F.min("temperature_c").alias("temperature_min"),
        F.max("temperature_c").alias("temperature_max"),
        F.avg("relative_humidity").alias("relative_humidity"),
        F.avg("wind_kmh").alias("wind_kmh"),
        F.max("wind_kmh").alias("wind_max"),
        F.max("gust_kmh").alias("gust_max"),
        F.avg("pressure_hpa").alias("pressure_hpa"),
        F.min("visibility_km").alias("visibility_min"),
        F.max(weather.rlike("RA|DZ")).alias("has_rain"),
        F.max(weather.rlike("TS")).alias("has_thunder"),
        F.count("*").alias("observations"),
    )
    local = F.col("hour_utc") + F.expr(f"INTERVAL {LOCAL_UTC_OFFSET_HOURS} HOURS")
    return result.withColumns({"local_date": F.to_date(local), "local_hour": F.hour(local)})


def daily(hours: DataFrame) -> DataFrame:
    return hours.groupBy("station", "local_date").agg(
        F.count("*").alias("hours"),
        (F.count("*") / 24).alias("coverage"),
        F.avg("temperature_c").alias("temperature_mean"),
        F.min("temperature_min").alias("temperature_min"),
        F.max("temperature_max").alias("temperature_max"),
        F.avg("relative_humidity").alias("humidity_mean"),
        F.max("wind_max").alias("wind_max"),
        F.max("gust_max").alias("gust_max"),
        F.avg("pressure_hpa").alias("pressure_mean"),
        F.min("visibility_min").alias("visibility_min"),
        F.sum(F.col("has_rain").cast("int")).alias("rain_hours"),
        F.sum(F.col("has_thunder").cast("int")).alias("thunder_hours"),
    )


def monthly(hours: DataFrame) -> DataFrame:
    """Monthly mean that stays comparable when the reporting schedule changes.

    Older decades report every three hours and drop night shifts; recent ones every 30 minutes.
    Only the eight synoptic hours are used, each weighted equally, and all eight are required.
    """
    per_hour = (
        hours.where((F.hour("hour_utc") % 3 == 0) & F.col("temperature_c").isNotNull())
        .groupBy(
            "station",
            F.year("local_date").alias("year"),
            F.month("local_date").alias("month"),
            F.hour("hour_utc").alias("utc_hour"),
        )
        .agg(F.avg("temperature_c").alias("value"), F.count("*").alias("days"))
        .where(F.col("days") >= MIN_DAYS_PER_HOUR)
    )
    return (
        per_hour.groupBy("station", "year", "month")
        .agg(F.avg("value").alias("temperature_mean"), F.count("*").alias("synoptic_hours"))
        .where(F.col("synoptic_hours") == SYNOPTIC_HOURS)
    )


def annual(months: DataFrame) -> DataFrame:
    return (
        months.groupBy("station", "year")
        .agg(F.avg("temperature_mean").alias("temperature_mean"), F.count("*").alias("months"))
        .where(F.col("months") == 12)
    )


def trends(years: DataFrame) -> DataFrame:
    y, x = F.col("temperature_mean"), F.col("year").cast("double")
    return (
        years.where(F.col("year") >= TREND_SINCE)
        .groupBy("station")
        .agg(
            (F.regr_slope(y, x) * 10).alias("celsius_per_decade"),
            F.regr_r2(y, x).alias("r2"),
            F.count("*").alias("years"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .where(F.col("years") >= TREND_MIN_YEARS)
    )


def climatology(hours: DataFrame) -> DataFrame:
    return (
        hours.where(F.year("local_date").between(*REFERENCE_YEARS))
        .groupBy("station", F.month("local_date").alias("month"), "local_hour")
        .agg(
            F.avg("temperature_c").alias("temperature_mean"),
            F.stddev("temperature_c").alias("temperature_std"),
            F.count("temperature_c").alias("samples"),
        )
    )


def quality(silver: DataFrame, cleaned: DataFrame) -> DataFrame:
    keys = ["source", "station", "year"]
    stats = (
        silver.withColumn("year", F.year("event_time"))
        .groupBy(*keys)
        .agg(
            F.count("*").alias("rows"),
            F.sum((F.size("qc_flags") > 0).cast("int")).alias("flagged_rows"),
            *[F.count(name).alias(f"valid_{name}") for name in MEASURES],
        )
    )
    removed = {"spikes": "is_spike", "climate_outliers": "is_climate_outlier", "wind_errors": "is_wind_error"}
    offline = (
        cleaned.withColumn("year", F.year("event_time"))
        .groupBy(*keys)
        .agg(*[F.count_if(flag).alias(name) for name, flag in removed.items()])
    )
    return stats.join(offline, keys, "left").fillna(0, subset=list(removed))


def reconcile(live: DataFrame, observations: DataFrame) -> DataFrame:
    """Compare every ThingSpeak row with the METAR it was copied from."""
    delayed = F.unix_timestamp("event_time") - MIRROR_DELAY_MINUTES * 60
    mirror = live.where(F.col("source") == "thingspeak").select(
        "event_id",
        "event_time",
        F.timestamp_seconds(delayed - F.pmod(delayed, F.lit(1800))).alias("metar_time"),
        "temperature_c",
        "relative_humidity",
        "pressure_hpa",
        "wind_kmh",
    )
    report = observations.where(F.col("station") == THINGSPEAK_REFERENCE).select(
        F.col("event_time").alias("metar_time"),
        F.col("temperature_c").alias("ref_temperature"),
        F.col("relative_humidity").alias("ref_humidity"),
        F.col("pressure_hpa").alias("ref_pressure"),
        F.col("wind_kmh").alias("ref_wind"),
    )

    def agrees(left: str, right: str, tolerance: float):
        return (F.abs(F.col(left) - F.col(right)) <= tolerance).cast("double")

    return mirror.join(report, "metar_time", "left").withColumns(
        {
            "agree_temperature": agrees("temperature_c", "ref_temperature", 0.1),
            "agree_humidity": agrees("relative_humidity", "ref_humidity", 1.0),
            "agree_pressure": agrees("pressure_hpa", "ref_pressure", 0.6),
            "agree_wind": agrees("wind_kmh", "ref_wind", 0.06),
        }
    )
