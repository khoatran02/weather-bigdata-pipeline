"""Bronze to silver rules. Pure DataFrame functions built from native Spark expressions (no UDF)."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

ENVELOPE = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("station", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
        T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
    ]
)

BRONZE_SCHEMA = (
    "value string, kafka_key string, topic string, kafka_partition int, kafka_offset long, "
    "kafka_time timestamp, ingest_date date"
)

SOURCES = ("thingspeak", "aviationweather", "iem")
KNOT_KMH = 1.852
MILE_KM = 1.609344
INHG_HPA = 33.8639

# Physical plausibility for Viet Nam. A value outside is nulled and flagged, the row survives.
LIMITS = {
    "temperature_c": (-5.0, 46.0),
    "dew_point_c": (-20.0, 35.0),
    "relative_humidity": (0.0, 100.5),
    "wind_dir_deg": (0.0, 360.0),
    "wind_kmh": (0.0, 100 * KNOT_KMH),
    "gust_kmh": (0.0, 140 * KNOT_KMH),
    "pressure_hpa": (940.0, 1050.0),
    "visibility_km": (0.0, 160.0),
    "rain_raw": (0.0, 500.0),
}
MEASURES = list(LIMITS)
SILVER_COLUMNS = [
    "event_id",
    "source",
    "station",
    "event_time",
    "ingested_at",
    *MEASURES,
    "weather",
    "raw_metar",
    "qc_flags",
    "payload_hash",
    "kafka_partition",
    "kafka_offset",
]


def from_kafka(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.col("value").cast("string").alias("value"),
        F.col("key").cast("string").alias("kafka_key"),
        "topic",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_time"),
        F.to_date("timestamp").alias("ingest_date"),
    )


def relative_humidity(temperature: Column, dew_point: Column) -> Column:
    """Magnus formula."""

    def saturation(x: Column) -> Column:
        return F.exp(F.lit(17.625) * x / (F.lit(243.04) + x))

    valid = temperature.between(-60, 70) & dew_point.between(-60, 70)
    return F.when(valid, F.lit(100.0) * saturation(dew_point) / saturation(temperature))


def standardize(bronze: DataFrame) -> DataFrame:
    """One schema and one set of units for all sources; adds qc_flags and reject_reason."""
    frame = bronze.withColumn("e", F.from_json("value", ENVELOPE)).select(
        "*",
        "e.event_id",
        "e.source",
        "e.station",
        "e.schema_version",
        F.col("e.event_time").try_cast("timestamp").alias("event_time"),
        F.col("e.ingested_at").try_cast("timestamp").alias("ingested_at"),
        F.col("e.payload").alias("p"),
    )

    p = F.col("p")
    source = F.col("source")

    def raw(thingspeak=None, aviation=None, iem=None) -> Column:
        """The source-specific payload value as a string."""
        result = F.lit(None).cast("string")
        for name, key in zip(SOURCES, (thingspeak, aviation, iem)):
            if key:
                result = F.when(source == name, p[key]).otherwise(result)
        return result

    frame = frame.withColumns(
        {
            "_temperature_c": raw("field1", "temp", "tmpc"),
            "_dew_point_c": raw(None, "dewp", "dwpc"),
            "_wind_dir_deg": raw(None, "wdir", "drct"),
            "_wind": raw("field5", "wspd", "sknt"),
            "_gust": raw(None, "wgst", "gust"),
            "_pressure": raw("field4", "altim", "alti"),
            "_visibility": F.regexp_replace(raw(None, "visib", "vsby"), r"\+$", ""),
            "weather": raw(None, "wxString", "wxcodes"),
            "raw_metar": raw(None, "rawOb", "metar"),
        }
    )

    number = lambda name: F.col(name).try_cast("double")  # noqa: E731
    temperature, dew_point = number("_temperature_c"), number("_dew_point_c")
    # The QNH group of the raw report is exact; IEM's altimeter is rounded to 0.01 inHg.
    qnh = F.regexp_extract("raw_metar", r"\bQ(\d{4})\b", 1).try_cast("double")

    frame = frame.withColumns(
        {
            "temperature_c": temperature,
            "dew_point_c": dew_point,
            "relative_humidity": F.when(source == "thingspeak", p["field2"].try_cast("double")).otherwise(
                relative_humidity(temperature, dew_point)
            ),
            "wind_dir_deg": number("_wind_dir_deg"),
            # ThingSpeak field 5 is already km/h (every value is a multiple of one knot).
            "wind_kmh": F.when(source == "thingspeak", number("_wind")).otherwise(number("_wind") * KNOT_KMH),
            "gust_kmh": number("_gust") * KNOT_KMH,
            "pressure_hpa": F.when(source == "iem", F.coalesce(qnh, number("_pressure") * INHG_HPA)).otherwise(
                number("_pressure")
            ),
            "visibility_km": number("_visibility") * MILE_KM,
            # The unit of field 3 is undocumented: stored untouched and never summed as millimetres.
            "rain_raw": F.when(source == "thingspeak", p["field3"].try_cast("double")),
        }
    )

    def present_but_not_numeric(name: str) -> Column:
        return F.col(name).isNotNull() & number(name).isNull()

    def out_of_range(name: str) -> Column:
        low, high = LIMITS[name]
        return F.col(name).isNotNull() & ~F.col(name).between(low, high)

    flags = [F.when(out_of_range(name), F.lit(f"range:{name}")) for name in MEASURES]
    flags += [
        F.when(present_but_not_numeric(name), F.lit(f"unparseable:{name[1:]}"))
        for name in ("_temperature_c", "_dew_point_c", "_wind", "_pressure")
    ]
    dew_above_temperature = F.coalesce(F.col("dew_point_c") > F.col("temperature_c"), F.lit(False))
    gust_below_wind = F.coalesce(F.col("gust_kmh") < F.col("wind_kmh"), F.lit(False))
    flags += [
        F.when(dew_above_temperature, F.lit("consistency:dew_point")),
        F.when(gust_below_wind, F.lit("consistency:gust")),
    ]
    frame = frame.withColumn("qc_flags", F.array_compact(F.array(*flags)))

    def flagged(*names: str) -> Column:
        return F.arrays_overlap("qc_flags", F.array(*[F.lit(n) for n in names]))

    bad = {name: flagged(f"range:{name}") for name in MEASURES}
    bad["dew_point_c"] = flagged("range:dew_point_c", "consistency:dew_point")
    # Humidity is derived, so it is only as good as both of its inputs.
    bad["relative_humidity"] = flagged(
        "range:relative_humidity", "range:temperature_c", "range:dew_point_c", "consistency:dew_point"
    )
    bad["gust_kmh"] = flagged("range:gust_kmh", "consistency:gust")
    frame = frame.withColumns({name: F.when(~bad[name], F.col(name)) for name in MEASURES})

    unusable = F.coalesce("temperature_c", "wind_kmh", "pressure_hpa", "visibility_km").isNull()
    reason = (
        F.when(F.col("event_id").isNull(), "invalid_envelope")
        .when(F.coalesce(F.col("schema_version"), F.lit(0)) != 1, "unsupported_schema")
        .when(~F.coalesce(source.isin(*SOURCES), F.lit(False)), "unknown_source")
        .when(F.col("station").isNull() | (F.col("station") == ""), "missing_station")
        .when(F.col("event_time").isNull() | F.col("ingested_at").isNull(), "invalid_timestamp")
        .when(F.col("event_time") > F.col("ingested_at") + F.expr("INTERVAL 1 HOUR"), "event_in_future")
        .when(unusable, "no_usable_measurement")
    )
    return frame.withColumns({"reject_reason": reason, "payload_hash": F.xxhash64(F.to_json("p"))})


def accepted(frame: DataFrame) -> DataFrame:
    return frame.where(F.col("reject_reason").isNull()).select(*SILVER_COLUMNS)


def rejected(frame: DataFrame) -> DataFrame:
    return frame.where(F.col("reject_reason").isNotNull()).select(
        "reject_reason", "value", "topic", "kafka_partition", "kafka_offset", "kafka_time"
    )


def latest_per_event(frame: DataFrame) -> DataFrame:
    """Deterministic de-duplication: the most recently ingested copy wins."""
    order = F.struct("ingested_at", "kafka_offset", "kafka_partition")
    return frame.groupBy("event_id").agg(F.max_by(F.struct(*frame.columns), order).alias("r")).select("r.*")
