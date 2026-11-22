"""Data-quality gate. A failed check gives a non-zero exit and stops the Airflow DAG."""

import json
import time

from pyspark.sql import functions as F

from weather_pipeline.common import influx
from weather_pipeline.common.config import Settings
from weather_pipeline.common.session import spark_session
from weather_pipeline.streaming.transforms import LIMITS

STORM_KMH = 74  # 40 kt sustained
MAX_STORM_DAYS_PER_YEAR = 6


def run_checks(spark, settings: Settings) -> list[dict]:
    read = lambda layer, name: spark.read.format("delta").load(settings.table(layer, name))  # noqa: E731
    results = []

    def check(name: str, value, passed) -> None:
        results.append({"check": name, "value": float(value or 0), "passed": bool(passed)})

    for lane in ("live", "history"):
        silver = read("silver", lane)
        total = silver.count()
        repeated = total - silver.select("station", "event_id").distinct().count()
        check(f"silver_{lane}_event_id_unique", repeated, repeated == 0)
        if not total:
            continue
        outside = sum(
            silver.select(*[F.count_if(~F.col(n).between(*bounds)).alias(n) for n, bounds in LIMITS.items()]).first()
        )
        check(f"silver_{lane}_values_in_range", outside, outside == 0)

        # Offsets identify a message, so this proves bronze never stored one twice.
        bronze = read("bronze", lane)
        twice = bronze.count() - bronze.select("topic", "kafka_partition", "kafka_offset").distinct().count()
        check(f"bronze_{lane}_offsets_unique", twice, twice == 0)

        refused = read("quarantine", lane).count()
        share = refused / (total + refused)
        check(f"quarantine_{lane}_share_below_5pct", share, share < 0.05)

    observations = read("gold", "observations")
    repeated = observations.count() - observations.select("station", "event_time").distinct().count()
    check("gold_observation_key_unique", repeated, repeated == 0)

    impossible = read("gold", "daily").where("hours > 24 OR temperature_min > temperature_max").count()
    check("gold_daily_consistent", impossible, impossible == 0)

    # Range checks cannot see a typo inside the valid range. Too many storm days in a year can.
    stormy = (
        read("gold", "daily")
        .where(f"wind_max >= {STORM_KMH}")
        .groupBy("station", F.year("local_date"))
        .count()
        .where(f"count > {MAX_STORM_DAYS_PER_YEAR}")
        .count()
    )
    check("gold_storm_days_are_rare", stormy, stormy == 0)

    live = read("silver", "live").where("source = 'aviationweather'")
    age = live.select((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("event_time"))) / 3600)
    hours = age.first()[0]
    check("live_fresher_than_3h", hours, hours is not None and hours < 3)
    return results


def main() -> None:
    settings = Settings()
    spark = spark_session(f"{settings.namespace}-checks")
    results = run_checks(spark, settings)
    now = int(time.time())
    influx.write_lines(
        settings,
        influx.METRICS,
        [
            influx.line("quality_check", {"check": r["check"]}, {"value": r["value"], "passed": r["passed"]}, now)
            for r in results
        ],
    )
    print(json.dumps(results, indent=1), flush=True)
    spark.stop()
    if failed := [r["check"] for r in results if not r["passed"]]:
        raise SystemExit(f"quality gate failed: {failed}")


if __name__ == "__main__":
    main()
