"""Helpers for scripts/fault_tolerance.py. They refuse to touch a non-test namespace."""

import argparse
import datetime as dt
import json
import time

from weather_pipeline.common.config import Settings
from weather_pipeline.ingest.envelope import envelope, utc_iso

START = dt.datetime(2010, 1, 1, tzinfo=dt.timezone.utc)
LANES = {"history": ("iem", "tmpc"), "live": ("aviationweather", "temp")}


def produce(settings: Settings, count: int, rate: float) -> dict:
    """Publish a known workload: `count` unique events, 10% exact duplicates and 1% garbage."""
    from weather_pipeline.ingest.kafka import ReliablePublisher

    publisher = ReliablePublisher(settings.kafka)
    ingested = utc_iso(dt.datetime.now(dt.timezone.utc))
    expected = {lane: {"bronze": 0, "silver": 0, "quarantine": 0, "temperature_sum": 0} for lane in LANES}
    started, sent = time.monotonic(), 0
    for i in range(count):
        lane = "live" if i % 10 == 0 else "history"
        source, field = LANES[lane]
        station, temperature = f"T{i % 7:03d}", 10 + i % 25
        moment = utc_iso(START + dt.timedelta(minutes=i))
        value = envelope(source, station, moment, moment, ingested, {field: temperature})
        topic = settings.topic(f"raw.{lane}")
        values = [value]
        if i % 20 in (0, 1):  # one duplicate per lane in every 20 events
            values.append(value)
        if i % 100 == 3:
            values.append(f"garbage {i}".encode())
            expected[lane]["quarantine"] += 1
        for item in values:
            publisher.send(topic, station, item)
        expected[lane]["bronze"] += len(values)
        expected[lane]["silver"] += 1
        expected[lane]["temperature_sum"] += temperature
        sent += len(values)
        if (ahead := sent / rate - (time.monotonic() - started)) > 0:
            time.sleep(ahead)
    publisher.flush()
    return expected


def verify(settings: Settings, expected: dict, timeout: int) -> dict:
    from weather_pipeline.common.session import spark_session

    spark = spark_session(f"{settings.namespace}-verify")
    deadline = time.monotonic() + timeout
    while True:
        actual = {}
        for lane in LANES:
            read = lambda layer, lane=lane: spark.read.format("delta").load(settings.table(layer, lane))  # noqa: E731
            bronze, silver = read("bronze"), read("silver")
            actual[lane] = {
                "bronze": bronze.count(),
                "silver": silver.count(),
                "quarantine": read("quarantine").count(),
                "temperature_sum": int(silver.agg({"temperature_c": "sum"}).first()[0] or 0),
                "bronze_distinct_offsets": bronze.select("topic", "kafka_partition", "kafka_offset").distinct().count(),
                "silver_distinct_events": silver.select("event_id").distinct().count(),
            }
        wanted = {
            lane: {**e, "bronze_distinct_offsets": e["bronze"], "silver_distinct_events": e["silver"]}
            for lane, e in expected.items()
        }
        overshot = any(actual[lane][key] > wanted[lane][key] for lane in LANES for key in ("bronze", "silver"))
        if actual == wanted or overshot or time.monotonic() > deadline:
            spark.stop()
            return {"passed": actual == wanted, "expected": wanted, "actual": actual}
        time.sleep(10)


def cleanup(settings: Settings) -> None:
    from confluent_kafka.admin import AdminClient

    from weather_pipeline.common.session import spark_session

    admin = AdminClient({"bootstrap.servers": settings.kafka})
    mine = [t for t in admin.list_topics(timeout=20).topics if t.startswith(f"{settings.namespace}.")]
    if mine:
        for future in admin.delete_topics(mine).values():
            future.result()
    spark = spark_session(f"{settings.namespace}-cleanup")
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{settings.lake_root}/{settings.namespace}")
    path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("produce", "verify", "cleanup"))
    parser.add_argument("--count", type=int, default=300_000)
    parser.add_argument("--rate", type=float, default=3000)
    parser.add_argument("--expected", type=json.loads)
    parser.add_argument("--timeout", type=int, default=420)
    args = parser.parse_args()
    settings = Settings()
    if not settings.namespace.startswith("e2e"):
        raise SystemExit("refusing to run outside an e2e namespace")
    if args.action == "produce":
        print("RESULT " + json.dumps(produce(settings, args.count, args.rate)), flush=True)
    elif args.action == "verify":
        print("RESULT " + json.dumps(verify(settings, args.expected, args.timeout)), flush=True)
    else:
        cleanup(settings)


if __name__ == "__main__":
    main()
