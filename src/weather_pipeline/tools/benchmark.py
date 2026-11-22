"""Benchmark workloads. scripts/benchmark.py controls the cluster resources."""

import argparse
import json
import statistics
import time

from pyspark.sql import functions as F

from weather_pipeline.common.config import Settings
from weather_pipeline.common.session import spark_session

COLUMNS = ["station", "source", "event_time", "temperature_c", "wind_kmh", "pressure_hpa", "relative_humidity"]
REPEATS = 3


def timed(action) -> dict:
    action()  # warm-up: JIT, connections and file listings
    runs = []
    for _ in range(REPEATS):
        started = time.monotonic()
        action()
        runs.append(round(time.monotonic() - started, 2))
    return {"median_s": statistics.median(runs), "runs_s": runs}


def path_size(spark, path: str) -> int:
    target = spark._jvm.org.apache.hadoop.fs.Path(path)
    return target.getFileSystem(spark._jsc.hadoopConfiguration()).getContentSummary(target).getLength()


def prepare(spark, settings: Settings, root: str, factor: int) -> dict:
    source = spark.read.format("delta").load(settings.table("silver", "history")).select(*COLUMNS)
    replicated = source.crossJoin(F.broadcast(spark.range(factor).withColumnRenamed("id", "replica")))
    replicated.repartition(64).write.mode("overwrite").parquet(f"{root}/scale")
    plain = source.repartition(16)
    plain.write.mode("overwrite").json(f"{root}/json")
    plain.write.mode("overwrite").option("header", True).csv(f"{root}/csv")
    plain.write.mode("overwrite").parquet(f"{root}/parquet")
    partitioned = source.repartition("station").sortWithinPartitions("station", "event_time")
    partitioned.write.format("delta").mode("overwrite").partitionBy("station").save(f"{root}/delta")
    return {"rows": source.count(), "replicated_rows": spark.read.parquet(f"{root}/scale").count()}


def scaling(spark, root: str) -> dict:
    def run():
        frame = spark.read.parquet(f"{root}/scale")
        keys = ["station", "replica", F.date_trunc("hour", "event_time").alias("hour")]
        frame.groupBy(*keys).agg(
            F.avg("temperature_c"),
            F.max("wind_kmh"),
            F.min("pressure_hpa"),
            F.stddev("relative_humidity"),
        ).write.format("noop").mode("overwrite").save()

    return timed(run)


def formats(spark, root: str) -> dict:
    schema = spark.read.format("delta").load(f"{root}/delta").select(*COLUMNS).schema
    readers = {
        "json": lambda: spark.read.schema(schema).json(f"{root}/json"),
        "csv": lambda: spark.read.schema(schema).option("header", True).csv(f"{root}/csv"),
        "parquet": lambda: spark.read.parquet(f"{root}/parquet"),
        "delta": lambda: spark.read.format("delta").load(f"{root}/delta"),
    }
    result = {}
    for name, read in readers.items():
        full = lambda read=read: read().groupBy("station").agg(F.avg("temperature_c")).collect()  # noqa: E731
        selective = lambda read=read: (  # noqa: E731
            read()
            .where("station = 'VVDN' AND event_time >= '2024-01-01' AND event_time < '2025-01-01'")
            .agg(F.avg("temperature_c"))
            .collect()
        )
        result[name] = {
            "bytes": path_size(spark, f"{root}/{name}"),
            "full_scan": timed(full),
            "one_station_one_year": timed(selective),
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "scaling", "formats", "cleanup"))
    parser.add_argument("--factor", type=int, default=20)
    args = parser.parse_args()
    settings = Settings()
    root = f"{settings.lake_root}/benchmark"
    spark = spark_session(f"benchmark-{args.action}")
    if args.action == "prepare":
        result = prepare(spark, settings, root, args.factor)
    elif args.action == "scaling":
        result = scaling(spark, root)
    elif args.action == "formats":
        result = formats(spark, root)
    else:
        path = spark._jvm.org.apache.hadoop.fs.Path(root)
        result = {"deleted": path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)}
    print("RESULT " + json.dumps(result), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
