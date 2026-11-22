"""Replay the cached archive through Kafka, so history uses the same path as live data."""

import argparse
import csv
import datetime as dt
import gzip
import json
import logging
import time
from collections.abc import Iterator
from pathlib import Path

from weather_pipeline.common.config import Settings
from weather_pipeline.ingest.envelope import envelope, utc_iso
from weather_pipeline.ingest.kafka import ReliablePublisher

log = logging.getLogger("replay")


def records(path: Path, ingested_at: str) -> Iterator[tuple[str, bytes]]:
    with gzip.open(path, "rt", newline="", encoding="utf-8", errors="replace") as handle:
        for row in csv.DictReader(handle):
            station, valid = row.pop("station"), row.pop("valid")
            # Archive timestamps are UTC ("2026-11-10 00:30"); malformed ones are rejected by Spark.
            moment = f"{valid.replace(' ', 'T')}:00Z"
            yield station, envelope("iem", station, moment, moment, ingested_at, row)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("/data/cache"))
    parser.add_argument("--rate", type=float, default=0, help="messages per second, 0 is unlimited")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    files = sorted(args.directory.glob("*.csv.gz"))
    if not files:
        raise SystemExit(f"no archive in {args.directory}; run `make archive` first")
    settings = Settings()
    topic = settings.topic("raw.history")
    publisher = ReliablePublisher(settings.kafka)
    ingested_at = utc_iso(dt.datetime.now(dt.timezone.utc))
    started, count, size = time.monotonic(), 0, 0
    for path in files:
        for station, value in records(path, ingested_at):
            publisher.send(topic, station, value)
            count += 1
            size += len(value)
            if args.rate and (ahead := count / args.rate - (time.monotonic() - started)) > 0:
                time.sleep(ahead)
            if count == args.limit:
                break
        log.info("queued %s (%d so far)", path.name, count)
        if args.limit and count >= args.limit:
            break
    publisher.flush()
    seconds = time.monotonic() - started
    summary = {
        "messages": count,
        "bytes": size,
        "seconds": round(seconds, 1),
        "messages_per_second": round(count / seconds),
    }
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
