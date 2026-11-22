"""InfluxDB line-protocol sink without third-party dependencies, usable on executors."""

import datetime as dt
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping
from decimal import Decimal
from functools import partial

from pyspark.sql import DataFrame

from weather_pipeline.common.config import Settings

LIVE = "weather_live"
ANALYTICS = "weather_analytics"
METRICS = "pipeline_metrics"
LIVE_RETENTION_DAYS = 90
BATCH_SIZE = 5000


def escape(value) -> str:
    text = str(value).replace("\\", "\\\\").replace("\n", " ").replace("\r", " ")
    return text.replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")


def field_value(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
        return f'"{text}"'
    if isinstance(value, (bool, int, float, Decimal)):
        number = float(value)
        # All numbers are floats so that a field never changes type between batches.
        return repr(number) if math.isfinite(number) else None
    raise TypeError(f"unsupported field type: {type(value).__name__}")


def line(measurement: str, tags: Mapping, fields: Mapping, epoch: int) -> str | None:
    encoded = [(name, field_value(value)) for name, value in fields.items()]
    body = ",".join(f"{escape(name)}={value}" for name, value in encoded if value is not None)
    if not body:
        return None
    labels = "".join(f",{escape(k)}={escape(v)}" for k, v in sorted(tags.items()) if v is not None and str(v) != "")
    return f"{escape(measurement)}{labels} {body} {epoch}"


def write_lines(settings: Settings, bucket: str, lines: list[str], attempts: int = 5) -> None:
    if not lines or not settings.influx_url:
        return
    query = urllib.parse.urlencode({"org": settings.influx_org, "bucket": bucket, "precision": "s"})
    request = urllib.request.Request(
        f"{settings.influx_url}/api/v2/write?{query}",
        data="\n".join(lines).encode(),
        headers={"Authorization": f"Token {settings.influx_token}", "Content-Type": "text/plain"},
    )
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=30):
                return
        except urllib.error.HTTPError as error:
            if error.code < 500 and error.code != 429:
                raise RuntimeError(f"InfluxDB rejected write: {error.read()[:300]!r}") from error
            if attempt == attempts:
                raise
        except OSError:
            if attempt == attempts:
                raise
        time.sleep(2**attempt)


def delete_measurement(settings: Settings, bucket: str, measurement: str) -> None:
    if not settings.influx_url:
        return
    query = urllib.parse.urlencode({"org": settings.influx_org, "bucket": bucket})
    body = {
        "start": "1900-01-01T00:00:00Z",
        "stop": "2100-01-01T00:00:00Z",
        "predicate": f'_measurement="{measurement}"',
    }
    request = urllib.request.Request(
        f"{settings.influx_url}/api/v2/delete?{query}",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Token {settings.influx_token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=120):
        pass


def _write_partition(settings, bucket, measurement, tags, fields, time_column, rows: Iterable):
    pending = []
    for row in rows:
        moment = row[time_column]
        if moment is None:
            continue
        # Spark hands executors naive datetimes in the process timezone, which is UTC in our images.
        epoch = int(moment.replace(tzinfo=dt.timezone.utc).timestamp())
        encoded = line(measurement, {t: row[t] for t in tags}, {f: row[f] for f in fields}, epoch)
        if encoded:
            pending.append(encoded)
        if len(pending) >= BATCH_SIZE:
            write_lines(settings, bucket, pending)
            pending = []
    write_lines(settings, bucket, pending)


def write_frame(
    frame: DataFrame,
    settings: Settings,
    bucket: str,
    measurement: str,
    tags: list[str],
    fields: list[str],
    time_column: str,
) -> None:
    """Idempotent: the same measurement, tags and timestamp overwrite each other."""
    # No early return without InfluxDB: streaming state only commits if the batch is consumed.
    frame.select(*dict.fromkeys([time_column, *tags, *fields])).foreachPartition(
        partial(_write_partition, settings, bucket, measurement, tags, fields, time_column)
    )
