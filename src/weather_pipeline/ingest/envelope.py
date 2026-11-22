"""The message contract shared by every producer and the Spark jobs."""

import datetime as dt
import json

SCHEMA_VERSION = 1


def utc_iso(moment: dt.datetime) -> str:
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def envelope(source: str, station: str, key: str, event_time: str, ingested_at: str, payload: dict) -> bytes:
    """Wrap an untouched source record; payload values are strings, parsing belongs to Spark."""
    message = {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{source}:{station}:{key}",
        "source": source,
        "station": station,
        "event_time": event_time,
        "ingested_at": ingested_at,
        "payload": {k: str(v) for k, v in payload.items() if v is not None and v != ""},
    }
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode()
