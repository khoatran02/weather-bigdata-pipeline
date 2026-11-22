"""Adapters that turn public APIs into envelopes. The parse functions are pure."""

import datetime as dt
import json
from collections.abc import Iterator
from typing import NamedTuple

from weather_pipeline.common.config import THINGSPEAK_STATION
from weather_pipeline.ingest import http
from weather_pipeline.ingest.envelope import envelope, utc_iso

THINGSPEAK_URL = "https://api.thingspeak.com/channels/{}/feeds.json?results={}"
AVIATION_URL = "https://aviationweather.gov/api/data/metar?ids={}&format=json&hours={}"


class Record(NamedTuple):
    station: str
    identity: str  # changes when the upstream record is new or has been corrected
    event_time: str
    value: bytes


def parse_thingspeak(body: bytes, ingested_at: str) -> Iterator[Record]:
    for feed in json.loads(body)["feeds"]:
        station, key, moment = THINGSPEAK_STATION, str(feed["entry_id"]), feed["created_at"]
        # entry_id restarts if the owner clears the channel, so the timestamp is part of the key.
        key = f"{key}@{moment}"
        value = envelope("thingspeak", station, key, moment, ingested_at, feed)
        yield Record(station, f"thingspeak:{key}", moment, value)


def parse_aviation(body: bytes, ingested_at: str) -> Iterator[Record]:
    if not body.strip():  # the API answers 204 when there are no reports
        return
    for report in json.loads(body):
        station = report["icaoId"]
        moment = utc_iso(dt.datetime.fromtimestamp(report["obsTime"], dt.timezone.utc))
        payload = {k: v for k, v in report.items() if not isinstance(v, (list, dict))}
        value = envelope("aviationweather", station, moment, moment, ingested_at, payload)
        identity = f"aviationweather:{station}:{moment}:{report.get('receiptTime')}"
        yield Record(station, identity, moment, value)


def fetch_thingspeak(channel: str, full: bool, ingested_at: str) -> list[Record]:
    body = http.get(THINGSPEAK_URL.format(channel, 8000 if full else 100), attempts=3)
    return list(parse_thingspeak(body, ingested_at))


def fetch_aviation(stations: str, full: bool, ingested_at: str) -> list[Record]:
    body = http.get(AVIATION_URL.format(stations, 48 if full else 3), attempts=3)
    return list(parse_aviation(body, ingested_at))
