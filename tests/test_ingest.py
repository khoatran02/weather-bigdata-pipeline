import datetime as dt
import gzip
import json

import pytest

from weather_pipeline.common import influx
from weather_pipeline.ingest import archive, collector, replay, sources

NOW = "2026-11-22T13:00:00Z"


def test_thingspeak_key_survives_a_channel_reset():
    feeds = [
        {"entry_id": 1, "created_at": "2026-11-11T02:15:24Z", "field1": "25.0", "field3": None},
        {"entry_id": 1, "created_at": "2026-12-01T00:00:00Z", "field1": "22.0"},
    ]
    records = list(sources.parse_thingspeak(json.dumps({"feeds": feeds}).encode(), NOW))
    assert len({r.identity for r in records}) == 2
    assert "field3" not in json.loads(records[0].value)["payload"]


def test_corrected_metar_has_a_new_identity_and_the_same_event_id():
    report = {"icaoId": "VVDN", "obsTime": 1795350600, "temp": 25, "receiptTime": "a", "clouds": [{}]}
    first = next(sources.parse_aviation(json.dumps([report]).encode(), NOW))
    second = next(sources.parse_aviation(json.dumps([{**report, "receiptTime": "b"}]).encode(), NOW))
    assert first.event_time == "2026-11-22T12:30:00Z"
    assert first.identity != second.identity
    assert json.loads(first.value)["event_id"] == json.loads(second.value)["event_id"]


def test_empty_aviation_response():
    assert list(sources.parse_aviation(b"", NOW)) == []


class FakePublisher:
    def __init__(self, fail=False):
        self.fail, self.sent = fail, []

    def send(self, topic, key, value):
        self.sent.append(value)

    def flush(self):
        if self.fail:
            raise RuntimeError("broker down")


def test_poller_resends_until_kafka_acknowledges():
    record = sources.Record("VVDN", "id-1", NOW, b"{}")
    poller = collector.Poller("test", lambda full, at: [record], 1)
    with pytest.raises(RuntimeError):
        poller.poll(FakePublisher(fail=True), "topic", NOW)
    assert poller.full and not poller.seen
    assert poller.poll(FakePublisher(), "topic", NOW) == 1
    assert poller.poll(FakePublisher(), "topic", NOW) == 0


def test_archive_periods_are_contiguous():
    ranges = list(archive.periods(dt.date(1956, 12, 31), dt.date(1975, 5, 1)))
    assert [(a.year, b.year) for a, b in ranges] == [(1956, 1960), (1960, 1970), (1970, 1975)]


def test_extending_the_archive_removes_the_shorter_file(tmp_path, monkeypatch):
    station = {"station": "VVDN", "archive_begin": "2020-01-01"}
    monkeypatch.setattr(archive, "stations", lambda countries: [station])
    monkeypatch.setattr(archive.http, "get", lambda url, timeout: b"station,valid,tmpc\n")
    monkeypatch.setattr(archive.time, "sleep", lambda seconds: None)
    for day in (21, 22):
        archive.download(tmp_path, ["VN"], dt.date(2020, 1, 1), dt.date(2026, 11, day))
    assert [p.name for p in tmp_path.glob("*.gz")] == ["VVDN_2020-01-01_2026-11-22.csv.gz"]


def test_replay_reads_the_cache(tmp_path):
    path = tmp_path / "VVDN.csv.gz"
    path.write_bytes(gzip.compress(b"station,valid,tmpc\nVVDN,2026-11-22 12:30,25.00\n"))
    station, value = next(replay.records(path, NOW))
    assert station == "VVDN"
    assert json.loads(value)["event_time"] == "2026-11-22T12:30:00Z"


def test_line_protocol_escapes_and_drops_unusable_values():
    encoded = influx.line("obs", {"name": "Da Nang,VN", "x": None}, {"t": 25, "bad": float("nan"), "s": 'a"b'}, 7)
    assert encoded == 'obs,name=Da\\ Nang\\,VN t=25.0,s="a\\"b" 7'
    assert influx.line("obs", {}, {"t": None}, 7) is None
