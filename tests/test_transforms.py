import pytest

from weather_pipeline.ingest.envelope import envelope
from weather_pipeline.streaming import transforms

AT = "2026-11-22T12:30:00Z"
INGESTED = "2026-11-22T12:36:00Z"


def event(source, payload, event_time=AT, ingested=INGESTED, station="VVDN", key=AT):
    return envelope(source, station, key, event_time, ingested, payload)


def iem(**overrides):
    row = {"tmpc": "25.00", "dwpc": "22.00", "sknt": "3.00", "alti": "30.09", "vsby": "6.21"}
    return event("iem", {**row, **overrides})


def one(bronze, value):
    return transforms.standardize(bronze(value)).first()


def test_all_sources_agree_on_units(bronze):
    thingspeak = event("thingspeak", {"field1": "25.0", "field2": "83", "field4": "1019", "field5": "5.6"})
    aviation = event("aviationweather", {"temp": 25, "dewp": 22, "wspd": 3, "altim": 1019, "visib": "6+"})
    archive = iem(metar="VVDN 221230Z 03003KT 9999 FEW020 25/22 Q1019 NOSIG")
    rows = transforms.standardize(bronze(thingspeak, aviation, archive)).collect()
    assert len(rows) == 3
    for row in rows:
        assert row.reject_reason is None
        assert row.qc_flags == []
        assert row.temperature_c == 25
        assert row.relative_humidity == pytest.approx(83, abs=1)
        assert row.wind_kmh == pytest.approx(5.556, abs=0.05)
        assert row.pressure_hpa == pytest.approx(1019, abs=0.01)


def test_altimeter_is_the_fallback_when_qnh_is_truncated(bronze):
    row = one(bronze, iem(metar="VVDN 030330Z 03005MPS 6000 25/22 Q101 TEMPO 2000 RA"))
    assert row.pressure_hpa == pytest.approx(30.09 * 33.8639)


@pytest.mark.parametrize(
    ("column", "raw", "value", "flag"),
    [
        # All four are real values from the Da Nang archive.
        ("wind_kmh", "sknt", "1360.69", "range:wind_kmh"),
        ("temperature_c", "tmpc", "56.00", "range:temperature_c"),
        ("pressure_hpa", "alti", "2.98", "range:pressure_hpa"),
        ("gust_kmh", "gust", "971.92", "range:gust_kmh"),
    ],
)
def test_implausible_value_is_nulled_but_row_survives(bronze, column, raw, value, flag):
    row = one(bronze, iem(**{raw: value}))
    assert row[column] is None
    assert flag in row.qc_flags
    assert row.reject_reason is None


def test_bad_temperature_also_invalidates_derived_humidity(bronze):
    assert one(bronze, iem(tmpc="56.00")).relative_humidity is None


def test_dew_point_above_temperature(bronze):
    row = one(bronze, iem(tmpc="20", dwpc="24"))
    assert (row.temperature_c, row.dew_point_c, row.relative_humidity) == (20, None, None)
    assert "consistency:dew_point" in row.qc_flags


def test_gust_below_wind(bronze):
    row = one(bronze, iem(sknt="20", gust="5"))
    assert row.gust_kmh is None
    assert "consistency:gust" in row.qc_flags


def test_unparseable_number_does_not_fail_under_ansi_mode(bronze):
    row = one(bronze, iem(tmpc="M"))
    assert row.temperature_c is None
    assert "unparseable:temperature_c" in row.qc_flags


def test_variable_wind_direction_is_not_an_error(bronze):
    row = one(bronze, event("aviationweather", {"temp": 25, "wdir": "VRB", "wspd": 2}))
    assert row.wind_dir_deg is None
    assert row.qc_flags == []


def test_rain_keeps_the_raw_number(bronze):
    assert one(bronze, event("thingspeak", {"field1": "25", "field3": "0.15"})).rain_raw == 0.15


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("not json", "invalid_envelope"),
        ("{}", "invalid_envelope"),
        ({"event_id": "x", "schema_version": 9, "source": "iem"}, "unsupported_schema"),
        ({"event_id": "x", "schema_version": 1, "source": "nope"}, "unknown_source"),
        ({"event_id": "x", "schema_version": 1, "source": "iem"}, "missing_station"),
        (event("iem", {"tmpc": "25"}, event_time="yesterday"), "invalid_timestamp"),
        (event("iem", {"tmpc": "25"}, event_time="2027-01-01T00:00:00Z"), "event_in_future"),
        (event("iem", {"metar": "VVDN NIL"}), "no_usable_measurement"),
    ],
)
def test_bad_records_are_quarantined_with_a_reason(bronze, value, reason):
    frame = transforms.standardize(bronze(value))
    assert transforms.rejected(frame).first().reject_reason == reason
    assert transforms.accepted(frame).count() == 0


def test_accepted_and_rejected_partition_the_input(bronze):
    frame = transforms.standardize(bronze(iem(), "garbage", iem(tmpc="26")))
    assert transforms.accepted(frame).count() == 2
    assert transforms.rejected(frame).count() == 1


def test_latest_ingested_copy_wins(silver):
    old = event("iem", {"tmpc": "24"}, ingested="2026-11-22T12:36:00Z")
    new = event("iem", {"tmpc": "25"}, ingested="2026-11-22T12:50:00Z")
    rows = transforms.latest_per_event(silver(new, old, old)).collect()
    assert [r.temperature_c for r in rows] == [25]
