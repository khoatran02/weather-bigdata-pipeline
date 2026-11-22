import pytest

from weather_pipeline.ingest.envelope import envelope
from weather_pipeline.streaming import analytics


def metar(time, **payload):
    moment = f"2026-11-22T{time}:00Z"
    return envelope("aviationweather", "VVDN", moment, moment, "2026-11-22T13:00:00Z", payload)


def test_windows_ignore_duplicate_delivery(silver):
    a, b = metar("10:00", temp=20, altim=1010), metar("10:30", temp=30, altim=1014)
    rows = analytics.rolling_windows(silver(a, a, a, b)).collect()
    window = next(r for r in rows if r.window_start.hour == 10 and r.window_start.minute == 0)
    assert window.observations == 2
    assert window.temperature_mean == pytest.approx(25)
    assert window.pressure_range == 4


def test_window_mean_skips_missing_values(silver):
    rows = analytics.rolling_windows(silver(metar("10:00", temp=20), metar("10:30", wspd=4))).collect()
    window = next(r for r in rows if r.window_start.hour == 10 and r.window_start.minute == 0)
    assert window.temperature_mean == 20


def test_only_one_up_to_date_window_per_observation_time(silver):
    frame = silver(metar("10:00", temp=20), metar("10:30", temp=21), metar("11:00", temp=22))
    current = analytics.up_to_date_windows(analytics.rolling_windows(frame)).collect()
    assert len(current) == len({r.last_event for r in current}) == 3


@pytest.mark.parametrize(
    ("payload", "kind"),
    [
        ({"wspd": 25}, "strong_wind"),
        ({"wspd": 20, "wgst": 36}, "gale_gust"),
        ({"temp": 36, "dewp": 27}, "dangerous_heat"),
        ({"altim": 995}, "low_pressure"),
        ({"visib": "0.5"}, "low_visibility"),
        ({"temp": 25, "wxString": "TS"}, "thunderstorm"),
        ({"temp": 25, "wxString": "+SHRA"}, "heavy_rain"),
    ],
)
def test_alert_rules(silver, payload, kind):
    assert [r.kind for r in analytics.alerts(silver(metar("10:00", **payload))).collect()] == [kind]


def test_ordinary_weather_raises_no_alert(silver):
    payload = {"temp": 25, "dewp": 22, "wspd": 5, "altim": 1019, "visib": "6+", "wxString": "-RA"}
    assert analytics.alerts(silver(metar("10:00", **payload))).count() == 0
