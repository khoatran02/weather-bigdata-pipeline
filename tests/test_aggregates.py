import datetime as dt

import pytest

from weather_pipeline.batch import aggregates
from weather_pipeline.ingest.envelope import envelope
from weather_pipeline.ml import features

INGESTED = "2026-11-22T13:00:00Z"


def row(source, moment, temperature, station="VVDN"):
    key = {"iem": "tmpc", "aviationweather": "temp", "thingspeak": "field1"}[source]
    return envelope(source, station, moment, moment, INGESTED, {key: temperature})


def test_canonical_prefers_the_live_report(silver):
    at = "2026-11-22T10:00:00Z"
    live = silver(row("aviationweather", at, 26), row("thingspeak", at, 30, "TS3574112"))
    merged = aggregates.canonical(silver(row("iem", at, 25)), live).collect()
    assert [(r.station, r.temperature_c) for r in merged] == [("VVDN", 26)]


def test_spike_is_removed_and_step_change_is_kept(silver):
    values = [25, 25, 40, 25, 25, 15, 15]
    frame = silver(*[row("iem", f"2026-11-22T0{h}:00:00Z", v) for h, v in enumerate(values)])
    cleaned = aggregates.remove_spikes(frame).orderBy("event_time").collect()
    assert [r.temperature_c for r in cleaned] == [25, 25, None, 25, 25, 15, 15]


def winds(silver, knots, station="VVPB", start="2017-11-02", **middle):
    """Half-hourly reports; `middle` adds fields to the central one."""
    events = []
    for i, value in enumerate(knots):
        moment = f"{start}T{i // 2:02d}:{30 * (i % 2):02d}:00Z"
        payload = {"sknt": value, **(middle if i == len(knots) // 2 else {})}
        events.append(envelope("iem", station, moment, moment, INGESTED, payload))
    cleaned = aggregates.remove_wind_errors(silver(*events)).orderBy("event_time").collect()
    return [r.is_wind_error for r in cleaned]


def test_wind_typos_are_removed(silver):
    assert winds(silver, [3, 3, 53, 6, 7]) == [False, False, True, False, False]  # VVPB 2012-10-13
    assert winds(silver, [4, 5, 65, 66, 5]) == [False, False, True, True, False]  # two in a row, VVPB 2017


def test_real_storms_are_kept(silver):
    assert not any(winds(silver, [35, 38, 40, 38, 40]))  # typhoon Ketsana, Da Nang 2009
    assert not any(winds(silver, [31, 32, 41, 44, 44]))  # typhoon Yagi, Cat Bi 2024
    assert not any(winds(silver, [5, 6, 40, 7, 5], gust=55))
    assert not any(winds(silver, [5, 6, 35, 7, 5], wxcodes="TSRA"))


def test_known_double_converted_period(silver):
    assert all(winds(silver, [8, 14, 21], station="VVTS", start="1989-07-23"))


def test_climate_outlier_is_station_and_month_specific(spark):
    start = dt.datetime(2000, 6, 1)
    rows = [
        ("VVTS", start.replace(year=2000 + y) + dt.timedelta(hours=h), 24.0 + h % 10, 80.0)
        for y in range(3)
        for h in range(720)
    ]
    rows.append(("VVTS", dt.datetime(2003, 6, 25, 19, 30), 3.0, 80.0))  # a real error, seen in 1992
    frame = spark.createDataFrame(
        rows, "station string, event_time timestamp, temperature_c double, relative_humidity double"
    )
    flagged = aggregates.remove_climate_outliers(frame).where("is_climate_outlier").collect()
    assert [(r.event_time.year, r.temperature_c) for r in flagged] == [(2003, None)]


def test_daily_uses_the_local_calendar_day(silver):
    frame = silver(row("iem", "2026-11-20T16:00:00Z", 22), row("iem", "2026-11-20T17:00:00Z", 24))
    days = {r.local_date: r.temperature_mean for r in aggregates.daily(aggregates.hourly(frame)).collect()}
    assert days == {dt.date(2026, 11, 20): 22, dt.date(2026, 11, 21): 24}


def test_monthly_mean_is_not_biased_by_sampling_frequency(spark):
    rows = []
    for day in range(1, 31):
        for hour in range(24):
            moment = dt.datetime(2024, 6, day, hour)
            local = moment + dt.timedelta(hours=7)
            if local.month == 6:
                # The hours in between are extreme and must not leak into the result.
                rows.append(("VVDN", moment, local.date(), 20.0 if hour % 3 == 0 else 99.0))
    hours = spark.createDataFrame(rows, "station string, hour_utc timestamp, local_date date, temperature_c double")
    assert [r.temperature_mean for r in aggregates.monthly(hours).collect()] == [20.0]
    day_shift_only = hours.where("hour(hour_utc) < 12")
    assert aggregates.monthly(day_shift_only).count() == 0


def test_trend_slope(spark):
    rows = [("VVDN", year, 25 + 0.02 * (year - 1980)) for year in range(1980, 2010)]
    years = spark.createDataFrame(rows, "station string, year int, temperature_mean double")
    assert aggregates.trends(years).first().celsius_per_decade == pytest.approx(0.2)


def test_thingspeak_is_matched_to_the_delayed_metar(silver):
    live = silver(
        row("thingspeak", "2026-11-22T12:28:00Z", 25, "TS3574112"),
        row("aviationweather", "2026-11-22T12:00:00Z", 25),
    )
    reference = live.where("source = 'aviationweather'")
    match = aggregates.reconcile(live, reference).first()
    assert match.metar_time == dt.datetime(2026, 11, 22, 12, 0)
    assert match.agree_temperature == 1


def test_features_never_pair_across_a_gap(spark):
    schema = "station string, time timestamp, temperature_c double, dew_point_c double, relative_humidity double, "
    schema += "pressure_hpa double, wind_u double, wind_v double"
    times = [0, 1, 2, 3, 4, 9, 10]  # hours 5 to 8 are missing
    rows = [("VVDN", dt.datetime(2024, 6, 1, h), 20.0 + h, 15.0, 80.0, 1010.0, 0.0, 0.0) for h in times]
    hours = spark.createDataFrame(rows, schema)
    normals = spark.createDataFrame(
        [("VVDN", 6, hour, 25.0) for hour in range(24)], "station string, month int, hour int, normal double"
    )
    built = features.build(hours, normals, 1)
    assert sorted(r.time.hour for r in built.collect()) == [3, 4]
    labelled = features.with_label(built, hours, 1).collect()
    assert [(r.time.hour, r.label) for r in labelled] == [(3, 1.0)]
