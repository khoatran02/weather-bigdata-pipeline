#!/usr/bin/env python3
"""Generate the provisioned Grafana dashboards (run `make dashboards` after editing)."""

import json
from pathlib import Path

OUTPUT = Path(__file__).resolve().parent.parent / "docker/grafana/dashboards"
INFLUX = {"type": "influxdb", "uid": "influx"}
PROMETHEUS = {"type": "prometheus", "uid": "prometheus"}

# Validated for colour-vision deficiency on Grafana's dark surface. Colour follows the station.
STATIONS = {
    "VVDN": ("Đà Nẵng", "#3987e5"),
    "VVNB": ("Nội Bài", "#d95926"),
    "VVTS": ("Tân Sơn Nhất", "#199e70"),
    "VVCI": ("Cát Bi", "#c98500"),
    "VVPB": ("Phú Bài", "#d55181"),
    "VVDL": ("Liên Khương", "#008300"),
    "VVVD": ("Vân Đồn", "#9085e9"),
}
DAY_MS = 24 * 3600 * 1000
BLUE, ORANGE, AQUA = "#3987e5", "#d95926", "#199e70"
PICKED = "r.station =~ /^${station:regex}$/"


def station_overrides() -> list[dict]:
    return [
        {
            "matcher": {"id": "byName", "options": code},
            "properties": [
                {"id": "color", "value": {"mode": "fixed", "fixedColor": color}},
                {"id": "displayName", "value": f"{name} ({code})"},
            ],
        }
        for code, (name, color) in STATIONS.items()
    ]


def named(*series: tuple[str, str, str]) -> list[dict]:
    return [
        {
            "matcher": {"id": "byName", "options": key},
            "properties": [
                {"id": "color", "value": {"mode": "fixed", "fixedColor": color}},
                {"id": "displayName", "value": label},
            ],
        }
        for key, label, color in series
    ]


def flux(bucket: str, measurement: str, where: str, *steps: str, start="v.timeRangeStart", stop="v.timeRangeStop"):
    lines = [
        f'from(bucket: "{bucket}")',
        f"  |> range(start: {start}, stop: {stop})",
        f'  |> filter(fn: (r) => r._measurement == "{measurement}" and {where})',
    ]
    return "\n".join(lines + [f"  |> {step}" for step in steps])


def one_series_per(column: str) -> list[str]:
    """Name each series after a tag, so legends and colour overrides can match on it."""
    return [
        f'keep(columns: ["_time", "_value", "{column}"])',
        "group()",
        f'pivot(rowKey: ["_time"], columnKey: ["{column}"], valueColumn: "_value")',
        'sort(columns: ["_time"])',
    ]


def panel(
    title,
    kind,
    box,
    queries,
    *,
    unit="none",
    description="",
    overrides=None,
    options=None,
    custom=None,
    datasource=INFLUX,
    extra=None,
):
    targets = []
    for index, query in enumerate(queries if isinstance(queries, list) else [queries]):
        target = {"refId": chr(65 + index), "datasource": datasource}
        if datasource is INFLUX:
            target["query"] = query
        else:
            target["expr"], target["legendFormat"] = query
        targets.append(target)
    defaults = {"unit": unit, "color": {"mode": "palette-classic"}, "custom": custom or {}, **(extra or {})}
    x, y, w, h = box
    return {
        "title": title,
        "type": kind,
        "description": description,
        "datasource": datasource,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": options or {},
    }


def line_chart(title, box, queries, *, bars=False, points=False, **kwargs):
    style = "bars" if bars else "points" if points else "line"
    custom = {
        "drawStyle": style,
        "lineWidth": 2,
        "fillOpacity": 80 if bars else 0,
        "pointSize": 8,
        "showPoints": "always" if points else "never",
        "spanNulls": True,
        "axisSoftMin": 0 if bars else None,
    }
    options = {
        "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
        "tooltip": {"mode": "multi", "sort": "desc"},
    }
    return panel(title, "timeseries", box, queries, options=options, custom=custom, **kwargs)


def break_gaps(chart: dict, milliseconds: int, markers: bool = False) -> dict:
    """Draw missing periods as gaps rather than as straight lines."""
    style = chart["fieldConfig"]["defaults"]["custom"]
    style.update(spanNulls=milliseconds, insertNulls=milliseconds)
    if markers:
        style.update(showPoints="always", pointSize=5)
    return chart


def labelled(table: dict, *columns: tuple[str, str, int]) -> dict:
    """Order and rename table columns: (field, label, decimals)."""
    table["transformations"] = [
        {"id": "organize", "options": {"indexByName": {name: i for i, (name, _, _) in enumerate(columns)}}}
    ]
    table["fieldConfig"]["overrides"] = [
        {
            "matcher": {"id": "byName", "options": name},
            "properties": [{"id": "displayName", "value": label}, {"id": "decimals", "value": decimals}],
        }
        for name, label, decimals in columns
    ]
    return table


def stat(title, box, query, color=BLUE, **kwargs):
    options = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "colorMode": "none",
        "graphMode": "none",
        "textMode": "value",
    }
    extra = {
        "color": {"mode": "fixed", "fixedColor": color},
        "decimals": kwargs.pop("decimals", 1),
        "noValue": kwargs.pop("no_value", "Chưa có"),
    }
    return panel(title, "stat", box, query, options=options, extra=extra, **kwargs)


def station_variable(multi: bool) -> dict:
    return {
        "name": "station",
        "label": "Trạm",
        "type": "custom",
        "query": ",".join(STATIONS),
        "multi": multi,
        "includeAll": multi,
        "current": {"text": "All", "value": "$__all"} if multi else {"text": "VVDN", "value": "VVDN"},
        "options": [],
    }


def realtime() -> dict:
    metar = f'r.source == "aviationweather" and {PICKED}'
    focus = 'r.source == "aviationweather" and r.station == "VVDN"'
    panels = []
    tiles = [
        ("Nhiệt độ", "temperature_c", "celsius", 1),
        ("Độ ẩm", "relative_humidity", "percent", 0),
        ("Gió", "wind_kmh", "velocitykmh", 1),
        ("Áp suất", "pressure_hpa", "pressurehpa", 0),
    ]
    for index, (title, field, unit, decimals) in enumerate(tiles):
        query = flux("weather_live", "observation", f'{focus} and r._field == "{field}"', "last()", start="-6h")
        panels.append(stat(f"Đà Nẵng: {title}", (index * 5, 0, 5, 4), query, unit=unit, decimals=decimals))
    fired = flux("weather_live", "alert", 'r._field == "value"', "group()", "count()")
    panels.append(stat("Cảnh báo trong kỳ", (20, 0, 4, 4), fired, decimals=0, no_value="0"))

    charts = [
        ("Nhiệt độ (METAR)", "temperature_c", "celsius", (0, 4, 12, 9)),
        ("Độ ẩm tương đối", "relative_humidity", "percent", (12, 4, 12, 9)),
        ("Tốc độ gió", "wind_kmh", "velocitykmh", (0, 13, 12, 9)),
        ("Áp suất QNH", "pressure_hpa", "pressurehpa", (12, 13, 12, 9)),
    ]
    for title, field, unit, box in charts:
        query = flux("weather_live", "observation", f'{metar} and r._field == "{field}"', *one_series_per("station"))
        panels.append(line_chart(title, box, query, unit=unit, overrides=station_overrides()))

    versus = flux(
        "weather_live",
        "observation",
        'r._field == "temperature_c" and (r.station == "TS3574112" or r.station == "VVDN")',
        *one_series_per("source"),
    )
    panels.append(
        line_chart(
            "Kênh ThingSpeak và METAR gốc (VVDN)",
            (0, 22, 12, 9),
            versus,
            unit="celsius",
            description="Kênh 3574112 sao chép METAR Đà Nẵng, trễ khoảng 7 đến 37 phút.",
            overrides=named(("thingspeak", "ThingSpeak", ORANGE), ("aviationweather", "METAR", BLUE)),
        )
    )
    observed = flux(
        "weather_live",
        "observation",
        f'{focus} and r._field == "temperature_c"',
        'keep(columns: ["_time", "_value"])',
        'rename(columns: {_value: "observed"})',
        start="-30h",
    )
    predicted = flux(
        "weather_live",
        "forecast",
        'r.station == "VVDN"',
        *one_series_per("horizon"),
        start="-30h",
        stop="7h",
    )
    forecast = line_chart(
        "Đà Nẵng: dự báo Spark MLlib và thực đo",
        (12, 22, 12, 9),
        [observed, predicted],
        points=True,
        unit="celsius",
        overrides=named(
            ("observed", "Thực đo", "#c3c2b7"),
            ("1", "Dự báo +1 giờ", BLUE),
            ("3", "Dự báo +3 giờ", ORANGE),
            ("6", "Dự báo +6 giờ", AQUA),
        ),
    )
    panels.append(forecast)

    rolling = flux("weather_live", "window_3h", f'{metar} and r._field == "wind_max"', *one_series_per("station"))
    panels.append(
        line_chart(
            "Gió lớn nhất trong cửa sổ trượt 3 giờ (streaming)",
            (0, 31, 12, 9),
            rolling,
            unit="velocitykmh",
            overrides=station_overrides(),
        )
    )
    table = flux(
        "weather_live",
        "alert",
        f'r._field == "value" and {PICKED}',
        "group()",
        'sort(columns: ["_time"], desc: true)',
        "limit(n: 100)",
        'keep(columns: ["_time", "station", "kind", "severity", "_value"])',
    )
    panels.append(panel("Cảnh báo từ Spark", "table", (12, 31, 12, 9), table))
    return dashboard(
        "realtime", "1. Thời tiết thời gian thực", "now-30h", panels, [station_variable(True)], "1m", "now+7h"
    )


def history() -> dict:
    panels = []
    years = flux("weather_analytics", "annual", PICKED, *one_series_per("station"))
    yearly = line_chart(
        "Nhiệt độ trung bình năm",
        (0, 0, 16, 10),
        years,
        unit="celsius",
        overrides=station_overrides(),
        description="8 giờ synop có trọng số bằng nhau, chỉ lấy năm đủ 12 tháng. Chuỗi chưa được đồng nhất hoá.",
    )
    # Years without full coverage must show as gaps, not as straight lines across a decade.
    break_gaps(yearly, 2 * 366 * DAY_MS, markers=True)
    panels.append(yearly)
    slope = flux(
        "weather_analytics",
        "trend",
        "true",
        "last()",
        'pivot(rowKey: ["station"], columnKey: ["_field"], valueColumn: "_value")',
        "group()",
        'keep(columns: ["station", "celsius_per_decade", "r2", "years", "first_year", "last_year"])',
        start="-30d",
        stop="now()",
    )
    table = panel("Xu hướng từ 1980 (chuỗi chưa đồng nhất hoá)", "table", (16, 0, 8, 10), slope)
    columns = [
        ("station", "Trạm", 0),
        ("celsius_per_decade", "°C/10 năm", 2),
        ("r2", "R²", 2),
        ("years", "Số năm", 0),
        ("first_year", "Từ", 0),
        ("last_year", "Đến", 0),
    ]
    panels.append(labelled(table, *columns))
    charts = [
        ("Nhiệt độ ngày cao nhất (max theo cửa sổ)", "temperature_max", "celsius", "max", (0, 10, 12, 9)),
        ("Nhiệt độ ngày thấp nhất (min theo cửa sổ)", "temperature_min", "celsius", "min", (12, 10, 12, 9)),
        ("Gió lớn nhất", "wind_max", "velocitykmh", "max", (0, 19, 12, 9)),
        ("Số giờ quan trắc mỗi ngày", "hours", "none", "mean", (12, 19, 12, 9)),
    ]
    for title, field, unit, how, box in charts:
        query = flux(
            "weather_analytics",
            "daily",
            f'{PICKED} and r._field == "{field}"',
            f"aggregateWindow(every: 90d, fn: {how}, createEmpty: false)",
            *one_series_per("station"),
        )
        chart = line_chart(title, box, query, unit=unit, overrides=station_overrides())
        panels.append(break_gaps(chart, 200 * DAY_MS))
    return dashboard("history", "2. Khí hậu lịch sử (gold)", "1980-01-01T00:00:00Z", panels, [station_variable(True)])


def operations() -> dict:
    panels = [
        stat("Broker Kafka", (0, 0, 4, 4), [("kafka_brokers", "brokers")], datasource=PROMETHEUS, decimals=0),
        stat(
            "Partition thiếu replica",
            (4, 0, 4, 4),
            [("sum(kafka_topic_partition_under_replicated_partition)", "partitions")],
            datasource=PROMETHEUS,
            decimals=0,
        ),
        stat(
            "Spark worker",
            (8, 0, 4, 4),
            [('count(up{job="spark-workers"} == 1)', "workers")],
            datasource=PROMETHEUS,
            decimals=0,
        ),
        stat(
            "Offset chưa xử lý",
            (12, 0, 4, 4),
            flux(
                "pipeline_metrics",
                "streaming",
                'r._field == "offsets_behind"',
                "last()",
                "group()",
                "max()",
                start="-10m",
            ),
            decimals=0,
        ),
        stat(
            "Kiểm tra chất lượng lỗi",
            (16, 0, 4, 4),
            flux(
                "pipeline_metrics",
                "quality_check",
                'r._field == "passed"',
                "last()",
                "map(fn: (r) => ({r with _value: 1.0 - r._value}))",
                "group()",
                "sum()",
                start="-14d",
            ),
            decimals=0,
        ),
        stat(
            "Message đã vào Kafka",
            (20, 0, 4, 4),
            [('sum(kafka_topic_partition_current_offset{topic=~"weather[.].*"})', "messages")],
            datasource=PROMETHEUS,
            unit="short",
        ),
    ]
    queries = named(
        ("bronze_live", "Bronze live", BLUE),
        ("bronze_history", "Bronze history", ORANGE),
        ("silver_live", "Silver live", AQUA),
        ("silver_history", "Silver history", "#c98500"),
        ("windows_live", "Windows live", "#d55181"),
    )
    charts = [
        ("Số dòng mỗi micro-batch", "input_rows", "short", (0, 4, 12, 9)),
        ("Thời gian micro-batch", "batch_ms", "ms", (12, 4, 12, 9)),
        ("Tốc độ xử lý (dòng/giây)", "processed_rows_per_second", "short", (0, 13, 12, 9)),
        ("Dòng trong state store", "state_rows", "short", (12, 13, 12, 9)),
    ]
    for title, field, unit, box in charts:
        query = flux(
            "pipeline_metrics",
            "streaming",
            f'r._field == "{field}"',
            "aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)",
            *one_series_per("query"),
        )
        chart = line_chart(title, box, query, unit=unit, overrides=queries)
        # A query only reports timings when it had work to do.
        panels.append(break_gaps(chart, 90_000, markers=True))

    agreement = flux(
        "weather_analytics",
        "reconciliation",
        "true",
        'group(columns: ["_field"])',
        "mean()",
        "map(fn: (r) => ({r with _value: r._value * 100.0}))",
        "group()",
        'keep(columns: ["_field", "_value"])',
        start="-90d",
    )
    panels.append(panel("ThingSpeak khớp METAR (%)", "table", (0, 22, 8, 8), agreement, extra={"decimals": 1}))
    checks = flux(
        "pipeline_metrics",
        "quality_check",
        "true",
        "last()",
        'pivot(rowKey: ["check"], columnKey: ["_field"], valueColumn: "_value")',
        "group()",
        'keep(columns: ["check", "passed", "value"])',
        start="-14d",
    )
    panels.append(panel("Cổng chất lượng dữ liệu", "table", (8, 22, 8, 8), checks))
    scores = flux(
        "weather_analytics",
        "model_metrics",
        'r._field == "mae"',
        "last()",
        "group()",
        'keep(columns: ["horizon", "method", "_value"])',
        'sort(columns: ["horizon", "method"])',
        start="-90d",
    )
    panels.append(panel("MAE trên tập test (°C)", "table", (16, 22, 8, 8), scores, extra={"decimals": 2}))
    return dashboard("operations", "3. Vận hành và chất lượng", "now-1h", panels, [], "30s")


def dashboard(uid, title, start, panels, variables, refresh="", end="now") -> dict:
    for number, item in enumerate(panels, 1):
        item["id"] = number
    return {
        "uid": uid,
        "title": title,
        "tags": ["weather-bigdata"],
        "timezone": "Asia/Ho_Chi_Minh",
        "schemaVersion": 41,
        "refresh": refresh,
        "time": {"from": start, "to": end},
        "templating": {"list": variables},
        "links": [{"type": "dashboards", "tags": ["weather-bigdata"], "title": "Dashboards", "keepTime": False}],
        "panels": panels,
    }


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for build in (realtime, history, operations):
        board = build()
        text = json.dumps(board, indent=1, ensure_ascii=False) + "\n"
        (OUTPUT / f"{board['uid']}.json").write_text(text, encoding="utf-8")
        print("wrote", board["uid"])


if __name__ == "__main__":
    main()
