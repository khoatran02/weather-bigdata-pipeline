"""Download the public IEM METAR archive into a local, resumable cache."""

import argparse
import datetime as dt
import gzip
import json
import logging
import time
import urllib.parse
from pathlib import Path

from weather_pipeline.ingest import http

log = logging.getLogger("archive")

CGI = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
NETWORK = "https://mesonet.agron.iastate.edu/geojson/network/{}__ASOS.geojson"
COLUMNS = ("tmpc", "dwpc", "sknt", "drct", "gust", "alti", "mslp", "vsby", "wxcodes", "metar")
HEADER = b"station,valid,"


def stations(countries: list[str]) -> list[dict]:
    found = []
    for country in countries:
        for feature in json.loads(http.get(NETWORK.format(country)))["features"]:
            props = feature["properties"]
            lon, lat = feature["geometry"]["coordinates"]
            found.append(
                {
                    "station": props["sid"],
                    "name": props["sname"],
                    "country": country,
                    "lat": lat,
                    "lon": lon,
                    "archive_begin": props["archive_begin"],
                }
            )
    return found


def periods(begin: dt.date, end: dt.date, years: int = 10):
    """Yield [start, stop) ranges aligned to multiples of `years`."""
    start = begin
    while start < end:
        stop = min(dt.date((start.year // years + 1) * years, 1, 1), end)
        yield start, stop
        start = stop


def archive_url(station: str, start: dt.date, stop: dt.date) -> str:
    query = [("station", station)] + [("data", column) for column in COLUMNS]
    for suffix, day in (("1", start), ("2", stop)):
        query += [(f"year{suffix}", day.year), (f"month{suffix}", day.month), (f"day{suffix}", day.day)]
    query += [("tz", "Etc/UTC"), ("format", "onlycomma"), ("missing", "empty")]
    query += [("report_type", "3"), ("report_type", "4")]
    return f"{CGI}?{urllib.parse.urlencode(query)}"


def download(directory: Path, countries: list[str], since: dt.date, until: dt.date) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    catalog = stations(countries)
    (directory / "stations.json").write_text(json.dumps(catalog, indent=1))
    for item in catalog:
        begin = max(since, dt.date.fromisoformat(item["archive_begin"] or str(since)))
        for start, stop in periods(begin, until):
            target = directory / f"{item['station']}_{start}_{stop}.csv.gz"
            if target.exists():
                continue
            body = http.get(archive_url(item["station"], start, stop), timeout=600)
            # IEM reports overload as plain text with HTTP 200, so never cache without this check.
            if not body.startswith(HEADER):
                raise RuntimeError(f"unexpected IEM response for {target.name}: {body[:200]!r}")
            partial = target.with_suffix(".part")
            partial.write_bytes(gzip.compress(body, 6))
            partial.replace(target)
            # A later run extends the open decade, so its shorter predecessor is now redundant.
            for older in directory.glob(f"{item['station']}_{start}_*.csv.gz"):
                if older != target:
                    older.unlink()
            log.info("%s: %d rows", target.name, body.count(b"\n") - 1)
            time.sleep(2)  # the archive is a free service
    log.info("archive complete: %d stations", len(catalog))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path("data/cache"))
    parser.add_argument("--countries", default="VN", help="comma separated, e.g. VN,TH,LA,KH")
    parser.add_argument("--since", type=dt.date.fromisoformat, default=dt.date(1949, 1, 1))
    parser.add_argument("--until", type=dt.date.fromisoformat, default=dt.datetime.now(dt.timezone.utc).date())
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    download(args.directory, args.countries.split(","), args.since, args.until)


if __name__ == "__main__":
    main()
