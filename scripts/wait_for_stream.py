#!/usr/bin/env python3
"""Block until the history backfill has reached silver.

Both queries must report a batch that started after this script did and found nothing to read.
"""

import argparse
import datetime as dt
import json
import re
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def token() -> str:
    return re.search(r"^INFLUX_TOKEN=(.+)$", (ROOT / ".env").read_text(), re.M).group(1)


def busiest_recent_batch(since: dt.datetime) -> dict[str, float]:
    flux = f"""from(bucket: "pipeline_metrics")
      |> range(start: {since.strftime("%Y-%m-%dT%H:%M:%SZ")})
      |> filter(fn: (r) => r._measurement == "streaming" and r._field == "input_rows")
      |> max()
      |> keep(columns: ["query", "_value"])"""
    request = urllib.request.Request(
        "http://127.0.0.1:18086/api/v2/query?org=ktxldll",
        data=json.dumps({"query": flux, "dialect": {"header": True}}).encode(),
        headers={"Authorization": f"Token {token()}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        lines = [line.strip().split(",") for line in response.read().decode().splitlines() if line.strip()]
    result, header = {}, []
    for cells in lines:
        if "_value" in cells:
            header = cells
        elif header:
            row = dict(zip(header, cells))
            result[row["query"]] = float(row["_value"])
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    started = time.monotonic()
    quiet = 0
    time.sleep(15)  # give the trigger time to notice the new offsets
    while time.monotonic() - started < args.timeout:
        rows = busiest_recent_batch(dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30))
        caught_up = rows.get("bronze_history") == 0 and rows.get("silver_history") == 0
        quiet = quiet + 1 if caught_up else 0
        if quiet >= 3:  # an idle bronze can be one trigger ahead of silver
            print(f"silver caught up after {time.monotonic() - started:.0f}s")
            return
        time.sleep(10)
    subprocess.run(["docker", "compose", "logs", "--tail=30", "streaming"], cwd=ROOT, check=False)
    raise SystemExit("timed out waiting for the stream")


if __name__ == "__main__":
    main()
