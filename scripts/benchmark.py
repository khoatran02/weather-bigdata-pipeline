#!/usr/bin/env python3
"""Strong-scaling and storage-format benchmark on the local standalone cluster."""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CORES = (1, 2, 4, 8)


def compose(*args: str, check: bool = True) -> str:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, text=True, capture_output=True, check=check).stdout


def run(action: str, cores: int) -> dict:
    options = {
        "spark.cores.max": cores,
        "spark.executor.cores": 1,
        "spark.executor.memory": "900m",
        "spark.sql.shuffle.partitions": 32,
        "spark.scheduler.minRegisteredResourcesRatio": 1.0,
    }
    flags = [part for key, value in options.items() for part in ("--conf", f"{key}={value}")]
    command = ["run", "--rm", "--no-deps", "job", "submit", "weather_pipeline.tools.benchmark", *flags, "--", action]
    output = compose(*command)
    value = json.loads(next(line[7:] for line in output.splitlines() if line.startswith("RESULT ")))
    print(action, cores, value, flush=True)
    return value


def cpu_model() -> str:
    lines = Path("/proc/cpuinfo").read_text().splitlines()
    return next((line.split(":", 1)[1].strip() for line in lines if line.startswith("model name")), "unknown")


def main() -> None:
    # SIGTERM skips `finally` by default, which would leave paused services stopped.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    compose("stop", "streaming", "airflow")  # the benchmark must own all eight cores
    try:
        report = {"host": {"cpu": cpu_model(), "threads": os.cpu_count()}}
        report["dataset"] = run("prepare", 4)
        scaling = {cores: run("scaling", cores) for cores in CORES}
        single = scaling[CORES[0]]["median_s"]
        for cores, item in scaling.items():
            item["speedup"] = round(single / item["median_s"], 2)
            item["efficiency"] = round(item["speedup"] / cores, 2)
        report["scaling"] = scaling
        report["formats"] = run("formats", 4)
        target = ROOT / "docs/evidence/benchmark.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=1) + "\n")
        print(f"wrote {target}")
    finally:
        try:
            run("cleanup", 2)
        finally:
            compose("start", "streaming", "airflow", check=False)


if __name__ == "__main__":
    main()
