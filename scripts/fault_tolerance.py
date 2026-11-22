#!/usr/bin/env python3
"""End-to-end failure injection.

In a throw-away namespace: publish a known workload while a Kafka broker, the streaming driver
and a Spark worker are killed with SIGKILL, then require exact counts and checksums in the lake.
"""

import json
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = f"e2e{int(time.time())}"
DRIVER = f"ktxldll-weather-{NAMESPACE}"


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, text=True, capture_output=True, check=check)


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], text=True, capture_output=True, check=check)


def job(*args: str) -> list[str]:
    return ["run", "--rm", "--no-deps", "-e", f"WP_NAMESPACE={NAMESPACE}", "-e", "INFLUX_URL=", "job", *args]


def result(output: str) -> dict:
    return json.loads(next(line[7:] for line in output.splitlines() if line.startswith("RESULT ")))


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def wait_for_queries() -> None:
    for _ in range(90):
        if "query started: windows_live" in "".join(logs()):
            return
        time.sleep(2)
    raise RuntimeError("streaming driver did not start")


def logs() -> tuple[str, str]:
    process = docker("logs", DRIVER, check=False)
    return process.stdout, process.stderr


def main() -> int:
    # SIGTERM skips `finally` by default, which would leave paused services stopped.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    compose("stop", "airflow")  # the test driver needs the four cores used by batch jobs
    timeline = []
    try:
        compose(*job("python3", "-m", "weather_pipeline.ingest.topics"))
        environment = ["-e", f"WP_NAMESPACE={NAMESPACE}", "-e", "INFLUX_URL="]
        command = ["submit", "weather_pipeline.streaming.app"]
        compose("run", "-d", "--no-deps", "--name", DRIVER, *environment, "job", *command)
        wait_for_queries()
        log(f"streaming is up in namespace {NAMESPACE}")

        producer = subprocess.Popen(
            ["docker", "compose", *job("python3", "-m", "weather_pipeline.tools.e2e", "produce")],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        faults = [
            (15, "SIGKILL Kafka broker 2", lambda: compose("kill", "kafka-2")),
            (30, "SIGKILL streaming driver", lambda: docker("kill", DRIVER)),
            (40, "restart streaming driver from its checkpoint", lambda: docker("start", DRIVER)),
            (60, "SIGKILL Spark worker 1", lambda: compose("kill", "spark-worker-1")),
            (75, "restart Kafka broker 2", lambda: compose("start", "kafka-2")),
            (90, "restart Spark worker 1", lambda: compose("start", "spark-worker-1")),
        ]
        started = time.monotonic()
        for second, label, action in faults:
            time.sleep(max(0, second - (time.monotonic() - started)))
            still_producing = producer.poll() is None
            action()
            log(f"{label} (producer running: {still_producing})")
            timeline.append({"second": second, "event": label, "producer_running": still_producing})

        output, _ = producer.communicate(timeout=600)
        if producer.returncode:
            raise RuntimeError("producer failed")
        expected = result(output)
        log("producer finished, verifying")
        verify = ["submit", "weather_pipeline.tools.e2e", "--master", "local[3]", "--", "verify"]
        outcome = result(compose(*job(*verify, "--expected", json.dumps(expected))).stdout)
        outcome["timeline"] = timeline
        target = ROOT / "docs/evidence/fault-tolerance.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(outcome, indent=1) + "\n")
        log("PASS" if outcome["passed"] else "FAIL")
        print(json.dumps(outcome["actual"], indent=1))
        return 0 if outcome["passed"] else 1
    finally:
        docker("rm", "-f", DRIVER, check=False)
        compose("start", "kafka-2", "spark-worker-1", check=False)
        cleanup = ["submit", "weather_pipeline.tools.e2e", "--master", "local[2]", "--", "cleanup"]
        compose(*job(*cleanup), check=False)
        compose("start", "airflow", check=False)


if __name__ == "__main__":
    sys.exit(main())
