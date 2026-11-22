#!/usr/bin/env python3
"""Create .env with random local secrets. An existing file is never overwritten."""

import os
import secrets
from pathlib import Path

NAMES = ["INFLUX_TOKEN", "INFLUX_PASSWORD", "GRAFANA_PASSWORD", "AIRFLOW_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY"]


def main() -> None:
    path = Path(__file__).resolve().parent.parent / ".env"
    if path.exists():
        print(".env already exists")
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        for name in NAMES:
            handle.write(f"{name}={secrets.token_hex(20)}\n")
    print("created .env")


if __name__ == "__main__":
    main()
