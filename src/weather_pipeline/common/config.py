"""Runtime configuration. Everything comes from the environment."""

import os
from dataclasses import dataclass, field

THINGSPEAK_STATION = "TS3574112"
THINGSPEAK_REFERENCE = "VVDN"  # the channel republishes this airport's METAR
LOCAL_UTC_OFFSET_HOURS = 7


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    namespace: str = field(default_factory=lambda: env("WP_NAMESPACE", "weather"))
    kafka: str = field(default_factory=lambda: env("KAFKA_BOOTSTRAP", "kafka-1:9092"))
    lake_root: str = field(default_factory=lambda: env("LAKE_ROOT", "s3a://lakehouse"))
    channel: str = field(default_factory=lambda: env("THINGSPEAK_CHANNEL", "3574112"))
    stations: str = field(default_factory=lambda: env("METAR_STATIONS", "VVDN,VVNB,VVTS,VVCI,VVPB,VVDL,VVVD"))
    influx_url: str = field(default_factory=lambda: env("INFLUX_URL"))
    influx_org: str = field(default_factory=lambda: env("INFLUX_ORG", "ktxldll"))
    influx_token: str = field(default_factory=lambda: env("INFLUX_TOKEN"), repr=False)

    def topic(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.lake_root}/{self.namespace}/{layer}/{name}"

    def checkpoint(self, query: str) -> str:
        return self.table("_checkpoints", query)

    def model(self, horizon: int) -> str:
        return self.table("models", f"temperature_h{horizon}")
