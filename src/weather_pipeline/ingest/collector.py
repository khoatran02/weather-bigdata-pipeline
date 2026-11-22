"""Long-running poller: public APIs to Kafka (at-least-once; Spark removes duplicates)."""

import datetime as dt
import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from weather_pipeline.common.config import Settings, env
from weather_pipeline.ingest import sources
from weather_pipeline.ingest.envelope import utc_iso
from weather_pipeline.ingest.kafka import ReliablePublisher

log = logging.getLogger("collector")


@dataclass
class Poller:
    name: str
    fetch: Callable[[bool, str], list[sources.Record]]
    interval: float
    due: float = 0
    full: bool = True  # catch up on start-up and after any failure
    seen: set[str] = field(default_factory=set)

    def poll(self, publisher, topic: str, ingested_at: str) -> int:
        try:
            records = self.fetch(self.full, ingested_at)
            fresh = sorted((r for r in records if r.identity not in self.seen), key=lambda r: r.event_time)
            for record in fresh:
                publisher.send(topic, record.station, record.value)
            publisher.flush()
        except Exception:
            self.full = True
            raise
        # Only remember records once Kafka has acknowledged them.
        self.seen.update(r.identity for r in fresh)
        self.full = False
        return len(fresh)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings()
    stop = threading.Event()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, lambda *_: stop.set())

    publisher = ReliablePublisher(settings.kafka)
    pollers = [
        Poller(
            "thingspeak",
            lambda full, at: sources.fetch_thingspeak(settings.channel, full, at),
            float(env("THINGSPEAK_INTERVAL", "60")),
        ),
        Poller(
            "aviationweather",
            lambda full, at: sources.fetch_aviation(settings.stations, full, at),
            float(env("AVIATION_INTERVAL", "120")),
        ),
    ]
    while not stop.is_set():
        for poller in (p for p in pollers if p.due <= time.monotonic()):
            try:
                now = utc_iso(dt.datetime.now(dt.timezone.utc))
                count = poller.poll(publisher, settings.topic("raw.live"), now)
                log.info("%s: published %d new records", poller.name, count)
            except Exception:
                log.exception("%s: poll failed", poller.name)
            poller.due = time.monotonic() + poller.interval
        stop.wait(1)


if __name__ == "__main__":
    main()
