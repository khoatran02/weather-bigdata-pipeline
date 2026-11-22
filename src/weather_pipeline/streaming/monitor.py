import logging
import time

from pyspark.sql.streaming import StreamingQueryListener

from weather_pipeline.common import influx
from weather_pipeline.common.config import Settings

log = logging.getLogger("streaming")


def progress_fields(progress) -> dict:
    state = progress.stateOperators or []
    fields = {
        "batch_id": progress.batchId,
        "input_rows": progress.numInputRows,
        "input_rows_per_second": progress.inputRowsPerSecond,
        "processed_rows_per_second": progress.processedRowsPerSecond,
        "batch_ms": (progress.durationMs or {}).get("triggerExecution"),
        "state_rows": sum(s.numRowsTotal for s in state),
        "late_rows_dropped": sum(s.numRowsDroppedByWatermark for s in state),
    }
    behind = [(s.metrics or {}).get("maxOffsetsBehindLatest") for s in progress.sources]
    if known := [float(value) for value in behind if value is not None]:
        fields["offsets_behind"] = max(known)
    return fields


class ProgressListener(StreamingQueryListener):
    """Spark does not commit consumer offsets to Kafka, so lag is reported from here."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.names = {}

    def onQueryStarted(self, event):
        self.names[str(event.id)] = event.name
        log.info("query started: %s", event.name)

    def onQueryProgress(self, event):
        progress = event.progress
        fields = progress_fields(progress)
        if progress.numInputRows:
            log.info(
                "%s batch=%s rows=%s ms=%s", progress.name, progress.batchId, progress.numInputRows, fields["batch_ms"]
            )
        self.publish(progress.name, fields)

    def onQueryIdle(self, event):
        # An idle query sends no progress events. Without this heartbeat it looks dead and its lag looks stale.
        if name := self.names.get(str(event.id)):
            self.publish(name, {"input_rows": 0, "offsets_behind": 0})

    def publish(self, name: str, fields: dict) -> None:
        point = influx.line("streaming", {"query": name}, fields, int(time.time()))
        try:
            if point:
                influx.write_lines(self.settings, influx.METRICS, [point], attempts=1)
        except Exception as error:  # monitoring must never stop the pipeline
            log.warning("cannot write metrics: %s", error)

    def onQueryTerminated(self, event):
        log.warning("query terminated: %s %s", event.id, event.exception)
