from confluent_kafka import KafkaException, Producer


class ReliablePublisher:
    """Idempotent producer that only reports success after every message is acknowledged."""

    def __init__(self, bootstrap: str):
        self._failed = 0
        self._error = None
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
                "batch.num.messages": 10000,
                "queue.buffering.max.messages": 500000,
                "delivery.timeout.ms": 120000,
            }
        )

    def _delivered(self, error, _message) -> None:
        if error is not None:
            self._failed += 1
            self._error = error

    def send(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key.encode(), value=value, on_delivery=self._delivered)
                break
            except BufferError:  # local queue full: let deliveries drain
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(130)
        failed, error = self._failed, self._error
        self._failed, self._error = 0, None
        if remaining or failed:
            raise KafkaException(f"{failed} failed and {remaining} undelivered messages: {error}")
