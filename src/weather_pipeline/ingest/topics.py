"""Create the topics (idempotent)."""

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from weather_pipeline.common.config import Settings

DAY_MS = 86_400_000
TOPICS = {  # name: (partitions, retention days)
    "raw.live": (6, 30),
    "raw.history": (6, 30),
    "dlq": (3, 30),
    "alerts": (3, 7),
}


def main() -> None:
    settings = Settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka})
    wanted = [
        NewTopic(
            settings.topic(name),
            num_partitions=partitions,
            replication_factor=3,
            config={"min.insync.replicas": "2", "retention.ms": str(days * DAY_MS)},
        )
        for name, (partitions, days) in TOPICS.items()
    ]
    for name, future in admin.create_topics(wanted, request_timeout=30).items():
        try:
            future.result()
            print(f"created {name}")
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print(f"exists  {name}")


if __name__ == "__main__":
    main()
