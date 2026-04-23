"""
Create Kafka topics used by the speed layer.

Topics:
  - raw_events      (3 partitions) — raw job events from the producer
  - raw_events_dlq  (1 partition)  — dead-letter queue for malformed events

Kafka fundamentals first:
A topic is like a named stream of messages.
A producer writes messages into a topic.
A consumer reads messages from a topic.
A partition splits a topic into parallel lanes so multiple consumers or tasks can read faster.
replication_factor=1.This means each partition has only one copy.
A DLQ (dead-letter queue) is a separate topic where broken or malformed events are sent instead of crashing the main pipeline.
"""

import os

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)  # This is the Kafka broker address the client first connects to.
RAW_TOPIC = os.getenv(
    "RAW_TOPIC", "raw_events"
)  # This is the main topic where raw job events are published.
DLQ_TOPIC = os.getenv(
    "DLQ_TOPIC", "raw_events_dlq"
)  # This is the dead-letter queue where malformed events are sent.


def main():
    # Kafka’s management client.create topic. delete topic. inspect cluster metadata
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    topics = [
        NewTopic(RAW_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]

    futures = admin.create_topics(topics)

    # Kafka admin operations are asynchronous. The .create_topics() call returns a dict of futures immediately.
    # .result() waits for each topic creation to complete or fail.
    # This is a simple, synchronous way to handle topic creation.
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as exc:
            print(f"Topic {topic} may already exist: {exc}")


if __name__ == "__main__":
    main()
