"""Produce and consume against kafka-wire with kafka-python.

    pip install kafka-python
    python roundtrip.py
"""
import os
import sys

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BROKERS = os.environ.get("KAFKA_WIRE_BROKERS", "127.0.0.1:9092").split(",")
TOPIC = "demo.python"

# The broker carries opaque bytes, so anything serializable to bytes works.
# These deliberately include a value that is not text.
MESSAGES = [
    b"a plain line",
    '{"id": 1, "note": "json is just bytes here"}'.encode(),
    bytes(range(256)),          # every byte value, including NUL
    b"",                        # empty is not the same as absent
]


def main() -> int:
    admin = KafkaAdminClient(bootstrap_servers=BROKERS)
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    producer = KafkaProducer(bootstrap_servers=BROKERS)
    for m in MESSAGES:
        producer.send(TOPIC, value=m, key=b"k")
    producer.flush()
    producer.close()
    print(f"produced {len(MESSAGES)} records to {TOPIC}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
        # Omit group_id to read without joining a group. Set it to commit
        # offsets and share partitions with other consumers.
        group_id=None,
    )
    received = [msg.value for msg in consumer]
    consumer.close()

    if received != MESSAGES:
        print(f"MISMATCH: sent {len(MESSAGES)} records, got {len(received)} back", file=sys.stderr)
        return 1
    print(f"consumed {len(received)} records, byte-identical to what was sent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
