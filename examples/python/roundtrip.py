"""Produce and consume against kafka-wire with kafka-python.

    pip install -r requirements.txt
    python roundtrip.py

Sends one record of each of several shapes, reads them back, and checks that
every byte survived. Exits non-zero if anything did not.
"""
import json
import platform
import os
import sys
import time
import uuid

import kafka
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BROKERS = os.environ.get("KAFKA_WIRE_BROKERS", "127.0.0.1:9092").split(",")

# A fresh topic per run, so running this twice does not read the previous
# run's records back and report a mismatch that is really just history.
TOPIC = f"demo.python.{uuid.uuid4().hex[:8]}"

# The broker carries opaque bytes, so anything serializable to bytes works.
# These deliberately include shapes that break anything treating a value as
# text: a leading NUL, every byte value, and an empty payload.
MESSAGES = [
    b"a plain line",
    '{"id": 1, "note": "json is just bytes here"}'.encode(),
    bytes(range(256)),
    b"",
    "こんにちは · Kafka".encode(),
]

TIMEOUT_S = 60


def main() -> int:
    admin = KafkaAdminClient(bootstrap_servers=BROKERS, request_timeout_ms=15000)
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    # kafka-python 3.x turns idempotent producing on by default. Idempotence
    # needs InitProducerId, which a broker with no transaction coordinator does
    # not offer, so it has to be switched off. kafka-python 2.x defaults it off
    # and does not accept the argument at all, hence the version check rather
    # than passing it unconditionally.
    producer_args = {"bootstrap_servers": BROKERS, "request_timeout_ms": 15000, "max_block_ms": 15000, "acks": "all"}
    if tuple(int(p) for p in kafka.__version__.split(".")[:1]) >= (3,):
        producer_args["enable_idempotence"] = False
    producer = KafkaProducer(**producer_args)
    # Resolve every future. send() is asynchronous and flush() does not raise
    # on a per-record failure, so a producer that silently dropped everything
    # would otherwise look like a success right up until the consumer found
    # an empty topic.
    futures = [producer.send(TOPIC, value=m, key=b"k") for m in MESSAGES]
    producer.flush(timeout=30)
    for i, f in enumerate(futures):
        f.get(timeout=30)  # raises if the broker rejected this record
    producer.close()
    print(f"produced {len(MESSAGES)} records to {TOPIC}", flush=True)

    # Omit group_id to read without joining a consumer group. Set it to a
    # string to commit offsets and share partitions with other consumers.
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        auto_offset_reset="earliest",
        group_id=None,
    )

    # Poll against an explicit deadline rather than relying on the iterator's
    # own idle timeout. A consumer that has just subscribed may need a
    # metadata refresh before it has an assignment, and a single short poll
    # can legitimately return nothing while that happens.
    received = []
    deadline = time.time() + TIMEOUT_S
    while len(received) < len(MESSAGES) and time.time() < deadline:
        batches = consumer.poll(timeout_ms=1000, max_records=len(MESSAGES))
        for records in batches.values():
            received.extend(records)
    consumer.close()

    if len(received) != len(MESSAGES) or any(r.value != m or r.key != b"k" or r.partition != 0 or r.offset != i for i, (r, m) in enumerate(zip(received, MESSAGES))):
        print(
            f"MISMATCH: sent {len(MESSAGES)} records, got {len(received)} back",
            file=sys.stderr,
        )
        for i, (sent, got) in enumerate(zip(MESSAGES, received)):
            if sent != got.value:
                print(f"  record {i}: sent {len(sent)} bytes, got {len(got.value)}", file=sys.stderr)
        return 1

    print(f"consumed {len(received)} records, byte-identical to what was sent")
    print(json.dumps({"schema":"kafka-wire.client-check","version":1,"client":"kafka-python","client_version":kafka.__version__,"language":"python","runtime":platform.python_version(),"status":"passed","records":len(received),"checks":["create-topic","produce-acks","byte-fidelity","key-fidelity","partition-order"],"settings":{"idempotence":False,"compression":"none","partitions":1,"security":"PLAINTEXT","group":"none"}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
