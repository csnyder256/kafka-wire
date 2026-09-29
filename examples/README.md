# Runnable client examples

Each example creates a unique single-partition topic, waits for produce acknowledgements,
reads five records from the beginning and checks value bytes, key bytes and order.
Payloads cover text, JSON, every byte value, an empty value and Unicode. Successful
runs print a machine-readable client-check receipt as their last JSON line.
Node and Java also check classic group membership and commit/fetch the final offset.
Created topics and groups remain on an external broker; run these against a test broker.

Start `kafka-wire serve`, then run commands from the repository root:

| Language | Pinned client | Run |
|---|---|---|
| Go | franz-go, version in go.mod | `go run ./examples/go` |
| Python | kafka-python, requirements.txt | `python -m pip install -r examples/python/requirements.txt` then `python examples/python/roundtrip.py` |
| Node | KafkaJS, package-lock.json | `npm ci --prefix examples/nodejs` then `npm start --prefix examples/nodejs` |
| Java 17+ / Maven 3.9+ | Apache Kafka, pom.xml | `mvn -f examples/java/pom.xml compile exec:java` |
| Shell | kcat | [One-liners](shell/kcat.md) |

`KAFKA_WIRE_BROKERS` overrides the comma-separated bootstrap addresses. The examples
use PLAINTEXT and no compression so those dimensions are explicit in every receipt.
They do not imply that every configuration or Kafka feature is supported.

## Repeatable compatibility checks

With Go, Python, Node and Java/Maven installed and client dependencies above prepared:

    python scripts/compatibility.py --managed --output ./client-checks

This builds the current broker, starts it on owned loopback ports with temporary
storage, runs the four examples, and always stops that broker. The result is a
JSON receipt bundle and a portable interactive HTML dashboard. A failed or missing
client remains failed or unverified; the command exits nonzero. No existing server
or data directory is used. `--clients go,python` selects a subset explicitly.

To check your own test broker, omit `--managed` and pass `--brokers host:9092`.
Reports label it as external: repository hashes then identify the example source,
not the identity or implementation of that server.

## Compatibility boundaries

Disable producer idempotence. kafka-wire has no transaction coordinator and does
not advertise InitProducerId. For modern Java consumers select `group.protocol=classic`;
the new consumer group protocol, transactions, Kafka Streams, replication and
exactly-once semantics are not supported. Acks=all on one node proves an acknowledgement,
not replicated durability. Check [durability](../docs/durability.md) before deployment.

[Compatibility dashboard](../docs/compatibility/index.html) shows advertised API ranges
separately from tested client behavior and allows local receipt import/export.
