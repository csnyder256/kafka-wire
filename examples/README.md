# Client examples

kafka-wire speaks the Kafka wire protocol, so these use ordinary Kafka client
libraries. None of them import anything from this project.

Every example assumes a broker at `127.0.0.1:9092`. Start one with:

```sh
kafka-wire serve
```

| Directory | Library | Notes |
|---|---|---|
| `go/` | `twmb/franz-go` | |
| `python/` | `kafka-python` | Also works with `confluent-kafka` and `most clients`. |
| `nodejs/` | `kafkajs` | |
| `java/` | `org.apache.kafka:kafka-clients` | Needs `enable.idempotence=false`, see below. |
| `shell/` | `kcat` | One-liners, no project needed. |

## The one setting every example sets

kafka-wire implements no transaction coordinator, so it does not offer
`InitProducerId`. The Apache Kafka Java client has defaulted
`enable.idempotence=true` since 3.0 and treats the missing API as fatal, so
Java producers must set:

```properties
enable.idempotence=false
```

librdkafka-based clients (confluent-kafka for Python, Go, .NET, node-rdkafka)
default it to `false` already and need no change. kafka-python, most clients,
kafkajs and franz-go are unaffected unless you turn idempotence on yourself.

What you give up is producer-side deduplication on retry. Consumers should
already be idempotent for other reasons; see `docs/durability.md`.
