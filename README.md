<h1 align="center">kafka-wire</h1>

<p align="center">
  <strong>A Kafka-compatible message broker in a single Go binary.</strong><br>
  Your disk plus any S3 bucket. No ZooKeeper, no KRaft, no JVM, no cluster to operate.
</p>

<p align="center">
  <a href="https://github.com/csnyder256/kafka-wire/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/csnyder256/kafka-wire/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="MIT licensed" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
  <a href="https://go.dev/"><img alt="Go 1.25+" src="https://img.shields.io/badge/go-1.25%2B-00ADD8.svg"></a>
  <a href="https://github.com/csnyder256/kafka-wire/pkgs/container/kafka-wire"><img alt="container image" src="https://img.shields.io/badge/ghcr.io-kafka--wire-2496ED.svg"></a>
</p>

---

Real Kafka clients connect to it. `kcat`, `kafka-python`, KafkaJS, franz-go, Sarama,
confluent-kafka, and the Apache Kafka Java client all speak to it without a shim, a
proxy, or a patched driver, because it implements the Kafka wire protocol rather than
imitating it.

It is one process and one directory. There is no coordination service, no broker
cluster, no separate schema component, and nothing to install before it runs.

```
┌──────────────┐   Kafka wire protocol    ┌──────────────────────────────────┐
│ any Kafka    │ ───────────────────────► │            kafka-wire            │
│ client, any  │ ◄─────────────────────── │  wire → log → segments on disk   │
│ language     │                          └──────────────┬───────────────────┘
└──────────────┘                            sealed + old │
                                                         ▼
                                          ┌──────────────────────────────────┐
                                          │  cold tier (optional)            │
                                          │  none · a directory · any S3     │
                                          │  AWS MinIO R2 B2 Ceph Wasabi …   │
                                          └──────────────────────────────────┘
```

## Sixty seconds

```sh
docker run -p 9092:9092 -v kw:/data \
  -e KAFKA_WIRE_AUTH_ALLOWANON=true \
  ghcr.io/csnyder256/kafka-wire:latest
```

That is the whole setup. Now use it from anywhere:

```sh
# with the binary's own client, no libraries to install
kafka-wire topic create demo
echo hello | kafka-wire produce demo
kafka-wire consume demo --from-beginning
```

```python
# or with a normal Kafka library
from kafka import KafkaProducer, KafkaConsumer
KafkaProducer(bootstrap_servers="localhost:9092").send("demo", b"hello")
for msg in KafkaConsumer("demo", bootstrap_servers="localhost:9092",
                         auto_offset_reset="earliest"):
    print(msg.value)
```

No configuration file was written, and nothing was configured. Run
`kafka-wire config init` when you want one.

## What it is

- **One binary.** `kafka-wire serve` is the broker. The same binary is also the
  producer, the consumer, the topic admin, and the diagnostic tool.
- **Kafka on the wire.** 19 protocol APIs, consumer groups, SASL/SCRAM, TLS, ACLs,
  and Prometheus metrics. Your existing client library and your existing code work.
- **Opaque to your data.** Keys, values and headers are bytes. JSON, Avro, Protobuf,
  MessagePack, CBOR, images, PDFs, gzip blobs, and invalid UTF-8 all round-trip
  byte-identically, and the test suite asserts exactly that.
- **Cold storage that is not AWS-shaped.** Sealed segments can tier to a directory
  or to any S3-compatible store, and consumers fetching old offsets get them
  transparently restored.

## What it is not

This is the section to read before adopting it.

- **Not replicated.** One node, one copy. If the disk dies, the data that had not
  been archived dies with it. There is no failover and no quorum. See
  [docs/durability.md](docs/durability.md) for what that does and does not cost you.
- **Not a Kafka feature replica.** No transactions, no exactly-once semantics, no
  Kafka Streams, no Kafka Connect runtime, no log compaction, no KRaft.
- **Not a distributed system.** If you need multi-node durability or throughput
  beyond one machine, run Apache Kafka or Redpanda. That is not a hedge; it is the
  correct answer for that requirement.

If your workload fits on one machine and you would rather own it than operate a
cluster, this is built for you. If it does not, it is not.

## Does my Kafka client work?

Yes, with one caveat that has a one-line fix.

kafka-wire has no transaction coordinator, so it does not implement
`InitProducerId`. Any client that turns idempotent producing on by default
needs it switched off. That currently means the Apache Kafka **Java** client
(default since 3.0) and **kafka-python 3.x**:

```properties
enable.idempotence=false     # Java, Spring, Quarkus, Micronaut
```

```python
KafkaProducer(..., enable_idempotence=False)   # kafka-python 3.x
```

Clients built on librdkafka default it off already, as do kafka-python 2.x,
KafkaJS and Sarama. franz-go turns it on by default but degrades gracefully
when the broker does not advertise the API, so it works either way.
Everything else works unchanged.

What you give up is producer-side deduplication of retries. It is not emulated,
because a broker that accepted `InitProducerId` and then ignored sequence
numbers would be claiming a guarantee it does not provide.

| Client | Works | Note |
|---|---|---|
| `kcat` / kafkacat | yes | |
| kafka-python, most clients | yes | verified in CI |
| confluent-kafka (Python, Go, .NET), librdkafka, node-rdkafka | yes | idempotence already defaults off |
| KafkaJS | yes | |
| franz-go | yes | verified in CI |
| Sarama, segmentio/kafka-go | yes | |
| Apache Kafka Java client 3.x / 4.x | yes | `enable.idempotence=false` |
| Spring for Apache Kafka, Quarkus, Micronaut | yes | same producer setting |
| Kafka Connect, Debezium | partial | source connectors work; anything requiring transactions does not |
| Kafka Streams, ksqlDB | no | needs transactions and internal topic management |

## Does it work with MinIO, Cloudflare R2, Backblaze B2, Wasabi, or Ceph?

Yes, and none of them are a special case. The cold tier is one interface with three
implementations: `none`, `fs`, and `s3`. The same conformance suite runs against the
filesystem backend and against a live S3 server in CI, so they cannot drift.

```yaml
archive:
  backend: s3
  s3:
    bucket: my-bucket
    endpoint: http://minio:9000    # omit entirely for AWS
    addressing: path               # MinIO, Ceph and SeaweedFS want path
```

| Store | Endpoint | Region | Addressing |
|---|---|---|---|
| AWS S3 | omit | your real region | auto |
| MinIO | `http://host:9000` | `us-east-1` | `path` |
| Cloudflare R2 | `https://ACCOUNT.r2.cloudflarestorage.com` | `auto` | auto |
| Backblaze B2 | `https://s3.REGION.backblazeb2.com` | e.g. `us-west-004` | auto |
| Ceph RADOS Gateway | your gateway URL | site-defined | `path` |
| SeaweedFS | `http://host:8333` | `us-east-1` | `path` |
| Wasabi | `https://s3.REGION.wasabisys.com` | e.g. `us-east-1` | auto |
| Garage | your node URL | operator-defined | `path` |
| Google Cloud Storage | `https://storage.googleapis.com` | `auto` | auto |
| Hetzner, OVH, Scaleway, DigitalOcean Spaces, Storj, Tigris | provider URL | provider region | auto |
| A directory, an NFS mount, a NAS | `backend: fs` | | |

Credentials come from the config, or from the standard `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` variables that every S3-compatible vendor documents, or
from the instance credential endpoint. Nothing here assumes an AWS account.

There is a deliberate engineering choice behind this: the S3 driver is built on
`minio-go`, not the AWS SDK. Since `aws-sdk-go-v2/service/s3` v1.73.0 the AWS SDK
computes a checksum on every upload by default and switches the request to a
streaming `aws-chunked` trailer. Stores catch up at their own pace, and several
did not implement it for a long time. Rather than track which release of which
provider is currently compatible, the driver simply never sends the trailer.

## Do I need ZooKeeper or KRaft?

No. There is nothing to elect, because there is one node. That is the entire
tradeoff of this project stated in one sentence.

## Where can I run it?

Anywhere that gives you a persistent disk and a raw TCP port.

| | |
|---|---|
| Docker / Compose | [`deploy/docker`](deploy/docker) |
| Kubernetes | [`deploy/kubernetes`](deploy/kubernetes) (StatefulSet, PVC, probes, PDB) |
| systemd on a VM | [`deploy/systemd`](deploy/systemd) (hardened unit) |
| Nomad | [`deploy/nomad`](deploy/nomad) |
| Fly.io, Railway, Render, Koyeb, Hetzner, EC2 | [`deploy/paas`](deploy/paas/README.md) |
| Raspberry Pi / ARM64 | multi-arch images and binaries |

Platforms with an ephemeral filesystem or HTTP-only routing (Cloud Run, Heroku,
DigitalOcean App Platform, Vercel) **cannot** run this, and
[`deploy/paas`](deploy/paas/README.md) says so plainly rather than letting you find
out in production.

**The one setting people get wrong everywhere:** a Kafka client connects to your
bootstrap address, then throws it away and reconnects to whatever the broker
advertises. Behind NAT, a port mapping, or a load balancer, set:

```yaml
listeners:
  advertisedhost: kafka.example.com
  advertisedport: 9092
```

Otherwise the client connects, gets told to go to an address that means something
different on its side of the network, and hangs. `kafka-wire doctor` prints the
address clients will be given, so you can check it before your users do.

## Configuration

kafka-wire starts with no configuration at all. When you want some, there is one
canonical file and a mechanical mapping to environment variables:

```
storage.datadir   ->   KAFKA_WIRE_STORAGE_DATADIR
archive.s3.bucket ->   KAFKA_WIRE_ARCHIVE_S3_BUCKET
```

Uppercase the dotted path, replace `.` with `_`, prefix with `KAFKA_WIRE_`.
Environment beats file, because the file is usually baked into an image and the
environment is what your orchestrator injects. Secrets can come from
`KAFKA_WIRE_ADMIN_TOKEN_FILE` style indirection so they never appear in a process
listing.

```sh
kafka-wire config init          # a starter file, twelve lines
kafka-wire config reference     # every setting, its type, default, and meaning
kafka-wire config print         # what is in effect, and where each value came from
kafka-wire config validate      # fail before you deploy, not after
kafka-wire doctor               # check ports, disk, advertised address, object store
```

There is **no giant `.env.example` listing every possible integration.** That
pattern gets copied wholesale, and settings nobody chose end up in production.
`config print` shows the resolved value *and its source* for every setting instead,
which is the question people actually have.

Bad values are startup errors that name the variable at fault, not silent fallbacks
to a default. Several configurations that would corrupt or lose data are refused
outright: a segment-to-part ratio above the 10,000-part limit every S3 store
enforces, an archive retention shorter than the archive age, half-configured TLS,
and a non-loopback listener with authentication disabled.

Full reference: [docs/configuration.md](docs/configuration.md).

## Protocol coverage

Advertised via `ApiVersions`, so clients negotiate correctly rather than discovering
gaps at runtime.

| API | Versions | | API | Versions |
|---|---|---|---|---|
| Produce | 3-9 | | JoinGroup | 0-7 |
| Fetch | 4-11 | | SyncGroup | 0-5 |
| ListOffsets | 0-5 | | Heartbeat | 0-4 |
| Metadata | 0-9 | | LeaveGroup | 0-4 |
| OffsetCommit | 0-8 | | FindCoordinator | 0-3 |
| OffsetFetch | 0-6 | | DescribeGroups | 0-5 |
| CreateTopics | 0-6 | | ListGroups | 0-4 |
| DeleteTopics | 0-5 | | DescribeConfigs | 0-4 |
| ApiVersions | 0-3 | | SaslHandshake / SaslAuthenticate | 0-1 / 0-2 |

Anything not in that table is not implemented. Notably absent: `InitProducerId`,
`AddPartitionsToTxn`, `EndTxn` and `TxnOffsetCommit` (no transactions),
`DeleteGroups`, `CreatePartitions`, `OffsetForLeaderEpoch`, `DescribeLogDirs`,
`DescribeCluster`, `AlterConfigs`, `IncrementalAlterConfigs`, `DeleteRecords`,
`OffsetDelete`, and the ACL and SCRAM admin APIs. An unimplemented API returns a
correctly-typed response carrying `UNSUPPORTED_VERSION`, so a client fails
cleanly instead of desynchronizing.

## Durability, stated plainly

The default `fsyncmode: interval` acknowledges a write once the operating system has
it, and fsyncs on a timer. This is the same tradeoff Apache Kafka makes by default,
where safety comes from replication instead. kafka-wire has no replication, so:

- A process crash loses nothing. The log is recovered and validated on restart.
- A **machine** crash can lose the last few seconds of writes. Set
  `fsyncmode: always` to trade throughput for that window.
- A **disk** failure loses everything not yet archived. The cold tier is the answer:
  archived segments carry a SHA-256 that is verified on every restore.

Do not use this as the only copy of data you cannot regenerate.
[docs/durability.md](docs/durability.md) has the full accounting.

## Benchmarks

Measured on the machine below with the command below. Reproduce it on yours:

```sh
go test ./e2e/ -run XXX -bench Throughput -benchtime 1x
```

| Payload | Records/s | Throughput |
|---|---|---|
| 256 B | ~2,200,000 | ~537 MiB/s |
| 4 KiB | ~180,000 | ~704 MiB/s |

4 concurrent producers, 200,000 records per run, incompressible random payloads,
compression disabled, default `fsyncmode: interval`, loopback, on an Intel i9-11900K
with NVMe storage running Windows.

Read that as a shape, not a capacity plan: these are page-cache-backed writes on one
fast machine with no consumers competing, and your disk, network and payload will
dominate. The point is that a single-node broker is not the bottleneck for the
workloads it targets.

## Where this came from

I built this as **ClarusStream**, the message broker for the Clarus contract
platform, to replace a managed Kafka service that cost more than the rest of the
infrastructure combined. It has carried that platform's production traffic since
May 2026.

This repository is a rebuilt, vendor-neutral version of that broker: the proven
protocol and storage core, with every assumption about *my* stack removed and
replaced with a choice. Making it general surfaced real defects that a single-client
deployment never exercised, including a request-header parsing bug that broke every
modern client, a response field whose Go zero value silently redirected consumers to
a nonexistent broker, and a consumer-group coordinator that delivered every record
to every member of a group. All three are fixed here and covered by tests.

## Documentation

| | |
|---|---|
| [Getting started](docs/getting-started.md) | first broker, first message |
| [Configuration](docs/configuration.md) | every setting |
| [Cold storage](docs/storage.md) | object stores, tiering, restore |
| [Security](docs/security.md) | SASL, TLS, ACLs, exposure |
| [Durability](docs/durability.md) | what survives what |
| [Operations](docs/operations.md) | metrics, backup, upgrades, troubleshooting |
| [Examples](examples/) | Go, Python, Node, Java, shell |

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).
Security reports: [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

Apache Kafka is a registered trademark of the Apache Software Foundation.
kafka-wire is an independent project, is not affiliated with or endorsed by the
Apache Software Foundation, and the name is used only to describe the wire protocol
this software is compatible with.
