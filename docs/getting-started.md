# Getting started

## Install

Pick one.

```sh
# container
docker run -p 9092:9092 -v kw:/data -e KAFKA_WIRE_AUTH_ALLOWANON=true \
  ghcr.io/csnyder256/kafka-wire:latest

# go install
go install github.com/csnyder256/kafka-wire/cmd/kafka-wire@latest

# a release binary
curl -sSL https://github.com/csnyder256/kafka-wire/releases/latest/download/kafka-wire_linux_amd64.tar.gz | tar xz
```

## Run it

```sh
kafka-wire serve
```

It prints the address to give your clients, the data directory it is using, and
where its configuration came from. No config file is required.

By default it listens on `127.0.0.1:9092`, which means only this machine can
reach it. That is deliberate: read [security.md](security.md) before changing it.

## Send and read a message

The binary is also a client, so you can confirm it works before installing a
library:

```sh
kafka-wire topic create demo
echo "hello" | kafka-wire produce demo
kafka-wire consume demo --from-beginning
```

Useful variations:

```sh
kafka-wire topic list
kafka-wire topic describe demo          # start and end offsets per partition
kafka-wire consume demo -n 10 --format json
kafka-wire consume demo --group workers # join a group and commit offsets
```

## Connect your application

Point any Kafka client at `localhost:9092`. Nothing about your code changes.
Working round-trip examples in five languages are in [`examples/`](../examples).

If your client turns idempotent producing on by default, switch it off. That
is the Apache Kafka Java client and kafka-python 3.x today:
`enable.idempotence=false`, or `enable_idempotence=False` in Python. It is the
only accommodation kafka-wire asks for, and the README explains why.

## When something does not work

```sh
kafka-wire doctor
```

It checks the data directory, both ports, the address clients will actually be
told to connect to, and the object store if you configured one.

Three failures account for almost everything:

**"It connects, then hangs."** The advertised address is wrong. A Kafka client
throws away the address it dialed and reconnects to whatever the broker
advertised. Behind Docker, NAT, or a load balancer, set
`listeners.advertisedhost` and `listeners.advertisedport`.

**"The broker refuses to start."** Read the message: it names the setting at
fault and lists the ways to resolve it. The most common one is binding a
non-loopback address with authentication off, which is refused on purpose.

**"UnsupportedVersionException", or "broker does not support
InitProducerIdRequest".** The client defaults idempotent producing on. Set
`enable.idempotence=false` (Java) or `enable_idempotence=False`
(kafka-python 3.x).

## Next

- [Configuration](configuration.md)
- [Cold storage on any S3](storage.md)
- [Security](security.md)
- [Durability](durability.md)
- [Operations](operations.md)
