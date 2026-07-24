# Security

## The default posture

kafka-wire binds to `127.0.0.1`, not `0.0.0.0`, and **refuses to start** on a
non-loopback address with authentication disabled:

```
configuration is not usable:
  listeners.kafka is "0.0.0.0:9092", which accepts connections from other
  machines, but auth.saslenabled is false.
      Anyone who can reach that port could read and write every topic.
      Fix it one of these ways:
        - bind to localhost:      listeners.kafka: 127.0.0.1:9092
        - turn on authentication: auth.saslenabled: true  (see docs/security.md)
        - accept the risk:        auth.allowanon: true    (private network only)
```

The third option exists so that an open broker is always a decision somebody
made rather than a default nobody noticed. Inside a private container network,
`auth.allowanon: true` is a perfectly reasonable choice; on a public IP it is
how data gets stolen.

The admin API applies the same rule: a non-loopback bind requires `admin.token`.

## Authentication

kafka-wire implements SASL/SCRAM-SHA-256. SCRAM proves knowledge of a password
without sending it, so a password never crosses the wire in the clear.

**The users file holds passwords in plain text.** The broker derives the SCRAM
credential from it at load time rather than storing a derived credential, so
that file is exactly as sensitive as a password list: keep it readable only by
the broker's user, and keep it out of your image and your repository. The salt
is derived deterministically from the username rather than being random per
installation, so a precomputed attack against one deployment carries to
another. Treat this as authentication, not as a hashed credential store.

```yaml
auth:
  saslenabled: true
  usersfile: /etc/kafka-wire/users.json
```

The users file maps a principal to its password:

```json
{
  "users": {
    "orders-service": "the-password-for-this-service",
    "analytics-reader": "a-different-password"
  }
}
```

Every key in that object becomes a login, so do not put comments or
placeholders in it.

Give every application its own principal. Shared credentials cannot be revoked
individually and make an audit trail useless.

Client side:

```properties
# Java / Spring
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="orders-service" password="...";
enable.idempotence=false
```

```python
# kafka-python
KafkaProducer(
    bootstrap_servers="broker:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="orders-service",
    sasl_plain_password="...",
)
```

```sh
kcat -b broker:9092 -L \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-256 \
  -X sasl.username=orders-service -X sasl.password=...
```

## TLS

```yaml
tls:
  certfile: /etc/kafka-wire/tls/server.crt
  keyfile:  /etc/kafka-wire/tls/server.key
  minversion: "1.2"
  # Setting clientca requires and verifies client certificates (mutual TLS).
  clientca: /etc/kafka-wire/tls/ca.crt
```

Setting only one of `certfile` and `keyfile` is a startup error, because the
alternative is silently serving plaintext on a port everyone believes is
encrypted.

**A Kafka listener is raw TCP.** An HTTP reverse proxy such as nginx, Caddy, or
an ingress controller cannot terminate it, and neither can an HTTP-only load
balancer. Use a TCP-mode load balancer, or terminate TLS in the broker.

For a self-signed certificate in development:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout server.key -out server.crt -subj "/CN=localhost"
```

Clients will need that certificate in their trust store, or they will reject the
connection, which is the system working correctly.

## Access control

ACLs activate when an ACL file with at least one entry exists; there is no
separate on switch. Manage entries through the admin API:

```sh
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/acls
```

An entry binds a principal to an operation on a resource: read or write on a
topic, read on a consumer group. Once any entry exists, a principal with no
matching entry is denied.

**Scope, stated plainly:** authorization is enforced on Produce, Fetch,
OffsetCommit, OffsetFetch and the consumer group APIs. It is **not** enforced
on CreateTopics, DeleteTopics, Metadata, ListOffsets, DescribeConfigs,
ListGroups or DescribeGroups, so an authenticated principal can still create
and delete topics. If that matters for your deployment, do not expose the
broker to principals you would not trust with topic administration.

Grant the narrowest permission that works. A service that only publishes needs
write on one topic and nothing else, and that is what limits the damage when its
credentials leak.

## Exposing a broker to the internet

Try not to. If you must:

1. SASL on, with a distinct principal per client.
2. TLS on, ideally mutual.
3. ACL entries defined, so unlisted principals are denied.
4. The admin port bound to a private interface with a token set.
5. A firewall that allows 9092 only from the addresses that need it.
6. `auth.allowanon` **not** set, so the guard stays armed.

A broker on a public IP with authentication off will be found. It is a writable
data store on a well-known port, and the internet scans for those continuously.

## Supply chain

Release binaries and container images are built by GitHub Actions from a tagged
commit, and the workflow is in the repository. Dependencies are pinned by
`go.sum`, and the build is `-trimpath` with no cgo, so it is reproducible from
the same tag and toolchain.

Report vulnerabilities via [SECURITY.md](../SECURITY.md).
