# Security

## Reporting a vulnerability

Open a [private security advisory](https://github.com/csnyder256/kafka-wire/security/advisories/new).
Please do not open a public issue for anything exploitable.

This is a solo-maintained project. You should expect an acknowledgement within a
week. If a fix is warranted I will work on it in a private fork and credit you in
the release notes unless you would rather I did not.

## What kafka-wire assumes about its environment

The threat model is a broker on a network you control, reachable by clients you
mostly trust, holding data you care about.

**In scope:**

- Unauthenticated network access to the Kafka or admin port.
- A malicious or malformed protocol frame. The wire layer is a parser fed by the
  network, so every length on the wire is treated as hostile: frame sizes are
  bounded before allocation, client-id and tagged-field lengths are checked
  against the frame, and varints are rejected if they over-run.
- Path traversal through topic names, which reach the filesystem and object keys.
- Cross-principal reads when ACLs are on.

**Out of scope:**

- An attacker with write access to the data directory. They own the log.
- Denial of service by a client that is authorized to produce.
- Side channels between colocated tenants on shared hardware.

## Defaults that exist to prevent an incident

- The broker **binds to `127.0.0.1` by default**, not `0.0.0.0`.
- It **refuses to start** on a non-loopback address with authentication disabled,
  unless `auth.allowanon` is set. That setting exists so an open broker is always
  a decision somebody made, never an accident. The error explains all three ways
  to resolve it.
- The admin API refuses to serve on a non-loopback address without `admin.token`.
- TLS is all-or-nothing: setting a certificate without a key is an error rather
  than a silent fallback to plaintext.
- Secrets can be supplied by `KAFKA_WIRE_*_FILE` indirection so they never appear
  in the process environment or a crash dump.

## Hardening a real deployment

1. Turn on SASL/SCRAM-SHA-256 and give each application its own principal.
2. Turn on TLS. Note that a Kafka listener is raw TCP, so an HTTP reverse proxy
   cannot terminate it.
3. Turn on ACLs and grant the narrowest topic and group permissions that work.
4. Keep the admin port on a private interface, with a token set.
5. Run the container as a non-root user. The published image already does.

See [docs/security.md](docs/security.md) for the how-to.
