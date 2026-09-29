# Deploy Kafka Wire

## Native binary

Download the archive matching your operating system and CPU from the
[latest release](https://github.com/csnyder256/kafka-wire/releases/latest).
Linux, macOS, Windows and FreeBSD builds have no external Go runtime dependency.
Verify the archive against `checksums.txt`, extract it, and run
`kafka-wire --help`. The release embeds its version and commit.

## Container

```sh
docker run --rm ghcr.io/csnyder256/kafka-wire:0.1.4 --help
```

Images support Linux AMD64 and ARM64. Follow the [README configuration and
security guide](README.md) to mount configuration and persistent storage at
`/data`, and set advertised addresses appropriate for your clients. Expose ports
only to the intended private network; authentication and TLS settings belong in
runtime configuration. Anonymous mode is an explicit opt-in.

## Upgrade and recovery

Stop the broker cleanly, back up configuration and `/data`, then change the
binary or pin the new image version. Keep the previous binary/image and backup
for rollback. Test your own producer and consumer against the new version before
upgrading a shared broker. Transactional producers and read-committed isolation
are not implemented. Release publication is gated by tests and vulnerability checks.
