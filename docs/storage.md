# Storage and the cold tier

## How the log is laid out

```
<datadir>/
  topics/<topic>/<partition>/
    00000000000000000000.log        record batches, verbatim
    00000000000000000000.index      sparse offset index
    00000000000000000000.timeindex  sparse timestamp index
  groups/<group>.json               committed offsets and membership
  metadata/                         cluster, topics, archive manifest
  cache/                            segments restored from cold storage
```

Records are stored in Kafka's own v2 record-batch format and are never decoded.
On produce, the batch header and CRC are validated and the bytes are appended
as-is; on fetch they are streamed back. The only field the broker rewrites is
the 8-byte base offset, which sits outside the CRC's coverage precisely so that
brokers can assign offsets without recomputing it.

That is why the broker is indifferent to your serialization format: it never
looks inside a record.

## The cold tier

Sealed segments older than `archive.age` are uploaded. After
`archive.localretention` the local `.log` is deleted while its much smaller
index files stay behind. A consumer asking for an offset that now lives only in
cold storage triggers a transparent restore into an on-disk LRU cache, verified
against the SHA-256 recorded at upload time.

None of this is on by default.

```yaml
archive:
  backend: none    # none | fs | s3
```

### `fs`: a directory

```yaml
archive:
  backend: fs
  fs:
    path: /mnt/nas/kafka-wire
```

Point it at an NFS or SMB mount and you have off-box durability without running
an object store at all. This backend is also what the test suite uses, so it is
exercised on every commit rather than being a fallback nobody runs.

### `s3`: any S3-compatible store

```yaml
archive:
  backend: s3
  prefix: kafka-wire/
  s3:
    bucket: my-bucket
    endpoint: http://minio:9000     # omit for AWS
    region: us-east-1               # "auto" for Cloudflare R2 and Tigris
    addressing: path                # MinIO, Ceph, SeaweedFS
```

Credentials resolve in this order: the config, then `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, then `~/.aws/credentials`, then
the instance credential endpoint. Prefer
`KAFKA_WIRE_ARCHIVE_S3_SECRETKEY_FILE` over writing a secret into a file that
ends up inside an image.

### The settings that bite

**`addressing`.** AWS and most hosted providers address a bucket as a
subdomain. MinIO, Ceph and SeaweedFS expect it in the path. Getting this wrong
produces a DNS failure or a 404 that reads like a permissions problem. `auto`
guesses from the endpoint; set it explicitly when the guess is wrong.

**`partsize` against `segmentbytes`.** Every S3 implementation caps a multipart
upload at 10,000 parts, so `segmentbytes / partsize` has to stay below that. The
broker refuses to start otherwise, because the alternative is finding out after
transferring an entire segment.

**`partsize` against memory.** One buffer of `partsize` bytes is held per
in-flight upload, and `archive.concurrency` of them can run at once. Storj wants
64 MiB parts, which is a 128 MiB resident cost at the default concurrency of 2.

**`storageclass`.** Leave it empty. Consumers fetch from this bucket, so an
archival class makes reads fail until each object is restored. The broker
refuses the known archival classes rather than letting you discover that later.

## Interrupted uploads resume

A 1 GiB segment takes a while to upload, and deploys are not scheduled around
it. After every accepted part the broker checkpoints the upload id and the parts
already stored. On restart it asks the store what survived and continues from
the first missing part instead of re-sending the whole segment.

The checkpoint is trusted only while the segment's digest still matches, so a
resume can never splice new bytes onto an upload of different content.

If the object turns out to be complete already, which happens when the process
died between the final part and the manifest write, it is adopted rather than
re-uploaded. If nothing survives, the stale upload is aborted so the store stops
billing for an incomplete multipart.

## Backups

The data directory is the database. A filesystem snapshot, or a copy taken while
the broker is stopped, is a complete backup. While it is running, snapshot the
volume rather than copying files, so you do not catch a segment mid-append.

Archived segments in the cold tier are already a second copy, and each carries a
checksum that is verified on the way back.
