# Configuration reference

This file is generated. To regenerate it:

```sh
kafka-wire config reference > docs/configuration.md
```

The generator reads the same struct the broker reads at startup, so this
reference cannot drift from the code.

## Resolution order

Lowest priority to highest:

1. compiled-in defaults
2. a config file (`--config`, `$KAFKA_WIRE_CONFIG`, `./kafka-wire.yaml`, `~/.config/kafka-wire/kafka-wire.yaml`, `/etc/kafka-wire/kafka-wire.yaml`)
3. `KAFKA_WIRE_*` environment variables
4. `KAFKA_WIRE_*_FILE` variables, which read the value from a file (use these for secrets)
5. the bootstrap flags `--data-dir`, `--kafka-listen`, `--admin-listen`, `--log-level`

Environment beats file deliberately: the file is usually baked into a
container image, and the environment is what an orchestrator injects at run
time. Setting both the plain and the `_FILE` form of the same variable is an
error rather than a silent precedence puzzle.

There is no `${VAR}` interpolation inside the YAML, and there will not be.


LISTENERS
---------

  listeners.kafka  (string, default 127.0.0.1:9092)
    address the Kafka protocol listener binds to. Use 0.0.0.0:9092 to accept remote clients, but read the security note first
    env: KAFKA_WIRE_LISTENERS_KAFKA

  listeners.admin  (string, default 127.0.0.1:8080)
    address the HTTP admin and Prometheus listener binds to
    env: KAFKA_WIRE_LISTENERS_ADMIN

  listeners.advertisedhost  (string, default "")
    hostname clients are told to reconnect to. Empty derives it from listeners.kafka. Set this behind NAT, a load balancer, or Docker port mapping
    env: KAFKA_WIRE_LISTENERS_ADVERTISEDHOST

  listeners.advertisedport  (size, default 0)
    port clients are told to reconnect to. 0 derives it from listeners.kafka. Set this when the published port differs from the bound port
    env: KAFKA_WIRE_LISTENERS_ADVERTISEDPORT

STORAGE
-------

  storage.datadir  (string, default ./kafka-wire-data)
    directory holding topics, consumer group state, and metadata. Must be on a durable filesystem
    env: KAFKA_WIRE_STORAGE_DATADIR

  storage.segmentbytes  (size, default 1GiB)
    roll to a new log segment once the active one exceeds this size
    env: KAFKA_WIRE_STORAGE_SEGMENTBYTES

  storage.segmentage  (duration, default 168h)
    roll to a new log segment once the active one is this old, even if it is not full
    env: KAFKA_WIRE_STORAGE_SEGMENTAGE

  storage.indexinterval  (size, default 16KiB)
    add a sparse index entry every this many bytes of log. Lower means faster lookups and bigger indexes
    env: KAFKA_WIRE_STORAGE_INDEXINTERVAL

  storage.retentionage  (duration, default 168h)
    delete log segments older than this. 0 disables age-based retention
    env: KAFKA_WIRE_STORAGE_RETENTIONAGE

  storage.retentionsize  (size, default -1)
    delete oldest segments once a partition exceeds this many bytes. -1 means unlimited
    env: KAFKA_WIRE_STORAGE_RETENTIONSIZE

  storage.fsyncmode  (string, default interval)
    durability policy: none (fastest, relies on the OS), interval (fsync on a timer), always (fsync every append, slowest and safest)
    env: KAFKA_WIRE_STORAGE_FSYNCMODE

  storage.fsyncinterval  (duration, default 5s)
    how often to fsync when storage.fsyncmode is interval
    env: KAFKA_WIRE_STORAGE_FSYNCINTERVAL

  storage.diskfreemin  (float, default 0.10)
    pause writes when the fraction of free disk space drops below this. 0 disables the guard
    env: KAFKA_WIRE_STORAGE_DISKFREEMIN

ARCHIVE
-------

  archive.backend  (string, default none)
    cold storage tier: none, fs, or s3. s3 covers AWS plus every S3-compatible store
    env: KAFKA_WIRE_ARCHIVE_BACKEND

  archive.prefix  (string, default kafka-wire/)
    key prefix for archived segments. Lets several brokers share one bucket
    env: KAFKA_WIRE_ARCHIVE_PREFIX

  archive.age  (duration, default 1h)
    a sealed segment becomes eligible for upload once it is this old
    env: KAFKA_WIRE_ARCHIVE_AGE

  archive.localretention  (duration, default 24h)
    delete the local copy of an archived segment after this long. Its indexes are kept
    env: KAFKA_WIRE_ARCHIVE_LOCALRETENTION

  archive.concurrency  (size, default 2)
    how many segment uploads may run at once. Multiply by archive.s3.partsize to budget memory
    env: KAFKA_WIRE_ARCHIVE_CONCURRENCY

  archive.cachebytes  (size, default 2GiB)
    size of the on-disk LRU cache holding segments restored from cold storage
    env: KAFKA_WIRE_ARCHIVE_CACHEBYTES

  archive.fs.path  (string, default "")
    directory to archive segments into when archive.backend is fs. Point this at an NFS or SMB mount for off-box durability
    env: KAFKA_WIRE_ARCHIVE_FS_PATH

  archive.s3.bucket  (string, default "")
    bucket name. Required when archive.backend is s3
    env: KAFKA_WIRE_ARCHIVE_S3_BUCKET

  archive.s3.endpoint  (string, default "")
    S3 API endpoint. Empty means AWS. Examples: http://minio:9000, https://ACCOUNT.r2.cloudflarestorage.com, https://storage.googleapis.com
    env: KAFKA_WIRE_ARCHIVE_S3_ENDPOINT

  archive.s3.region  (string, default us-east-1)
    region string. Cloudflare R2 and Tigris want auto. Many self-hosted stores ignore it but still require a value
    env: KAFKA_WIRE_ARCHIVE_S3_REGION

  archive.s3.addressing  (string, default auto)
    bucket addressing: auto, path, or virtual. MinIO, Ceph and SeaweedFS generally need path
    env: KAFKA_WIRE_ARCHIVE_S3_ADDRESSING

  archive.s3.accesskey  (string, default "")
    access key. Empty falls through to AWS_ACCESS_KEY_ID, then the shared credentials file, then the instance credential endpoint
    env: KAFKA_WIRE_ARCHIVE_S3_ACCESSKEY

  archive.s3.secretkey  (string, default "")
    secret key. Prefer KAFKA_WIRE_ARCHIVE_S3_SECRETKEY_FILE over putting this in a config file
    env: KAFKA_WIRE_ARCHIVE_S3_SECRETKEY

  archive.s3.sessiontoken  (string, default "")
    session token for temporary credentials
    env: KAFKA_WIRE_ARCHIVE_S3_SESSIONTOKEN

  archive.s3.insecure  (bool, default false)
    talk plain HTTP instead of HTTPS. Only for an in-cluster store on a trusted network
    env: KAFKA_WIRE_ARCHIVE_S3_INSECURE

  archive.s3.cafile  (string, default "")
    PEM bundle for a store using a private certificate authority
    env: KAFKA_WIRE_ARCHIVE_S3_CAFILE

  archive.s3.skipverify  (bool, default false)
    do not verify the store TLS certificate. Debugging only
    env: KAFKA_WIRE_ARCHIVE_S3_SKIPVERIFY

  archive.s3.partsize  (size, default 8MiB)
    multipart part size. Minimum 5MiB on most stores. Set 64MiB for Storj. segmentbytes divided by this must stay under 10000
    env: KAFKA_WIRE_ARCHIVE_S3_PARTSIZE

  archive.s3.storageclass  (string, default "")
    value for x-amz-storage-class. Leave empty to omit the header. Never use an archival class: consumers fetch from this bucket
    env: KAFKA_WIRE_ARCHIVE_S3_STORAGECLASS

LIMITS
------

  limits.maxrequestbytes  (size, default 4MiB)
    largest single protocol request accepted. Must exceed the largest batch any producer sends
    env: KAFKA_WIRE_LIMITS_MAXREQUESTBYTES

  limits.maxconnections  (size, default 1024)
    cap on concurrent client connections. 0 means unlimited
    env: KAFKA_WIRE_LIMITS_MAXCONNECTIONS

  limits.memorybytes  (size, default 0)
    soft heap ceiling handed to the Go runtime, with 20 percent reserved as headroom. 0 disables it
    env: KAFKA_WIRE_LIMITS_MEMORYBYTES

AUTH
----

  auth.saslenabled  (bool, default false)
    require SASL authentication on the Kafka listener
    env: KAFKA_WIRE_AUTH_SASLENABLED

  auth.usersfile  (string, default "")
    path to the JSON file holding SCRAM credentials. Required when auth.saslenabled is true
    env: KAFKA_WIRE_AUTH_USERSFILE

  auth.allowanon  (bool, default false)
    permit a non-loopback listener with authentication disabled. The broker refuses to start without this, on purpose
    env: KAFKA_WIRE_AUTH_ALLOWANON

TLS
---

  tls.certfile  (string, default "")
    PEM certificate for the Kafka listener. Set both certfile and keyfile to enable TLS
    env: KAFKA_WIRE_TLS_CERTFILE

  tls.keyfile  (string, default "")
    PEM private key for the Kafka listener
    env: KAFKA_WIRE_TLS_KEYFILE

  tls.clientca  (string, default "")
    PEM bundle of client certificate authorities. Setting it requires and verifies client certificates (mutual TLS)
    env: KAFKA_WIRE_TLS_CLIENTCA

  tls.minversion  (string, default 1.2)
    minimum TLS version: 1.2 or 1.3
    env: KAFKA_WIRE_TLS_MINVERSION

ADMIN
-----

  admin.token  (string, default "")
    bearer token required by the admin API. Empty leaves the admin API open, which is refused on a non-loopback bind
    env: KAFKA_WIRE_ADMIN_TOKEN

  admin.enabled  (bool, default true)
    serve the HTTP admin API and Prometheus metrics
    env: KAFKA_WIRE_ADMIN_ENABLED

CLUSTER
-------

  cluster.id  (string, default kafka-wire)
    cluster identifier reported to clients in Metadata
    env: KAFKA_WIRE_CLUSTER_ID

  cluster.brokerid  (size, default 1)
    numeric broker id reported to clients
    env: KAFKA_WIRE_CLUSTER_BROKERID

SHUTDOWN
--------

  shutdown.grace  (duration, default 25s)
    how long to finish in-flight work after a termination signal. Keep it below your orchestrator's kill timeout: Kubernetes terminationGracePeriodSeconds, Docker stop_grace_period, Nomad kill_timeout, systemd TimeoutStopSec
    env: KAFKA_WIRE_SHUTDOWN_GRACE

LOG
---

  log.level  (string, default info)
    debug, info, warn, or error
    env: KAFKA_WIRE_LOG_LEVEL

  log.format  (string, default text)
    text for a human at a terminal, json for a log pipeline
    env: KAFKA_WIRE_LOG_FORMAT
