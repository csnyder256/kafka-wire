# Operations

## Health and metrics

The admin listener serves:

| Path | Purpose |
|---|---|
| `/health` | liveness and readiness. 200 once the log has been recovered and the listener is accepting. |
| `/metrics` | Prometheus exposition |
| `/v1/cluster` | cluster identity and broker address |
| `/v1/topics`, `/v1/topics/{name}` | topics, partitions, offsets, sizes |
| `/v1/groups`, `/v1/groups/{id}` | consumer groups, members, committed offsets |
| `/v1/archive` | archived segments and in-flight uploads |
| `/v1/acls`, `/v1/acls/{principal}` | access control entries |
| `/v1/replay/reset-offset` | move a consumer group's committed offset |

Everything except `/health` requires `Authorization: Bearer <admin.token>` when
a token is set. `/health` stays open because orchestrator probes cannot carry
credentials.

Note that `/health` does not answer until log recovery finishes. On a large data
directory that can take a while, which is why the Kubernetes manifest uses a
`startupProbe` with a generous failure threshold and a tighter liveness probe
afterwards.

## What to alert on

In rough order of how much trouble you are in:

1. **Disk filling.** The guard pauses writes below `storage.diskfreemin`, and
   producers start seeing retriable errors. Alert well before that point.
2. **Consumer group lag growing without bound.** The consumer is slower than the
   producer, or it is dead.
3. **Archive failures.** If uploads are failing, cold storage has stopped being
   a second copy, and ordinary retention will eventually delete the local one.
4. **No produce traffic on a topic that normally has some.** Usually a client
   problem, but it is the earliest signal of one.
5. **Restore failures.** Consumers reading old offsets cannot make progress.

## Day-2 tasks

### Moving to a bigger disk

Stop the broker, copy the data directory (`rsync -a` or a volume snapshot),
point `storage.datadir` at the new location, start it. Offsets, groups and the
archive manifest all live inside that directory, so nothing else moves.

### Changing retention

`storage.retentionage` and `storage.retentionsize` are enforced by a sweep that
runs every minute. Lowering them deletes eligible segments on the next sweep, so
lower them deliberately.

### Upgrading

Replace the binary and restart. The on-disk format is Kafka's record-batch
format plus JSON metadata, and both are read forward-compatibly. Take a snapshot
first anyway.

### Rotating an admin token

Set the new value and restart. There is no online reload, on purpose: a
configuration that can change underneath a running process is a configuration
you cannot reason about from its file.

### A corrupt segment

Symptoms: fetches for one offset range fail while the rest of the topic is fine.
Move the offending `.log` file aside and restart. If it had been archived, it is
restored from cold storage on the next fetch that needs it. See
[durability.md](durability.md).

## Troubleshooting

**Clients connect and then hang.** The advertised address is wrong. Run
`kafka-wire doctor`, which prints exactly what clients will be told. Fix with
`listeners.advertisedhost` and `listeners.advertisedport`.

**`UnsupportedVersionException` from a Java producer.** Set
`enable.idempotence=false`. kafka-wire has no transaction coordinator.

**Producers get `MESSAGE_TOO_LARGE`, or the connection drops on a big batch.**
`limits.maxrequestbytes` is below the producer's batch size. Raise it on the
broker, or lower `max.request.size` and `batch.size` on the producer.

**The broker will not start and says the configuration is not usable.** Read the
message. It names the setting, explains the consequence, and lists the fixes.
Every one of those checks exists because the alternative was data loss or a
silent misconfiguration.

**Archive uploads fail against a non-AWS store.** Nearly always
`archive.s3.addressing`. MinIO, Ceph and SeaweedFS want `path`. Run
`kafka-wire doctor`, which makes a real request to the bucket and reports what
came back.

**The disk guard tripped.** Free space or expand the volume. Writes resume on
their own once free space is back above the threshold.

**"another kafka-wire is already using the data directory".** Exactly what it
says: a second broker tried to open a data directory another process holds.
Two brokers sharing one directory append to the same segment files and corrupt
the log, so the second one refuses to start. Stop the other process, or give
this one its own `storage.datadir`. The lock is an open file handle, so a
crashed broker never leaves a stale lock behind.

## Logs

```yaml
log:
  level: info     # debug | info | warn | error
  format: text    # text for a terminal, json for a log pipeline
```

Log keys are stable and dotted, so they can be indexed:
`archive.upload.completed`, `archive.upload.resumed`, `archive.reconcile.*`,
`wire.connection_limit`, `shutdown.*`. Routine client disconnects are logged at
debug, not warning, so the warning channel stays worth reading.
