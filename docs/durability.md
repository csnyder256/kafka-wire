# Durability

The honest accounting of what survives what.

## The short version

kafka-wire keeps one copy of your data on one machine. Losing that machine's
disk loses everything that had not been archived. There is no replication, no
failover, and no quorum, and no configuration setting changes that.

If that is unacceptable for your data, use a replicated system. That is not a
disclaimer, it is the correct engineering answer.

## What each failure costs

| Failure | Result |
|---|---|
| The broker process crashes or is killed | Nothing acknowledged is lost. The log is validated on restart and truncated to the last complete record batch. |
| The machine loses power | Up to `storage.fsyncinterval` of the most recent writes can be lost, under the default `fsyncmode: interval`. |
| The disk fails | Everything not yet archived to the cold tier is gone. |
| A segment is corrupted on disk | Detected by CRC on read. That segment fails; earlier and later ones are unaffected. |
| The object store loses an archived segment | Detected on restore by SHA-256 mismatch, and refused rather than served as garbage. |

## fsync

```yaml
storage:
  fsyncmode: interval    # none | interval | always
  fsyncinterval: 5s
```

- **`interval`** (the default): a write is acknowledged once the operating
  system has it, and data reaches the platter on a timer. This is the same
  tradeoff Apache Kafka makes by default, where durability comes from
  replication rather than from fsync. kafka-wire has no replication, so the
  exposure is real: a power loss can cost the last few seconds.
- **`always`**: every append is fsynced before it is acknowledged. A machine
  crash loses nothing that was acknowledged. Throughput drops by roughly an
  order of magnitude on spinning disks, and noticeably on SSDs.
- **`none`**: the operating system decides when. Fastest, and only appropriate
  for data you can regenerate from somewhere else.

`acks=all` from a producer means "the leader has it". With one replica the
leader is the only replica, so `acks=all` and `acks=1` are the same thing here.
Nothing lies to the client about this; there is simply only one copy.

## What the cold tier buys

Once a segment is archived it exists in two places, and the second one is
usually somebody else's redundant storage. That turns "the disk died" from total
loss into "everything older than `archive.age` survived".

It is not a backup of the last hour. Recent data lives only on local disk until
its segment seals and ages out.

## Recovering from a corrupt segment

The log is checked on startup and truncated at the first incomplete batch. That
is the normal outcome of an unclean shutdown and needs no intervention.

If a sealed segment is corrupt, the fetch that reaches it fails while the rest of
the partition keeps working. Move the file aside and restart. If that segment had
been archived, it is restored from cold storage on the next fetch that needs it.

## Choosing well

Good fits: application events, background job queues, audit trails that also land
somewhere else, change feeds, telemetry, and anything reproducible from an
upstream system.

Bad fits: the only record of a financial transaction; anything where losing five
seconds of writes is a reportable incident; anything that has to survive the loss
of a single machine without a human being involved.
