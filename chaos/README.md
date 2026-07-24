# Adversarial verification harness

A perpetual attack loop, not a test suite. It generates a randomized universe
of principals, topics, consumer groups and ACL grants, drives a baseline
workload through it, and then spends the rest of its time trying to break out
of it: reading another principal's topic, joining another principal's group,
replaying another principal's offsets, brute-forcing the admin API, corrupting
metadata underneath a live read, and feeding the wire protocol malformed
frames.

If any of it succeeds, the run halts, dumps a forensic snapshot, and exits
non-zero.

```sh
pip install -r chaos/requirements.txt
./chaos/run.sh 120 4          # 120 seconds, 4 principals
```

That script does the whole dance, because SCRAM credentials have to exist on
disk before the broker starts: provision without auth, restart with auth,
attack.

## What it asserts

The isolation property is the one worth attacking, because it is the one whose
failure is silent. A broker that drops messages gets noticed in minutes. A
broker that shows principal A's records to principal B can run for a year
before anyone finds out.

| # | Invariant |
|---|---|
| 1 | A principal reaches only the topics its ACL grants |
| 2 | Replay and offset-reset requests never cross a principal boundary |
| 3 | Offset restoration never resolves to another principal's segment |
| 4 | Restoring from cold storage never serves another principal's objects |
| 5 | Corrupt metadata fails closed rather than leaking |
| 6 | Concurrent reads never expose another principal's records |
| 7 | A deleted principal leaves nothing retrievable behind |
| 8 | Parallel restores never overlap namespaces |
| 9 | No API path bypasses ACL enforcement |
| 10 | No fallback silently downgrades principal resolution |

Invariant 9 is the reason the harness exists in this repository. Building it
is what surfaced the fact that authorization was enforced on Produce and Fetch
but not on CreateTopics, DeleteTopics, Metadata, ListOffsets or
DescribeGroups: a principal with no grants could still enumerate and delete
every topic in the cluster. The harness would not pass against the broker as
it was.

Note that invariant 9 counts `UNKNOWN_TOPIC_OR_PARTITION` as a denial
alongside `TOPIC_AUTHORIZATION_FAILED`. Hiding a topic from a principal that
cannot read it is the stronger posture, because an authorization error is
itself an existence oracle, and the harness should not penalize the safer
behavior.

## Layout

```
chaos/
  daemon.py        orchestrator: provision, workload, attack, assert, report
  topology.py      randomized universe, deterministic from --seed
  workload.py      baseline produce/fetch/commit traffic to hide attacks in
  invariants.py    the assertions, and the violation record format
  differential.py  compares broker answers against the topology's own truth
  concurrency.py   races two operations to catch time-of-check bugs
  forensics.py     on violation: dump state, config, and a repro command
  adversary/       acl_bypass, admin_brute, cross_principal, replay_offset
  fuzzers/         protocol frames, and property-based key and offset fuzzing
  injectors/       fault injection into the broker, the disk, and object storage
```

## Reproducing a failure

Every run prints its seed, and the topology is fully derived from it:

```sh
CHAOS_SEED=4242 ./chaos/run.sh 120 4
```

Same seed, same principals, same passwords, same topic names. This mattered
enough to fix: the harness originally generated principal names with `uuid4`
and passwords with `secrets`, both of which ignore the seed, so `--seed`
reproduced the shape of a topology but none of its identities, and a
provisioning run could not hand credentials to the run that used them.

## Where it came from

This is the verification engine built for ClarusStream, the broker's private
ancestor, where the isolation boundary was between tenants of a multi-tenant
SaaS. The public broker has no tenants, so the same property is expressed
against the thing it does have: SASL principals and their ACL grants. The
attacks, the invariants and the forensic machinery are otherwise unchanged.
