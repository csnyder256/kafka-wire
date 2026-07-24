"""Randomized multi-principal topology generator.

Spins up a synthetic test universe:
  - N randomized principals, each with:
      - principal_id (UUID)
      - SCRAM principal name (sasl-user-<short>)
      - SCRAM password (random 24-char)
      - HMAC key (32 random bytes for ownership signatures)
      - principal-scoped topic prefix (`ns.<id>.`)
      - 1+ topics under that prefix
      - 1+ consumer groups under a matching `ns.<id>.` group prefix
  - 1 platform principal with cross-cutting access (the existing 4
    services run as this in production)
  - 1 adversary principal with NO grants (used by adversary modules
    to probe negative space)

The topology is the SOURCE OF TRUTH the differential validator
compares against. Every owned object, offset, and replay stream is
recorded here at generation time.

Topology patterns A/B/C/D from the chaos spec:
  Pattern A:  s3://global-bucket/principal-id/document-id           (default)
  Pattern B:  s3://enterprise-principal-bucket/document-id          (per-principal bucket)
  Pattern C:  s3://region/principal/date/document                   (region-scoped)
  Pattern D:  hybrid tiered layouts                               (mix of A/B/C)

Each generated topology randomly samples one pattern; the framework
asserts the invariants hold under every pattern.
"""
from __future__ import annotations

import dataclasses
import hashlib
import random
import secrets
import string
import time
import uuid
from typing import Any


# Patterns the framework cycles through.
PATTERN_A = "global_bucket_per_principal_prefix"
PATTERN_B = "per_principal_bucket"
PATTERN_C = "region_per_principal_per_date"
PATTERN_D = "hybrid_tiered"


@dataclasses.dataclass
class Principal:
    principal_id: str
    principal: str
    password: str
    hmac_key_hex: str
    topic_prefix: str
    group_prefix: str
    topics: list[str]
    groups: list[str]

    # Per-principal S3 layout (varies by pattern).
    s3_bucket: str
    s3_prefix: str

    # Tracking: every object/offset/segment we've created for this
    # principal. Used by invariants/differential validator as the
    # ground-truth set; any retrieval claiming to belong to this
    # principal must reference one of these.
    owned_topics: set[str] = dataclasses.field(default_factory=set)
    owned_objects: set[str] = dataclasses.field(default_factory=set)
    produced_offsets: dict[tuple[str, int], int] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Topology:
    pattern: str
    seed: int
    created_at: float
    principals: list[Principal]
    platform_principal: str
    platform_password: str
    adversary_principal: str
    adversary_password: str
    archive_hmac_key_hex: str
    default_bucket: str

    # Quick lookup helpers (populated post-generation).
    principal_by_principal: dict[str, Principal] = dataclasses.field(default_factory=dict)
    topic_to_principal: dict[str, str] = dataclasses.field(default_factory=dict)


def generate_topology(
    *,
    n_principals: int = 8,
    n_topics_per_principal: int = 3,
    n_groups_per_principal: int = 2,
    pattern: str | None = None,
    seed: int | None = None,
) -> Topology:
    """Build a fresh randomized topology. Deterministic given the seed."""
    seed = seed if seed is not None else random.SystemRandom().randint(1, 2**31 - 1)
    rng = random.Random(seed)

    pattern = pattern or rng.choice([PATTERN_A, PATTERN_B, PATTERN_C, PATTERN_D])
    default_bucket = "chaos-archive-" + _short_id(rng)
    archive_hmac_key = _hex_key(rng)

    principals: list[Principal] = []
    for _ in range(n_principals):
        tid = _hex_id(rng, 12)
        principal = f"chaos-principal-{tid[:8]}"
        password = _random_password(rng)
        hmac_key = _hex_key(rng)
        topic_prefix = f"principal.{tid}."
        group_prefix = f"principal.{tid}."

        topics = [topic_prefix + _word(rng) for _ in range(n_topics_per_principal)]
        groups = [group_prefix + _word(rng) for _ in range(n_groups_per_principal)]

        # Per-pattern S3 layout.
        if pattern == PATTERN_A:
            bucket = default_bucket
            prefix = f"principals/{tid}/"
        elif pattern == PATTERN_B:
            bucket = f"chaos-principal-{tid}-archive"
            prefix = ""
        elif pattern == PATTERN_C:
            region = rng.choice(["us-east-2", "us-west-2", "eu-west-1"])
            date = time.strftime("%Y-%m-%d", time.gmtime())
            bucket = f"chaos-{region}-archive"
            prefix = f"{tid}/{date}/"
        else:  # PATTERN_D: hybrid
            sub = rng.choice([PATTERN_A, PATTERN_B, PATTERN_C])
            if sub == PATTERN_A:
                bucket, prefix = default_bucket, f"principals/{tid}/"
            elif sub == PATTERN_B:
                bucket, prefix = f"chaos-principal-{tid}-archive", ""
            else:
                region = rng.choice(["us-east-2", "us-west-2"])
                date = time.strftime("%Y-%m-%d", time.gmtime())
                bucket, prefix = f"chaos-{region}-archive", f"{tid}/{date}/"

        principals.append(Principal(
            principal_id=tid,
            principal=principal,
            password=password,
            hmac_key_hex=hmac_key,
            topic_prefix=topic_prefix,
            group_prefix=group_prefix,
            topics=topics,
            groups=groups,
            s3_bucket=bucket,
            s3_prefix=prefix,
        ))

    plat_user = f"chaos-platform-{_short_id(rng)}"
    adv_user = f"chaos-adversary-{_short_id(rng)}"

    topo = Topology(
        pattern=pattern,
        seed=seed,
        created_at=time.time(),
        principals=principals,
        platform_principal=plat_user,
        platform_password=_random_password(rng),
        adversary_principal=adv_user,
        adversary_password=_random_password(rng),
        archive_hmac_key_hex=archive_hmac_key,
        default_bucket=default_bucket,
    )
    # Populate lookup maps.
    for t in principals:
        topo.principal_by_principal[t.principal] = t
        for tp in t.topics:
            topo.topic_to_principal[tp] = t.principal_id

    return topo


def _hex_id(rng: random.Random, n: int) -> str:
    """Seeded hex id. uuid4 and secrets deliberately ignore any seed, so using
    them here made --seed unable to reproduce a topology, which is the one
    thing --seed exists for: provisioning writes credentials in one process
    and the attack run has to derive the same ones."""
    return "".join(rng.choice("0123456789abcdef") for _ in range(n))


def _hex_key(rng: random.Random) -> str:
    return _hex_id(rng, 64)


def _short_id(rng: random.Random) -> str:
    return "".join(rng.choice(string.ascii_lowercase + string.digits) for _ in range(8))


def _random_password(rng: random.Random) -> str:
    return "".join(rng.choice(string.ascii_letters + string.digits) for _ in range(24))


def _word(rng: random.Random) -> str:
    # Short corpus: enough variety for adversary modules without
    # giving them too many dimensions.
    corpus = ["events", "audit", "billing", "documents", "payloads",
              "metrics", "telemetry", "queue", "stream", "feed",
              "uploads", "downloads", "api", "internal", "external"]
    return rng.choice(corpus) + "." + _short_id(rng)[:4]


def topology_fingerprint(topo: Topology) -> str:
    """Stable hash of the topology for forensic reproducibility."""
    h = hashlib.sha256()
    h.update(f"{topo.pattern}|{topo.seed}|{topo.default_bucket}".encode())
    for t in topo.principals:
        h.update(f"|{t.principal_id}|{t.principal}|{t.s3_bucket}|{t.s3_prefix}".encode())
        for tp in t.topics:
            h.update(f"|{tp}".encode())
    return h.hexdigest()[:16]


def topology_to_dict(topo: Topology) -> dict[str, Any]:
    """Serialize for forensic dumps. Passwords + HMAC keys are
    INCLUDED: the dump is meant to fully reproduce a scenario, not
    be shared externally. Forensic artifacts must be treated as
    secrets equivalent to broker credentials."""
    return {
        "pattern": topo.pattern,
        "seed": topo.seed,
        "created_at": topo.created_at,
        "default_bucket": topo.default_bucket,
        "archive_hmac_key_hex": topo.archive_hmac_key_hex,
        "platform_principal": topo.platform_principal,
        "platform_password": topo.platform_password,
        "adversary_principal": topo.adversary_principal,
        "adversary_password": topo.adversary_password,
        "principals": [
            {
                "principal_id": t.principal_id,
                "principal": t.principal,
                "password": t.password,
                "hmac_key_hex": t.hmac_key_hex,
                "topic_prefix": t.topic_prefix,
                "group_prefix": t.group_prefix,
                "topics": t.topics,
                "groups": t.groups,
                "s3_bucket": t.s3_bucket,
                "s3_prefix": t.s3_prefix,
                "owned_topics": list(t.owned_topics),
                "owned_objects": list(t.owned_objects),
            }
            for t in topo.principals
        ],
    }
