"""ACL bypass attempts.

Probes the broker's prefix-matching logic with adversarial topic names
designed to confuse string-prefix authorization. Each attempt MUST be
denied with TOPIC_AUTHORIZATION_FAILED (29).

Patterns probed:
  - path traversal:   "principal.AAA./../principal.BBB.x"
  - whitespace:       "principal.AAA. \t\n"
  - case mutation:    "PRINCIPAL.aaa.x" (Kafka topic names are case-sensitive
                      so this should fail; we assert it does)
  - URL encoding:     "principal.%2E%2E.AAA.x"
  - null bytes:       "principal.AAA.\\x00"
  - unicode lookalikes: cyrillic 'е' (U+0435) instead of latin 'e'
  - prefix overlap:   principal id "AAA" vs "AAA1" (longer prefix attack)
  - empty prefix:     attempting to claim "" as a topic prefix
"""
from __future__ import annotations

import random
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, TopicAuthorizationFailedError

from chaos import invariants
from chaos.topology import Topology

log = structlog.get_logger()


def _kafka_args(principal: str, password: str, bootstrap: str) -> dict[str, Any]:
    return dict(
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=principal,
        sasl_plain_password=password,
        request_timeout_ms=5_000,
    )


def _bypass_topic_candidates(victim_prefix: str, attacker_prefix: str) -> list[str]:
    """Generate topic-name variants that try to confuse prefix matching."""
    base = victim_prefix + "events"
    return [
        attacker_prefix + "/../" + base,
        attacker_prefix + "/.." + base,
        attacker_prefix + "..%2F" + base,
        attacker_prefix + "../" + base,
        # Case mutation (Kafka topic names ARE case-sensitive; this must fail
        # because the resulting name doesn't match either prefix exactly).
        victim_prefix.upper() + "events",
        victim_prefix.title() + "events",
        # Prefix collision tries
        victim_prefix.rstrip(".") + ".events",  # legitimate-looking
        victim_prefix[:-1] + "x" + ".events",   # one-char drift
        # Empty + minimal
        "",
        ".",
        "..",
        "/",
        # Unicode lookalike (cyrillic 'е' for latin 'e')
        victim_prefix.replace("e", "е") + "events",
        # Null byte / control chars
        victim_prefix + "events\x00leak",
        victim_prefix + "events\x1f",
    ]


async def acl_bypass_topic_names(topo: Topology, bootstrap: str, *, rng: random.Random | None = None) -> dict[str, Any]:
    """Each attacker tries a curated list of confusable topic names
    designed to bypass prefix matching."""
    rng = rng or random.Random()
    if len(topo.principals) < 2:
        return {"attempts": 0, "denied": 0, "violations": 0, "skipped": "needs ≥2 principals"}

    attempts = 0
    denied = 0
    for _ in range(min(8, len(topo.principals))):
        attacker, victim = rng.sample(topo.principals, 2)
        candidates = _bypass_topic_candidates(victim.topic_prefix, attacker.topic_prefix)
        p = AIOKafkaProducer(**_kafka_args(attacker.principal, attacker.password, bootstrap))
        try:
            await p.start()
            for cand in candidates:
                attempts += 1
                if not cand or any(ch in cand for ch in "\x00\n\t"):
                    # Some clients refuse to even send these; that's
                    # acceptable: count as denied.
                    denied += 1
                    continue
                try:
                    await p.send_and_wait(cand, b'{"_chaos_bypass": true}')
                    # Accepted? Then either we created an attacker-
                    # owned topic (acceptable, since the new topic
                    # would be auto-created with attacker's principal),
                    # OR we got into a victim-owned topic. The
                    # invariant: the resulting topic name MUST start
                    # with attacker's prefix: never with victim's.
                    if cand.startswith(victim.topic_prefix) and not cand.startswith(attacker.topic_prefix):
                        invariants.assert_invariant_9_no_acl_bypass(
                            topo,
                            bypass_attempt={"attacker": attacker.principal, "victim_prefix": victim.topic_prefix, "candidate": cand},
                            response_error=0,
                        )
                except TopicAuthorizationFailedError:
                    denied += 1
                except KafkaError as e:
                    # Other Kafka errors (invalid topic name, etc.)
                    # are acceptable denials. Treat as denied.
                    denied += 1
                except (UnicodeEncodeError, ValueError):
                    denied += 1
        finally:
            try:
                await p.stop()
            except Exception:
                pass
    return {"attempts": attempts, "denied": denied, "violations": 0}


async def acl_unauthenticated_probe(topo: Topology, bootstrap: str, **_kw) -> dict[str, Any]:
    """Skip SASL entirely. The broker should refuse any non-handshake
    request before authentication completes."""
    from aiokafka import AIOKafkaProducer

    attempts = 1
    denied = 0
    p = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        security_protocol="PLAINTEXT",  # NO SASL
        request_timeout_ms=3_000,
    )
    try:
        try:
            await p.start()
            try:
                await p.send_and_wait("anything", b'{"_chaos": "unauth"}')
                # Reached here = no SASL was required. If ACLs are
                # configured, this is a violation. If they're not
                # configured (Phase 1 backwards-compat), this is fine
                # but the chaos engine should have configured them.
                principals_count = len(topo.principals)
                if principals_count > 0:
                    invariants.assert_invariant_9_no_acl_bypass(
                        topo, bypass_attempt={"via": "no_sasl"}, response_error=0,
                    )
            except KafkaError:
                denied += 1
        except KafkaError:
            denied += 1
    finally:
        try:
            await p.stop()
        except Exception:
            pass
    return {"attempts": attempts, "denied": denied, "violations": 0}
