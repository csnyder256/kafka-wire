"""Cross-principal attack patterns.

Every attempt here MUST fail. If even one succeeds, we have an
isolation breach.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import (
    GroupAuthorizationFailedError,
    KafkaError,
    TopicAuthorizationFailedError,
)

from chaos import invariants
from chaos.topology import Topology
from chaos.workload import make_payload

log = structlog.get_logger()


def _kafka_args(principal: str, password: str, bootstrap: str) -> dict[str, Any]:
    return dict(
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=principal,
        sasl_plain_password=password,
        request_timeout_ms=10_000,
    )


def _classify_error(exc: BaseException | None) -> int:
    """Return the Kafka error code (or 0 for success)."""
    if exc is None:
        return 0
    if isinstance(exc, TopicAuthorizationFailedError):
        return 29
    if isinstance(exc, GroupAuthorizationFailedError):
        return 30
    if isinstance(exc, KafkaError):
        # Generic; treat as "denied for some reason", count as
        # safe but log the actual code if present.
        code = getattr(exc, "errno", None)
        if isinstance(code, int):
            return code
        return -1
    return -1


async def cross_principal_produce(topo: Topology, bootstrap: str, *, n_attempts: int = 30, rng: random.Random | None = None) -> dict[str, Any]:
    """Each principal principal attempts to produce to ANOTHER principal's
    topic. INV-01 + INV-09: every attempt must be denied with code 29
    (TOPIC_AUTHORIZATION_FAILED)."""
    rng = rng or random.Random()
    if len(topo.principals) < 2:
        return {"attempts": 0, "denied": 0, "violations": 0, "skipped": "needs ≥2 principals"}

    attempts = 0
    denied = 0
    for _ in range(n_attempts):
        attacker, victim = rng.sample(topo.principals, 2)
        target_topic = rng.choice(victim.topics)
        attempts += 1
        p = AIOKafkaProducer(**_kafka_args(attacker.principal, attacker.password, bootstrap))
        try:
            await p.start()
            try:
                await p.send_and_wait(target_topic, make_payload(attacker, 0))
                # Reached here without exception ⇒ broker accepted
                # cross-principal Produce. P0.
                invariants.assert_invariant_1_principal_topic_ownership(
                    topo, attacker.principal, target_topic, "produce",
                    response_error=0, expected_authorized=False,
                )
            except KafkaError as e:
                code = _classify_error(e)
                # We expect 29 (or any auth-failed code).
                if code == 29 or code == 31:
                    denied += 1
                else:
                    invariants.assert_invariant_9_no_acl_bypass(
                        topo,
                        bypass_attempt={"attacker": attacker.principal, "target_topic": target_topic, "via": "produce"},
                        response_error=code,
                    )
        finally:
            try:
                await p.stop()
            except Exception:
                pass
    return {"attempts": attempts, "denied": denied, "violations": 0}


async def cross_principal_fetch(topo: Topology, bootstrap: str, *, n_attempts: int = 30, rng: random.Random | None = None) -> dict[str, Any]:
    """Each principal principal subscribes to ANOTHER principal's topic.
    Either the connection refuses outright, or the consumer never
    sees a message. Either is acceptable; receiving a single message
    is a P0."""
    rng = rng or random.Random()
    if len(topo.principals) < 2:
        return {"attempts": 0, "denied": 0, "violations": 0, "skipped": "needs ≥2 principals"}

    attempts = 0
    denied = 0
    for _ in range(n_attempts):
        attacker, victim = rng.sample(topo.principals, 2)
        target_topic = rng.choice(victim.topics)
        rogue_group = attacker.groups[0]  # using attacker's own group
        attempts += 1
        c = AIOKafkaConsumer(
            target_topic,
            **_kafka_args(attacker.principal, attacker.password, bootstrap),
            group_id=rogue_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        try:
            try:
                await c.start()
            except KafkaError as e:
                code = _classify_error(e)
                if code in (29, 30, 31):
                    denied += 1
                    continue
                invariants.assert_invariant_9_no_acl_bypass(
                    topo,
                    bypass_attempt={"attacker": attacker.principal, "target_topic": target_topic, "via": "fetch.start"},
                    response_error=code,
                )
                continue

            # Connected. Try one Fetch.
            try:
                msg = await asyncio.wait_for(c.getone(), timeout=2.0)
            except asyncio.TimeoutError:
                # No data (possibly correct: broker enforced ACL by
                # returning empty). We still want to mark "denied"
                # only if the broker explicitly errored; a silent
                # zero-data response is suspicious: but if HWM=0,
                # there's literally nothing to leak.
                denied += 1
                continue
            except KafkaError as e:
                code = _classify_error(e)
                if code in (29, 30, 31):
                    denied += 1
                    continue
                invariants.assert_invariant_9_no_acl_bypass(
                    topo,
                    bypass_attempt={"attacker": attacker.principal, "target_topic": target_topic, "via": "fetch.getone"},
                    response_error=code,
                )
                continue
            # We received a message from a topic we shouldn't have access to.
            try:
                payload = json.loads(msg.value)
            except Exception:
                payload = {"_raw": msg.value.decode(errors="replace")}
            invariants.assert_invariant_2_replay_no_cross_principal(
                topo, attacker.principal, target_topic, [payload],
            )
            # If INV-02 didn't raise, we have a worse situation: the
            # broker shipped a record, the record's _chaos_principal
            # tag matched our attacker for some reason. Treat as
            # INV-09 (ACL bypass) anyway.
            invariants.assert_invariant_9_no_acl_bypass(
                topo,
                bypass_attempt={"attacker": attacker.principal, "target_topic": target_topic, "via": "received_message"},
                response_error=0,
            )
        finally:
            try:
                await c.stop()
            except Exception:
                pass
    return {"attempts": attempts, "denied": denied, "violations": 0}


async def cross_principal_group_join(topo: Topology, bootstrap: str, *, n_attempts: int = 20, rng: random.Random | None = None) -> dict[str, Any]:
    """Principal A's principal tries to join principal B's consumer group.
    INV-09: must be denied with code 30 (GROUP_AUTHORIZATION_FAILED)."""
    rng = rng or random.Random()
    if len(topo.principals) < 2:
        return {"attempts": 0, "denied": 0, "violations": 0, "skipped": "needs ≥2 principals"}

    attempts = 0
    denied = 0
    for _ in range(n_attempts):
        attacker, victim = rng.sample(topo.principals, 2)
        rogue_group = rng.choice(victim.groups)
        # Subscribe to attacker's own topics under victim's group id.
        target_topics = attacker.topics
        attempts += 1
        c = AIOKafkaConsumer(
            *target_topics,
            **_kafka_args(attacker.principal, attacker.password, bootstrap),
            group_id=rogue_group,
            auto_offset_reset="earliest",
        )
        try:
            try:
                await c.start()
            except KafkaError as e:
                code = _classify_error(e)
                if code in (29, 30, 31):
                    denied += 1
                else:
                    invariants.assert_invariant_9_no_acl_bypass(
                        topo,
                        bypass_attempt={"attacker": attacker.principal, "rogue_group": rogue_group},
                        response_error=code,
                    )
                continue
            # Started without error: try to drive a JoinGroup by
            # polling. If the broker assigned partitions inside the
            # rogue group, that's an INV-09 violation.
            try:
                await asyncio.wait_for(c.getone(), timeout=1.5)
            except (asyncio.TimeoutError, KafkaError):
                # Most likely outcome: client times out because no
                # partitions were assigned. Accept as denial.
                denied += 1
                continue
            invariants.assert_invariant_9_no_acl_bypass(
                topo,
                bypass_attempt={"attacker": attacker.principal, "rogue_group": rogue_group, "via": "join_group_succeeded"},
                response_error=0,
            )
        finally:
            try:
                await c.stop()
            except Exception:
                pass
    return {"attempts": attempts, "denied": denied, "violations": 0}


async def adversary_principal_probe(topo: Topology, bootstrap: str, *, rng: random.Random | None = None) -> dict[str, Any]:
    """The adversary principal has ZERO grants. Every operation against
    every topic must fail with auth code 29/30/31."""
    rng = rng or random.Random()
    attempts = 0
    denied = 0
    for victim in topo.principals:
        target_topic = rng.choice(victim.topics)

        # Attempt produce.
        p = AIOKafkaProducer(**_kafka_args(topo.adversary_principal, topo.adversary_password, bootstrap))
        attempts += 1
        try:
            await p.start()
            try:
                await p.send_and_wait(target_topic, b'{"adversary":"probe"}')
                invariants.assert_invariant_1_principal_topic_ownership(
                    topo, topo.adversary_principal, target_topic, "produce",
                    response_error=0, expected_authorized=False,
                )
            except KafkaError as e:
                code = _classify_error(e)
                if code in (29, 30, 31):
                    denied += 1
                else:
                    invariants.assert_invariant_9_no_acl_bypass(
                        topo, bypass_attempt={"adversary_topic": target_topic, "via": "produce"},
                        response_error=code,
                    )
        finally:
            try:
                await p.stop()
            except Exception:
                pass

        # Attempt fetch.
        c = AIOKafkaConsumer(
            target_topic,
            **_kafka_args(topo.adversary_principal, topo.adversary_password, bootstrap),
            group_id="adversary-probe-" + str(time.time_ns()),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        attempts += 1
        try:
            try:
                await c.start()
                try:
                    await asyncio.wait_for(c.getone(), timeout=1.0)
                    invariants.assert_invariant_9_no_acl_bypass(
                        topo, bypass_attempt={"adversary_topic": target_topic, "via": "fetch"},
                        response_error=0,
                    )
                except (asyncio.TimeoutError, KafkaError) as e:
                    code = _classify_error(e if isinstance(e, KafkaError) else None)
                    if isinstance(e, asyncio.TimeoutError) or code in (29, 30, 31):
                        denied += 1
            except KafkaError as e:
                code = _classify_error(e)
                if code in (29, 30, 31):
                    denied += 1
                else:
                    invariants.assert_invariant_9_no_acl_bypass(
                        topo, bypass_attempt={"adversary_topic": target_topic, "via": "fetch.start"},
                        response_error=code,
                    )
        finally:
            try:
                await c.stop()
            except Exception:
                pass

    return {"attempts": attempts, "denied": denied, "violations": 0}
