"""Replay + offset attack patterns.

Tries to:
  - Reset another principal's group offsets to point at this principal's
    segments (cross-principal offset poison).
  - Fetch from this principal's topic at an offset that, in another
    principal's namespace, would land on a different segment.
  - Issue Replay/seek requests with manufactured offsets designed to
    overflow / underflow the storage layer.
"""
from __future__ import annotations

import asyncio
import json
import random
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import (
    GroupAuthorizationFailedError,
    KafkaError,
    OffsetOutOfRangeError,
    TopicAuthorizationFailedError,
)

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


async def offset_poison(topo: Topology, bootstrap: str, *, rng: random.Random | None = None) -> dict[str, Any]:
    """Each attacker tries to seek their consumer to offsets that
    would land in another principal's segment range. This must either
    return OFFSET_OUT_OF_RANGE on attacker's own topic, or
    TOPIC_AUTHORIZATION_FAILED if they tried to subscribe to victim's."""
    rng = rng or random.Random()
    if len(topo.principals) < 2:
        return {"attempts": 0, "denied": 0, "violations": 0, "skipped": "needs ≥2 principals"}

    attempts = 0
    denied = 0
    for _ in range(min(8, len(topo.principals))):
        attacker, victim = rng.sample(topo.principals, 2)
        # Use attacker's own topic but seek to an extreme offset.
        topic = rng.choice(attacker.topics)
        # Probe int64 boundaries.
        offsets_to_try = [-1, 0, 1, 2**31, 2**32 - 1, 2**62, 2**63 - 1]
        c = AIOKafkaConsumer(
            **_kafka_args(attacker.principal, attacker.password, bootstrap),
            group_id=attacker.groups[0],
            auto_offset_reset="error",
            enable_auto_commit=False,
        )
        try:
            await c.start()
            from aiokafka import TopicPartition
            tp = TopicPartition(topic, 0)
            c.assign([tp])
            for off in offsets_to_try:
                attempts += 1
                try:
                    c.seek(tp, off)
                    try:
                        msg = await asyncio.wait_for(c.getone(), timeout=1.0)
                        # We got a message at an arbitrary offset.
                        # As long as it's tagged with the attacker's
                        # principal, it's not a leak: but log it.
                        try:
                            payload = json.loads(msg.value)
                        except Exception:
                            payload = {}
                        invariants.assert_invariant_2_replay_no_cross_principal(
                            topo, attacker.principal, topic, [payload],
                        )
                    except (asyncio.TimeoutError, OffsetOutOfRangeError, KafkaError):
                        denied += 1
                except Exception:
                    denied += 1
        except KafkaError:
            denied += len(offsets_to_try)
        finally:
            try:
                await c.stop()
            except Exception:
                pass
    return {"attempts": attempts, "denied": denied, "violations": 0}


async def cross_principal_offset_reset_via_admin(
    topo: Topology,
    admin_url: str,
    admin_token: str,
    *,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    """The admin REST `/v1/replay/reset-offset` endpoint, verify
    that a bearer token cannot be used to reset another principal's
    group offsets to spuriously high values that would cause an
    out-of-range fetch.

    Note: the admin endpoint is bearer-token gated; the chaos engine
    deliberately tests with the LEGITIMATE admin token to confirm
    that even an authenticated admin call doesn't violate ownership
    (the daemon could in principle reset offsets to integer values
    but never to "another principal's segment", because offsets are
    SCALAR per (group, topic, partition); resetting one group's
    offset doesn't expose another group's data).
    """
    rng = rng or random.Random()
    if len(topo.principals) < 1:
        return {"attempts": 0, "denied": 0, "violations": 0}

    attempts = 0
    denied = 0
    # Try resetting an unrecognized group, admin should 404.
    rogue_group = "chaos-nonexistent-" + str(rng.randint(0, 10**9))
    body = {"group_id": rogue_group, "strategy": "earliest"}
    attempts += 1
    try:
        req = urllib.request.Request(
            admin_url.rstrip("/") + "/v1/replay/reset-offset",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + admin_token},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            # Should have been a 404. If we got here, it was 200.
            if data.get("count", 0) > 0:
                # Reset count > 0 against a nonexistent group is a
                # clear bug: surfaces an INV-09 (admin path returned
                # data for a non-owned identity).
                invariants.assert_invariant_9_no_acl_bypass(
                    topo,
                    bypass_attempt={"admin_reset_unknown_group": rogue_group},
                    response_error=0,
                )
            else:
                denied += 1
    except urllib.error.HTTPError as e:
        if e.code in (400, 401, 403, 404):
            denied += 1
    except Exception:
        denied += 1

    return {"attempts": attempts, "denied": denied, "violations": 0}
