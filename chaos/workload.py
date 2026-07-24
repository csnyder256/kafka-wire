"""Legitimate workload generator.

Each principal gets a worker that produces + consumes its own topics
under its own SCRAM principal. The differential validator predicts
each result; the invariant engine checks every response.

This file is the BASELINE: pure, well-behaved multi-principal traffic.
The adversary modules (chaos.adversary.*) layer on top with
illegal access patterns.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from typing import Any

import structlog

from chaos import differential, invariants
from chaos.topology import Principal, Topology

log = structlog.get_logger()


def make_payload(principal: Principal, seq: int, *, contaminate_with: str | None = None) -> bytes:
    """Build a record payload tagged with the principal. The tag lets
    Invariant 2 (cross-principal replay) detect leakage post-fetch.

    `contaminate_with` is an adversary hook, when set, the payload
    is tagged with a DIFFERENT principal's id to simulate metadata
    tampering. The broker's response should still surface ONLY the
    requesting principal's records, ignoring the tag.
    """
    return json.dumps({
        "_chaos_principal": contaminate_with or principal.principal_id,
        "_chaos_seq": seq,
        "_chaos_emitted_at": time.time(),
        "_chaos_uuid": uuid.uuid4().hex,
        "noise": "x" * random.randint(0, 256),
    }, separators=(",", ":")).encode("utf-8")


async def principal_producer(
    *,
    topo: Topology,
    principal: Principal,
    bootstrap: str,
    rng: random.Random,
    rate_per_sec: float,
    stop: asyncio.Event,
) -> dict[str, Any]:
    """Drive Produce calls as `principal`. Returns stats."""
    from aiokafka import AIOKafkaProducer

    p = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=principal.principal,
        sasl_plain_password=principal.password,
        client_id=f"chaos-prod-{principal.principal}",
        request_timeout_ms=15_000,
    )
    await p.start()
    sent = 0
    last_seq = 0
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    try:
        while not stop.is_set():
            topic = rng.choice(principal.topics)
            seq = last_seq
            last_seq += 1
            payload = make_payload(principal, seq)
            try:
                md = await p.send_and_wait(topic, payload, key=str(seq % 4).encode())
                principal.produced_offsets[(topic, md.partition)] = max(
                    principal.produced_offsets.get((topic, md.partition), -1),
                    md.offset,
                )
                sent += 1
            except Exception as e:
                log.warning("chaos.producer.error", principal=principal.principal_id, err=str(e))
            if interval > 0:
                await asyncio.sleep(interval)
    finally:
        await p.stop()
    return {"principal": principal.principal_id, "sent": sent}


async def principal_consumer(
    *,
    topo: Topology,
    principal: Principal,
    bootstrap: str,
    rng: random.Random,
    stop: asyncio.Event,
    received_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Drive Fetch calls as `principal`. Verifies INV-02 + INV-CHAIN on
    every record received."""
    from aiokafka import AIOKafkaConsumer

    group = rng.choice(principal.groups)
    c = AIOKafkaConsumer(
        *principal.topics,
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=principal.principal,
        sasl_plain_password=principal.password,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        request_timeout_ms=15_000,
        client_id=f"chaos-cons-{principal.principal}",
    )
    await c.start()
    consumed = 0
    try:
        while not stop.is_set():
            try:
                msg = await asyncio.wait_for(c.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                payload = json.loads(msg.value)
            except json.JSONDecodeError:
                continue
            received_records.append({
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "principal": payload.get("_chaos_principal"),
                "seq": payload.get("_chaos_seq"),
                "principal": principal.principal,
            })

            # INV-02: every consumed record must belong to this principal.
            invariants.assert_invariant_2_replay_no_cross_principal(
                topo,
                principal.principal,
                msg.topic,
                [payload],
            )

            # INV-CHAIN: ownership chain consistency.
            invariants.assert_invariant_ownership_chain(
                topo,
                principal.principal_id,
                msg.topic,
                msg.partition,
                None,  # no archive entry on hot-path fetch
            )

            consumed += 1
    finally:
        await c.stop()
    return {"principal": principal.principal_id, "consumed": consumed}
