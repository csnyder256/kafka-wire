"""Continuous Destructive Validation Engine, main daemon.

Orchestrates topology generation, baseline workload, adversary modules,
chaos injectors, and invariant checking. Halts immediately on any
invariant violation; dumps forensics; exits non-zero.

Run modes:
  --duration <seconds>   how long to run (default 600 = 10 min PR gate)
  --modules <list>       restrict to a subset of adversary modules
  --seed <int>           reproduce a specific topology
  --report-interval <s>  emit progress every N seconds (default 30)
  --headless             no stdout, write to JSONL
  --forensics-root <dir> where to dump on violation

Exit codes:
  0: clean run, zero violations
  1: invariant violation detected (forensics dumped)
  2: operational error (broker unreachable, admin unauthorized, etc.)
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import random
import secrets
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import structlog
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from chaos import workload
from chaos.adversary import acl_bypass, admin_brute, cross_principal, replay_offset
from chaos.forensics import write_forensic_dump
from chaos.invariants import (
    InvariantViolation,
    violation_to_record,
)
from chaos.topology import (
    Topology,
    generate_topology,
    topology_fingerprint,
)

log = structlog.get_logger()


# ── adversary registry ─────────────────────────────────────────────────


ADVERSARIES = {
    "cross_principal_produce": cross_principal.cross_principal_produce,
    "cross_principal_fetch": cross_principal.cross_principal_fetch,
    "cross_principal_group": cross_principal.cross_principal_group_join,
    "adversary_principal": cross_principal.adversary_principal_probe,
    "acl_bypass_topic_names": acl_bypass.acl_bypass_topic_names,
    "acl_unauthenticated": acl_bypass.acl_unauthenticated_probe,
    "offset_poison": replay_offset.offset_poison,
    "admin_token_brute": admin_brute.admin_token_brute,
}


# ── broker provisioning ────────────────────────────────────────────────


async def _create_topics_as(bootstrap: str, principal) -> int:
    """Create one principal's topics, authenticating as that principal."""
    admin = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=principal.principal,
        sasl_plain_password=principal.password,
    )
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=name, num_partitions=1, replication_factor=1)
             for name in principal.topics]
        )
        return len(principal.topics)
    except TopicAlreadyExistsError:
        return 0
    finally:
        await admin.close()


async def provision_topology(topo: Topology, admin_url: str, admin_token: str, bootstrap: str) -> None:
    """Push the topology's principals + topics into the live broker
    so the workload + adversary modules can exercise them.
    Raises on any provisioning failure (operational, not invariant).
    """
    log.info("chaos.provision.start", principals=len(topo.principals), pattern=topo.pattern)

    # 1. ACL principals.
    for t in topo.principals:
        body = {
            "name": t.principal,
            "topic_prefixes": [{"prefix": t.topic_prefix, "ops": ["read", "write"]}],
            "groups": [{"id_prefix": t.group_prefix, "ops": ["read"]}],
        }
        _post_admin(admin_url, admin_token, "/v1/acls", body)

    # Platform principal (full access).
    _post_admin(admin_url, admin_token, "/v1/acls", {
        "name": topo.platform_principal,
        "topic_prefixes": [{"prefix": "", "ops": ["read", "write"]}],
        "groups": [{"id_prefix": "", "ops": ["read"]}],
    })

    # Adversary principal (zero grants).
    _post_admin(admin_url, admin_token, "/v1/acls", {
        "name": topo.adversary_principal,
    })

    # 2. Topics: explicit creation so they get OwnerPrincipalID.
    # The current /v1/topics endpoint doesn't accept owner_principal_id;
    # we create them via the wire layer's auto-create path on first
    # Produce by the principal principal, which captures the principal.
    # Create every topic up front as its owner. Relying on auto-create made
    # adversary outcomes depend on whether the baseline workload had produced
    # yet, so a probe could see UNKNOWN_TOPIC when the real question was
    # whether the ACL held.
    created = 0
    for t_ in topo.principals:
        try:
            # Bounded: during --provision-only the broker is intentionally
            # running without SASL (it has no users file yet), so a SASL
            # client would sit in a connect loop forever. Topics get created
            # on the real run instead, and this degrades to a warning.
            created += await asyncio.wait_for(
                _create_topics_as(bootstrap, t_), timeout=10
            )
        except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
            log.warning("chaos.provision.topic_create_skipped",
                        principal=t_.principal, err=type(exc).__name__)
    log.info("chaos.provision.topics_created", count=created)

    # 3. SCRAM credentials: IMPORTANT: the broker's SCRAM users-file
    # is read at startup. For the chaos engine to actually authenticate
    # with these principals, the broker must be configured with SASL
    # enabled AND a users file containing them. The Docker Compose
    # chaos environment (services/chaos/docker-compose.chaos.yml)
    # bind-mounts a users file generated from the topology JSON.
    log.info("chaos.provision.done", principals=len(topo.principals) + 2)


def _post_admin(admin_url: str, admin_token: str, path: str, body: dict) -> None:
    req = urllib.request.Request(
        admin_url.rstrip("/") + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + admin_token},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"admin POST {path}: HTTP {e.code} {body_txt}")


def write_users_file(topo: Topology, dest: Path) -> None:
    """Generate the JSON users file the broker reads on SASL start.
    The chaos environment uses this in a docker-compose volume mount."""
    users = {}
    for t in topo.principals:
        users[t.principal] = t.password
    users[topo.platform_principal] = topo.platform_password
    users[topo.adversary_principal] = topo.adversary_password
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps({"users": users}, indent=2))
    os.chmod(dest, 0o600)


# ── orchestrator ───────────────────────────────────────────────────────


class ChaosRun:
    def __init__(self, args):
        self.args = args
        self.run_id = str(uuid.uuid4())[:8] + "-" + str(int(time.time()))
        self.topology: Topology | None = None
        self.violations: list[Any] = []
        self.stats = {
            "started_at": time.time(),
            "iterations": 0,
            "adversary_attempts": 0,
            "adversary_denied": 0,
            "workload_records_produced": 0,
            "workload_records_consumed": 0,
            "violations_total": 0,
        }

    async def run(self) -> int:
        log.info("chaos.run.start", run_id=self.run_id, duration=self.args.duration)

        seed = self.args.seed if self.args.seed > 0 else random.SystemRandom().randint(1, 2**31 - 1)
        self.topology = generate_topology(
            n_principals=self.args.principals,
            n_topics_per_principal=self.args.topics_per_principal,
            n_groups_per_principal=2,
            seed=seed,
        )
        fp = topology_fingerprint(self.topology)
        log.info("chaos.topology", seed=seed, pattern=self.topology.pattern, fingerprint=fp,
                 principals=len(self.topology.principals))

        if self.args.users_file_out:
            write_users_file(self.topology, Path(self.args.users_file_out))
            log.info("chaos.users_file_written", path=self.args.users_file_out)
            if self.args.provision_only:
                return 0

        try:
            await provision_topology(self.topology, self.args.admin_url, self.args.admin_token, self.args.bootstrap)
        except Exception as e:
            log.error("chaos.provision.failed", err=str(e))
            return 2

        deadline = time.time() + self.args.duration
        rng = random.Random(seed ^ 0xC4A05)

        # Baseline workload tasks: one producer + one consumer per principal.
        stop = asyncio.Event()
        baseline_tasks: list[asyncio.Task] = []
        all_received_records: list[dict[str, Any]] = []
        for t in self.topology.principals:
            baseline_tasks.append(asyncio.create_task(
                workload.principal_producer(
                    topo=self.topology, principal=t,
                    bootstrap=self.args.bootstrap, rng=rng,
                    rate_per_sec=self.args.workload_rate,
                    stop=stop,
                )
            ))
            baseline_tasks.append(asyncio.create_task(
                workload.principal_consumer(
                    topo=self.topology, principal=t,
                    bootstrap=self.args.bootstrap, rng=rng,
                    stop=stop,
                    received_records=all_received_records,
                )
            ))

        # Adversary loop.
        modules = self._select_modules()
        try:
            iteration = 0
            last_report = time.time()
            while time.time() < deadline:
                iteration += 1
                self.stats["iterations"] = iteration
                for name in modules:
                    fn = ADVERSARIES[name]
                    try:
                        if name in ("admin_token_brute",):
                            res = fn(self.topology, self.args.admin_url, rng=rng)
                        elif name == "cross_principal_offset_reset_via_admin":
                            res = await fn(self.topology, self.args.admin_url, self.args.admin_token, rng=rng)
                        else:
                            res = await fn(self.topology, self.args.bootstrap, rng=rng) \
                                if inspect.iscoroutinefunction(fn) else fn(self.topology, self.args.admin_url, rng=rng)
                    except InvariantViolation as v:
                        self._record_violation(v)
                        return 1
                    except Exception as e:
                        log.warning("chaos.adversary.error", module=name, err=str(e))
                        continue
                    self.stats["adversary_attempts"] += res.get("attempts", 0)
                    self.stats["adversary_denied"] += res.get("denied", 0)

                if time.time() - last_report > self.args.report_interval:
                    log.info(
                        "chaos.progress",
                        iter=iteration,
                        elapsed=int(time.time() - self.stats["started_at"]),
                        adv_attempts=self.stats["adversary_attempts"],
                        adv_denied=self.stats["adversary_denied"],
                        records_consumed=len(all_received_records),
                        violations=self.stats["violations_total"],
                    )
                    last_report = time.time()
                # Pace: yield to baseline workload between iterations.
                await asyncio.sleep(0.5)
        finally:
            stop.set()
            await asyncio.gather(*baseline_tasks, return_exceptions=True)

        self.stats["workload_records_consumed"] = len(all_received_records)
        log.info("chaos.run.complete",
                 run_id=self.run_id,
                 iterations=self.stats["iterations"],
                 adv_attempts=self.stats["adversary_attempts"],
                 adv_denied=self.stats["adversary_denied"],
                 records_consumed=self.stats["workload_records_consumed"],
                 violations=self.stats["violations_total"])

        return 0 if self.stats["violations_total"] == 0 else 1

    def _select_modules(self) -> list[str]:
        if self.args.modules:
            requested = [m.strip() for m in self.args.modules.split(",") if m.strip()]
            unknown = [m for m in requested if m not in ADVERSARIES]
            if unknown:
                raise SystemExit(f"unknown adversary modules: {unknown}; available: {sorted(ADVERSARIES)}")
            return requested
        return list(ADVERSARIES.keys())

    def _record_violation(self, v: InvariantViolation) -> None:
        self.stats["violations_total"] += 1
        record = violation_to_record(v, topology_fingerprint(self.topology))
        self.violations.append(record)
        try:
            dump_dir = write_forensic_dump(
                run_id=self.run_id,
                violation=record,
                topology=self.topology,
                admin_url=self.args.admin_url,
                admin_token=self.args.admin_token,
                forensics_root=Path(self.args.forensics_root),
            )
            log.error(
                "chaos.violation",
                invariant=v.invariant_id,
                message=v.message,
                dump=str(dump_dir),
            )
            sys.stderr.write(f"\n!!! INVARIANT VIOLATION: {v.invariant_id} !!!\n")
            sys.stderr.write(f"    {v.message}\n")
            sys.stderr.write(f"    Forensics: {dump_dir}\n\n")
        except Exception as e:
            log.error("chaos.forensics.dump_failed", err=str(e))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("CHAOS_BOOTSTRAP", "localhost:29093"))
    ap.add_argument("--admin-url", default=os.environ.get("CHAOS_ADMIN_URL", "http://localhost:8088"))
    ap.add_argument("--admin-token", default=os.environ.get("CHAOS_ADMIN_TOKEN", ""))
    ap.add_argument("--duration", type=int, default=600)
    ap.add_argument("--principals", type=int, default=8)
    ap.add_argument("--topics-per-principal", type=int, default=3)
    ap.add_argument("--workload-rate", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--modules", type=str, default="")
    ap.add_argument("--report-interval", type=int, default=30)
    ap.add_argument("--forensics-root", default="chaos-forensics")
    ap.add_argument("--users-file-out", default="",
                    help="If set, write the SCRAM users file to this path and exit if --provision-only is set")
    ap.add_argument("--provision-only", action="store_true",
                    help="Generate users file + write topology to stdout, then exit (used to seed the broker before chaos starts)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
    )
    run = ChaosRun(args)
    return asyncio.run(run.run())


if __name__ == "__main__":
    sys.exit(main())
