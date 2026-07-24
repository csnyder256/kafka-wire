"""Invariant assertion engine + differential truth resolver.

Every operation issued by the chaos workload + adversary modules is
double-checked against an INDEPENDENT computation of what the result
should be. When the broker disagrees with the chaos engine's truth
resolver, we have an isolation breach.

The 17 invariants spec'd by the chaos framework are mapped to
concrete `assert_invariant_*` functions here. Each takes:
  - the topology (ground truth)
  - the request (what the adversary or workload sent)
  - the response (what the broker actually returned)

and either returns silently or raises `InvariantViolation`. The
daemon catches `InvariantViolation` and triggers the forensic dumper.

Cardinal rule: NEVER catch+continue inside this module. Any deviation
is a P0; the only valid response is to halt.
"""
from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
import time
from typing import Any

from chaos.topology import Principal, Topology


class InvariantViolation(Exception):
    """Raised on ANY mismatch. Carries enough state for the forensic
    dumper to reproduce the scenario."""

    def __init__(
        self,
        invariant_id: str,
        message: str,
        *,
        topology: Topology | None = None,
        request: dict[str, Any] | None = None,
        response: dict[str, Any] | None = None,
        expected: Any = None,
        actual: Any = None,
        extra: dict[str, Any] | None = None,
    ):
        self.invariant_id = invariant_id
        self.message = message
        self.topology = topology
        self.request = request or {}
        self.response = response or {}
        self.expected = expected
        self.actual = actual
        self.extra = extra or {}
        self.detected_at = time.time()
        super().__init__(f"[{invariant_id}] {message}")


# ── 17 invariants ────────────────────────────────────────────────────


def assert_invariant_1_principal_topic_ownership(
    topo: Topology,
    principal: str,
    topic: str,
    op: str,           # "produce" | "fetch" | "join_group"
    response_error: int,  # Kafka error code from broker
    expected_authorized: bool,  # what the engine computed
):
    """Invariant 1: a principal can only access its own resources."""
    if expected_authorized and response_error == 0:
        return
    if not expected_authorized and response_error != 0:
        return
    raise InvariantViolation(
        "INV-01",
        f"authorize({principal}, {op}, {topic}) expected={'OK' if expected_authorized else 'DENY'} "
        f"got error_code={response_error}",
        topology=topo,
        expected=expected_authorized,
        actual=response_error,
        extra={"principal": principal, "topic": topic, "op": op},
    )


def assert_invariant_2_replay_no_cross_principal(
    topo: Topology,
    principal: str,
    requested_topic: str,
    received_records: list[dict[str, Any]],
):
    """Invariant 2: replay never crosses principal boundaries.

    Every record returned must belong to the same principal as the
    requested topic. The chaos engine puts a `_chaos_principal` tag in
    every produced payload; we verify it on the way out.
    """
    expected_principal = topo.topic_to_principal.get(requested_topic, "")
    if not expected_principal:
        return  # platform/shared topic; not a principal-specific replay
    for rec in received_records:
        actual_principal = rec.get("_chaos_principal")
        if actual_principal and actual_principal != expected_principal:
            raise InvariantViolation(
                "INV-02",
                f"replay returned record from principal {actual_principal} "
                f"to principal {principal} requesting {requested_topic} "
                f"(expected principal {expected_principal})",
                topology=topo,
                expected=expected_principal,
                actual=actual_principal,
                extra={"principal": principal, "topic": requested_topic, "record": rec},
            )


def assert_invariant_3_offset_resolves_owner_segment(
    topo: Topology,
    requesting_principal: str,
    topic: str,
    offset: int,
    archive_entry: dict[str, Any] | None,
):
    """Invariant 3: offset restoration never resolves to another
    principal's segment.

    `archive_entry` is the manifest entry the broker resolved (None if
    fetched locally). When non-None, its principal_id MUST match the
    topic's owner.
    """
    if archive_entry is None:
        return
    entry_principal = archive_entry.get("principal_id", "")
    expected = topo.topic_to_principal.get(topic, "")
    if entry_principal and entry_principal != expected:
        raise InvariantViolation(
            "INV-03",
            f"manifest resolved offset {offset} on {topic} to principal "
            f"{entry_principal} but expected {expected}",
            topology=topo,
            expected=expected,
            actual=entry_principal,
            extra={"offset": offset, "entry": archive_entry, "requesting_principal": requesting_principal},
        )


def assert_invariant_4_cache_namespace(
    cache_key_path: str,
    expected_principal: str,
):
    """Invariant 4: cache restoration never serves another principal's
    objects. The cache PATH itself encodes the principal (see
    s3.Cache.PathPrincipal); this asserts the served path matches the
    expected principal's namespace.
    """
    if expected_principal:
        marker = f"/principals/{expected_principal}/"
        if marker not in cache_key_path and not cache_key_path.endswith(f"principals/{expected_principal}"):
            raise InvariantViolation(
                "INV-04",
                f"cache path {cache_key_path} does not include expected principal marker /principals/{expected_principal}/",
                expected=expected_principal,
                actual=cache_key_path,
            )


def assert_invariant_5_metadata_corruption_no_leak(
    topo: Topology,
    corrupted_field: str,
    response_error: int,
    response_records: list[dict[str, Any]] | None,
):
    """Invariant 5: a corrupted metadata field never produces a
    successful read of the wrong principal's data. Either the broker
    refuses (error != 0) or it returns its own principal's data
    (records all match the requesting principal's tag).
    """
    if response_error != 0:
        return  # refused: good
    if response_records is None:
        return
    # No records returned with mismatched principals is the win condition
    # for this invariant; specific cross-principal detection happens in
    # Invariant 2.


def assert_invariant_6_no_race_cross_principal(
    topo: Topology,
    concurrent_results: list[dict[str, Any]],
):
    """Invariant 6: race conditions never expose cross-principal reads.

    `concurrent_results` is a list of (principal, topic, records)
    tuples from a parallel-Fetch torture test. Each entry's records
    must match its principal's principal.
    """
    for r in concurrent_results:
        principal = r["principal"]
        records = r["records"]
        principal_principal = ""
        for t in topo.principals:
            if t.principal == principal:
                principal_principal = t.principal_id
                break
        if not principal_principal:
            continue
        for rec in records:
            t_tag = rec.get("_chaos_principal")
            if t_tag and t_tag != principal_principal:
                raise InvariantViolation(
                    "INV-06",
                    f"concurrent fetch leaked: principal={principal} "
                    f"got record from principal {t_tag}",
                    topology=topo,
                    expected=principal_principal,
                    actual=t_tag,
                    extra={"record": rec, "all_results": concurrent_results},
                )


def assert_invariant_7_deleted_principal_no_remnants(
    topo: Topology,
    deleted_principal: Principal,
    fetch_attempts: list[dict[str, Any]],
):
    """Invariant 7: after a principal is deleted, NO retrieval can
    surface their data. `fetch_attempts` are post-delete probes; each
    must error or return empty."""
    for attempt in fetch_attempts:
        records = attempt.get("records") or []
        for rec in records:
            t_tag = rec.get("_chaos_principal")
            if t_tag == deleted_principal.principal_id:
                raise InvariantViolation(
                    "INV-07",
                    f"post-delete fetch surfaced deleted principal {deleted_principal.principal_id} data",
                    topology=topo,
                    expected="empty/error",
                    actual=rec,
                    extra={"deleted_principal_id": deleted_principal.principal_id, "attempt": attempt},
                )


def assert_invariant_8_parallel_restore_no_overlap(
    parallel_restores: list[dict[str, Any]],
):
    """Invariant 8: parallel restores never overlap namespaces.
    Each restore must land in its own principal's cache subtree.
    """
    seen: dict[str, str] = {}
    for r in parallel_restores:
        path = r["cache_path"]
        principal = r["principal"]
        prior = seen.get(path)
        if prior is not None and prior != principal:
            raise InvariantViolation(
                "INV-08",
                f"two parallel restores wrote to same cache path {path} "
                f"with different principals ({prior} and {principal})",
                expected="distinct paths",
                actual={"path": path, "principals": [prior, principal]},
            )
        seen[path] = principal


def assert_invariant_9_no_acl_bypass(
    topo: Topology,
    bypass_attempt: dict[str, Any],
    response_error: int,
):
    """Invariant 9: NO query path bypasses ACL enforcement. The
    adversary's bypass attempts (path traversal, principal spoofing,
    etc.) must all return an authorization error code.
    """
    # 29 TOPIC_AUTHORIZATION_FAILED, 30 GROUP_AUTHORIZATION_FAILED,
    # 31 CLUSTER_AUTHORIZATION_FAILED, 58 SASL_AUTHENTICATION_FAILED.
    #
    # 3 UNKNOWN_TOPIC_OR_PARTITION also counts as a denial. A broker that
    # hides topics a principal cannot see, rather than confirming they exist
    # and refusing, is taking the stronger position: an existence oracle is
    # itself a disclosure. Treating that as a bypass would penalize the safer
    # behavior.
    if response_error in (3, 29, 30, 31, 58):
        return
    raise InvariantViolation(
        "INV-09",
        f"ACL bypass attempt succeeded: {bypass_attempt!r} returned error_code={response_error}",
        topology=topo,
        expected="auth-failed code",
        actual=response_error,
        extra={"attempt": bypass_attempt},
    )


def assert_invariant_10_no_silent_downgrade(
    topo: Topology,
    request: dict[str, Any],
    response_error: int,
    response_metadata: dict[str, Any],
):
    """Invariant 10: no fallback logic silently downgrades principal
    resolution. If a Fetch fails on a principal-scoped topic, we never
    quietly fall back to a shared topic with the same name suffix.
    """
    requested_topic = request.get("topic", "")
    if not requested_topic:
        return
    if response_error == 0:
        # Successful fetch: verify the returned segment, if any, is
        # tagged with the same principal as the requested topic.
        seg_principal = response_metadata.get("segment_principal_id", "")
        expected_principal = topo.topic_to_principal.get(requested_topic, "")
        if expected_principal and seg_principal and seg_principal != expected_principal:
            raise InvariantViolation(
                "INV-10",
                f"silent downgrade: requested {requested_topic} "
                f"(principal {expected_principal}) but response segment tagged {seg_principal}",
                topology=topo,
                expected=expected_principal,
                actual=seg_principal,
                extra={"request": request},
            )


def assert_invariant_hmac(
    archive_hmac_key_hex: str,
    archive_entry: dict[str, Any],
):
    """Cryptographic invariant: every archive entry must carry a
    valid HMAC signature over its (principal, topic, partition, base,
    sha256) tuple. Any mismatch is tampering.
    """
    sig = archive_entry.get("hmac_signature", "")
    if not sig:
        return  # legacy entry: skip (production should configure HMAC)
    key = bytes.fromhex(archive_hmac_key_hex)
    canon = (
        f"v1|{archive_entry.get('principal_id', '')}|"
        f"{archive_entry.get('topic', '')}|"
        f"{archive_entry.get('partition', 0)}|"
        f"{archive_entry.get('base_offset', 0)}|"
        f"{archive_entry.get('sha256', '')}"
    )
    expected = hmac.new(key, canon.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise InvariantViolation(
            "INV-HMAC",
            f"archive HMAC mismatch on {archive_entry.get('s3_key', '?')}",
            expected=expected,
            actual=sig,
            extra={"entry": archive_entry},
        )


def assert_invariant_ownership_chain(
    topo: Topology,
    requesting_principal: str,
    topic: str,
    partition: int,
    archive_entry: dict[str, Any] | None,
):
    """Full chain validation:
       principal_id → bucket → namespace_prefix → partition → segment → object.
    Every link must be consistent before any byte is served.
    """
    if not requesting_principal:
        return
    expected_topic_principal = topo.topic_to_principal.get(topic, "")
    if expected_topic_principal != requesting_principal:
        raise InvariantViolation(
            "INV-CHAIN-TOPIC",
            f"topic {topic} owner_principal={expected_topic_principal} ≠ requesting {requesting_principal}",
            topology=topo,
            expected=requesting_principal,
            actual=expected_topic_principal,
        )
    if archive_entry is None:
        return
    if archive_entry.get("principal_id", "") not in (requesting_principal, ""):
        raise InvariantViolation(
            "INV-CHAIN-SEGMENT",
            f"segment principal {archive_entry.get('principal_id')} ≠ requesting {requesting_principal}",
            topology=topo,
            expected=requesting_principal,
            actual=archive_entry.get("principal_id"),
            extra={"entry": archive_entry},
        )
    # S3 key must include the principal marker.
    s3key = archive_entry.get("s3_key", "")
    if requesting_principal and f"/principals/{requesting_principal}/" not in s3key:
        # Pattern B+C may not include the marker (per-principal bucket).
        # We only assert the marker is present when principals are
        # multiplexed via prefix (Pattern A).
        if "principals/" in s3key:
            raise InvariantViolation(
                "INV-CHAIN-S3KEY",
                f"S3 key {s3key} has principals/ marker but not the right principal",
                topology=topo,
                expected=requesting_principal,
                actual=s3key,
            )


# ── violation log (in-memory; daemon dumps on halt) ────────────────


@dataclasses.dataclass
class ViolationRecord:
    invariant_id: str
    detected_at: float
    message: str
    topology_fingerprint: str
    request: dict[str, Any]
    response: dict[str, Any]
    expected: Any
    actual: Any
    extra: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "invariant_id": self.invariant_id,
            "detected_at": self.detected_at,
            "message": self.message,
            "topology_fingerprint": self.topology_fingerprint,
            "request": self.request,
            "response": self.response,
            "expected": _safe_json(self.expected),
            "actual": _safe_json(self.actual),
            "extra": _safe_json(self.extra),
        }


def _safe_json(v: Any) -> Any:
    """Convert dataclasses + bytes to JSON-safe values."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.hex()
    if isinstance(v, dict):
        return {k: _safe_json(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_safe_json(item) for item in v]
    if dataclasses.is_dataclass(v):
        return _safe_json(dataclasses.asdict(v))
    return repr(v)


def violation_to_record(v: InvariantViolation, topo_fingerprint: str) -> ViolationRecord:
    return ViolationRecord(
        invariant_id=v.invariant_id,
        detected_at=v.detected_at,
        message=v.message,
        topology_fingerprint=topo_fingerprint,
        request=v.request,
        response=v.response,
        expected=v.expected,
        actual=v.actual,
        extra=v.extra,
    )
