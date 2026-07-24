"""Differential truth resolver.

Independently computes what a request SHOULD return, without consulting
the broker. The chaos engine compares this against what the broker
actually returned. Mismatch = catastrophic.

The resolver does NOT make any network calls. It works purely from
the in-memory topology + the produce-log we maintain client-side.
This is what makes it adversarial: if the broker's authorize/lookup
disagree with our pure-function computation, we know one is wrong.

Two consumers of this module:
  - The workload generator: predicts the result of every legitimate
    request before issuing it; asserts on receipt.
  - The adversary modules: predicts that every illegal request must
    fail; asserts the broker actually denies it.
"""
from __future__ import annotations

from typing import Any

from chaos.topology import Topology


# Op = "produce" | "fetch" | "join_group" | "offset_commit"


def expect_authorized(topo: Topology, principal: str, topic_or_group: str, op: str) -> bool:
    """Return True iff this principal SHOULD be allowed to perform
    `op` on `topic_or_group`. Mirrors the broker's ACL logic exactly
    so any divergence shows up as an invariant violation."""
    if principal == topo.platform_principal:
        # Platform principal has cross-cutting access on the
        # adversarial test topology too.
        return True
    if principal == topo.adversary_principal:
        return False
    principal = topo.principal_by_principal.get(principal)
    if principal is None:
        return False
    if op in ("produce", "fetch"):
        return _matches_topic_prefix(principal, topic_or_group)
    if op in ("join_group", "offset_commit"):
        return _matches_group_prefix(principal, topic_or_group)
    return False


def _matches_topic_prefix(principal, topic: str) -> bool:
    return topic.startswith(principal.topic_prefix)


def _matches_group_prefix(principal, group: str) -> bool:
    return group.startswith(principal.group_prefix)


def expect_topic_owner(topo: Topology, topic: str) -> str:
    """Return the principal_id that should own this topic."""
    return topo.topic_to_principal.get(topic, "")


def expect_record_set(topo: Topology, principal: str, topic: str, fetch_offset: int, hwm: int) -> set[int]:
    """Return the set of record sequence numbers a successful fetch
    should produce. The chaos engine tags every produced record with
    its sequence; we rebuild the expected sequence set from the
    workload's per-principal produce log.

    For a principal principal: every sequence between fetch_offset and
    hwm-1 inclusive. For an adversary: empty set (any record means a
    leak).
    """
    if principal == topo.adversary_principal:
        return set()
    if not topo.topic_to_principal.get(topic):
        return set()
    if topo.topic_to_principal[topic] != topo.principal_by_principal.get(principal, _NoPrincipal).principal_id_or_empty():
        return set()
    if fetch_offset >= hwm:
        return set()
    return set(range(fetch_offset, hwm))


class _NoPrincipal:
    @staticmethod
    def principal_id_or_empty() -> str:
        return ""


# Inject `principal_id_or_empty` onto the Principal dataclass so the
# differential resolver can ask "what's this principal's principal" with
# a single call regardless of platform/principal/adversary identity.
def _patch_principal():
    from chaos.topology import Principal

    def principal_id_or_empty(self) -> str:
        return self.principal_id

    Principal.principal_id_or_empty = principal_id_or_empty  # type: ignore[attr-defined]


_patch_principal()
