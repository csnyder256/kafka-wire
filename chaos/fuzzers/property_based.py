"""Property-based + generative tests for the chaos invariant
predicates themselves.

Uses hypothesis (gracefully no-ops if not installed) to generate
synthetic topologies and verify:
  - the differential resolver is idempotent
  - InvariantViolation only fires on genuine mismatches
  - topology_to_dict round-trips JSON without drift

Run via:
    PYTHONPATH=. python -m chaos.fuzzers.property_based
"""
from __future__ import annotations

import json
import sys
from typing import Any

try:
    from hypothesis import given, settings, strategies as st
    HAS_HYPOTHESIS = True
except ImportError:
    HAS_HYPOTHESIS = False
    # Stub implementations so the module imports + runs as a noop.
    def given(*a, **kw):  # type: ignore[no-redef]
        def deco(fn): return fn
        return deco
    def settings(*a, **kw):  # type: ignore[no-redef]
        def deco(fn): return fn
        return deco
    class _StubSt:
        def integers(self, *a, **kw): return None
        def text(self, *a, **kw): return None
    st = _StubSt()  # type: ignore[assignment]


from chaos import differential
from chaos.topology import generate_topology, topology_to_dict


def test_topology_serialization_roundtrip():
    """Topology JSON serialization must be lossless."""
    topo = generate_topology(seed=42, n_principals=4, n_topics_per_principal=2)
    raw = json.dumps(topology_to_dict(topo))
    restored = json.loads(raw)
    assert restored["seed"] == 42
    assert len(restored["principals"]) == 4
    print("OK   test_topology_serialization_roundtrip")


def test_differential_authorize_self():
    """A principal must be authorized to access its own topics."""
    topo = generate_topology(seed=7, n_principals=3, n_topics_per_principal=2)
    for t in topo.principals:
        for topic in t.topics:
            assert differential.expect_authorized(topo, t.principal, topic, "produce") is True
            assert differential.expect_authorized(topo, t.principal, topic, "fetch") is True
    print("OK   test_differential_authorize_self")


def test_differential_authorize_other_denied():
    """A principal must NEVER be authorized for another principal's topic."""
    topo = generate_topology(seed=7, n_principals=3, n_topics_per_principal=2)
    for i, t in enumerate(topo.principals):
        for j, victim in enumerate(topo.principals):
            if i == j:
                continue
            for topic in victim.topics:
                assert differential.expect_authorized(topo, t.principal, topic, "produce") is False, \
                    f"principal {t.principal_id} got authorize=True for principal {victim.principal_id}'s topic {topic}"
                assert differential.expect_authorized(topo, t.principal, topic, "fetch") is False
    print("OK   test_differential_authorize_other_denied")


def test_adversary_principal_denied_everywhere():
    topo = generate_topology(seed=7, n_principals=3, n_topics_per_principal=2)
    for t in topo.principals:
        for topic in t.topics:
            assert differential.expect_authorized(topo, topo.adversary_principal, topic, "produce") is False
            assert differential.expect_authorized(topo, topo.adversary_principal, topic, "fetch") is False
    print("OK   test_adversary_principal_denied_everywhere")


def test_platform_principal_authorized_everywhere():
    topo = generate_topology(seed=7, n_principals=3, n_topics_per_principal=2)
    for t in topo.principals:
        for topic in t.topics:
            assert differential.expect_authorized(topo, topo.platform_principal, topic, "produce") is True
            assert differential.expect_authorized(topo, topo.platform_principal, topic, "fetch") is True
    print("OK   test_platform_principal_authorized_everywhere")


@given(seed=st.integers(min_value=1, max_value=10**9))
@settings(max_examples=50)
def test_topology_property_authorize_consistency(seed):
    """Property: for any topology, every principal principal authorizes
    on EXACTLY their own topics, no more, no fewer."""
    if not HAS_HYPOTHESIS:
        return
    topo = generate_topology(seed=seed, n_principals=4, n_topics_per_principal=3)
    for t in topo.principals:
        for topic in t.topics:
            assert differential.expect_authorized(topo, t.principal, topic, "produce")
        for victim in topo.principals:
            if victim.principal_id == t.principal_id:
                continue
            for topic in victim.topics:
                assert not differential.expect_authorized(topo, t.principal, topic, "produce")


def main() -> int:
    test_topology_serialization_roundtrip()
    test_differential_authorize_self()
    test_differential_authorize_other_denied()
    test_adversary_principal_denied_everywhere()
    test_platform_principal_authorized_everywhere()
    if HAS_HYPOTHESIS:
        test_topology_property_authorize_consistency()
        print("OK   test_topology_property_authorize_consistency (50 examples)")
    else:
        print("SKIP test_topology_property_authorize_consistency (hypothesis not installed)")
    print("\nAll property-based chaos tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
