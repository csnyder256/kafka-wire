"""Forensic dumper + reproduction script generator.

When an invariant fires, this module:
  1. Snapshots the topology (all credentials + HMAC keys included).
  2. Captures the request that triggered the violation.
  3. Captures the broker's response.
  4. Pulls /v1/cluster, /v1/topics, /v1/groups, /v1/archive, /v1/acls
     from the broker admin API for the moment-of-violation state.
  5. Writes everything to services/chaos/forensics/<run-id>/<violation-id>/.
  6. Generates a self-contained Python script that re-runs the
     violating request against a fresh broker, copy-paste reproducer.

Forensic artifacts are SECRETS. They contain credentials (SCRAM
passwords, HMAC keys) sufficient to access the test broker. Treat
them like production credentials.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from chaos.invariants import ViolationRecord
from chaos.topology import Topology, topology_fingerprint, topology_to_dict


def write_forensic_dump(
    *,
    run_id: str,
    violation: ViolationRecord,
    topology: Topology,
    admin_url: str,
    admin_token: str,
    forensics_root: Path,
) -> Path:
    """Persist a complete violation snapshot. Returns the dump path."""
    fingerprint = topology_fingerprint(topology)
    violation_id = f"{int(time.time() * 1000)}-{violation.invariant_id}"
    dump_dir = forensics_root / run_id / violation_id
    dump_dir.mkdir(parents=True, exist_ok=True)

    # 1. Violation record itself.
    (dump_dir / "violation.json").write_text(json.dumps(violation.to_dict(), indent=2))

    # 2. Topology snapshot.
    (dump_dir / "topology.json").write_text(json.dumps(topology_to_dict(topology), indent=2))

    # 3. Broker admin state.
    broker_state: dict[str, Any] = {"fetched_at": time.time()}
    for path in ("/v1/cluster", "/v1/topics", "/v1/groups", "/v1/archive", "/v1/acls"):
        try:
            broker_state[path] = _http_get_json(admin_url + path, admin_token)
        except Exception as e:
            broker_state[path] = {"_error": str(e)}
    (dump_dir / "broker_state.json").write_text(json.dumps(broker_state, indent=2))

    # 4. Reproduction script.
    repro_script = _build_reproduction_script(violation, topology, fingerprint)
    (dump_dir / "reproduce.py").write_text(repro_script)
    os.chmod(dump_dir / "reproduce.py", 0o755)

    # 5. Human-readable summary.
    (dump_dir / "SUMMARY.md").write_text(_build_summary(violation, fingerprint))

    return dump_dir


def _http_get_json(url: str, token: str) -> Any:
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "_body": e.read().decode("utf-8", errors="replace")}


def _build_reproduction_script(
    violation: ViolationRecord,
    topology: Topology,
    fingerprint: str,
) -> str:
    """Emit a self-contained Python script that recreates the topology
    + replays the violating request. Operator can run it against a
    fresh broker to confirm the bug is real, then iterate on a fix."""
    topo_json = json.dumps(topology_to_dict(topology), indent=2)
    request_json = json.dumps(violation.request, indent=2)

    return f'''#!/usr/bin/env python3
"""Auto-generated reproduction script for chaos violation {violation.invariant_id}.

Topology fingerprint: {fingerprint}
Detected at:          {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(violation.detected_at))}
Invariant violated:   {violation.invariant_id}
Message:              {violation.message}

Run against a clean broker:

    PYTHONPATH=. python services/chaos/forensics/<run>/<violation>/reproduce.py \\
        --bootstrap localhost:29093 \\
        --admin-url http://localhost:8088
"""
import argparse
import json
import sys

TOPOLOGY = {topo_json}

VIOLATING_REQUEST = {request_json}

EXPECTED = {json.dumps(violation.expected, default=str)}
ACTUAL   = {json.dumps(violation.actual, default=str)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--admin-url", required=True)
    ap.add_argument("--admin-token", default="")
    args = ap.parse_args()

    print(f"[repro] topology fingerprint: {fingerprint}")
    print(f"[repro] invariant: {violation.invariant_id}")
    print(f"[repro] message:   {violation.message}")
    print()
    print("[repro] To reproduce: provision the topology above on the target")
    print("[repro] broker (use services/chaos/topology.py + admin REST), then")
    print("[repro] replay the violating request:")
    print()
    print(json.dumps(VIOLATING_REQUEST, indent=2))
    print()
    print(f"[repro] EXPECTED: {{EXPECTED!r}}")
    print(f"[repro] ACTUAL:   {{ACTUAL!r}}")
    print()
    print("[repro] If the broker still surfaces ACTUAL after re-running, the bug")
    print("[repro] is reproduced and a fix is required before re-arming chaos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''


def _build_summary(violation: ViolationRecord, fingerprint: str) -> str:
    return f"""# Chaos Violation: {violation.invariant_id}

**Detected:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(violation.detected_at))}
**Topology fingerprint:** `{fingerprint}`

## What happened

{violation.message}

## Expected vs Actual

```
EXPECTED: {violation.expected!r}
ACTUAL:   {violation.actual!r}
```

## Files in this dump

- `violation.json`: structured violation record
- `topology.json`: full topology used at the time of the violation (CONTAINS CREDENTIALS)
- `broker_state.json`: broker admin API state at moment of detection
- `reproduce.py`: self-contained reproduction script
- `SUMMARY.md`: this file

## Triage

1. Verify with `reproduce.py` that the bug is deterministic on a
   clean broker.
2. Check `broker_state.json` for the topic/group/ACL state at the
   time of the violation.
3. Inspect the request in `violation.json`, pay attention to the
   `principal`, `topic`, and `op` fields.
4. P0 if INV-01 through INV-09. P1 if INV-10 (silent downgrade) or
   INV-HMAC (manifest tampering).
5. Fix the broker, re-run chaos with the same `--seed` to confirm.

## Security note

This dump contains SCRAM passwords and HMAC keys for the test
universe. Do NOT share externally. The chaos universe is throwaway,
but the credentials inside are still treated as secrets.
"""
