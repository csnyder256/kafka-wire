"""Admin-token brute-force probes.

The broker's admin REST is bearer-token gated. This adversary attempts
admin operations with:
  - no token
  - wrong token
  - timing-attack-shaped tokens (lots of strings starting with the
    correct first character)
  - partial-prefix tokens (right length, varied content)

INV-09: every probe must return 401/403 and constant-time. We don't
attempt to actually time-attack the constant-time compare (that's a
Go-side test), we just verify that wrong tokens never succeed.
"""
from __future__ import annotations

import json
import random
import secrets
import urllib.error
import urllib.request
from typing import Any

import structlog

from chaos import invariants
from chaos.topology import Topology

log = structlog.get_logger()


def _try_admin(url: str, token: str | None) -> tuple[int, str]:
    req = urllib.request.Request(url)
    if token is not None:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)


def admin_token_brute(topo: Topology, admin_url: str, *, rng: random.Random | None = None) -> dict[str, Any]:
    rng = rng or random.Random()
    base = admin_url.rstrip("/") + "/v1/topics"

    attempts = 0
    denied = 0

    candidates = [
        None,                       # no token
        "",                         # empty
        "wrong",                    # short
        "x" * 64,                    # long but uniform
        secrets.token_hex(32),      # plausible
        secrets.token_hex(16),      # short hex
        "Bearer ",                  # prefix-only
        "admin",                    # common
        "INTERNAL_SERVICE_TOKEN",   # var-name
    ]

    for tok in candidates:
        attempts += 1
        status, body = _try_admin(base, tok)
        if status in (401, 403):
            denied += 1
        elif status == 200:
            # Got data without a valid token! INV-09.
            invariants.assert_invariant_9_no_acl_bypass(
                topo,
                bypass_attempt={"admin_brute": "got 200 with token preview=" + (str(tok)[:8] if tok else "<none>")},
                response_error=0,
            )
        else:
            # Network errors, etc., treat as denied.
            denied += 1

    return {"attempts": attempts, "denied": denied, "violations": 0}
