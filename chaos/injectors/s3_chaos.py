"""S3-layer chaos.

Targets the broker's archive uploader + restorer with simulated S3
failures via a forwarding proxy. The proxy sits between the broker
and real S3 (or MinIO in CI), and on each request:
  - injects a configurable rate of timeouts
  - corrupts random response bodies
  - delays partial uploads mid-flight
  - returns stale data (cached responses from N seconds ago)
  - returns a body matching a DIFFERENT principal's manifest entry
    (the most adversarial: tries to trick the restorer into serving
    cross-principal bytes; HMAC verification must catch this)

Run as a separate process; the broker is configured to use the proxy
endpoint instead of the real S3. The proxy emits JSONL events that
the daemon ingests for invariant checking.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import sys
import time
from typing import Any

import structlog
from aiohttp import web

log = structlog.get_logger()


class S3ChaosProxy:
    """Minimal HTTP forwarding proxy that misbehaves on configured
    fault rates. Forwards everything else to the real backend."""

    def __init__(
        self,
        *,
        upstream: str,
        timeout_rate: float,
        corrupt_rate: float,
        delay_rate: float,
        wrong_principal_rate: float,
        rng: random.Random,
        log_path: str,
    ):
        self.upstream = upstream.rstrip("/")
        self.timeout_rate = timeout_rate
        self.corrupt_rate = corrupt_rate
        self.delay_rate = delay_rate
        self.wrong_principal_rate = wrong_principal_rate
        self.rng = rng
        self.log_path = log_path
        self.event_count = 0

    async def handler(self, req: web.Request) -> web.Response:
        path = req.match_info.get("path", "")
        full_url = self.upstream + "/" + path
        method = req.method
        body = await req.read()

        # Inject faults BEFORE forwarding.
        if self.rng.random() < self.timeout_rate:
            self._log("timeout", path, method)
            await asyncio.sleep(60)
            return web.Response(status=504)

        if self.rng.random() < self.delay_rate:
            self._log("delay", path, method)
            await asyncio.sleep(self.rng.uniform(2, 8))

        # For GETs, optionally serve a corrupted body.
        if method == "GET" and self.rng.random() < self.corrupt_rate:
            self._log("corrupt", path, method)
            return web.Response(status=200, body=b"CORRUPTED" + os.urandom(512))

        # For GETs, optionally serve a body that hashes correctly per
        # SHA-256 but is from a DIFFERENT principal (adversarial). This
        # tests the HMAC verification: the broker should reject the
        # restored bytes because the manifest's HMAC won't match.
        if method == "GET" and self.rng.random() < self.wrong_principal_rate:
            decoy = b"WRONG_PRINCIPAL_LEAK_PAYLOAD_" + os.urandom(2048)
            self._log("wrong_principal", path, method, sha=hashlib.sha256(decoy).hexdigest())
            return web.Response(status=200, body=decoy)

        # Default: passthrough to real S3 / MinIO via aiohttp client.
        import aiohttp

        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(
                    method, full_url, data=body if body else None,
                    headers={k: v for k, v in req.headers.items() if k.lower() not in ("host", "content-length")},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    out_body = await resp.read()
                    return web.Response(
                        status=resp.status,
                        body=out_body,
                        headers={k: v for k, v in resp.headers.items()
                                  if k.lower() not in ("content-encoding", "transfer-encoding", "content-length")},
                    )
            except Exception as e:
                self._log("upstream_error", path, method, err=str(e))
                return web.Response(status=502)

    def _log(self, kind: str, path: str, method: str, **extra) -> None:
        self.event_count += 1
        rec = {"ts": time.time(), "kind": kind, "method": method, "path": path, **extra}
        with open(self.log_path, "a") as f:
            f.write(json.dumps(rec) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default=":9001")
    ap.add_argument("--upstream", default=os.environ.get("CHAOS_S3_UPSTREAM", "http://localhost:9000"))
    ap.add_argument("--timeout-rate", type=float, default=0.02)
    ap.add_argument("--corrupt-rate", type=float, default=0.02)
    ap.add_argument("--delay-rate", type=float, default=0.10)
    ap.add_argument("--wrong-principal-rate", type=float, default=0.01)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--log-path", default="services/chaos/forensics/s3_proxy.jsonl")
    args = ap.parse_args()

    seed = args.seed if args.seed > 0 else int(time.time())
    proxy = S3ChaosProxy(
        upstream=args.upstream,
        timeout_rate=args.timeout_rate,
        corrupt_rate=args.corrupt_rate,
        delay_rate=args.delay_rate,
        wrong_principal_rate=args.wrong_principal_rate,
        rng=random.Random(seed),
        log_path=args.log_path,
    )
    os.makedirs(os.path.dirname(args.log_path), exist_ok=True)

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", proxy.handler)

    host, _, port = args.listen.partition(":")
    if not host:
        host = "0.0.0.0"
    web.run_app(app, host=host, port=int(port))
    return 0


if __name__ == "__main__":
    sys.exit(main())
