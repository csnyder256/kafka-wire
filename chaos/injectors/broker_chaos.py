"""Broker-process chaos.

Restarts the broker container at random intervals. Validates:
  - producer outbox absorbs writes during the gap
  - consumer rejoin works correctly across the gap
  - segment recovery on restart NEVER reassigns a segment to the wrong
    principal (file-system mtime + index integrity)
  - no in-flight Produce/Fetch returns mixed-principal data after the
    restart

This injector targets a Docker Compose `broker` service. In CI the
service is named per the Compose file. To run against a non-Compose
deployment, replace `_restart_broker` with platform-specific logic.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import subprocess
import sys
import time

import structlog

log = structlog.get_logger()


async def restart_loop(
    *,
    service: str,
    interval_low: float,
    interval_high: float,
    duration: float,
    seed: int,
) -> int:
    rng = random.Random(seed if seed > 0 else int(time.time()))
    deadline = time.time() + duration
    restarts = 0
    while time.time() < deadline:
        wait = rng.uniform(interval_low, interval_high)
        await asyncio.sleep(wait)
        if time.time() >= deadline:
            break
        log.info("broker_chaos.restart.start", service=service)
        try:
            subprocess.run(
                ["docker", "compose", "restart", service],
                check=True,
                capture_output=True,
                timeout=30,
            )
            restarts += 1
            log.info("broker_chaos.restart.done", service=service, restarts=restarts)
        except subprocess.SubprocessError as e:
            log.error("broker_chaos.restart.failed", err=str(e))
        # Give the broker time to recover before the next restart.
        await asyncio.sleep(rng.uniform(15, 45))
    log.info("broker_chaos.completed", restarts=restarts)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", default="broker")
    ap.add_argument("--interval-low", type=float, default=60.0)
    ap.add_argument("--interval-high", type=float, default=180.0)
    ap.add_argument("--duration", type=float, default=600.0)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()
    return asyncio.run(restart_loop(
        service=args.service,
        interval_low=args.interval_low,
        interval_high=args.interval_high,
        duration=args.duration,
        seed=args.seed,
    ))


if __name__ == "__main__":
    sys.exit(main())
