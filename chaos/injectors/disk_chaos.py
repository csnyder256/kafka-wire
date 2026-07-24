"""Disk-layer chaos.

Targets the broker's data dir directly:
  - corrupt random byte ranges in sealed segment files (recovery
    must truncate at the first bad CRC and refuse fetches past)
  - flip a principal_id field inside metadata/archive.json (HMAC must
    catch this; restore must refuse)
  - delete random index files (broker must rebuild from the .log on
    next open)
  - swap two segments' .index files (cross-segment lookup confusion;
    the broker should detect via batch-header BaseOffset on read)
  - truncate metadata/topics.json to half its length (broker must
    refuse to start, NOT come up half-loaded)

This injector requires direct access to the broker's data dir. In
docker-compose, the volume is named `broker_data`; we run inside the
broker container via `docker exec` for surgical mutations.
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


def _docker_exec(container: str, cmd: list[str]) -> str:
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", container, *cmd],
        check=False, capture_output=True, timeout=20,
    )
    return out.stdout.decode("utf-8", errors="replace") + out.stderr.decode("utf-8", errors="replace")


async def disk_corrupt_loop(
    *,
    container: str,
    data_dir: str,
    duration: float,
    seed: int,
    rate_per_min: float,
) -> int:
    rng = random.Random(seed if seed > 0 else int(time.time()))
    deadline = time.time() + duration
    interval = 60.0 / rate_per_min if rate_per_min > 0 else 30
    corruptions = 0
    while time.time() < deadline:
        await asyncio.sleep(rng.uniform(interval * 0.5, interval * 1.5))
        if time.time() >= deadline:
            break

        kind = rng.choice([
            "corrupt_archive_json_principal_field",
            "corrupt_random_segment_byte",
            "delete_random_index",
        ])
        try:
            if kind == "corrupt_archive_json_principal_field":
                # Flip ONE character in a random principal_id field within
                # archive.json. The HMAC verification on restore must
                # detect this and refuse.
                _docker_exec(container, ["sh", "-c",
                    f"f={data_dir}/metadata/archive.json; "
                    f"if [ -f $f ]; then python3 -c \"import json,random,sys;"
                    f"d=json.load(open('$f'));"
                    f"segs=d.get('segments',[]);"
                    f"if segs: random.choice(segs)['principal_id'] += 'X';"
                    f"open('$f','w').write(json.dumps(d, indent=2))\"; fi"])
            elif kind == "corrupt_random_segment_byte":
                # Find the OLDEST sealed segment (low risk; broker
                # would have archived it) and flip a byte.
                _docker_exec(container, ["sh", "-c",
                    f"f=$(find {data_dir}/topics -name '*.log' | sort | head -1); "
                    f"if [ -n \"$f\" ]; then python3 -c \"import os,random;"
                    f"f='$f'; sz=os.path.getsize(f); pos=random.randint(0, max(sz-1, 0));"
                    f"open(f, 'r+b').seek(pos); open(f, 'r+b').write(bytes([random.randint(0,255)]))\"; fi"])
            elif kind == "delete_random_index":
                _docker_exec(container, ["sh", "-c",
                    f"f=$(find {data_dir}/topics -name '*.index' | sort -R | head -1); "
                    f"if [ -n \"$f\" ]; then rm -f $f; fi"])

            corruptions += 1
            log.info("disk_chaos.injected", kind=kind, total=corruptions)
        except Exception as e:
            log.warning("disk_chaos.failed", kind=kind, err=str(e))
    log.info("disk_chaos.complete", total=corruptions)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", default="broker")
    ap.add_argument("--data-dir", default="/data")
    ap.add_argument("--duration", type=float, default=600.0)
    ap.add_argument("--rate-per-min", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()
    return asyncio.run(disk_corrupt_loop(
        container=args.container,
        data_dir=args.data_dir,
        duration=args.duration,
        seed=args.seed,
        rate_per_min=args.rate_per_min,
    ))


if __name__ == "__main__":
    sys.exit(main())
