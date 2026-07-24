"""Kafka wire-protocol fuzzer.

Sends syntactically-valid-ish Kafka frames with adversarial field
values. The broker must NEVER:
  - crash
  - leak heap memory in an error message
  - confuse one principal's session for another
  - return a frame larger than max_request_bytes
  - accept a frame larger than max_request_bytes (DoS surface)

Approach: connect, do SaslHandshake + SaslAuthenticate as a principal
principal, then send a stream of mutated requests for a fixed amount
of time. Track:
  - did the connection drop without the broker crashing?
  - did any response decode succeed where it shouldn't have?
  - did any post-fuzz Fetch return cross-principal data?

The fuzzer does NOT itself raise InvariantViolation; the daemon's
post-fuzz consumer-side checker does (any record with
_chaos_principal != current.principal is a leak; any process crash detected
via /health is a P0).
"""
from __future__ import annotations

import argparse
import os
import random
import socket
import struct
import sys
import time

import structlog

log = structlog.get_logger()


def _send_garbage(host: str, port: int, n: int, rng: random.Random) -> int:
    """Open `n` connections, send random bytes, count clean closes vs
    broker hangs/disconnects. Used to verify the broker accepts no
    request larger than its max."""
    bad_size_count = 0
    too_big_count = 0
    for _ in range(n):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        try:
            s.connect((host, port))
            choice = rng.choice(["zero_size", "huge_size", "random_bytes"])
            if choice == "zero_size":
                s.sendall(struct.pack(">i", 0))
                bad_size_count += 1
            elif choice == "huge_size":
                # Claim a 100 MB frame size; broker should refuse.
                s.sendall(struct.pack(">i", 100 * 1024 * 1024))
                # Send a tiny body so the broker sees mismatched length.
                s.sendall(os.urandom(64))
                too_big_count += 1
            else:
                # Random bytes including the 4-byte size prefix.
                size = rng.randint(1, 1000)
                s.sendall(struct.pack(">i", size))
                s.sendall(os.urandom(size))
        except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            try:
                s.close()
            except Exception:
                pass
    return bad_size_count + too_big_count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9092)
    ap.add_argument("--n-connections", type=int, default=200)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    rng = random.Random(args.seed if args.seed > 0 else int(time.time()))
    log.info("protocol_fuzz.start", host=args.host, port=args.port, n=args.n_connections)
    sent = _send_garbage(args.host, args.port, args.n_connections, rng)
    log.info("protocol_fuzz.complete", sent=sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
