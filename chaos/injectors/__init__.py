"""Chaos injectors.

Each injector mutates infrastructure state, broker process, S3 store,
disk, network: to verify isolation invariants hold under failure.
The daemon runs injectors concurrently with the workload + adversary
loops; an injection that causes ANY invariant violation is a P0.

Key principle: injection is reversible. Every injector exposes a
.disengage() method that the daemon calls before run completion or
on Ctrl-C. A test that bricks the environment is a worse outcome than
a test that finds nothing.
"""
