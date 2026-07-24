"""Adversary modules.

Each module probes the inverse of one or more invariants. They run
concurrently with the legitimate workload; every "successful" attack
(broker did NOT deny what we tried) raises InvariantViolation.

Module catalog:
  - cross_principal_fetch: principal A's principal Fetches principal B's topic
  - cross_principal_produce: principal A produces into principal B's topic
  - cross_principal_group: principal A joins principal B's consumer group
  - acl_bypass: brute-force topic + group naming patterns to find a
                permissive matcher
  - path_traversal: object key fuzzing with ../ . // \\\\ etc.
  - namespace_confusion: similarly-prefixed topic names ("principal.AB",
                          "principal.A.B", "principal.A/B")
  - offset_poison: fetch offsets manufactured to land in another
                   principal's segment range
  - replay_confusion: reset another principal's group to YOUR offsets
  - cache_poisoning: race a restore against another principal's request
  - hmac_tamper: edit archive.json to flip a principal_id, attempt restore
  - deleted_principal_remnants: provision principal, populate, delete, probe
  - admin_token_bruteforce: guess admin tokens to bypass auth gate

Every adversary returns a tuple of (attempts, denied, violations).
The daemon aggregates these into the run report.
"""
