"""Prometheus exporter for chaos-engine metrics.

Exposes a /metrics endpoint that observability tooling (Grafana,
Prom, the broker's own dashboard at /broker) can scrape to surface
chaos-run health in real time.

Metrics published:
  chaos_iterations_total
  chaos_adversary_attempts_total{module}
  chaos_adversary_denied_total{module}
  chaos_invariant_violations_total{invariant}
  chaos_workload_records_produced_total{principal}
  chaos_workload_records_consumed_total{principal}
  chaos_run_duration_seconds
  chaos_topology_pattern_info{pattern, fingerprint}

The exporter is in-process: embedded by services/chaos/daemon.py
when run with --expose-metrics. CI is configured to scrape it via
the Docker Compose `prometheus` service (when present).
"""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Info,
    generate_latest,
)


CHAOS_ITERATIONS = Counter(
    "chaos_iterations_total",
    "Total chaos-loop iterations executed in this run.",
)
CHAOS_ADVERSARY_ATTEMPTS = Counter(
    "chaos_adversary_attempts_total",
    "Adversary attempts issued, by module.",
    ["module"],
)
CHAOS_ADVERSARY_DENIED = Counter(
    "chaos_adversary_denied_total",
    "Adversary attempts that the broker correctly denied.",
    ["module"],
)
CHAOS_VIOLATIONS = Counter(
    "chaos_invariant_violations_total",
    "Total invariant violations detected (should be 0).",
    ["invariant"],
)
CHAOS_PRODUCED = Counter(
    "chaos_workload_records_produced_total",
    "Records produced by principal during chaos run.",
    ["principal"],
)
CHAOS_CONSUMED = Counter(
    "chaos_workload_records_consumed_total",
    "Records consumed by principal during chaos run.",
    ["principal"],
)
CHAOS_RUN_DURATION = Gauge(
    "chaos_run_duration_seconds",
    "Wall time the current chaos run has been active.",
)
CHAOS_TOPOLOGY = Info(
    "chaos_topology",
    "Identity of the current chaos topology.",
)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.end_headers()
        self.wfile.write(generate_latest())

    def log_message(self, fmt, *args):
        pass


def start(host: str = "0.0.0.0", port: int = 9100) -> threading.Thread:
    srv = HTTPServer((host, port), _Handler)
    th = threading.Thread(target=srv.serve_forever, daemon=True, name="chaos-prom-exporter")
    th.start()
    return th
