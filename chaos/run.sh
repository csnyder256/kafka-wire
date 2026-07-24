#!/usr/bin/env bash
# End-to-end adversarial run against a throwaway broker.
#
#   ./chaos/run.sh [duration_seconds] [principals]
#
# Three phases, because SCRAM credentials have to exist on disk before the
# broker starts: provision without auth, restart with auth, then attack.
set -euo pipefail

DURATION="${1:-120}"
PRINCIPALS="${2:-4}"
SEED="${CHAOS_SEED:-$RANDOM}"
WORK="$(mktemp -d)"
# The brute-force module needs a token to fail against. Without one the admin
# API is open on loopback by design, and the harness correctly calls that a
# bypass.
ADMIN_TOKEN="chaos-$(od -An -N16 -tx1 /dev/urandom | tr -dc 'a-f0-9')"
# Override when the interpreter on PATH is not the one with the harness
# dependencies installed, for example PYTHON="py -3" under Git Bash.
PYTHON="${PYTHON:-python3}"
KAFKA_PORT=19601
ADMIN_PORT=18601

cleanup() { [[ -n "${BROKER_PID:-}" ]] && kill "$BROKER_PID" 2>/dev/null || true; }
trap cleanup EXIT

go build -o "$WORK/kafka-wire" ./cmd/kafka-wire

start_broker() {
  KAFKA_WIRE_LISTENERS_KAFKA="127.0.0.1:$KAFKA_PORT" \
  KAFKA_WIRE_LISTENERS_ADMIN="127.0.0.1:$ADMIN_PORT" \
  KAFKA_WIRE_STORAGE_DATADIR="$WORK/data" \
  KAFKA_WIRE_LOG_LEVEL=warn \
  "$@" "$WORK/kafka-wire" serve > "$WORK/broker.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 30); do
    (echo > "/dev/tcp/127.0.0.1/$KAFKA_PORT") 2>/dev/null && return 0
    sleep 1
  done
  echo "broker did not start"; cat "$WORK/broker.log"; exit 1
}

echo "== phase 1: provision principals and write the users file (seed $SEED)"
start_broker env
$PYTHON -m chaos.daemon --bootstrap "127.0.0.1:$KAFKA_PORT" \
  --admin-url "http://127.0.0.1:$ADMIN_PORT" --admin-token "$ADMIN_TOKEN" --seed "$SEED" \
  --principals "$PRINCIPALS" --provision-only --users-file-out "$WORK/users.json"
kill "$BROKER_PID"; wait "$BROKER_PID" 2>/dev/null || true

echo "== phase 2: restart with SASL and those credentials"
start_broker env KAFKA_WIRE_AUTH_SASLENABLED=true KAFKA_WIRE_AUTH_USERSFILE="$WORK/users.json"

echo "== phase 3: attack for ${DURATION}s"
# Hard ceiling on the phase. A harness that overruns its own --duration is a
# harness nobody will put in CI, so the budget is enforced from outside rather
# than trusted.
timeout $(( DURATION + 120 )) $PYTHON -m chaos.daemon \
  --bootstrap "127.0.0.1:$KAFKA_PORT" \
  --admin-url "http://127.0.0.1:$ADMIN_PORT" --admin-token "$ADMIN_TOKEN" --seed "$SEED" \
  --principals "$PRINCIPALS" --duration "$DURATION" \
  --forensics-root "$WORK/forensics"

echo "== clean run. Reproduce this exact topology with CHAOS_SEED=$SEED"
