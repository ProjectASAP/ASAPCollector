#!/usr/bin/env bash
# Cross-language CDM e2e: a REAL Go edge (precompute + monitor.Engine +
# grpcclient) drives the REAL Rust monitor coordinator (tonic MonitorService)
# over a live bidi gRPC stream. Asserts the global-threshold alert fires.
#
# This validates the one seam unit tests can't: the Go-generated client and the
# Rust-generated server interoperating over an actual stream (not a byte
# fixture). No Docker / full backend needed.
#
# Usage: monitor_e2e.sh [port]
set -euo pipefail

PORT="${1:-45319}"
AGG_ID=1
TAU=100
WINDOW_MS=3600000

# Repo roots (this script lives in ASAPCollector/deploy/mvp-multinode/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COLLECTOR_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BACKEND_ROOT="$(cd "$COLLECTOR_ROOT/../ASAPQuery-backend" && pwd)"
GRPCCLIENT_DIR="$COLLECTOR_ROOT/asap-precompute-go/monitor/grpcclient"

HARNESS_LOG="$(mktemp)"
HARNESS_PID=""
cleanup() {
  [[ -n "$HARNESS_PID" ]] && kill "$HARNESS_PID" 2>/dev/null || true
  rm -f "$HARNESS_LOG" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Building Rust coordinator harness"
( cd "$BACKEND_ROOT" && cargo build --bin monitor_coordinator_harness 2>&1 | tail -1 )
HARNESS_BIN="$BACKEND_ROOT/target/debug/monitor_coordinator_harness"

echo "==> Building Go edge driver"
( cd "$GRPCCLIENT_DIR" && GOFLAGS=-mod=mod go build -o /tmp/e2edriver ./cmd/e2edriver )

echo "==> Starting coordinator on :$PORT (agg_id=$AGG_ID tau=$TAU window_ms=$WINDOW_MS)"
"$HARNESS_BIN" "$PORT" "$AGG_ID" "$TAU" "$WINDOW_MS" 30 >"$HARNESS_LOG" 2>&1 &
HARNESS_PID=$!

# Wait for the harness to report readiness.
for _ in $(seq 1 50); do
  grep -q "HARNESS_READY" "$HARNESS_LOG" 2>/dev/null && break
  sleep 0.1
done
if ! grep -q "HARNESS_READY" "$HARNESS_LOG"; then
  echo "FAIL: coordinator did not become ready"; cat "$HARNESS_LOG"; exit 1
fi

echo "==> Running edge driver (sum climbs past tau)"
/tmp/e2edriver "localhost:$PORT" "$AGG_ID" "$TAU" 5 200 || true

# The harness exits 0 on alert; give it a moment to flush + exit.
wait "$HARNESS_PID" 2>/dev/null || true
HARNESS_PID=""

echo "----- coordinator output -----"
cat "$HARNESS_LOG"
echo "------------------------------"
if grep -q "MONITOR_ALERT" "$HARNESS_LOG"; then
  echo "PASS: global-threshold alert fired over live Go↔Rust gRPC stream"
  exit 0
else
  echo "FAIL: no alert fired"
  exit 1
fi
