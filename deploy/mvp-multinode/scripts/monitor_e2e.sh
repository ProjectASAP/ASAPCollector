#!/usr/bin/env bash
# Cross-language coordinated-sampling e2e: a REAL Go edge (precompute +
# monitor.Engine + grpcclient) drives the REAL Rust monitor coordinator
# (tonic MonitorService) over a live bidi gRPC stream. Asserts a real
# coordinated-sampling grant (SlackGrant.sample_p) comes back.
#
# Global-threshold alerting is retired (see asap-precompute-go/monitor and
# ASAPQuery-backend data_plane::monitor package docs) — this script no longer
# waits for or asserts an alert.
#
# This validates the one seam unit tests can't: the Go-generated client and the
# Rust-generated server interoperating over an actual stream (not a byte
# fixture). No Docker / full backend needed.
#
# Usage: monitor_e2e.sh [port]
set -euo pipefail

PORT="${1:-45319}"
AGG_ID=1
TAU=100  # accepted by the harness/driver for CLI back-compat; unused
WINDOW_MS=3600000

# Repo roots (this script lives in ASAPCollector/deploy/mvp-multinode/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COLLECTOR_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BACKEND_ROOT="$(cd "$COLLECTOR_ROOT/../ASAPQuery-backend" && pwd)"
GRPCCLIENT_DIR="$COLLECTOR_ROOT/asap-precompute-go/monitor/grpcclient"

# The e2edriver observes a Sum series labeled svc=checkout; Sum monitors key
# by the series GROUP key (asap-precompute-go precompute.go groupKeyBytes), so
# the harness's monitor config must register under that same key or the
# driver's registration is rejected as unconfigured.
MON_KEY="svc=checkout"

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

echo "==> Starting coordinator on :$PORT (agg_id=$AGG_ID window_ms=$WINDOW_MS key=$MON_KEY)"
"$HARNESS_BIN" "$PORT" "$AGG_ID" "$TAU" "$WINDOW_MS" 30 "$MON_KEY" >"$HARNESS_LOG" 2>&1 &
HARNESS_PID=$!

# Wait for the harness to report readiness.
for _ in $(seq 1 50); do
  grep -q "HARNESS_READY" "$HARNESS_LOG" 2>/dev/null && break
  sleep 0.1
done
if ! grep -q "HARNESS_READY" "$HARNESS_LOG"; then
  echo "FAIL: coordinator did not become ready"; cat "$HARNESS_LOG"; exit 1
fi

echo "==> Running edge driver (periodic rate reports)"
/tmp/e2edriver "localhost:$PORT" "$AGG_ID" "$TAU" 5 200 || true

# The harness exits 0 once it observes a real grant; give it a moment to flush + exit.
wait "$HARNESS_PID" 2>/dev/null || true
HARNESS_PID=""

echo "----- coordinator output -----"
cat "$HARNESS_LOG"
echo "------------------------------"
if grep -q "MONITOR_GRANT" "$HARNESS_LOG"; then
  echo "PASS: coordinated-sampling grant observed over live Go↔Rust gRPC stream"
  exit 0
else
  echo "FAIL: no grant observed"
  exit 1
fi
