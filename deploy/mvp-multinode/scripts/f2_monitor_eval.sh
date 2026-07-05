#!/usr/bin/env bash
# Cross-language F2 / geometric distributed-monitoring eval.
#
# Stands up the Rust f2_monitor_harness (the CDM coordinator) and drives it with
# the Go f2driver (N edges, each a monitor.F2Engine over the real gRPC
# transport), in both `distributed` (ship every window) and `geometric`
# (Sharfman–Schuster–Keren safe-zone, ship on local violation) modes, over two
# workloads:
#   * stable — global F2 hovers below τ (the realistic monitoring scenario)
#   * ramp   — global F2 grows monotonically and crosses τ
#
# It prints, per (workload, mode): whether the alert fired, total communication
# bytes (edge→coord sketch bytes + coord→edge C_ref broadcast bytes), and the
# edge ship/silent counts. Geometric should ≪ distributed on `stable` while
# reaching the same alert decision.
#
# Usage: f2_monitor_eval.sh [base_port]
set -euo pipefail

BASE_PORT="${1:-4360}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"       # ASAPCollector
BACKEND="${ASAP_BACKEND_DIR:-$(cd "$REPO_ROOT/../ASAPQuery-backend" && pwd)}"
GO_DIR="$REPO_ROOT/asap-precompute-go"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; pkill -P $$ 2>/dev/null || true' EXIT

# Monitor + workload parameters.
TAU=1000000; EPS=0.1; ROWS=5; COLS=256; EDGES=4; STEPS=20; DRIFT=8; TIMEOUT=18

echo "building harness (Rust) + f2driver (Go)…"
( cd "$BACKEND" && cargo build --release --bin f2_monitor_harness >/dev/null 2>&1 )
HARNESS="$BACKEND/target/release/f2_monitor_harness"
DRIVER="$WORK/f2driver"
( cd "$GO_DIR/monitor/grpcclient" && go build -o "$DRIVER" ./cmd/f2driver/ )

run () { # workload mode port
  local pat="$1" mode="$2" port="$3"
  # Separate statement: ${pat} in the same `local` would expand before the
  # assignments take effect (unbound under set -u).
  local out="$WORK/h_${pat}_${mode}.out"
  "$HARNESS" "$port" "$mode" 1 "$TAU" "$EPS" "$ROWS" "$COLS" 3600000 "$TIMEOUT" >"$out" 2>/dev/null &
  local hp=$!
  for _ in $(seq 1 50); do grep -q HARNESS_READY "$out" && break; sleep 0.1; done
  "$DRIVER" "127.0.0.1:$port" "$mode" 1 "$TAU" "$EPS" "$ROWS" "$COLS" "$EDGES" "$STEPS" "$DRIFT" "$pat" \
    2>"$WORK/d_${pat}_${mode}.err"
  sleep 0.4
  local alert total ships silent
  alert=$(grep -c MONITOR_ALERT "$out" || true)
  total=$(grep F2_COMM "$out" | tail -1 | sed -E 's/.*total_bytes=([0-9]+).*/\1/')
  ships=$(sed -E 's/.*ships=([0-9]+).*/\1/' <<<"$(tail -1 "$WORK/d_${pat}_${mode}.err")")
  silent=$(sed -E 's/.*silent=([0-9]+).*/\1/' <<<"$(tail -1 "$WORK/d_${pat}_${mode}.err")")
  printf "  %-8s %-12s alert=%-2s total_bytes=%-9s ships=%-3s silent=%-3s\n" \
    "$pat" "$mode" "$alert" "$total" "$ships" "$silent"
  kill "$hp" 2>/dev/null || true; wait "$hp" 2>/dev/null || true
}

echo "== stable workload (F2 stays below tau) =="
run stable distributed "$BASE_PORT"
run stable geometric   "$((BASE_PORT+1))"
echo "== ramp workload (F2 crosses tau) =="
run ramp distributed "$((BASE_PORT+2))"
run ramp geometric   "$((BASE_PORT+3))"
