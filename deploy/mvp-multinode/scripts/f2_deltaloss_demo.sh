#!/usr/bin/env bash
# F2 delta-loss resilience demo. Runs the SAME ramp/geometric scenario three
# ways — no loss / a CORRUPT C_ref delta / a SILENTLY-DROPPED C_ref delta,
# injected on edge-0 via the f2driver's F2_INJECT knob — and compares alert
# firing, ships, and ref_errs. The safety property under test: the global alert
# must STILL fire in every case (no silent missed violation).
#
#   corrupt : edge-0's Nth delta bytes are replaced with garbage → OnRef decode
#             fails → the engine marks the reference needFull and force-ships
#             (detected; ref_errs increments).
#   drop    : edge-0's Nth delta is swallowed → the engine never sees it, so
#             needFull is NOT set (a pure mid-stream loss has no sequence gap to
#             detect); safety then rests on the coordinator's periodic Full
#             keyframe + the other edges' true sketches in the global merge.
#
# Usage: f2_deltaloss_demo.sh [base_port]
set -euo pipefail

BASE_PORT="${1:-4380}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"       # ASAPCollector
BACKEND="${ASAP_BACKEND_DIR:-$(cd "$REPO_ROOT/../ASAPQuery-backend" && pwd)}"
GO_DIR="$REPO_ROOT/asap-precompute-go"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; pkill -P $$ 2>/dev/null || true' EXIT

TAU=1000000; EPS=0.1; ROWS=5; COLS=256; EDGES=4; STEPS=20; DRIFT=8; TIMEOUT=18

echo "building harness (Rust) + f2driver (Go)…"
( cd "$BACKEND" && cargo build --release --bin f2_monitor_harness >/dev/null 2>&1 )
HARNESS="$BACKEND/target/release/f2_monitor_harness"
DRIVER="$WORK/f2driver"
( cd "$GO_DIR/monitor/grpcclient" && go build -o "$DRIVER" ./cmd/f2driver/ )

run () { # label port inject_mode inject_nth
  local label="$1" port="$2" mode="${3:-}" nth="${4:-2}"
  local out="$WORK/h_${label}.out" err="$WORK/d_${label}.err"
  "$HARNESS" "$port" geometric 1 "$TAU" "$EPS" "$ROWS" "$COLS" 3600000 "$TIMEOUT" >"$out" 2>/dev/null &
  local hp=$!
  for _ in $(seq 1 50); do grep -q HARNESS_READY "$out" && break; sleep 0.1; done
  F2_INJECT="$mode" F2_INJECT_NTH="$nth" \
    "$DRIVER" "127.0.0.1:$port" geometric 1 "$TAU" "$EPS" "$ROWS" "$COLS" "$EDGES" "$STEPS" "$DRIFT" ramp \
    2>"$err"
  sleep 0.5
  local alert total ships silent errs
  alert=$(grep -c MONITOR_ALERT "$out" || true)
  total=$(grep F2_COMM "$out" | tail -1 | sed -E 's/.*total_bytes=([0-9]+).*/\1/')
  ships=$(sed -E 's/.*ships=([0-9]+).*/\1/'   <<<"$(grep 'f2driver: done' "$err")")
  silent=$(sed -E 's/.*silent=([0-9]+).*/\1/' <<<"$(grep 'f2driver: done' "$err")")
  errs=$(sed -E 's/.*ref_errs=([0-9]+).*/\1/' <<<"$(grep 'f2driver: done' "$err")")
  printf "  %-14s alert=%-2s total_bytes=%-9s ships=%-3s silent=%-3s ref_errs=%s\n" \
    "$label" "$alert" "$total" "$ships" "$silent" "$errs"
  kill "$hp" 2>/dev/null || true; wait "$hp" 2>/dev/null || true
}

echo "== ramp / geometric — delta-loss injected on edge-0 (2nd delta) =="
run no-loss       "$BASE_PORT"
run corrupt-delta "$((BASE_PORT+1))" corrupt 2
run drop-delta    "$((BASE_PORT+2))" drop    2
echo
echo "Safety: alert MUST be 1 in all three rows. corrupt → ref_errs>0 (edge-0"
echo "detected the divergence and force-shipped); drop → ref_errs=0 (silent loss"
echo "undetected by that edge; recovery via the coordinator's periodic keyframe)."
