#!/usr/bin/env bash
# REAL-dataset whole-sketch F2 eval — replays the DEBS 2022 trading-day trace
# (per-symbol market events) instead of a synthetic workload. Each symbol is a
# monitored key; the global F2 = Σ_symbol (cumulative event count)² grows over
# the day (a real ramp), and we alert when it crosses τ ("trading became too
# concentrated"). Compares raw (ship every event) vs distributed vs geometric.
#
# Pass 1 (python) bins the first N events into STEPS sub-windows and assigns
# symbols to EDGES edges → a compact `step edge symbol count` trace. Pass 2 runs
# the f2driver (F2_TRACE replay) against the f2_monitor_harness coordinator.
#
# Usage: f2_debs_eval.sh [base_port]
set -uo pipefail
BASE_PORT="${1:-4770}"
SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SD}/.." && pwd)"
REPO_ROOT="$(cd "${MV}/../.." && pwd)"
BACKEND="${ASAP_BACKEND_DIR:-$(cd "${REPO_ROOT}/../ASAPQuery-backend" && pwd)}"
CSV_DEBS="${DEBS_CSV:-${REPO_ROOT}/datasets_eval/debs/data/debs2022-gc-trading-day-08-11-21.csv}"
TRACE="${F2_TRACE:-/tmp/debs_f2.trace}"

N_EVENTS="${N_EVENTS:-4000000}"; STEPS=20; EDGES=4; D=5; W="${W:-4096}"; EPS=0.1
TAU="${TAU:-25000000000}"   # ~ crosses at step ~14 for the 4M-event slice
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

echo "building trace from DEBS (${N_EVENTS} events → ${STEPS} steps, ${EDGES} edges)…"
python3 "${REPO_ROOT}/datasets_eval/debs/scripts/debs_f2_trace.py" \
  "$CSV_DEBS" "$N_EVENTS" "$STEPS" "$EDGES" "$TRACE" 2>"$WORK/f2traj.txt"
H=$(sed -nE 's/.*distinct_symbols\(H\)=([0-9]+).*/\1/p' "$WORK/f2traj.txt")
echo "  distinct symbols H=${H}; F2 trajectory in $WORK/f2traj.txt"

echo "building harness + f2driver…"
( cd "$BACKEND" && cargo build --release --bin f2_monitor_harness >/dev/null 2>&1 )
HARNESS="$BACKEND/target/release/f2_monitor_harness"
( cd "${REPO_ROOT}/asap-precompute-go/monitor/grpcclient" && go build -o "$WORK/f2driver" ./cmd/f2driver/ )

CSV="${F2_DEBS_OUT:-${MV}/eval-8node/f2_debs.csv}"
echo "mode,total_bytes,alert,ships,note" > "$CSV"

rawrun() {
  local err="$WORK/raw.err"
  F2_KEYS=$H F2_TRACE=$TRACE "$WORK/f2driver" - raw 1 "$TAU" "$EPS" "$D" "$W" "$EDGES" "$STEPS" 8 trace 2>"$err"
  local line=$(grep 'mode=raw' "$err"); local t=$(sed -E 's/.*total_bytes=([0-9]+).*/\1/'<<<"$line"); local a=$(sed -E 's/.*alert=([0-9]+).*/\1/'<<<"$line")
  printf "  %-12s total=%-11s alert=%s (ship every event)\n" raw "$t" "$a"; echo "raw,$t,$a,,ship-every-event" >> "$CSV"
}
skrun() {
  local mode=$1 port=$2
  local out="$WORK/h_$mode.out" err="$WORK/d_$mode.err"
  "$HARNESS" "$port" "$mode" 1 "$TAU" "$EPS" "$D" "$W" 3600000 40 >"$out" 2>/dev/null &
  local hp=$!; for _ in $(seq 1 100); do grep -q HARNESS_READY "$out" && break; sleep 0.2; done
  F2_KEYS=$H F2_TRACE=$TRACE "$WORK/f2driver" 127.0.0.1:$port "$mode" 1 "$TAU" "$EPS" "$D" "$W" "$EDGES" "$STEPS" 8 trace 2>"$err"
  sleep 0.5
  local t=$(grep F2_COMM "$out"|tail -1|sed -E 's/.*total_bytes=([0-9]+).*/\1/'); local a=$(grep -c MONITOR_ALERT "$out"); local sh=$(sed -E 's/.*ships=([0-9]+).*/\1/'<<<"$(grep done "$err")")
  printf "  %-12s total=%-11s alert=%-2s ships=%s\n" "$mode" "$t" "$a" "${sh:-—}"; echo "$mode,$t,$a,${sh:-},," >> "$CSV"
  kill "$hp" 2>/dev/null; wait "$hp" 2>/dev/null
}

echo "== REAL DEBS 2022 trading day: whole-sketch F2 (H=${H} symbols, d=${D} w=${W}, tau=${TAU}) =="
rawrun
skrun distributed "$BASE_PORT"
skrun geometric   "$((BASE_PORT+1))"
echo "recorded → $CSV  (F2 trajectory: $WORK/f2traj.txt kept? no — see debs_f2_trace.py stderr)"
