#!/usr/bin/env bash
# Cluster (real-NIC) whole-sketch F2 eval — the distributed version of
# f2_monitor_eval.sh: the Rust f2_monitor_harness (CDM coordinator) runs on the
# WARM node and the Go f2driver (4 edges, monitor.F2Engine over real gRPC) runs
# on a SOURCE node, so every sketch ship and C_ref broadcast crosses the
# 10 GbE fabric instead of loopback. Same protocol matrix as the local eval:
# {stable, ramp} × {distributed, geometric}; geometric should ≪ distributed on
# stable while reaching the same alert decision.
#
# Binaries are built locally and shipped to /tmp on the target nodes:
#   cargo build --release --bin f2_monitor_harness   → ${WARM_HOST}:/tmp/
#   go build ./cmd/f2driver                          → ${EDGE_HOST}:/tmp/
#
# Usage: f2_wholesketch_cluster.sh [base_port]
set -uo pipefail

BASE_PORT="${1:-4360}"
SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SD}/.." && pwd)"
REPO_ROOT="$(cd "${MV}/../.." && pwd)"
BACKEND="${ASAP_BACKEND_DIR:-$(cd "${REPO_ROOT}/../ASAPQuery-backend" && pwd)}"
# shellcheck disable=SC1090
source "${TOPOLOGY_ENV:-${MV}/topology.8node.env}"
read -r EDGE_HOST _ <<< "${SRC_HOSTS}"   # first source node runs the edges
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; ssh -o BatchMode=yes "${WARM_HOST}" "pkill -f f2_monitor_harness" 2>/dev/null || true' EXIT

EPS=0.1; ROWS=5; COLS=256; EDGES=4; STEPS=20; DRIFT=8; TIMEOUT=25
# Distinct-key cardinality H (default 2048 — a realistic per-metric series count,
# where the fixed d·w sketch cost is well below raw). τ auto-scales with H so the
# ramp keeps the same trajectory: ramp F2_max = H·(k·drift·STEPS)², so a τ ∝ H
# fires the alert at the same fractional step regardless of H (τ(4)=1e6 → 250000·H).
KEYS="${F2_KEYS:-2048}"
TAU=$(( 250000 * KEYS ))
export F2_KEYS="${KEYS}"

echo "building + shipping harness (→${WARM_HOST}) and f2driver (→${EDGE_HOST})…"
( cd "$BACKEND" && cargo build --release --bin f2_monitor_harness >/dev/null 2>&1 )
( cd "${REPO_ROOT}/asap-precompute-go/monitor/grpcclient" && go build -o /tmp/f2driver ./cmd/f2driver/ )
scp -q -o BatchMode=yes "$BACKEND/target/release/f2_monitor_harness" "${WARM_HOST}:/tmp/f2_monitor_harness"
scp -q -o BatchMode=yes /tmp/f2driver "${EDGE_HOST}:/tmp/f2driver"

run () { # workload mode port
  local pat="$1" mode="$2" port="$3"
  local out="$WORK/h_${pat}_${mode}.out" err="$WORK/d_${pat}_${mode}.err"
  ssh -o BatchMode=yes "${WARM_HOST}" \
    "/tmp/f2_monitor_harness $port $mode 1 $TAU $EPS $ROWS $COLS 3600000 $TIMEOUT" >"$out" 2>/dev/null &
  local hp=$!
  for _ in $(seq 1 80); do grep -q HARNESS_READY "$out" 2>/dev/null && break; sleep 0.2; done
  grep -q HARNESS_READY "$out" || { echo "  $pat/$mode: harness not ready — skipped"; kill "$hp" 2>/dev/null; return; }
  ssh -o BatchMode=yes "${EDGE_HOST}" \
    "F2_KEYS=$KEYS /tmp/f2driver ${WARM_IP}:$port $mode 1 $TAU $EPS $ROWS $COLS $EDGES $STEPS $DRIFT $pat" 2>"$err"
  sleep 0.5
  local alert total ships silent
  alert=$(grep -c MONITOR_ALERT "$out" || true)
  total=$(grep F2_COMM "$out" | tail -1 | sed -E 's/.*total_bytes=([0-9]+).*/\1/')
  ships=$(sed -E 's/.*ships=([0-9]+).*/\1/'   <<<"$(grep 'f2driver: done' "$err")")
  silent=$(sed -E 's/.*silent=([0-9]+).*/\1/' <<<"$(grep 'f2driver: done' "$err")")
  printf "  %-8s %-12s alert=%-2s total_bytes=%-9s ships=%-3s silent=%-3s\n" \
    "$pat" "$mode" "$alert" "$total" "$ships" "$silent"
  echo "$pat,$mode,$alert,$total,$ships,$silent" >> "$CSV"
  wait "$hp" 2>/dev/null || true
}

# Raw (no sketch aggregation) baseline: runs on the edge host; bytes counted
# from real serialized [ts,key,value] frames, alert from the EXACT global F2.
# On this tiny-H workload raw is cheaper than sketches by construction (sketch
# cost is fixed d·w) — sweep F2_KEYS for the crossover.
raw () { # workload
  local pat="$1"
  local err="$WORK/d_${pat}_raw.err"
  ssh -o BatchMode=yes "${EDGE_HOST}" \
    "F2_KEYS=$KEYS /tmp/f2driver - raw 1 $TAU $EPS $ROWS $COLS $EDGES $STEPS $DRIFT $pat" 2>"$err"
  local line total alert
  line=$(grep 'mode=raw' "$err")
  total=$(sed -E 's/.*total_bytes=([0-9]+).*/\1/' <<<"$line")
  alert=$(sed -E 's/.*alert=([0-9]+).*/\1/' <<<"$line")
  printf "  %-8s %-12s alert=%-2s total_bytes=%-9s (exact; no aggregation)\n" \
    "$pat" "raw" "$alert" "$total"
  echo "$pat,raw,$alert,$total,," >> "$CSV"
}

CSV="${F2_CLUSTER_OUT:-${MV}/eval-8node/f2_wholesketch_cluster.csv}"
echo "workload,mode,alert,total_bytes,ships,silent" > "$CSV"
echo "== whole-sketch F2 over the real NIC (H=${KEYS} keys, tau=${TAU}): edges on ${EDGE_HOST} → coordinator on ${WARM_HOST} =="
echo "== stable workload (F2 stays below tau) =="
raw stable
run stable distributed "$BASE_PORT"
run stable geometric   "$((BASE_PORT+1))"
echo "== ramp workload (F2 crosses tau) =="
raw ramp
run ramp distributed "$((BASE_PORT+2))"
run ramp geometric   "$((BASE_PORT+3))"
echo "recorded → $CSV"
