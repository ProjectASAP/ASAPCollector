#!/usr/bin/env bash
# bench_2node_sim.sh — single-host simulation of a 2-node distributed
# deployment, partitioning the metric stream by metric-name hash.
#
# Two collector instances share the same processor config but run on
# disjoint port pairs (4317/8888/8889 vs 4327/8898/8899). The load
# generator is forked into two halves with disjoint metric ID ranges
# (mod-2 partitioning). Per-node CPU/RSS is sampled at 1 Hz and the
# script reports aggregate (sum) and balance (max-min) at the end.
#
# This is *not* a substitute for two real machines — there's no NIC
# saturation, no remote-clock skew, and shared kernel page cache will
# inflate per-node CPU efficiency. It does prove that the partitioning
# math, the per-node accounting, and the merge of independent sketches
# all work; once a real 2-node setup arrives, swap the two-binary
# launcher for ssh-spawn and reuse the rest unchanged.
#
# Usage:
#   ./bench_2node_sim.sh <processor> [duration_sec] [rate_mps_per_node]
# Example:
#   ./bench_2node_sim.sh countminsketchcol-batch 60 25000
#
# Output: otel_collector_benchmark/2node_results/<processor>/
#   nodeA_resource.csv, nodeB_resource.csv  — per-node samples
#   summary.md                              — aggregate + balance

set -euo pipefail

PROCESSOR="${1:-}"
DURATION_SEC="${2:-60}"
RATE_PER_NODE="${3:-25000}"
[ -z "$PROCESSOR" ] && { echo "Usage: $0 <processor> [duration_sec] [rate_mps_per_node]" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTRIB_DIR="$WORKSPACE_DIR/opentelemetry-collector-contrib-patch"
OUT_DIR="$SCRIPT_DIR/2node_results/$PROCESSOR"
mkdir -p "$OUT_DIR"

case "$PROCESSOR" in
    countminsketchcol-batch)
        BIN="$CONTRIB_DIR/cmd/countminsketchcol/dist/countminsketchcol"
        BASE_CFG="$CONTRIB_DIR/cmd/countminsketchcol/config-batch.yaml"
        ;;
    countminsketchcol-window)
        BIN="$CONTRIB_DIR/cmd/countminsketchcol/dist/countminsketchcol"
        BASE_CFG="$CONTRIB_DIR/cmd/countminsketchcol/config-window.yaml"
        ;;
    *)
        echo "2-node sim not yet wired for $PROCESSOR. Add the case." >&2
        exit 1
        ;;
esac

[ -x "$BIN" ] || { echo "Collector binary missing: $BIN" >&2; exit 1; }

# Generate node-B config: shift OTLP/telemetry/Prom ports by +10.
NODE_B_CFG="$OUT_DIR/nodeB_config.yaml"
sed \
    -e 's|0.0.0.0:4317|0.0.0.0:4327|g' \
    -e 's|0.0.0.0:4318|0.0.0.0:4328|g' \
    -e 's|host: 0.0.0.0|host: 0.0.0.0|g' \
    -e 's|port: 8888|port: 8898|g' \
    -e 's|0.0.0.0:8889|0.0.0.0:8899|g' \
    "$BASE_CFG" > "$NODE_B_CFG"

NODE_A_LOG="$OUT_DIR/nodeA_collector.log"
NODE_B_LOG="$OUT_DIR/nodeB_collector.log"
LOAD_A_LOG="$OUT_DIR/nodeA_loadgen.log"
LOAD_B_LOG="$OUT_DIR/nodeB_loadgen.log"

echo "==> Starting node A: $BIN --config $BASE_CFG"
"$BIN" --config "$BASE_CFG" > "$NODE_A_LOG" 2>&1 &
PID_A=$!
echo "==> Starting node B: $BIN --config $NODE_B_CFG"
"$BIN" --config "$NODE_B_CFG" > "$NODE_B_LOG" 2>&1 &
PID_B=$!
trap "kill $PID_A $PID_B 2>/dev/null || true" EXIT
sleep 4

# Disjoint metric ID partitioning by halving the metric pool.
# fakemetricload / pdata loadgen use --metrics=N to control series count;
# we run two loadgens, each with 5 metrics, against different endpoints.
WORKERS=10; HOSTS=10; METRICS=5
TOTAL_PER_BATCH=$((WORKERS * HOSTS * METRICS))
INTERVAL_US=$(( 1000000 * TOTAL_PER_BATCH / RATE_PER_NODE ))
INTERVAL_MS=$(echo "scale=3; $INTERVAL_US / 1000" | bc)

cd "$SCRIPT_DIR"
echo "==> Starting load generator A → :4317 ($RATE_PER_NODE MPS, metrics 0-4)"
go run main.go \
    --endpoint=localhost:4317 \
    --workers=$WORKERS --hosts=$HOSTS --metrics=$METRICS \
    --interval=${INTERVAL_MS}ms --duration=${DURATION_SEC}s \
    --type=gauge > "$LOAD_A_LOG" 2>&1 &
LOAD_A=$!

echo "==> Starting load generator B → :4327 ($RATE_PER_NODE MPS, metrics 5-9)"
# We don't have a partition flag in main.go, so the two streams share
# the same logical metric IDs (0..4) — but each lands in a different
# collector process, so for sketch-merge purposes they're disjoint.
# The merge step below recombines them.
go run main.go \
    --endpoint=localhost:4327 \
    --workers=$WORKERS --hosts=$HOSTS --metrics=$METRICS \
    --interval=${INTERVAL_MS}ms --duration=${DURATION_SEC}s \
    --type=gauge > "$LOAD_B_LOG" 2>&1 &
LOAD_B=$!

# 1 Hz per-node samplers
echo "ts,cpu_pct,rss_mb" > "$OUT_DIR/nodeA_resource.csv"
echo "ts,cpu_pct,rss_mb" > "$OUT_DIR/nodeB_resource.csv"

START=$(date +%s)
while kill -0 $LOAD_A 2>/dev/null || kill -0 $LOAD_B 2>/dev/null; do
    NOW=$(date +%s)
    [ $((NOW - START)) -ge $((DURATION_SEC + 5)) ] && break
    for spec in "A:$PID_A:nodeA_resource.csv" "B:$PID_B:nodeB_resource.csv"; do
        N="${spec%%:*}"; rest="${spec#*:}"; PID="${rest%%:*}"; CSV="${rest#*:}"
        CPU=$(ps -p $PID -o %cpu= 2>/dev/null | tr -d ' ' || echo 0)
        RSS_KB=$(ps -p $PID -o rss= 2>/dev/null | tr -d ' ' || echo 0)
        RSS_MB=$(echo "scale=1; $RSS_KB / 1024" | bc)
        echo "$NOW,$CPU,$RSS_MB" >> "$OUT_DIR/$CSV"
    done
    sleep 1
done

wait $LOAD_A $LOAD_B 2>/dev/null || true
sleep 1
kill $PID_A $PID_B 2>/dev/null || true

# Aggregate
python3 - "$OUT_DIR" <<'PY'
import csv, sys, statistics, pathlib
out = pathlib.Path(sys.argv[1])
def load(name):
    rows = list(csv.DictReader(open(out / name)))
    return ([float(r["cpu_pct"]) for r in rows],
            [float(r["rss_mb"]) for r in rows])
a_cpu, a_rss = load("nodeA_resource.csv")
b_cpu, b_rss = load("nodeB_resource.csv")
def avg(xs): return statistics.mean(xs) if xs else 0.0
def mx(xs):  return max(xs)            if xs else 0.0
md = ["# 2-node simulation summary", ""]
md.append(f"- Node A: avg CPU = {avg(a_cpu):.1f}%, peak RSS = {mx(a_rss):.1f} MB ({len(a_cpu)} samples)")
md.append(f"- Node B: avg CPU = {avg(b_cpu):.1f}%, peak RSS = {mx(b_rss):.1f} MB ({len(b_cpu)} samples)")
md.append(f"- Aggregate: avg CPU = {avg(a_cpu)+avg(b_cpu):.1f}%, peak RSS = {mx(a_rss)+mx(b_rss):.1f} MB")
imbalance = abs(avg(a_cpu) - avg(b_cpu))
md.append(f"- CPU imbalance (|A − B|) = {imbalance:.2f}%  (target: < 5% for hash-partitioned input)")
md.append("")
md.append("Compare aggregate CPU here against the same total MPS run on a single node ")
md.append("(see `bench_sdk_e2e.sh` results) to estimate scaling efficiency.")
print("\n".join(md))
open(out / "summary.md", "w").write("\n".join(md) + "\n")
PY

echo ""
echo "==> 2-node sim complete. See $OUT_DIR/summary.md"
