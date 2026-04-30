#!/usr/bin/env bash
# bench_soak.sh — long-running steady-state soak benchmark.
#
# Runs a single OTel collector at a fixed MPS for hours/days, sampling
# CPU% and RSS every minute. Catches memory leaks, window-flush stalls,
# and accumulator growth that the short bench.sh sweep can't reveal.
#
# Usage:
#   ./bench_soak.sh <processor> [duration_hours] [rate_mps]
# Example:
#   ./bench_soak.sh countminsketchcol-batch 24 30000
#
# Output: otel_collector_benchmark/soak_results/<processor>/
#   resource_timeline.csv  — minute-resolution CPU%/RSS_MB/heap_MB
#   loadgen.log            — load generator stderr
#   collector.log          — collector stderr
#   summary.md             — drift analysis (mean, slope, max-min)

set -euo pipefail

PROCESSOR="${1:-}"
DURATION_HOURS="${2:-24}"
RATE_MPS="${3:-30000}"

if [ -z "$PROCESSOR" ]; then
    echo "Usage: $0 <processor> [duration_hours] [rate_mps]" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTRIB_DIR="$WORKSPACE_DIR/opentelemetry-collector-contrib-patch"
OUT_DIR="$SCRIPT_DIR/soak_results/$PROCESSOR"
mkdir -p "$OUT_DIR"

# Reuse bench.sh's binary / config resolution by sourcing the prefix.
# Simpler: hardcode the cmscol-batch path since this script is processor-specific.
case "$PROCESSOR" in
    countminsketchcol-batch|countminsketchcol-window)
        COLLECTOR_BIN="$CONTRIB_DIR/cmd/countminsketchcol/dist/countminsketchcol"
        if [[ "$PROCESSOR" == *-batch ]]; then
            CONFIG_FILE="$CONTRIB_DIR/cmd/countminsketchcol/config-batch.yaml"
        else
            CONFIG_FILE="$CONTRIB_DIR/cmd/countminsketchcol/config-window.yaml"
        fi
        ;;
    countsketchcol-batch|countsketchcol-window)
        COLLECTOR_BIN="$CONTRIB_DIR/cmd/countsketchcol/dist/countsketchcol"
        if [[ "$PROCESSOR" == *-batch ]]; then
            CONFIG_FILE="$CONTRIB_DIR/cmd/countsketchcol/config.yaml"
        else
            CONFIG_FILE="$CONTRIB_DIR/cmd/countsketchcol/config-window.yaml"
        fi
        ;;
    ddsketchcol-batch|ddsketchcol-window)
        COLLECTOR_BIN="$CONTRIB_DIR/cmd/ddsketchcol/ddsketchcol"
        if [[ "$PROCESSOR" == *-batch ]]; then
            CONFIG_FILE="$CONTRIB_DIR/cmd/ddsketchcol/config.yaml"
        else
            CONFIG_FILE="$CONTRIB_DIR/cmd/ddsketchcol/config-window.yaml"
        fi
        ;;
    *)
        echo "Soak not yet wired for $PROCESSOR. Add the case." >&2
        exit 1
        ;;
esac

if [ ! -x "$COLLECTOR_BIN" ]; then
    echo "Collector binary not found: $COLLECTOR_BIN" >&2
    echo "Build it with bash build_*.sh or builder --config <builder-config.yaml>" >&2
    exit 1
fi

# Same load shape as bench.sh: workers/hosts/metrics rectangle.
WORKERS=10; HOSTS=10; METRICS=10
TOTAL_PER_BATCH=$((WORKERS * HOSTS * METRICS))
INTERVAL_US=$(( 1000000 * TOTAL_PER_BATCH / RATE_MPS ))
INTERVAL_MS=$(echo "scale=3; $INTERVAL_US / 1000" | bc)
DURATION_SEC=$((DURATION_HOURS * 3600))

echo "==> Soak: $PROCESSOR @ ${RATE_MPS} MPS for ${DURATION_HOURS}h"
echo "    interval: ${INTERVAL_MS}ms, total samples: $((DURATION_SEC * RATE_MPS))"
echo "    output: $OUT_DIR"

# Start collector
"$COLLECTOR_BIN" --config "$CONFIG_FILE" > "$OUT_DIR/collector.log" 2>&1 &
COLLECTOR_PID=$!
trap "kill $COLLECTOR_PID 2>/dev/null || true" EXIT
sleep 3

# Start load gen in background
cd "$SCRIPT_DIR"
go run main.go \
    --endpoint=localhost:4317 \
    --workers=$WORKERS \
    --hosts=$HOSTS \
    --metrics=$METRICS \
    --interval=${INTERVAL_MS}ms \
    --duration=${DURATION_SEC}s \
    --type=gauge > "$OUT_DIR/loadgen.log" 2>&1 &
LOAD_PID=$!

# Header: minute-resolution sample
echo "ts_unix,minute,cpu_pct,rss_mb,heap_mb,fd_count" > "$OUT_DIR/resource_timeline.csv"
START=$(date +%s)
MINUTE=0
while kill -0 $COLLECTOR_PID 2>/dev/null && kill -0 $LOAD_PID 2>/dev/null; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START))
    if [ $ELAPSED -ge $DURATION_SEC ]; then break; fi

    CPU=$(ps -p $COLLECTOR_PID -o %cpu= 2>/dev/null | tr -d ' ' || echo "0")
    RSS_KB=$(ps -p $COLLECTOR_PID -o rss= 2>/dev/null | tr -d ' ' || echo "0")
    RSS_MB=$(echo "scale=1; $RSS_KB / 1024" | bc)
    HEAP_MB=$(curl -s http://localhost:8888/metrics 2>/dev/null \
        | awk '/^process_resident_memory_bytes/ && !/#/{printf "%.1f", $2/1048576}' \
        || echo "0")
    FD=$(ls /proc/$COLLECTOR_PID/fd 2>/dev/null | wc -l || echo "0")
    echo "$NOW,$MINUTE,$CPU,$RSS_MB,$HEAP_MB,$FD" >> "$OUT_DIR/resource_timeline.csv"

    MINUTE=$((MINUTE + 1))
    sleep 60
done

# Drift analysis
python3 - "$OUT_DIR/resource_timeline.csv" "$OUT_DIR/summary.md" <<'PY'
import csv, sys, statistics
inp, out = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(inp)))
if not rows:
    open(out, "w").write("# Soak summary\n\nNo samples captured.\n")
    sys.exit(0)
def col(name): return [float(r[name]) for r in rows]
def slope(ys):
    n = len(ys); xs = list(range(n))
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((x-mx)*(y-my) for x,y in zip(xs,ys))
    den = sum((x-mx)**2 for x in xs) or 1
    return num/den
rss = col("rss_mb"); heap = col("heap_mb"); cpu = col("cpu_pct")
md = [
    "# Soak summary",
    "",
    f"- Samples: {len(rows)} minutes",
    f"- RSS  mean={statistics.mean(rss):.1f}MB  max={max(rss):.1f}MB  slope={slope(rss):.3f}MB/min",
    f"- Heap mean={statistics.mean(heap):.1f}MB max={max(heap):.1f}MB slope={slope(heap):.3f}MB/min",
    f"- CPU  mean={statistics.mean(cpu):.1f}%  max={max(cpu):.1f}%",
    "",
    "Drift verdict: " + ("LEAK SUSPECTED" if slope(rss) > 0.5 else "stable"),
]
open(out, "w").write("\n".join(md) + "\n")
print("\n".join(md))
PY

echo ""
echo "==> Soak complete. See $OUT_DIR/summary.md"
