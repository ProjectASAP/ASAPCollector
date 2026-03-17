#!/bin/bash
# bench_series_agg.sh – Series-aggregation SDK benchmark.
#
# Measures the effect of collapsing N series into one sketch at the SDK side,
# transmitting sketch bytes to a no-op collector, versus a baseline of sending
# all raw samples (one LastValue point per series) to the same no-op collector.
#
# Sweep parameter: --groups  (series-per-sketch values)
#   series-per-sketch=1     → one sketch per series  (fine-grained)
#   series-per-sketch=K     → series are grouped in batches of K; K series → 1 sketch
#   series-per-sketch=0     → all series collapse into one sketch
#
# Metrics per run:
#   • SDK bandwidth  – /proc/net/dev loopback TX delta (inside e2esdkbench)
#   • SDK CPU        – syscall.Getrusage delta (inside e2esdkbench)
#   • SDK memory     – runtime.ReadMemStats per second (inside e2esdkbench)
#   • Collector CPU  – ps -o %cpu sampled every 1s (this script)
#   • Collector RSS  – ps -o rss  sampled every 1s (this script)
#
# Usage:
#   ./bench_series_agg.sh [--sketch <type>] [--duration <Ns>]
#                         [--series N] [--rate MPS]
#                         [--groups "1 10 100 1000"]
#                         [--output-dir <path>]
#
# Examples:
#   ./bench_series_agg.sh --sketch ddsketch --rate 50000 --groups "1 10 100 1000"
#   ./bench_series_agg.sh --sketch kll --duration 30s --series 500 --rate 25000

set -euo pipefail
export LC_NUMERIC=C

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTRIB_PATCH_DIR="$WORKSPACE_DIR/opentelemetry-collector-contrib-patch"
APP_DIR="$WORKSPACE_DIR/opentelemetry-app"
CMD_DIR="$CONTRIB_PATCH_DIR/cmd"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SKETCH_ARG="ddsketch"
DURATION_FLAG="60s"
OUTPUT_DIR="$(cd "$SCRIPT_DIR" && pwd)/benchmark_results/series_agg"
SERIES=1000
RATE=50000
GROUPS_ARG="1 10 100 1000"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sketch)     SKETCH_ARG="$2";    shift 2 ;;
        --duration)   DURATION_FLAG="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$(cd "$(dirname "$2")" 2>/dev/null && pwd)/$(basename "$2")"; shift 2 ;;
        --series)     SERIES="$2";        shift 2 ;;
        --rate)       RATE="$2";          shift 2 ;;
        --groups)     GROUPS_ARG="$2";    shift 2 ;;
        -h|--help)
            head -40 "$0" | grep "^#" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Convert duration string to integer seconds (e.g. "60s" → 60)
DURATION_SEC=$(echo "$DURATION_FLAG" | sed 's/[^0-9]//g')
[[ -z "$DURATION_SEC" ]] && DURATION_SEC=60

# Parse groups array
read -ra GROUPS <<< "$GROUPS_ARG"

# Samples per second per series derived from rate / series
SAMPLES_PER_SEC=$(echo "scale=6; $RATE / $SERIES" | bc)

# ---------------------------------------------------------------------------
# Collector binary / config (always nopcol for this benchmark)
# ---------------------------------------------------------------------------
COLLECTOR_BIN="$CMD_DIR/nopcol/dist/nopcol"
COLLECTOR_CONFIG="$CMD_DIR/nopcol/config-bench.yaml"
COLLECTOR_BUILD_CFG="$CMD_DIR/nopcol/builder-config.yaml"

build_nopcol() {
    if [[ -f "$COLLECTOR_BIN" && "${BUILD_ALWAYS:-0}" != "1" ]]; then
        echo "  -> Using existing nopcol binary: $COLLECTOR_BIN"
        return
    fi
    echo "  -> Building nopcol collector..."
    BUILDER_BIN="$HOME/go/bin/builder"
    if [[ ! -x "$BUILDER_BIN" ]]; then
        echo "[ERROR] OTel collector builder not found at $BUILDER_BIN"
        exit 1
    fi
    cd "$CONTRIB_PATCH_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        "$BUILDER_BIN" --config "$COLLECTOR_BUILD_CFG"
    echo "  -> nopcol build complete: $COLLECTOR_BIN"
}

# ---------------------------------------------------------------------------
# Collector resource monitor – writes col_<label>_resource.csv
# ---------------------------------------------------------------------------
start_col_monitor() {
    local pid="$1"
    local outfile="$2"

    echo "timestamp,cpu_percent,mem_mb" > "$outfile"
    (
        while kill -0 "$pid" 2>/dev/null; do
            local stats
            stats=$(ps -p "$pid" -o %cpu,rss --no-headers 2>/dev/null | awk '{$1=$1};1')
            if [[ -n "$stats" ]]; then
                local cpu mem_kb mem_mb
                cpu=$(echo "$stats" | cut -d' ' -f1)
                mem_kb=$(echo "$stats" | cut -d' ' -f2)
                [[ -z "$cpu" ]]    && cpu=0
                [[ -z "$mem_kb" ]] && mem_kb=0
                mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
                echo "$(date +%s),$cpu,$mem_mb" >> "$outfile"
            fi
            sleep 1
        done
    ) &
    COL_MONITOR_PID=$!
}

# ---------------------------------------------------------------------------
# csv_stats <file> <col_index> → prints "avg peak"
# ---------------------------------------------------------------------------
csv_stats() {
    local file="$1" col="$2"
    awk -F',' -v c="$col" '
        NR>1 { val=$c+0; sum+=val; if(val>pk) pk=val; cnt++ }
        END  { if(cnt>0) printf "%.2f %.2f", sum/cnt, pk; else printf "0 0" }
    ' "$file"
}

# ---------------------------------------------------------------------------
# Run a single e2esdkbench invocation.
# Arguments: sketch  series_per_sketch  label  out_dir
# Sets globals: LAST_SDK_JSON  LAST_COL_CSV
# ---------------------------------------------------------------------------
run_one() {
    local sketch="$1"
    local grp="$2"
    local label="$3"     # human-readable label for logs (e.g. "grp10" or "baseline")
    local out_dir="$4"

    mkdir -p "$out_dir"

    # Free ports from any previous run
    lsof -ti:4317 | xargs kill -9 2>/dev/null || true
    lsof -ti:8888 | xargs kill -9 2>/dev/null || true
    sleep 2

    # Start collector
    cd "$CONTRIB_PATCH_DIR"
    "$COLLECTOR_BIN" --config "$COLLECTOR_CONFIG" \
        > "$out_dir/collector_${label}_${RATE}mps.log" 2>&1 &
    COLLECTOR_PID=$!
    echo "    -> Collector PID: $COLLECTOR_PID (warming up 5s)..."
    sleep 5

    if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
        echo "    [ERROR] Collector failed to start; check $out_dir/collector_${label}_${RATE}mps.log"
        LAST_SDK_JSON=""
        LAST_COL_CSV=""
        return
    fi

    # Start collector resource monitor
    LAST_COL_CSV="$out_dir/col_${label}_${RATE}mps_resource.csv"
    start_col_monitor "$COLLECTOR_PID" "$LAST_COL_CSV"

    # Run SDK benchmark
    echo "    -> Running e2esdkbench (sketch=$sketch, series-per-sketch=$grp, rate=$RATE MPS)..."
    cd "$APP_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        go run ./cmd/e2esdkbench \
            --sketch-type="$sketch" \
            --endpoint="localhost:4317" \
            --series="$SERIES" \
            --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
            --series-per-sketch="$grp" \
            --duration="$DURATION_FLAG" \
            --rate-label="$RATE" \
            --output-dir="$out_dir" \
        2>&1 | tee "$out_dir/sdk_${label}_${RATE}mps.log"
    SDK_EXIT=${PIPESTATUS[0]}

    # Stop collector + monitor
    kill "$COL_MONITOR_PID" 2>/dev/null || true
    kill "$COLLECTOR_PID"   2>/dev/null || true
    wait  "$COLLECTOR_PID"  2>/dev/null || true
    sleep 1

    if [[ "$SDK_EXIT" -ne 0 ]]; then
        echo "    [WARNING] SDK benchmark exited with code $SDK_EXIT"
    fi

    # Locate the summary JSON written by e2esdkbench.
    # Filename pattern: <sketch>_<rate>mps_summary.json   (grp==1)
    #                   <sketch>_grp<N>_<rate>mps_summary.json  (grp!=1)
    if [[ "$grp" -eq 1 ]]; then
        LAST_SDK_JSON="$out_dir/${sketch}_${RATE}mps_summary.json"
    else
        LAST_SDK_JSON="$out_dir/${sketch}_grp${grp}_${RATE}mps_summary.json"
    fi
}

# ---------------------------------------------------------------------------
# print_row  sketch  grp  sdk_json  col_csv  [baseline_bytes]
# ---------------------------------------------------------------------------
print_row() {
    local sketch="$1" grp="$2" sdk_json="$3" col_csv="$4"
    local baseline_bytes="${5:-0}"

    [[ -f "$sdk_json" ]] || { echo "    [WARNING] JSON not found: $sdk_json"; return; }

    local total_bytes avg_bw_kb peak_bw_kb avg_heap sdk_cpu
    total_bytes=$(python3 -c "import json; d=json.load(open('$sdk_json')); print(d['total_bytes_sent'])" 2>/dev/null || echo 0)
    avg_bw_kb=$(python3 -c "import json; d=json.load(open('$sdk_json')); print(f\"{d['avg_bandwidth_bps']/1024:.2f}\")" 2>/dev/null || echo 0)
    peak_bw_kb=$(python3 -c "import json; d=json.load(open('$sdk_json')); print(f\"{d['peak_bandwidth_bps']/1024:.2f}\")" 2>/dev/null || echo 0)
    avg_heap=$(python3 -c "import json; d=json.load(open('$sdk_json')); print(f\"{d['avg_heap_alloc_mb']:.2f}\")" 2>/dev/null || echo 0)
    sdk_cpu=$(python3 -c "import json; d=json.load(open('$sdk_json')); print(f\"{d['sdk_cpu_percent']:.2f}\")" 2>/dev/null || echo 0)

    local col_cpu_avg col_cpu_peak col_mem_avg col_mem_peak
    read col_cpu_avg col_cpu_peak <<< "$(csv_stats "$col_csv" 2)"
    read col_mem_avg col_mem_peak <<< "$(csv_stats "$col_csv" 3)"

    local bw_reduction="N/A"
    if [[ "$baseline_bytes" -gt 0 && "$total_bytes" -gt 0 ]]; then
        bw_reduction=$(python3 -c "print(f\"{(1 - $total_bytes/$baseline_bytes)*100:.1f}%\")" 2>/dev/null || echo "N/A")
    fi

    printf "| %-10s | %7s | %10s | %11s | %10s | %8s | %8s | %8s | %10s |\n" \
        "$sketch" "grp${grp}" \
        "${avg_bw_kb} KB/s" "${peak_bw_kb} KB/s" \
        "${avg_heap} MB" "${sdk_cpu}%" \
        "${col_mem_avg} MB" "${col_cpu_avg}%" \
        "${bw_reduction}"
}

# ---------------------------------------------------------------------------
# Aggregate all JSON + resource CSVs into a single CSV
# ---------------------------------------------------------------------------
write_aggregate_csv() {
    local outfile="$OUTPUT_DIR/aggregate_summary.csv"
    echo "sketch_type,series_per_sketch,rate_mps,total_bytes_sent,avg_bandwidth_bps,peak_bandwidth_bps,avg_heap_alloc_mb,peak_heap_alloc_mb,sdk_cpu_percent,col_avg_cpu_pct,col_peak_cpu_pct,col_avg_mem_mb,col_peak_mem_mb,bw_reduction_vs_baseline_pct" \
        > "$outfile"

    # Collect baseline bytes for reduction calculation
    local baseline_json="$OUTPUT_DIR/baseline/baseline_${RATE}mps_summary.json"
    local baseline_bytes=0
    if [[ -f "$baseline_json" ]]; then
        baseline_bytes=$(python3 -c "import json; print(json.load(open('$baseline_json'))['total_bytes_sent'])" 2>/dev/null || echo 0)
    fi

    # Baseline row
    local col_csv="$OUTPUT_DIR/baseline/col_baseline_${RATE}mps_resource.csv"
    if [[ -f "$baseline_json" ]]; then
        python3 - "$baseline_json" "$col_csv" "$baseline_bytes" <<'PYEOF' >> "$outfile"
import json, sys, csv

jf, cf, bb = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(jf) as f:
    d = json.load(f)

col_cpu, col_mem = [], []
try:
    with open(cf) as f:
        for row in csv.DictReader(f):
            try:
                col_cpu.append(float(row.get('cpu_percent', 0) or 0))
                col_mem.append(float(row.get('mem_mb', 0) or 0))
            except ValueError:
                pass
except FileNotFoundError:
    pass

def avg(lst): return sum(lst)/len(lst) if lst else 0
def mx(lst):  return max(lst) if lst else 0

tb = d['total_bytes_sent']
red = (1 - tb/bb)*100 if bb > 0 else 0

print(",".join([
    "baseline", "1", str(d['rate_label_mps']),
    str(tb),
    f"{d['avg_bandwidth_bps']:.2f}", f"{d['peak_bandwidth_bps']:.2f}",
    f"{d['avg_heap_alloc_mb']:.2f}", f"{d['peak_heap_alloc_mb']:.2f}",
    f"{d['sdk_cpu_percent']:.2f}",
    f"{avg(col_cpu):.2f}", f"{mx(col_cpu):.2f}",
    f"{avg(col_mem):.2f}", f"{mx(col_mem):.2f}",
    f"{red:.1f}",
]))
PYEOF
    fi

    # Sketch group rows
    for grp in "${GROUPS[@]}"; do
        local sketch_dir="$OUTPUT_DIR/$SKETCH_ARG"
        local json_file col_csv_file
        if [[ "$grp" -eq 1 ]]; then
            json_file="$sketch_dir/${SKETCH_ARG}_${RATE}mps_summary.json"
        else
            json_file="$sketch_dir/${SKETCH_ARG}_grp${grp}_${RATE}mps_summary.json"
        fi
        col_csv_file="$sketch_dir/col_grp${grp}_${RATE}mps_resource.csv"

        [[ -f "$json_file" ]] || continue

        python3 - "$json_file" "$col_csv_file" "$baseline_bytes" <<'PYEOF' >> "$outfile"
import json, sys, csv

jf, cf, bb = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(jf) as f:
    d = json.load(f)

col_cpu, col_mem = [], []
try:
    with open(cf) as f:
        for row in csv.DictReader(f):
            try:
                col_cpu.append(float(row.get('cpu_percent', 0) or 0))
                col_mem.append(float(row.get('mem_mb', 0) or 0))
            except ValueError:
                pass
except FileNotFoundError:
    pass

def avg(lst): return sum(lst)/len(lst) if lst else 0
def mx(lst):  return max(lst) if lst else 0

tb = d['total_bytes_sent']
red = (1 - tb/bb)*100 if bb > 0 else 0

print(",".join([
    d['sketch_type'], str(d['series_per_sketch']), str(d['rate_label_mps']),
    str(tb),
    f"{d['avg_bandwidth_bps']:.2f}", f"{d['peak_bandwidth_bps']:.2f}",
    f"{d['avg_heap_alloc_mb']:.2f}", f"{d['peak_heap_alloc_mb']:.2f}",
    f"{d['sdk_cpu_percent']:.2f}",
    f"{avg(col_cpu):.2f}", f"{mx(col_cpu):.2f}",
    f"{avg(col_mem):.2f}", f"{mx(col_mem):.2f}",
    f"{red:.1f}",
]))
PYEOF
    done

    echo ""
    echo "  Aggregate summary written to: $outfile"
}

# ---------------------------------------------------------------------------
# Print final aggregate table from the CSV
# ---------------------------------------------------------------------------
print_aggregate_table() {
    local csvfile="$OUTPUT_DIR/aggregate_summary.csv"
    [[ -f "$csvfile" ]] || return

    echo ""
    echo "=================================================================="
    echo "  FINAL AGGREGATE REPORT – Series Aggregation Benchmark"
    echo "  Sketch: $SKETCH_ARG  |  Rate: $RATE MPS  |  Series: $SERIES"
    echo "=================================================================="
    echo ""
    printf "| %-10s | %7s | %10s | %11s | %10s | %8s | %8s | %8s | %10s |\n" \
        "Sketch" "Group" "BW avg" "BW peak" \
        "Heap avg" "SDK CPU%" "Col Mem" "Col CPU%" "BW reduction"
    printf "|%s|\n" "$(printf '%.0s-' {1..102})"

    tail -n +2 "$csvfile" | while IFS=',' read -r sketch grp rate tb avg_bw peak_bw avg_heap peak_heap sdk_cpu col_cpu_avg col_cpu_peak col_mem_avg col_mem_peak bw_red; do
        avg_bw_kb=$(echo "scale=2; $avg_bw / 1024" | bc)
        peak_bw_kb=$(echo "scale=2; $peak_bw / 1024" | bc)
        printf "| %-10s | %7s | %10s | %11s | %10s | %8s | %8s | %8s | %10s |\n" \
            "$sketch" "grp${grp}" \
            "${avg_bw_kb} KB/s" "${peak_bw_kb} KB/s" \
            "${avg_heap} MB" "${sdk_cpu}%" \
            "${col_mem_avg} MB" "${col_cpu_avg}%" \
            "${bw_red}%"
    done
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"

echo ""
echo "=================================================================="
echo "  SERIES AGGREGATION SDK BENCHMARK"
echo "  Sketch:   $SKETCH_ARG"
echo "  Rate:     $RATE MPS  |  Series: $SERIES  |  Duration: $DURATION_FLAG"
echo "  Groups:   ${GROUPS[*]}  (series-per-sketch values)"
echo "  Collector: nopcol (no-op receive + forward to /dev/null)"
echo "  Output:   $OUTPUT_DIR"
echo "=================================================================="

# Build nopcol if needed
echo ""
echo ">>> Building nopcol collector (if not already built)..."
build_nopcol

# ---------------------------------------------------------------------------
# Baseline run — raw LastValue gauges, one per series, no sketch aggregation
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo "  BASELINE RUN (raw samples, no sketch, series-per-sketch=1)"
echo "=================================================================="

BASELINE_DIR="$OUTPUT_DIR/baseline"
run_one "baseline" 1 "baseline" "$BASELINE_DIR"
BASELINE_JSON="$LAST_SDK_JSON"
BASELINE_COL_CSV="$LAST_COL_CSV"
BASELINE_BYTES=0
if [[ -f "$BASELINE_JSON" ]]; then
    BASELINE_BYTES=$(python3 -c "import json; print(json.load(open('$BASELINE_JSON'))['total_bytes_sent'])" 2>/dev/null || echo 0)
    echo ""
    echo "  Baseline total bytes sent: $BASELINE_BYTES"
fi

# ---------------------------------------------------------------------------
# Sketch group sweep
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo "  SKETCH GROUP SWEEP – $SKETCH_ARG (series-per-sketch in: ${GROUPS[*]})"
echo "=================================================================="

SKETCH_DIR="$OUTPUT_DIR/$SKETCH_ARG"
mkdir -p "$SKETCH_DIR"

echo ""
printf "| %-10s | %7s | %10s | %11s | %10s | %8s | %8s | %8s | %10s |\n" \
    "Sketch" "Group" "BW avg" "BW peak" \
    "Heap avg" "SDK CPU%" "Col Mem" "Col CPU%" "BW reduction"
printf "|%s|\n" "$(printf '%.0s-' {1..102})"

# Print baseline row first
if [[ -f "$BASELINE_JSON" ]]; then
    print_row "baseline" 1 "$BASELINE_JSON" "$BASELINE_COL_CSV" "$BASELINE_BYTES"
fi

for GRP in "${GROUPS[@]}"; do
    echo ""
    echo "  >> series-per-sketch: $GRP  (→ ~$((SERIES / (GRP > 0 ? GRP : SERIES) + (GRP > 0 && SERIES % GRP != 0 ? 1 : 0))) sketch(es) per export)"

    run_one "$SKETCH_ARG" "$GRP" "grp${GRP}" "$SKETCH_DIR"
    GRP_SDK_JSON="$LAST_SDK_JSON"
    GRP_COL_CSV="$LAST_COL_CSV"

    # col resource CSV is written to sketch_dir but named with grp label;
    # rename to match the expected pattern for aggregate_csv.
    EXPECTED_COL_CSV="$SKETCH_DIR/col_grp${GRP}_${RATE}mps_resource.csv"
    if [[ -f "$GRP_COL_CSV" && "$GRP_COL_CSV" != "$EXPECTED_COL_CSV" ]]; then
        cp "$GRP_COL_CSV" "$EXPECTED_COL_CSV" 2>/dev/null || true
    fi

    if [[ -f "$GRP_SDK_JSON" ]]; then
        print_row "$SKETCH_ARG" "$GRP" "$GRP_SDK_JSON" "${EXPECTED_COL_CSV}" "$BASELINE_BYTES"
    fi
done

# ---------------------------------------------------------------------------
# Aggregate + print
# ---------------------------------------------------------------------------
write_aggregate_csv
print_aggregate_table

echo "Done. All results in: $OUTPUT_DIR"
