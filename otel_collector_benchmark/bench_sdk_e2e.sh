#!/bin/bash
# bench_sdk_e2e.sh – End-to-end SDK (sdkSketch mode) → OTLP → Collector performance evaluation.
#
# Measures per run:
#   • SDK bandwidth   – gRPC wire bytes (measured inside the Go benchmark via stats.Handler)
#   • SDK CPU         – process user+sys time (measured inside the Go benchmark via syscall.Getrusage)
#   • SDK memory      – heap allocation (measured inside the Go benchmark via runtime.ReadMemStats)
#   • Collector CPU   – sampled from ps every second (this script)
#   • Collector memory– sampled from ps every second (this script)
#
# Usage:
#   ./bench_sdk_e2e.sh [--sketch <type|all>] [--duration <Ns>] [--output-dir <path>]
#                      [--series N] [--rates "10000 20000 30000 40000 50000"]
#
# samples-per-sec-per-series is derived as: rate / series
#
# Sketch types: ddsketch | kll | countsketch | countminsketch | hll | all
#
# Examples:
#   ./bench_sdk_e2e.sh --sketch all --duration 60s
#   ./bench_sdk_e2e.sh --sketch ddsketch --duration 30s --rates "10000 20000"

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
SKETCH_ARG="all"
DURATION_FLAG="60s"
OUTPUT_DIR="$(cd "$SCRIPT_DIR" && pwd)/benchmark_results/sdk_e2e"
SERIES=1000
RATES_ARG="10000 20000 30000 40000 50000"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sketch)      SKETCH_ARG="$2";    shift 2 ;;
        --duration)    DURATION_FLAG="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$(cd "$(dirname "$2")" 2>/dev/null && pwd)/$(basename "$2")"; shift 2 ;;
        --series)      SERIES="$2";        shift 2 ;;
        --rates)       RATES_ARG="$2";     shift 2 ;;
        -h|--help)
            head -30 "$0" | grep "^#" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Convert duration string to seconds for arithmetic (e.g. "60s" → 60)
DURATION_SEC=$(echo "$DURATION_FLAG" | sed 's/[^0-9]//g')
[[ -z "$DURATION_SEC" ]] && DURATION_SEC=60

# Expand "all" to every sketch type
if [[ "$SKETCH_ARG" == "all" ]]; then
    SKETCH_TYPES=("baseline" "ddsketch" "kll" "countsketch" "countminsketch" "hll")
else
    IFS=',' read -ra SKETCH_TYPES <<< "$SKETCH_ARG"
fi

# Parse rates array
read -ra RATES <<< "$RATES_ARG"

# ---------------------------------------------------------------------------
# Collector binary / config resolver
# ---------------------------------------------------------------------------
# Returns: COLLECTOR_BIN and COLLECTOR_CONFIG (sets vars in caller scope)
resolve_collector() {
    local sketch="$1"
    case "$sketch" in
        ddsketch)
            COLLECTOR_BIN="$CMD_DIR/ddsketchcol/ddsketchcol"
            COLLECTOR_CONFIG="$CMD_DIR/ddsketchcol/config.yaml"
            COLLECTOR_BUILD_CFG="$CMD_DIR/ddsketchcol/builder-config.yaml"
            ;;
        kll)
            COLLECTOR_BIN="$CONTRIB_PATCH_DIR/KLL"
            COLLECTOR_CONFIG="$CMD_DIR/kll/config.yaml"
            COLLECTOR_BUILD_CFG="$CMD_DIR/kll/build-config.yaml"
            ;;
        countsketch)
            COLLECTOR_BIN="$CMD_DIR/countsketchcol/dist/countsketchcol"
            COLLECTOR_CONFIG="$CMD_DIR/countsketchcol/config-batch.yaml"
            COLLECTOR_BUILD_CFG="$CMD_DIR/countsketchcol/builder-config.yaml"
            ;;
        countminsketch)
            COLLECTOR_BIN="$CMD_DIR/countminsketchcol/dist/countminsketchcol"
            COLLECTOR_CONFIG="$CMD_DIR/countminsketchcol/config-batch.yaml"
            COLLECTOR_BUILD_CFG="$CMD_DIR/countminsketchcol/builder-config.yaml"
            ;;
        hll)
            COLLECTOR_BIN="$CONTRIB_PATCH_DIR/HLL"
            COLLECTOR_CONFIG="$CMD_DIR/hllcol/config-bench.yaml"
            COLLECTOR_BUILD_CFG="$CMD_DIR/hllcol/build-config.yaml"
            ;;
        baseline)
            # nopcol: raw OTLP receive + nop processor + nop exporter — no sketch computation.
            COLLECTOR_BIN="$CMD_DIR/nopcol/dist/nopcol"
            COLLECTOR_CONFIG="$CMD_DIR/nopcol/config-bench.yaml"
            COLLECTOR_BUILD_CFG="$CMD_DIR/nopcol/builder-config.yaml"
            ;;
        *)
            echo "[ERROR] Unknown sketch type: $sketch"
            exit 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Build a collector if its binary is missing (or always if BUILD_ALWAYS=1)
# ---------------------------------------------------------------------------
build_collector() {
    local sketch="$1"
    resolve_collector "$sketch"

    if [[ -f "$COLLECTOR_BIN" && "${BUILD_ALWAYS:-0}" != "1" ]]; then
        echo "  -> Using existing binary: $COLLECTOR_BIN"
        return
    fi
    echo "  -> Building collector for $sketch ..."
    BUILDER_BIN="$HOME/go/bin/builder"
    if [[ ! -x "$BUILDER_BIN" ]]; then
        echo "[ERROR] OTel collector builder not found at $BUILDER_BIN"
        exit 1
    fi
    cd "$CONTRIB_PATCH_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
        "$BUILDER_BIN" --config "$COLLECTOR_BUILD_CFG"
    echo "  -> Build complete: $COLLECTOR_BIN"
}

# ---------------------------------------------------------------------------
# Bandwidth measurement helper (loopback interface, port 4317).
# Uses ss -ti to sum bytes_sent for all TCP connections to port 4317.
# ---------------------------------------------------------------------------
bw_bytes_sent_4317() {
    ss -tiH 'dport = :4317' 2>/dev/null \
        | grep -oP 'bytes_sent:\K[0-9]+' \
        | awk '{sum += $1} END {print sum+0}'
}

# ---------------------------------------------------------------------------
# Collector resource monitor – writes col_<rate>mps_resource.csv
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
                local cpu mem_kb
                cpu=$(echo "$stats" | cut -d' ' -f1)
                mem_kb=$(echo "$stats" | cut -d' ' -f2)
                [[ -z "$cpu" ]]    && cpu=0
                [[ -z "$mem_kb" ]] && mem_kb=0
                local mem_mb
                mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc)
                echo "$(date +%s),$cpu,$mem_mb" >> "$outfile"
            fi
            sleep 1
        done
    ) &
    COL_MONITOR_PID=$!
}

# ---------------------------------------------------------------------------
# Summarise a two-column CSV (timestamp, value) → avg, peak
# ---------------------------------------------------------------------------
csv_stats() {
    local file="$1" col="$2"
    awk -F',' -v c="$col" '
        NR>1 { val=$c+0; sum+=val; if(val>pk) pk=val; cnt++ }
        END  { if(cnt>0) printf "%.2f %.2f", sum/cnt, pk; else printf "0 0" }
    ' "$file"
}

# ---------------------------------------------------------------------------
# Print the combined report table
# ---------------------------------------------------------------------------
print_combined_row() {
    local sketch="$1" rate="$2"
    local sdk_json="$3" col_csv="$4"

    # Parse SDK summary JSON fields
    local sdk_bw_avg sdk_bw_peak sdk_heap_avg sdk_heap_peak sdk_cpu_pct sdk_total_bytes
    sdk_total_bytes=$(python3 -c "import json,sys; d=json.load(open('$sdk_json')); print(d['total_bytes_sent'])" 2>/dev/null || echo 0)
    sdk_bw_avg=$(python3 -c "import json,sys; d=json.load(open('$sdk_json')); print(f\"{d['avg_bandwidth_bps']/1024:.2f}\")" 2>/dev/null || echo 0)
    sdk_bw_peak=$(python3 -c "import json,sys; d=json.load(open('$sdk_json')); print(f\"{d['peak_bandwidth_bps']/1024:.2f}\")" 2>/dev/null || echo 0)
    sdk_heap_avg=$(python3 -c "import json,sys; d=json.load(open('$sdk_json')); print(f\"{d['avg_heap_alloc_mb']:.2f}\")" 2>/dev/null || echo 0)
    sdk_heap_peak=$(python3 -c "import json,sys; d=json.load(open('$sdk_json')); print(f\"{d['peak_heap_alloc_mb']:.2f}\")" 2>/dev/null || echo 0)
    sdk_cpu_pct=$(python3 -c "import json,sys; d=json.load(open('$sdk_json')); print(f\"{d['sdk_cpu_percent']:.2f}\")" 2>/dev/null || echo 0)

    # Collector stats from ps CSV (col 2=cpu, col 3=mem)
    local col_cpu_avg col_cpu_peak col_mem_avg col_mem_peak
    read col_cpu_avg col_cpu_peak <<< "$(csv_stats "$col_csv" 2)"
    read col_mem_avg col_mem_peak <<< "$(csv_stats "$col_csv" 3)"

    printf "| %-14s | %8d | %8s | %9s | %9s | %8s | %9s | %8s | %9s |\n" \
        "$sketch" "$rate" \
        "${sdk_bw_avg} KB/s" "${sdk_bw_peak} KB/s" \
        "${sdk_heap_avg} MB" "${sdk_heap_peak} MB" "${sdk_cpu_pct}%" \
        "${col_mem_avg} MB" "${col_cpu_avg}%"
}

# ---------------------------------------------------------------------------
# Single sketch-type benchmark loop
# ---------------------------------------------------------------------------
run_sketch_bench() {
    local sketch="$1"
    resolve_collector "$sketch"

    local sketch_dir="$OUTPUT_DIR/$sketch"
    mkdir -p "$sketch_dir"

    echo ""
    echo "=========================================================="
    echo "  SDK E2E BENCHMARK – sketch: $(echo $sketch | tr a-z A-Z)"
    echo "  Collector: $COLLECTOR_BIN"
    echo "  Config:    $COLLECTOR_CONFIG"
    echo "  Rates:     ${RATES[*]} MPS"
    echo "  Duration:  $DURATION_FLAG per rate"
    echo "  Output:    $sketch_dir"
    echo "=========================================================="

    # Print report table header
    echo ""
    printf "| %-14s | %8s | %8s | %9s | %9s | %8s | %9s | %8s | %9s |\n" \
        "Sketch" "Rate MPS" "SDK BW avg" "SDK BW peak" \
        "SDK Heap avg" "SDK Heap pk" "SDK CPU%" \
        "Col Mem avg" "Col CPU%"
    printf "|%s|\n" "$(printf '%.0s-' {1..120})"

    for RATE in "${RATES[@]}"; do
        echo ""
        echo "  >> Rate: $RATE MPS"

        # Free ports from any previous run
        lsof -ti:4317 | xargs kill -9 2>/dev/null || true
        lsof -ti:8888 | xargs kill -9 2>/dev/null || true
        sleep 2

        # --- Start collector ---
        cd "$CONTRIB_PATCH_DIR"
        "$COLLECTOR_BIN" --config "$COLLECTOR_CONFIG" > "$sketch_dir/collector_${RATE}mps.log" 2>&1 &
        COLLECTOR_PID=$!
        echo "  -> Collector PID: $COLLECTOR_PID (warming up 5s)..."
        sleep 5

        if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
            echo "  [ERROR] Collector failed to start; check $sketch_dir/collector_${RATE}mps.log"
            continue
        fi

        # --- Start collector resource monitor ---
        COL_RES_CSV="$sketch_dir/col_${RATE}mps_resource.csv"
        start_col_monitor "$COLLECTOR_PID" "$COL_RES_CSV"

        # --- Calculate samples-per-sec-per-series to hit target rate ---
        # samples_per_sec_per_series = rate / series
        SAMPLES_PER_SEC=$(echo "scale=6; $RATE / $SERIES" | bc)

        # --- Run SDK Go benchmark ---
        SDK_OUTDIR="$sketch_dir"
        echo "  -> Running SDK benchmark (series=$SERIES, samples/s/series=$SAMPLES_PER_SEC, duration=$DURATION_FLAG)..."
        cd "$APP_DIR"
        GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
            go run ./cmd/e2esdkbench \
                --sketch-type="$sketch" \
                --endpoint="localhost:4317" \
                --series="$SERIES" \
                --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
                --duration="$DURATION_FLAG" \
                --output-dir="$SDK_OUTDIR" \
            2>&1 | tee "$sketch_dir/sdk_${RATE}mps.log"
        SDK_EXIT=${PIPESTATUS[0]}

        # --- Stop collector ---
        kill "$COL_MONITOR_PID" 2>/dev/null || true
        kill "$COLLECTOR_PID"   2>/dev/null || true
        wait  "$COLLECTOR_PID"  2>/dev/null || true
        sleep 1

        if [[ "$SDK_EXIT" -ne 0 ]]; then
            echo "  [WARNING] SDK benchmark exited with code $SDK_EXIT"
        fi

        # --- Print combined report row ---
        SDK_JSON="$SDK_OUTDIR/${sketch}_${RATE}mps_summary.json"
        if [[ -f "$SDK_JSON" ]]; then
            print_combined_row "$sketch" "$RATE" "$SDK_JSON" "$COL_RES_CSV"
        else
            echo "  [WARNING] Summary JSON not found: $SDK_JSON"
        fi
    done

    echo ""
    echo "  Results saved to: $sketch_dir"
}

# ---------------------------------------------------------------------------
# Aggregate all JSON summaries into a single combined CSV
# ---------------------------------------------------------------------------
write_aggregate_csv() {
    local outfile="$OUTPUT_DIR/aggregate_summary.csv"
    echo "sketch_type,rate_mps,total_bytes_sent,avg_bandwidth_bps,peak_bandwidth_bps,avg_heap_alloc_mb,peak_heap_alloc_mb,sdk_cpu_percent,col_avg_cpu_pct,col_peak_cpu_pct,col_avg_mem_mb,col_peak_mem_mb" > "$outfile"

    for sketch in "${SKETCH_TYPES[@]}"; do
        for rate in "${RATES[@]}"; do
            local json_file="$OUTPUT_DIR/$sketch/${sketch}_${rate}mps_summary.json"
            local col_csv="$OUTPUT_DIR/$sketch/col_${rate}mps_resource.csv"
            [[ -f "$json_file" ]] || continue

            local row
            row=$(python3 - "$json_file" "$col_csv" <<'PYEOF'
import json, sys, csv

jf = sys.argv[1]
cf = sys.argv[2]

with open(jf) as f:
    d = json.load(f)

col_cpu_vals = []
col_mem_vals = []
try:
    with open(cf) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                col_cpu_vals.append(float(row.get('cpu_percent', 0) or 0))
                col_mem_vals.append(float(row.get('mem_mb', 0) or 0))
            except ValueError:
                pass
except FileNotFoundError:
    pass

def safe_avg(lst): return sum(lst)/len(lst) if lst else 0
def safe_max(lst): return max(lst) if lst else 0

print(",".join([
    d['sketch_type'],
    str(d['rate_label_mps']),
    str(d['total_bytes_sent']),
    f"{d['avg_bandwidth_bps']:.2f}",
    f"{d['peak_bandwidth_bps']:.2f}",
    f"{d['avg_heap_alloc_mb']:.2f}",
    f"{d['peak_heap_alloc_mb']:.2f}",
    f"{d['sdk_cpu_percent']:.2f}",
    f"{safe_avg(col_cpu_vals):.2f}",
    f"{safe_max(col_cpu_vals):.2f}",
    f"{safe_avg(col_mem_vals):.2f}",
    f"{safe_max(col_mem_vals):.2f}",
]))
PYEOF
)
            echo "$row" >> "$outfile"
        done
    done

    echo ""
    echo "=========================================================="
    echo "  Aggregate summary written to: $outfile"
    echo "=========================================================="
}

# ---------------------------------------------------------------------------
# Print final aggregate table from the CSV
# ---------------------------------------------------------------------------
print_aggregate_table() {
    local csvfile="$OUTPUT_DIR/aggregate_summary.csv"
    [[ -f "$csvfile" ]] || return

    echo ""
    echo "=========================================================="
    echo "  FINAL AGGREGATE REPORT – SDK E2E Benchmark"
    echo "  All sketch types in sdkSketch mode (SDK pre-aggregates)"
    echo "=========================================================="
    echo ""
    printf "| %-14s | %8s | %11s | %12s | %11s | %9s | %9s | %9s |\n" \
        "Sketch" "Rate MPS" "BW avg KB/s" "BW peak KB/s" \
        "Heap avg MB" "SDK CPU%" "Col CPU%" "Col Mem MB"
    printf "|%s|\n" "$(printf '%.0s-' {1..112})"

    tail -n +2 "$csvfile" | while IFS=',' read -r sketch rate total_bytes avg_bw peak_bw avg_heap peak_heap sdk_cpu col_cpu_avg col_cpu_peak col_mem_avg col_mem_peak; do
        avg_bw_kb=$(echo "scale=2; $avg_bw / 1024" | bc)
        peak_bw_kb=$(echo "scale=2; $peak_bw / 1024" | bc)
        printf "| %-14s | %8s | %11s | %12s | %11s | %9s | %9s | %9s |\n" \
            "$sketch" "$rate" "${avg_bw_kb}" "${peak_bw_kb}" \
            "${avg_heap}" "${sdk_cpu}%" "${col_cpu_avg}%" "${col_mem_avg}"
    done
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
mkdir -p "$OUTPUT_DIR"

echo ""
echo "=========================================================="
echo "  SDK E2E PERFORMANCE EVALUATION"
echo "  Mode: sdkSketch (SDK pre-aggregates before OTLP export)"
echo "  Sketches: ${SKETCH_TYPES[*]}"
echo "  Rates:    ${RATES[*]} MPS"
echo "  Duration: $DURATION_FLAG per rate"
echo "  Output:   $OUTPUT_DIR"
echo "=========================================================="

# Build all required collectors
echo ""
echo ">>> Building collectors..."
for sketch in "${SKETCH_TYPES[@]}"; do
    build_collector "$sketch"
done

# Run benchmarks
for sketch in "${SKETCH_TYPES[@]}"; do
    run_sketch_bench "$sketch"
done

# Aggregate results
write_aggregate_csv
print_aggregate_table

echo "Done. All results in: $OUTPUT_DIR"
