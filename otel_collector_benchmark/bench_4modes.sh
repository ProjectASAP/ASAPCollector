#!/bin/bash
# bench_4modes.sh — 4-mode OTel deployment benchmark.
#
# Compares the CPU, memory, and bandwidth at both sender (OTel SDK) and
# receiver (backend collector) across four transmission modes:
#
#   1. raw-unbatched  SDK raw Float64Gauge → agent batch(1s) → backend
#   2. raw-batched    SDK raw Float64Gauge → agent batch(10s) → backend
#   3. cms-full       SDK raw → agent CMS window(10s) full sketch → backend
#   4. cms-delta      SDK raw → agent CMS window(10s) delta sketch → backend
#   5. cs-full        SDK raw → agent CS window(10s) full sketch → backend
#   6. cs-delta       SDK raw → agent CS window(10s) delta sketch → backend
#
# NOTE on "batching raw at OTel SDK level":
#   The OTel SDK uses LastValue aggregation for Float64Gauge. At a 1s reader
#   interval, only the most recent value per series is exported — intermediate
#   samples within that second are silently dropped by the aggregation. Raw
#   unbatched (mode 1) exports every sample; raw batched (mode 2) accumulates
#   1s exports at the agent for 10s before forwarding. Both raw modes have
#   perfect fidelity limitations: the SDK does NOT buffer all intra-second
#   samples. For true all-sample raw transmission, use a sub-millisecond reader
#   interval (high bandwidth, not suitable for production comparison).
#
# Architecture:
#   e2esdkbench (baseline, 1s reader)
#       └─ OTLP gRPC :4317 ──► Agent Collector (mode-specific config)
#                                   └─ OTLP gRPC :4319 ──► Backend nopcol
#
# Bandwidth measured via /proc/net/dev loopback TX delta (inside SDK process)
# and ss bytes_sent delta on port 4319 (agent→backend, "receiver" bandwidth).
#
# Usage:
#   ./bench_4modes.sh [--sketch cms|cs|all] [--duration 60s]
#                     [--series 1000] [--rate 10000]
#                     [--output-dir ./benchmark_results/4modes]

set -euo pipefail
export LC_NUMERIC=C

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
OUTPUT_DIR="$SCRIPT_DIR/benchmark_results/4modes"
SERIES=1000
RATE=10000   # single rate for this benchmark (samples/s total)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sketch)     SKETCH_ARG="$2";    shift 2 ;;
        --duration)   DURATION_FLAG="$2"; shift 2 ;;
        --output-dir) OUTPUT_DIR="$(realpath -m "$2")"; shift 2 ;;
        --series)     SERIES="$2";        shift 2 ;;
        --rate)       RATE="$2";          shift 2 ;;
        -h|--help)
            head -40 "$0" | grep "^#" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

DURATION_SEC=$(echo "$DURATION_FLAG" | sed 's/[^0-9]//g')
[[ -z "$DURATION_SEC" ]] && DURATION_SEC=60

SAMPLES_PER_SEC=$(echo "scale=6; $RATE / $SERIES" | bc)

# Resolve which sketch types to run
if [[ "$SKETCH_ARG" == "all" ]]; then
    SKETCH_TYPES=("cms" "cs")
else
    IFS=',' read -ra SKETCH_TYPES <<< "$SKETCH_ARG"
fi

mkdir -p "$OUTPUT_DIR"

# ---------------------------------------------------------------------------
# Binary / config resolver
# ---------------------------------------------------------------------------
NOPCOL_BIN="$CMD_DIR/nopcol/dist/nopcol"
NOPCOL_BACKEND_CFG="$CMD_DIR/nopcol/config-bench-backend.yaml"

agent_bin_for() {
    local sketch="$1"
    case "$sketch" in
        cms) echo "$CMD_DIR/countminsketchcol/dist/countminsketchcol" ;;
        cs)  echo "$CMD_DIR/countsketchcol/dist/countsketchcol" ;;
    esac
}

agent_cfg_for() {
    local sketch="$1" mode="$2"
    case "$sketch" in
        cms) echo "$CMD_DIR/countminsketchcol/config-bench-${mode}.yaml" ;;
        cs)  echo "$CMD_DIR/countsketchcol/config-bench-${mode}.yaml" ;;
    esac
}

# ---------------------------------------------------------------------------
# Bandwidth helpers
# ---------------------------------------------------------------------------

# Bytes sent by agent to backend on port 4319 (via ss)
bw_agent_to_backend() {
    ss -tiH 'dport = :4319' 2>/dev/null \
        | grep -oP 'bytes_sent:\K[0-9]+' \
        | awk '{sum+=$1} END {print sum+0}'
}

# Bytes received by backend from agent on port 4319 (via ss — bytes_received on server side)
bw_backend_received() {
    ss -tiH 'sport = :4319' 2>/dev/null \
        | grep -oP 'bytes_received:\K[0-9]+' \
        | awk '{sum+=$1} END {print sum+0}'
}

# ---------------------------------------------------------------------------
# Resource monitor — writes timestamp,cpu_pct,mem_mb CSV
# ---------------------------------------------------------------------------
start_monitor() {
    local pid="$1" outfile="$2"
    echo "timestamp,cpu_percent,mem_mb" > "$outfile"
    (
        while kill -0 "$pid" 2>/dev/null; do
            stats=$(ps -p "$pid" -o %cpu,rss --no-headers 2>/dev/null | awk '{$1=$1};1')
            if [[ -n "$stats" ]]; then
                cpu=$(echo "$stats" | cut -d' ' -f1)
                mem_kb=$(echo "$stats" | cut -d' ' -f2)
                [[ -z "$cpu" ]] && cpu=0; [[ -z "$mem_kb" ]] && mem_kb=0
                mem_mb=$(echo "scale=2; $mem_kb / 1024" | bc 2>/dev/null || echo 0)
                echo "$(date +%s),$cpu,$mem_mb" >> "$outfile"
            fi
            sleep 1
        done
    ) >/dev/null 2>&1 &
    echo $!
}

csv_avg_peak() {
    local file="$1" col="$2"
    awk -F',' -v c="$col" '
        NR>1 { v=$c+0; sum+=v; if(v>pk)pk=v; n++ }
        END  { if(n) printf "%.2f/%.2f", sum/n, pk; else printf "0/0" }
    ' "$file"
}

# ---------------------------------------------------------------------------
# Kill helpers
# ---------------------------------------------------------------------------
kill_port() {
    local port="$1"
    lsof -ti:"$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Single mode run
# ---------------------------------------------------------------------------
run_mode() {
    local sketch="$1" mode="$2" mode_label="$3"
    local out_dir="$OUTPUT_DIR/${sketch}_${mode}"
    mkdir -p "$out_dir"

    local agent_bin agent_cfg
    agent_bin=$(agent_bin_for "$sketch")
    agent_cfg=$(agent_cfg_for "$sketch" "$mode")

    if [[ ! -f "$agent_bin" ]]; then
        echo "  [SKIP] Binary not found: $agent_bin"
        return
    fi
    if [[ ! -f "$agent_cfg" ]]; then
        echo "  [SKIP] Config not found: $agent_cfg"
        return
    fi

    echo ""
    echo "  ── ${sketch^^} / ${mode_label} ──────────────────────────────"

    # Free ports
    kill_port 4317; kill_port 4318; kill_port 4319; kill_port 8888; kill_port 8890
    sleep 2

    # --- Start backend nopcol (:4319) ---
    "$NOPCOL_BIN" --config "$NOPCOL_BACKEND_CFG" \
        > "$out_dir/backend.log" 2>&1 &
    BACKEND_PID=$!
    echo "  -> Backend nopcol PID $BACKEND_PID (:4319)"

    sleep 2

    # --- Start agent collector (:4317) ---
    cd "$CONTRIB_PATCH_DIR"
    "$agent_bin" --config "$agent_cfg" \
        > "$out_dir/agent.log" 2>&1 &
    AGENT_PID=$!
    echo "  -> Agent collector PID $AGENT_PID (:4317) — warming up 5s..."
    sleep 5

    if ! kill -0 "$AGENT_PID" 2>/dev/null; then
        echo "  [ERROR] Agent failed; check $out_dir/agent.log"
        kill "$BACKEND_PID" 2>/dev/null || true
        return
    fi

    # --- Start resource monitors ---
    AGENT_CSV="$out_dir/agent_resource.csv"
    BACKEND_CSV="$out_dir/backend_resource.csv"
    AGENT_MON_PID=$(start_monitor "$AGENT_PID" "$AGENT_CSV")
    BACKEND_MON_PID=$(start_monitor "$BACKEND_PID" "$BACKEND_CSV")

    # --- Measure pre-run loopback TX from agent side (port 4319) ---
    BW_PRE=$(bw_agent_to_backend)
    BACKEND_PRE=$(bw_backend_received)

    # --- Run SDK (baseline mode, 1s reader) ---
    echo "  -> SDK benchmark: series=$SERIES samples/s/series=$SAMPLES_PER_SEC duration=$DURATION_FLAG"
    E2ESDKBENCH_BIN="$APP_DIR/e2esdkbench"
    if [[ ! -f "$E2ESDKBENCH_BIN" ]]; then
        echo "  [BUILD] e2esdkbench binary not found, building..."
        cd "$APP_DIR" && \
            GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" \
            GOFLAGS="-mod=mod" go build -o e2esdkbench ./cmd/e2esdkbench 2>&1
    fi
    "$E2ESDKBENCH_BIN" \
        --sketch-type=baseline \
        --endpoint=localhost:4317 \
        --series="$SERIES" \
        --samples-per-sec-per-series="$SAMPLES_PER_SEC" \
        --duration="$DURATION_FLAG" \
        --rate-label="$RATE" \
        --output-dir="$out_dir" \
        2>&1 | tee "$out_dir/sdk.log"
    SDK_EXIT=${PIPESTATUS[0]}

    # --- Measure post-run bandwidth ---
    BW_POST=$(bw_agent_to_backend)
    BACKEND_POST=$(bw_backend_received)
    AGENT_TX_BYTES=$(( BW_POST - BW_PRE ))
    BACKEND_RX_BYTES=$(( BACKEND_POST - BACKEND_PRE ))

    # Wait a moment for final flush
    sleep 3

    # --- Stop everything ---
    kill "$AGENT_MON_PID" 2>/dev/null || true
    kill "$BACKEND_MON_PID" 2>/dev/null || true
    kill "$AGENT_PID" 2>/dev/null || true; wait "$AGENT_PID" 2>/dev/null || true
    kill "$BACKEND_PID" 2>/dev/null || true; wait "$BACKEND_PID" 2>/dev/null || true
    sleep 1

    # --- Parse SDK summary ---
    SDK_JSON="$out_dir/baseline_${RATE}mps_summary.json"
    sdk_bw_avg=0; sdk_heap_avg=0; sdk_cpu_pct=0; sdk_total_bytes=0
    if [[ -f "$SDK_JSON" ]]; then
        sdk_total_bytes=$(python3 -c "import json; d=json.load(open('$SDK_JSON')); print(d['total_bytes_sent'])" 2>/dev/null || echo 0)
        sdk_bw_avg=$(python3 -c "import json; d=json.load(open('$SDK_JSON')); print(f\"{d['avg_bandwidth_bps']/1024:.1f}\")" 2>/dev/null || echo 0)
        sdk_heap_avg=$(python3 -c "import json; d=json.load(open('$SDK_JSON')); print(f\"{d['avg_heap_alloc_mb']:.2f}\")" 2>/dev/null || echo 0)
        sdk_cpu_pct=$(python3 -c "import json; d=json.load(open('$SDK_JSON')); print(f\"{d['sdk_cpu_percent']:.2f}\")" 2>/dev/null || echo 0)
    fi

    # --- Collector stats ---
    agent_cpu=$(csv_avg_peak "$AGENT_CSV" 2)
    agent_mem=$(csv_avg_peak "$AGENT_CSV" 3)
    backend_cpu=$(csv_avg_peak "$BACKEND_CSV" 2)
    backend_mem=$(csv_avg_peak "$BACKEND_CSV" 3)

    # Derived: agent→backend BW per second
    # Use awk to ensure leading zero in fractional numbers (bc omits it, invalid JSON)
    agent_tx_kb=$(awk "BEGIN{printf \"%.2f\", $AGENT_TX_BYTES / 1024 / $DURATION_SEC}" 2>/dev/null || echo 0)
    backend_rx_kb=$(awk "BEGIN{printf \"%.2f\", $BACKEND_RX_BYTES / 1024 / $DURATION_SEC}" 2>/dev/null || echo 0)
    sdk_total_kb=$(awk "BEGIN{printf \"%.2f\", $sdk_total_bytes / 1024}" 2>/dev/null || echo 0)

    # --- Write result row to JSON ---
    cat > "$out_dir/result.json" <<RESULTEOF
{
  "sketch": "$sketch",
  "mode": "$mode",
  "mode_label": "$mode_label",
  "rate_mps": $RATE,
  "duration_sec": $DURATION_SEC,
  "sdk_total_kb": $sdk_total_kb,
  "sdk_avg_bw_kbps": $sdk_bw_avg,
  "sdk_heap_avg_mb": $sdk_heap_avg,
  "sdk_cpu_pct": $sdk_cpu_pct,
  "agent_tx_kb_total": $(awk "BEGIN{printf \"%.2f\", $AGENT_TX_BYTES / 1024}"),
  "agent_tx_kbps": $agent_tx_kb,
  "backend_rx_kb_total": $(awk "BEGIN{printf \"%.2f\", $BACKEND_RX_BYTES / 1024}"),
  "backend_rx_kbps": $backend_rx_kb,
  "agent_cpu_avg_peak": "$agent_cpu",
  "agent_mem_avg_peak_mb": "$agent_mem",
  "backend_cpu_avg_peak": "$backend_cpu",
  "backend_mem_avg_peak_mb": "$backend_mem"
}
RESULTEOF

    # --- Print row ---
    printf "  %-20s  SDK→Agent: %6s KB/s (%s KB total)   Agent→Backend: %6s KB/s\n" \
        "$mode_label" "$sdk_bw_avg" "$sdk_total_kb" "$agent_tx_kb"
    printf "  %-20s  SDK: CPU=%s%% Heap=%sMB   Agent: CPU=%s Mem=%sMB   Backend: CPU=%s Mem=%sMB\n" \
        "" "$sdk_cpu_pct" "$sdk_heap_avg" \
        "$agent_cpu" "$agent_mem" \
        "$backend_cpu" "$backend_mem"

    echo "  -> Results: $out_dir/"
}

# ---------------------------------------------------------------------------
# Print final table
# ---------------------------------------------------------------------------
print_table() {
    echo ""
    echo "=========================================================="
    echo "  4-MODE TRANSMISSION BENCHMARK RESULTS"
    echo "  Rate: $RATE MPS  Series: $SERIES  Duration: $DURATION_FLAG"
    echo "  NOTE: OTel SDK Float64Gauge uses LastValue — raw modes"
    echo "        transmit 1 point/series/s (not all intra-second samples)."
    echo "  Batching raw at sketch cadence: valid at agent (batch processor);"
    echo "  ALL samples within a 1s SDK export are already in one gRPC call."
    echo "=========================================================="
    echo ""
    printf "%-8s %-20s %12s %12s %12s %10s %12s %12s\n" \
        "Sketch" "Mode" "SDK→Agnt KB/s" "Agnt→Bcnd KB/s" "SDK Heap MB" "SDK CPU%" "Agent CPU%" "Agent Mem MB"
    printf "%s\n" "$(printf '%.0s-' {1..108})"

    for sketch in "${SKETCH_TYPES[@]}"; do
        for mode in raw-unbatched raw-batched "${sketch}-full" "${sketch}-delta"; do
            local json="$OUTPUT_DIR/${sketch}_${mode}/result.json"
            [[ -f "$json" ]] || continue
            python3 - "$json" "$sketch" "$mode" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1]))
sketch = sys.argv[2]
mode   = sys.argv[3]
labels = {
    "raw-unbatched":   "raw-unbatched(1s)",
    "raw-batched":     "raw-batched(10s)",
    f"{sketch}-full":  f"{sketch}-full",
    f"{sketch}-delta": f"{sketch}-delta",
}
label = labels.get(mode, mode)
print(f"{d['sketch']:<8} {label:<20} {d['sdk_avg_bw_kbps']:>12} {d['backend_rx_kbps']:>14} "
      f"{d['sdk_heap_avg_mb']:>12} {d['sdk_cpu_pct']:>10} "
      f"{d['agent_cpu_avg_peak'].split('/')[0]:>12} {d['agent_mem_avg_peak_mb'].split('/')[0]:>12}")
PYEOF
        done
    done
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo ""
echo "=========================================================="
echo "  4-MODE TRANSMISSION BENCHMARK"
echo "  Sketches: ${SKETCH_TYPES[*]}"
echo "  Rate: $RATE MPS  Series: $SERIES  Duration: $DURATION_FLAG"
echo "  Output: $OUTPUT_DIR"
echo "=========================================================="

# Check required binaries
for sketch in "${SKETCH_TYPES[@]}"; do
    bin=$(agent_bin_for "$sketch")
    if [[ ! -f "$bin" ]]; then
        echo "[ERROR] Missing binary: $bin — build it first"
        exit 1
    fi
done
if [[ ! -f "$NOPCOL_BIN" ]]; then
    echo "[ERROR] Missing nopcol binary: $NOPCOL_BIN"
    exit 1
fi

# Run all modes
for sketch in "${SKETCH_TYPES[@]}"; do
    run_mode "$sketch" "raw-unbatched"      "raw-unbatched(1s)"
    run_mode "$sketch" "raw-batched"        "raw-batched(10s)"
    run_mode "$sketch" "${sketch}-full"     "${sketch}-full"
    run_mode "$sketch" "${sketch}-delta"    "${sketch}-delta"
done

print_table

echo ""
echo "All results in: $OUTPUT_DIR"
