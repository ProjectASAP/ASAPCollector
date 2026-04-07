#!/bin/bash

# Centralized benchmark script for OpenTelemetry Collector processors
# Usage: ./bench.sh [nopcol|countsketchcol|countsketchcol-batch|countsketchcol-window|countminsketchcol-batch|countminsketchcol-window|kll|kll-batch|kll-window|sketchcollector-batch|sketchcollector-window|sketchcollector-sdk-batch|sketchcollector-sdk-window|kll-sdk-batch|kll-sdk-window|countsketchcol-sdk-batch|countsketchcol-sdk-window|countminsketchcol-sdk-batch|countminsketchcol-sdk-window|hllcol-batch|hllcol-window|hllcol-sdk-batch|hllcol-sdk-window|gorillacol|serfcol|serfcol-qt|serfcol-1e2|serfcol-1e4|serfcol-qt-1e2|serfcol-qt-1e4|serfcol-adj|serf-transmission-xor|serf-transmission-qt]
#
# SDK variants (*-sdk-*) use fakemetricload (opentelemetry-app/cmd/fakemetricload) as the
# load generator instead of otel_collector_benchmark. For ddsketch-sdk the OTel SDK
# pre-aggregates measurements into DDSketch before export; for kll/countsketch/countminsketch/hll
# the SDK emits gauge values which the collector processor then aggregates.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTRIB_PATCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CONTRIB_PATCH_DIR/.." && pwd)"

# Processor selection
PROCESSOR="${1:-}"
if [ -z "$PROCESSOR" ]; then
    echo "Usage: $0 [nopcol|countsketchcol|countsketchcol-batch|countsketchcol-window|countminsketchcol-batch|countminsketchcol-window|kll|kll-batch|kll-window|sketchcollector-batch|sketchcollector-window|sketchcollector-sdk-batch|sketchcollector-sdk-window|kll-sdk-batch|kll-sdk-window|countsketchcol-sdk-batch|countsketchcol-sdk-window|countminsketchcol-sdk-batch|countminsketchcol-sdk-window|hllcol-batch|hllcol-window|hllcol-sdk-batch|hllcol-sdk-window|gorillacol|serfcol|serfcol-qt|serfcol-1e2|serfcol-1e4|serfcol-qt-1e2|serfcol-qt-1e4|serfcol-adj|serf-transmission-xor|serf-transmission-qt]"
    exit 1
fi

# Validate processor name
case "$PROCESSOR" in
    nopcol|countsketchcol|countsketchcol-batch|countsketchcol-window|countminsketchcol-batch|countminsketchcol-window|kll|kll-batch|kll-window|sketchcollector-batch|sketchcollector-window|sketchcollector-sdk-batch|sketchcollector-sdk-window|kll-sdk-batch|kll-sdk-window|countsketchcol-sdk-batch|countsketchcol-sdk-window|countminsketchcol-sdk-batch|countminsketchcol-sdk-window|hllcol-batch|hllcol-window|hllcol-sdk-batch|hllcol-sdk-window|gorillacol|serfcol|serfcol-qt|serfcol-1e2|serfcol-1e4|serfcol-qt-1e2|serfcol-qt-1e4|serfcol-adj|serf-transmission-xor|serf-transmission-qt)
        ;;
    *)
        echo "Error: Invalid processor '$PROCESSOR'"
        echo "Valid options: nopcol, countsketchcol, countsketchcol-batch, countsketchcol-window, countminsketchcol-batch, countminsketchcol-window, kll, kll-batch, kll-window, sketchcollector-batch, sketchcollector-window, sketchcollector-sdk-batch, sketchcollector-sdk-window, kll-sdk-batch, kll-sdk-window, countsketchcol-sdk-batch, countsketchcol-sdk-window, countminsketchcol-sdk-batch, countminsketchcol-sdk-window, hllcol-batch, hllcol-window, hllcol-sdk-batch, hllcol-sdk-window, gorillacol, serfcol, serfcol-qt, serfcol-1e2, serfcol-1e4, serfcol-qt-1e2, serfcol-qt-1e4, serfcol-adj, serf-transmission-xor, serf-transmission-qt"
        exit 1
        ;;
esac

# Set processor-specific variables
PROCESSOR_DIR="$SCRIPT_DIR/$PROCESSOR"
BUILDER_BIN="$HOME/go/bin/builder"

# Default load generator mode: "pdata" uses otel_collector_benchmark; "sdk" uses fakemetricload.
LOAD_GEN_MODE="pdata"
SDK_SKETCH_TYPE=""

# Handle different config file names
if [ "$PROCESSOR" = "kll" ]; then
    BUILDER_CONFIG="$PROCESSOR_DIR/build-config.yaml"
    CONFIG_FILE="$PROCESSOR_DIR/config-bench.yaml"
    COLLECTOR_BIN="$CONTRIB_PATCH_DIR/KLL"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "kll-batch" ] || [ "$PROCESSOR" = "kll-window" ]; then
    KLL_DIR="$SCRIPT_DIR/kll"
    BUILDER_CONFIG="$KLL_DIR/build-config.yaml"
    COLLECTOR_BIN="$CONTRIB_PATCH_DIR/KLL"
    TELEMETRY_URL="http://localhost:8888/metrics"
    if [ "$PROCESSOR" = "kll-batch" ]; then
        CONFIG_FILE="$KLL_DIR/config.yaml"
    else
        CONFIG_FILE="$KLL_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "countsketchcol-batch" ] || [ "$PROCESSOR" = "countsketchcol-window" ]; then
    CS_DIR="$SCRIPT_DIR/countsketchcol"
    BUILDER_CONFIG="$CS_DIR/builder-config.yaml"
    COLLECTOR_BIN="$CS_DIR/dist/countsketchcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
    if [ "$PROCESSOR" = "countsketchcol-batch" ]; then
        CONFIG_FILE="$CS_DIR/config-batch.yaml"
    else
        CONFIG_FILE="$CS_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "countminsketchcol-batch" ] || [ "$PROCESSOR" = "countminsketchcol-window" ]; then
    CM_DIR="$SCRIPT_DIR/countminsketchcol"
    BUILDER_CONFIG="$CM_DIR/builder-config.yaml"
    COLLECTOR_BIN="$CM_DIR/dist/countminsketchcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
    if [ "$PROCESSOR" = "countminsketchcol-batch" ]; then
        CONFIG_FILE="$CM_DIR/config-batch.yaml"
    else
        CONFIG_FILE="$CM_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "sketchcollector-batch" ] || [ "$PROCESSOR" = "sketchcollector-window" ]; then
    DD_DIR="$SCRIPT_DIR/sketchcollector"
    BUILDER_CONFIG="$DD_DIR/builder-config.yaml"
    # Builder outputs ./cmd/sketchcollector/sketchcollector (see builder-config.yaml)
    COLLECTOR_BIN="$DD_DIR/sketchcollector"
    TELEMETRY_URL="http://localhost:8888/metrics"
    if [ "$PROCESSOR" = "sketchcollector-batch" ]; then
        CONFIG_FILE="$DD_DIR/config.yaml"
    else
        CONFIG_FILE="$DD_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "sketchcollector-sdk-batch" ] || [ "$PROCESSOR" = "sketchcollector-sdk-window" ]; then
    # SDK path: OTel SDK pre-aggregates into DDSketch before export.
    # Reuses the same collector binary and config as sketchcollector-batch/window.
    DD_DIR="$SCRIPT_DIR/sketchcollector"
    BUILDER_CONFIG="$DD_DIR/builder-config.yaml"
    COLLECTOR_BIN="$DD_DIR/sketchcollector"
    TELEMETRY_URL="http://localhost:8888/metrics"
    LOAD_GEN_MODE="sdk"
    SDK_SKETCH_TYPE="ddsketch"
    if [ "$PROCESSOR" = "sketchcollector-sdk-batch" ]; then
        CONFIG_FILE="$DD_DIR/config.yaml"
    else
        CONFIG_FILE="$DD_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "kll-sdk-batch" ] || [ "$PROCESSOR" = "kll-sdk-window" ]; then
    # SDK path: OTel SDK emits gauge values; KLL processor aggregates at collector.
    KLL_DIR="$SCRIPT_DIR/kll"
    BUILDER_CONFIG="$KLL_DIR/build-config.yaml"
    COLLECTOR_BIN="$CONTRIB_PATCH_DIR/KLL"
    TELEMETRY_URL="http://localhost:8888/metrics"
    LOAD_GEN_MODE="sdk"
    SDK_SKETCH_TYPE="kll"
    if [ "$PROCESSOR" = "kll-sdk-batch" ]; then
        CONFIG_FILE="$KLL_DIR/config.yaml"
    else
        CONFIG_FILE="$KLL_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "countsketchcol-sdk-batch" ] || [ "$PROCESSOR" = "countsketchcol-sdk-window" ]; then
    CS_DIR="$SCRIPT_DIR/countsketchcol"
    BUILDER_CONFIG="$CS_DIR/builder-config.yaml"
    COLLECTOR_BIN="$CS_DIR/dist/countsketchcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
    LOAD_GEN_MODE="sdk"
    SDK_SKETCH_TYPE="countsketch"
    if [ "$PROCESSOR" = "countsketchcol-sdk-batch" ]; then
        CONFIG_FILE="$CS_DIR/config-batch.yaml"
    else
        CONFIG_FILE="$CS_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "countminsketchcol-sdk-batch" ] || [ "$PROCESSOR" = "countminsketchcol-sdk-window" ]; then
    CM_DIR="$SCRIPT_DIR/countminsketchcol"
    BUILDER_CONFIG="$CM_DIR/builder-config.yaml"
    COLLECTOR_BIN="$CM_DIR/dist/countminsketchcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
    LOAD_GEN_MODE="sdk"
    SDK_SKETCH_TYPE="countminsketch"
    if [ "$PROCESSOR" = "countminsketchcol-sdk-batch" ]; then
        CONFIG_FILE="$CM_DIR/config-batch.yaml"
    else
        CONFIG_FILE="$CM_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "hllcol-batch" ] || [ "$PROCESSOR" = "hllcol-window" ]; then
    HLL_DIR="$SCRIPT_DIR/hllcol"
    BUILDER_CONFIG="$HLL_DIR/build-config.yaml"
    COLLECTOR_BIN="$CONTRIB_PATCH_DIR/HLL"
    TELEMETRY_URL="http://localhost:8888/metrics"
    if [ "$PROCESSOR" = "hllcol-batch" ]; then
        CONFIG_FILE="$HLL_DIR/config-bench.yaml"
    else
        CONFIG_FILE="$HLL_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "hllcol-sdk-batch" ] || [ "$PROCESSOR" = "hllcol-sdk-window" ]; then
    # SDK path: OTel SDK emits gauge values; HLL processor aggregates at collector.
    HLL_DIR="$SCRIPT_DIR/hllcol"
    BUILDER_CONFIG="$HLL_DIR/build-config.yaml"
    COLLECTOR_BIN="$CONTRIB_PATCH_DIR/HLL"
    TELEMETRY_URL="http://localhost:8888/metrics"
    LOAD_GEN_MODE="sdk"
    SDK_SKETCH_TYPE="hll"
    if [ "$PROCESSOR" = "hllcol-sdk-batch" ]; then
        CONFIG_FILE="$HLL_DIR/config-bench.yaml"
    else
        CONFIG_FILE="$HLL_DIR/config-window.yaml"
    fi
elif [ "$PROCESSOR" = "serfcol-qt" ]; then
    SERF_DIR="$SCRIPT_DIR/serfcol"
    BUILDER_CONFIG="$SERF_DIR/builder-config.yaml"
    CONFIG_FILE="$SERF_DIR/config-qt.yaml"
    COLLECTOR_BIN="$SERF_DIR/dist/serfcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "serfcol-1e2" ]; then
    SERF_DIR="$SCRIPT_DIR/serfcol"
    BUILDER_CONFIG="$SERF_DIR/builder-config.yaml"
    CONFIG_FILE="$SERF_DIR/config-1e2.yaml"
    COLLECTOR_BIN="$SERF_DIR/dist/serfcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "serfcol-1e4" ]; then
    SERF_DIR="$SCRIPT_DIR/serfcol"
    BUILDER_CONFIG="$SERF_DIR/builder-config.yaml"
    CONFIG_FILE="$SERF_DIR/config-1e4.yaml"
    COLLECTOR_BIN="$SERF_DIR/dist/serfcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "serfcol-qt-1e2" ]; then
    SERF_DIR="$SCRIPT_DIR/serfcol"
    BUILDER_CONFIG="$SERF_DIR/builder-config.yaml"
    CONFIG_FILE="$SERF_DIR/config-qt-1e2.yaml"
    COLLECTOR_BIN="$SERF_DIR/dist/serfcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "serfcol-qt-1e4" ]; then
    SERF_DIR="$SCRIPT_DIR/serfcol"
    BUILDER_CONFIG="$SERF_DIR/builder-config.yaml"
    CONFIG_FILE="$SERF_DIR/config-qt-1e4.yaml"
    COLLECTOR_BIN="$SERF_DIR/dist/serfcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "serfcol-adj" ]; then
    SERF_DIR="$SCRIPT_DIR/serfcol"
    BUILDER_CONFIG="$SERF_DIR/builder-config.yaml"
    CONFIG_FILE="$SERF_DIR/config-adj.yaml"
    COLLECTOR_BIN="$SERF_DIR/dist/serfcol"
    TELEMETRY_URL="http://localhost:8888/metrics"
elif [ "$PROCESSOR" = "serf-transmission-xor" ] || [ "$PROCESSOR" = "serf-transmission-qt" ]; then
    SERF_AGENT_DIR="$SCRIPT_DIR/serfagentcol"
    SERF_BACKEND_DIR="$SCRIPT_DIR/serfbackendcol"
    AGENT_BUILDER_CONFIG="$SERF_AGENT_DIR/builder-config.yaml"
    BACKEND_BUILDER_CONFIG="$SERF_BACKEND_DIR/builder-config.yaml"
    if [ "$PROCESSOR" = "serf-transmission-qt" ]; then
        AGENT_CONFIG_FILE="$SERF_AGENT_DIR/config-qt.yaml"
        BACKEND_CONFIG_FILE="$SERF_BACKEND_DIR/config-qt.yaml"
    else
        AGENT_CONFIG_FILE="$SERF_AGENT_DIR/config.yaml"
        BACKEND_CONFIG_FILE="$SERF_BACKEND_DIR/config.yaml"
    fi
    AGENT_BIN="$SERF_AGENT_DIR/dist/serfagentcol"
    BACKEND_BIN="$SERF_BACKEND_DIR/dist/serfbackendcol"
    # For the transmission mode, COLLECTOR_BIN points to the agent (for pkill/port management).
    COLLECTOR_BIN="$AGENT_BIN"
    CONFIG_FILE="$AGENT_CONFIG_FILE"
    BUILDER_CONFIG="$AGENT_BUILDER_CONFIG"
    # Agent telemetry on 8888; backend telemetry on 8890.
    TELEMETRY_URL="http://localhost:8888/metrics"
    BACKEND_TELEMETRY_URL="http://localhost:8890/metrics"
    SERF_TRANSMISSION_MODE=1
else
    BUILDER_CONFIG="$PROCESSOR_DIR/builder-config.yaml"
    CONFIG_FILE="$PROCESSOR_DIR/config.yaml"
    COLLECTOR_BIN="$PROCESSOR_DIR/dist/$PROCESSOR"
    TELEMETRY_URL="http://localhost:8888/metrics"
fi
RESULT_DIR="$WORKSPACE_DIR/otel_collector_benchmark/benchmark_results/$PROCESSOR"
LOAD_GEN_DIR="$WORKSPACE_DIR/otel_collector_benchmark"
FAKEMETRICLOAD_DIR="$WORKSPACE_DIR/opentelemetry-app"

# Duration for each test
DURATION_SEC=60
DURATION="${DURATION_SEC}s"

# Load Settings
WORKERS=10
HOSTS=10
METRICS=10
RATES=(10000 20000 30000 40000 50000)

# Processor display names
case "$PROCESSOR" in
    nopcol)
        PROCESSOR_NAME="NOP PROCESSOR"
        ;;
    countsketchcol)
        PROCESSOR_NAME="COUNTSKETCH PROCESSOR"
        ;;
    countsketchcol-batch)
        PROCESSOR_NAME="COUNTSKETCH PROCESSOR (batch mode)"
        ;;
    countsketchcol-window)
        PROCESSOR_NAME="COUNTSKETCH PROCESSOR (window mode)"
        ;;
    countminsketchcol-batch)
        PROCESSOR_NAME="COUNTMIN SKETCH PROCESSOR (batch mode)"
        ;;
    countminsketchcol-window)
        PROCESSOR_NAME="COUNTMIN SKETCH PROCESSOR (window mode)"
        ;;
    kll)
        PROCESSOR_NAME="KLL PROCESSOR"
        ;;
    kll-batch)
        PROCESSOR_NAME="KLL PROCESSOR (batch mode)"
        ;;
    kll-window)
        PROCESSOR_NAME="KLL PROCESSOR (window mode)"
        ;;
    sketchcollector-batch)
        PROCESSOR_NAME="DDSKETCH PROCESSOR (batch mode)"
        ;;
    sketchcollector-window)
        PROCESSOR_NAME="DDSKETCH PROCESSOR (window mode)"
        ;;
    sketchcollector-sdk-batch)
        PROCESSOR_NAME="DDSKETCH PROCESSOR (batch mode, SDK pre-aggregation)"
        ;;
    sketchcollector-sdk-window)
        PROCESSOR_NAME="DDSKETCH PROCESSOR (window mode, SDK pre-aggregation)"
        ;;
    kll-sdk-batch)
        PROCESSOR_NAME="KLL PROCESSOR (batch mode, SDK gauge path)"
        ;;
    kll-sdk-window)
        PROCESSOR_NAME="KLL PROCESSOR (window mode, SDK gauge path)"
        ;;
    countsketchcol-sdk-batch)
        PROCESSOR_NAME="COUNTSKETCH PROCESSOR (batch mode, SDK gauge path)"
        ;;
    countsketchcol-sdk-window)
        PROCESSOR_NAME="COUNTSKETCH PROCESSOR (window mode, SDK gauge path)"
        ;;
    countminsketchcol-sdk-batch)
        PROCESSOR_NAME="COUNTMIN SKETCH PROCESSOR (batch mode, SDK gauge path)"
        ;;
    countminsketchcol-sdk-window)
        PROCESSOR_NAME="COUNTMIN SKETCH PROCESSOR (window mode, SDK gauge path)"
        ;;
    hllcol-batch)
        PROCESSOR_NAME="HLL PROCESSOR (batch mode)"
        ;;
    hllcol-window)
        PROCESSOR_NAME="HLL PROCESSOR (window mode)"
        ;;
    hllcol-sdk-batch)
        PROCESSOR_NAME="HLL PROCESSOR (batch mode, SDK gauge path)"
        ;;
    hllcol-sdk-window)
        PROCESSOR_NAME="HLL PROCESSOR (window mode, SDK gauge path)"
        ;;
    gorillacol)
        PROCESSOR_NAME="GORILLA COMPRESSION PROCESSOR"
        ;;
    serfcol)
        PROCESSOR_NAME="SERF XOR COMPRESSION PROCESSOR"
        ;;
    serfcol-qt)
        PROCESSOR_NAME="SERF QT COMPRESSION PROCESSOR"
        ;;
    serfcol-1e2)
        PROCESSOR_NAME="SERF XOR COMPRESSION (max_diff=1e-2, loose)"
        ;;
    serfcol-1e4)
        PROCESSOR_NAME="SERF XOR COMPRESSION (max_diff=1e-4, tight)"
        ;;
    serfcol-qt-1e2)
        PROCESSOR_NAME="SERF QT COMPRESSION (max_diff=1e-2, loose)"
        ;;
    serfcol-qt-1e4)
        PROCESSOR_NAME="SERF QT COMPRESSION (max_diff=1e-4, tight)"
        ;;
    serfcol-adj)
        PROCESSOR_NAME="SERF XOR COMPRESSION (adjust_digit=100)"
        ;;
    serf-transmission-xor)
        PROCESSOR_NAME="SERF XOR TRANSMISSION (agent compress + backend decompress)"
        ;;
    serf-transmission-qt)
        PROCESSOR_NAME="SERF QT TRANSMISSION (agent compress + backend decompress)"
        ;;
esac

# Force international number format (prevents math errors)
export LC_NUMERIC=C

mkdir -p "$RESULT_DIR"
BIN_NAME=$(basename "$COLLECTOR_BIN")

echo "=========================================================="
echo "   $PROCESSOR_NAME BENCHMARK (Zipf Distribution)"
echo "=========================================================="
echo " Builder      : $BUILDER_BIN"
echo " Binary       : $COLLECTOR_BIN"
echo " Config       : $CONFIG_FILE"
echo " Results      : $RESULT_DIR"
echo " Duration     : $DURATION per scenario"
echo " Rates        : ${RATES[*]}"
echo " Telemetry URL: $TELEMETRY_URL"
echo "=========================================================="

# --- BUILD COLLECTOR ---
echo ""
echo ">>> Building collector..."
cd "$CONTRIB_PATCH_DIR"
# GONOSUMDB/GOPRIVATE required for github.com/ProjectASAP/sketchlib-go (private repo not in sum.golang.org)
GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" $BUILDER_BIN --config "$BUILDER_CONFIG"
if [ $? -ne 0 ]; then
    echo "[WARNING] Build failed, checking for existing binary..."
    if [ -f "$COLLECTOR_BIN" ]; then
        echo ">>> Using existing binary: $COLLECTOR_BIN"
    else
        echo "[ERROR] Build failed and no existing binary found!"
        exit 1
    fi
else
    echo ">>> Build successful!"
fi

# For transmission mode, also build the backend collector.
if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
    echo ""
    echo ">>> Building backend collector..."
    cd "$CONTRIB_PATCH_DIR"
    GONOSUMDB="github.com/ProjectASAP/*" GOPRIVATE="github.com/ProjectASAP/*" $BUILDER_BIN --config "$BACKEND_BUILDER_CONFIG"
    if [ $? -ne 0 ]; then
        echo "[WARNING] Backend build failed, checking for existing binary..."
        if [ -f "$BACKEND_BIN" ]; then
            echo ">>> Using existing backend binary: $BACKEND_BIN"
        else
            echo "[ERROR] Backend build failed and no existing binary found!"
            exit 1
        fi
    else
        echo ">>> Backend build successful!"
    fi
fi

# --- CLEANUP FUNCTION ---
cleanup() {
    echo ""
    echo "Stopping all background processes..."
    # Kill collector by full path to avoid killing this script
    pkill -f "$COLLECTOR_BIN" 2>/dev/null
    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        pkill -f "$BACKEND_BIN" 2>/dev/null
    fi
    kill $MONITOR_PID 2>/dev/null
    kill $MEMORY_MONITOR_PID 2>/dev/null
    kill $BACKEND_MEMORY_MONITOR_PID 2>/dev/null
    kill $LOAD_GEN_PID 2>/dev/null
    exit
}
trap cleanup SIGINT

# Ensure clean state - kill collector by full path and free ports
pkill -f "$COLLECTOR_BIN" 2>/dev/null
if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
    pkill -f "$BACKEND_BIN" 2>/dev/null
fi
# Free port 8888 (collector telemetry) and 8889 (Prometheus exporter used by
# correctness checks) to avoid reading stale metrics from a previous run.
lsof -ti:8888 | xargs kill -9 2>/dev/null
lsof -ti:8889 | xargs kill -9 2>/dev/null
if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
    lsof -ti:8890 | xargs kill -9 2>/dev/null
    lsof -ti:8891 | xargs kill -9 2>/dev/null
    lsof -ti:9000 | xargs kill -9 2>/dev/null
fi
sleep 3

# --- HELPER FUNCTIONS ---

# Get cumulative counter value from Prometheus metrics
# Handles metrics with labels like: metric_name{label="value"} 123
get_metric_value() {
    local metric_name=$1
    local url=$2
    # Sum all values for this metric (in case there are multiple label combinations)
    curl -s "$url" | grep "^${metric_name}" | grep -v "^#" | awk '{sum += $NF} END {print sum+0}'
}

# Get memory value in MB from Prometheus metrics
get_memory_mb() {
    local url=$1
    local bytes=$(curl -s "$url" | grep "^otelcol_process_memory_rss_bytes" | grep -v "^#" | awk '{print $2}')
    if [ -z "$bytes" ] || [ "$bytes" == "0" ]; then
        echo "0"
    else
        # Use awk to handle scientific notation and convert to MB
        echo "$bytes" | awk '{printf "%.2f", $1 / 1048576}'
    fi
}

# --- MAIN LOOP ---
CORRECTNESS_FAILED=0
for RATE in "${RATES[@]}"; do
    echo ""
    echo ">>> [SCENARIO] Testing Target Load: ${RATE} MPS"
    
    # START COLLECTOR
    cd "$CONTRIB_PATCH_DIR"
    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        # Transmission mode: start backend first, then agent.
        $BACKEND_BIN --config "$BACKEND_CONFIG_FILE" > /dev/null 2>&1 &
        sleep 2
        $COLLECTOR_BIN --config "$CONFIG_FILE" > /dev/null 2>&1 &
    else
        $COLLECTOR_BIN --config "$CONFIG_FILE" > /dev/null 2>&1 &
    fi

    echo "    -> Warming up collector (5s)..."
    sleep 5

    COLLECTOR_PID=$(pgrep -f "$COLLECTOR_BIN" | head -n 1)
    if [ -z "$COLLECTOR_PID" ]; then
        echo "    [ERROR] Collector failed to start."
        exit 1
    fi
    echo "    -> Collector PID: $COLLECTOR_PID"
    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        BACKEND_PID=$(pgrep -f "$BACKEND_BIN" | head -n 1)
        if [ -z "$BACKEND_PID" ]; then
            echo "    [ERROR] Backend collector failed to start."
            pkill -f "$COLLECTOR_BIN" 2>/dev/null
            exit 1
        fi
        echo "    -> Backend PID: $BACKEND_PID"
    fi

    # RECORD START METRICS (for delta calculations)
    CPU_START=$(get_metric_value "otelcol_process_cpu_seconds_total" "$TELEMETRY_URL")
    RECEIVER_START=$(get_metric_value "otelcol_receiver_accepted_metric_points_total" "$TELEMETRY_URL")
    EXPORTER_START=$(get_metric_value "otelcol_exporter_sent_metric_points_total" "$TELEMETRY_URL")

    # Set defaults if metrics not available yet
    [ -z "$CPU_START" ] && CPU_START=0
    [ -z "$RECEIVER_START" ] && RECEIVER_START=0
    [ -z "$EXPORTER_START" ] && EXPORTER_START=0

    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        BYTES_SENT_START=$(get_metric_value "serf_exporter_bytes_sent_bytes_total" "$TELEMETRY_URL")
        BYTES_RECV_START=$(get_metric_value "serf_receiver_bytes_received_bytes_total" "$BACKEND_TELEMETRY_URL")
        BACKEND_RECV_START=$(get_metric_value "otelcol_receiver_accepted_metric_points_total" "$BACKEND_TELEMETRY_URL")
        [ -z "$BYTES_SENT_START" ] && BYTES_SENT_START=0
        [ -z "$BYTES_RECV_START" ] && BYTES_RECV_START=0
        [ -z "$BACKEND_RECV_START" ] && BACKEND_RECV_START=0
        echo "    -> Start metrics recorded (CPU: $CPU_START, Receiver: $RECEIVER_START, BytesSent: $BYTES_SENT_START)"
    else
        echo "    -> Start metrics recorded (CPU: $CPU_START, Receiver: $RECEIVER_START, Exporter: $EXPORTER_START)"
    fi

    # START RESOURCE MONITOR (Background - using ps for CPU sampling)
    METRICS_FILE="$RESULT_DIR/resource_${RATE}mps.csv"
    echo "timestamp,cpu_percent,memory_mb" > "$METRICS_FILE"

    (
        while kill -0 $COLLECTOR_PID 2>/dev/null; do
            STATS=$(ps -p $COLLECTOR_PID -o %cpu,rss --no-headers 2>/dev/null | awk '{$1=$1};1')
            if [ ! -z "$STATS" ]; then
                CPU=$(echo $STATS | cut -d' ' -f1)
                MEM_KB=$(echo $STATS | cut -d' ' -f2)
                [ -z "$CPU" ] && CPU=0
                [ -z "$MEM_KB" ] && MEM_KB=0
                MEM_MB=$(echo "scale=2; $MEM_KB / 1024" | bc)
                echo "$(date +%s),$CPU,$MEM_MB" >> "$METRICS_FILE"
            fi
            sleep 1
        done
    ) &
    MONITOR_PID=$!

    # START MEMORY PEAK MONITOR (Background - using Prometheus metrics)
    MEMORY_FILE="$RESULT_DIR/memory_${RATE}mps.csv"
    echo "timestamp,memory_mb" > "$MEMORY_FILE"
    
    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            MEM_MB=$(get_memory_mb "$TELEMETRY_URL")
            echo "$(date +%s),$MEM_MB" >> "$MEMORY_FILE"
            sleep 1
        done
    ) &
    MEMORY_MONITOR_PID=$!

    # For transmission mode, also monitor backend memory.
    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        BACKEND_MEMORY_FILE="$RESULT_DIR/backend_memory_${RATE}mps.csv"
        echo "timestamp,memory_mb" > "$BACKEND_MEMORY_FILE"
        BACKEND_CPU_START=$(get_metric_value "otelcol_process_cpu_seconds_total" "$BACKEND_TELEMETRY_URL")
        [ -z "$BACKEND_CPU_START" ] && BACKEND_CPU_START=0
        (
            END_TIME=$(( $(date +%s) + DURATION_SEC ))
            while [ $(date +%s) -lt $END_TIME ]; do
                MEM_MB=$(get_memory_mb "$BACKEND_TELEMETRY_URL")
                echo "$(date +%s),$MEM_MB" >> "$BACKEND_MEMORY_FILE"
                sleep 1
            done
        ) &
        BACKEND_MEMORY_MONITOR_PID=$!
    fi

    # START LATENCY TEST (Background)
    LATENCY_FILE="$RESULT_DIR/latency_${RATE}mps.csv"
    echo "timestamp,latency_ms,http_code" > "$LATENCY_FILE"
    
    (
        END_TIME=$(( $(date +%s) + DURATION_SEC ))
        while [ $(date +%s) -lt $END_TIME ]; do
            RESPONSE=$(curl -o /dev/null -s -w "%{time_total},%{http_code}" "$TELEMETRY_URL")
            TIME_SEC=$(echo $RESPONSE | cut -d',' -f1 | tr ',' '.')
            HTTP_CODE=$(echo $RESPONSE | cut -d',' -f2)
            [ -z "$TIME_SEC" ] && TIME_SEC=0
            LATENCY_MS=$(awk -v t="$TIME_SEC" 'BEGIN {print t * 1000}')
            echo "$(date +%s),$LATENCY_MS,$HTTP_CODE" >> "$LATENCY_FILE"
            sleep 0.2
        done
    ) &
    LATENCY_PID=$!

    # START LOAD GENERATOR (Foreground)
    LOG_FILE="$RESULT_DIR/loadgen_${RATE}.log"
    echo "    -> Generating Load with Zipf distribution..."
    
    # Calculate interval to achieve target rate
    # Rate = (workers * hosts * metrics) / interval
    # interval = (workers * hosts * metrics) / rate
    TOTAL_METRICS_PER_BATCH=$((WORKERS * HOSTS * METRICS))
    
    # Calculate interval to achieve target rate
    # Use microseconds for better precision when needed (Go's time.Duration supports this)
    INTERVAL_SEC=$(echo "scale=6; $TOTAL_METRICS_PER_BATCH / $RATE" | bc)
    INTERVAL_US=$(echo "scale=0; ($INTERVAL_SEC * 1000000) / 1" | bc)
    
    # Ensure minimum interval of 1 microsecond
    if [ "$INTERVAL_US" -lt 1 ]; then
        INTERVAL_US=1
    fi
    
    # Use microseconds for precision, Go will parse "33333us" correctly
    INTERVAL="${INTERVAL_US}us"
    
    # Convert to readable format for display
    if [ "$INTERVAL_US" -ge 1000 ]; then
        INTERVAL_DISPLAY=$(echo "scale=3; $INTERVAL_US / 1000" | bc)"ms"
    else
        INTERVAL_DISPLAY="${INTERVAL_US}us"
    fi
    echo "    -> Calculated interval: ${INTERVAL_DISPLAY} (${TOTAL_METRICS_PER_BATCH} metrics/batch @ ${RATE} MPS)"

    if [ "$LOAD_GEN_MODE" = "sdk" ]; then
        # SDK path: uses fakemetricload which instruments via the OTel SDK.
        # For ddsketch the SDK pre-aggregates into DDSketch before export.
        # For kll/countsketch/countminsketch/hll the SDK emits gauge data points
        # which the collector-side processor then aggregates into sketches.
        cd "$FAKEMETRICLOAD_DIR"
        go run ./cmd/fakemetricload \
            --endpoint="localhost:4317" \
            --sketch-type="$SDK_SKETCH_TYPE" \
            --workers=$WORKERS \
            --hosts=$HOSTS \
            --metrics=$METRICS \
            --interval=$INTERVAL \
            --duration=$DURATION > "$LOG_FILE" 2>&1 &
    else
        cd "$LOAD_GEN_DIR"
        go run main.go \
            --endpoint="localhost:4317" \
            --workers=$WORKERS \
            --hosts=$HOSTS \
            --metrics=$METRICS \
            --interval=$INTERVAL \
            --duration=$DURATION \
            --type=gauge > "$LOG_FILE" 2>&1 &
    fi
    LOAD_GEN_PID=$!
    
    # Wait for load generator to finish
    wait $LOAD_GEN_PID

    echo "    -> Test finished. Analyzing..."

    # Correctness checks: verify sketch processors emit expected metrics.
    # - Dual input (raw Gauge/Sum + sketch inputs where applicable) is validated indirectly:
    #   load gen sends raw metrics; processors emit summary/sketch metrics on 8889.
    # - Batch vs window mode: batch emits per ConsumeMetrics; window emits on tick.
    #   Cardinality: we require at least the expected summary metric names to be present.

    # For ddsketch benchmarks, perform a quick correctness check on emitted quantiles
    if [[ "$PROCESSOR" == sketchcollector* ]]; then
        if command -v curl >/dev/null 2>&1; then
            PROM_OUTPUT=$(curl -s "http://localhost:8889/metrics")
            if [ -z "$PROM_OUTPUT" ]; then
                echo "    [SKETCH CHECK] FAIL — no output on port 8889"
                CORRECTNESS_FAILED=1
            else
                P50=$(echo "$PROM_OUTPUT" | awk '/ddsketch_quantile="0.5"/ && !/^#/{print $NF; exit}')
                P90=$(echo "$PROM_OUTPUT" | awk '/ddsketch_quantile="0.9"/ && !/^#/{print $NF; exit}')
                P99=$(echo "$PROM_OUTPUT" | awk '/ddsketch_quantile="0.99"/ && !/^#/{print $NF; exit}')
                # Cardinality: expect at least one line of sketch/quantile output (dual-input or raw-only).
                METRIC_COUNT=$(echo "$PROM_OUTPUT" | grep -c 'ddsketch_quantile=' 2>/dev/null || echo "0")
                if [ -z "$P50" ] || [ -z "$P90" ] || [ -z "$P99" ]; then
                    echo "    [SKETCH CHECK] FAIL — quantile metrics missing (p50=$P50 p90=$P90 p99=$P99)"
                    CORRECTNESS_FAILED=1
                elif [ "${METRIC_COUNT:-0}" -lt 1 ]; then
                    echo "    [SKETCH CHECK] FAIL — no ddsketch_quantile metrics (cardinality)"
                    CORRECTNESS_FAILED=1
                else
                    SKETCH_MSG=$(awk -v p50="$P50" -v p90="$P90" -v p99="$P99" 'BEGIN {
                        ok = (p50 <= p90) && (p90 <= p99) && (p50 >= 0.99) && (p99 <= 507)
                        if (ok)
                            printf "    [SKETCH CHECK] PASS  p50=%.2f  p90=%.2f  p99=%.2f\n", p50, p90, p99
                        else
                            printf "    [SKETCH CHECK] FAIL  p50=%.2f  p90=%.2f  p99=%.2f  (monotonicity or bounds violated)\n", p50, p90, p99
                    }')
                    echo "$SKETCH_MSG"
                    echo "$SKETCH_MSG" | grep -q "FAIL" && CORRECTNESS_FAILED=1
                fi
            fi
        else
            echo "    [SKETCH CHECK] SKIPPED — curl not available"
        fi
    fi

    # For KLL benchmarks, perform correctness check on emitted quantiles (_p50, _p90, _p99)
    if [[ "$PROCESSOR" == kll-batch ]] || [[ "$PROCESSOR" == kll-window ]] || \
       [[ "$PROCESSOR" == kll-sdk-batch ]] || [[ "$PROCESSOR" == kll-sdk-window ]]; then
        if command -v curl >/dev/null 2>&1; then
            PROM_OUTPUT=$(curl -s "http://localhost:8889/metrics")
            if [ -z "$PROM_OUTPUT" ]; then
                echo "    [KLL CHECK] FAIL — no output on port 8889"
                CORRECTNESS_FAILED=1
            else
                P50=$(echo "$PROM_OUTPUT" | awk '/_p50[^0-9]/ && !/^#/{print $NF; exit}')
                P90=$(echo "$PROM_OUTPUT" | awk '/_p90[^0-9]/ && !/^#/{print $NF; exit}')
                P99=$(echo "$PROM_OUTPUT" | awk '/_p99[^0-9]/ && !/^#/{print $NF; exit}')
                if [ -z "$P50" ] || [ -z "$P90" ] || [ -z "$P99" ]; then
                    echo "    [KLL CHECK] FAIL — quantile metrics missing (p50=$P50 p90=$P90 p99=$P99)"
                    CORRECTNESS_FAILED=1
                else
                    KLL_MSG=$(awk -v p50="$P50" -v p90="$P90" -v p99="$P99" 'BEGIN {
                        ok = (p50 <= p90) && (p90 <= p99) && (p50 >= 0.99) && (p99 <= 507)
                        if (ok)
                            printf "    [KLL CHECK] PASS  p50=%.2f  p90=%.2f  p99=%.2f\n", p50, p90, p99
                        else
                            printf "    [KLL CHECK] FAIL  p50=%.2f  p90=%.2f  p99=%.2f  (monotonicity or bounds violated)\n", p50, p90, p99
                    }')
                    echo "$KLL_MSG"
                    echo "$KLL_MSG" | grep -q "FAIL" && CORRECTNESS_FAILED=1
                fi
            fi
        else
            echo "    [KLL CHECK] SKIPPED — curl not available"
        fi
    fi

    # For CountSketch benchmarks, verify that countsketch_row and countsketch_col
    # metadata metrics are present on the Prometheus endpoint.
    if [[ "$PROCESSOR" == countsketchcol-batch ]] || [[ "$PROCESSOR" == countsketchcol-window ]] || \
       [[ "$PROCESSOR" == countsketchcol-sdk-batch ]] || [[ "$PROCESSOR" == countsketchcol-sdk-window ]]; then
        if command -v curl >/dev/null 2>&1; then
            PROM_OUTPUT=$(curl -s "http://localhost:8889/metrics")
            if [ -z "$PROM_OUTPUT" ]; then
                echo "    [CS CHECK] FAIL — no output on port 8889"
                CORRECTNESS_FAILED=1
            else
                ROW=$(echo "$PROM_OUTPUT" | awk '/^countsketch_row/ && !/^#/{found=1; exit} END{print found+0}')
                COL=$(echo "$PROM_OUTPUT" | awk '/^countsketch_col/ && !/^#/{found=1; exit} END{print found+0}')
                if [ "$ROW" = "1" ] && [ "$COL" = "1" ]; then
                    echo "    [CS CHECK] PASS  countsketch_row and countsketch_col present"
                else
                    echo "    [CS CHECK] FAIL  missing metrics (row=$ROW col=$COL)"
                    CORRECTNESS_FAILED=1
                fi
            fi
        else
            echo "    [CS CHECK] SKIPPED — curl not available"
        fi
    fi

    # For CountMinSketch benchmarks, verify that countmin_sketch metrics are present
    # on the Prometheus endpoint.
    if [[ "$PROCESSOR" == countminsketchcol-batch ]] || [[ "$PROCESSOR" == countminsketchcol-window ]] || \
       [[ "$PROCESSOR" == countminsketchcol-sdk-batch ]] || [[ "$PROCESSOR" == countminsketchcol-sdk-window ]]; then
        if command -v curl >/dev/null 2>&1; then
            PROM_OUTPUT=$(curl -s "http://localhost:8889/metrics")
            if [ -z "$PROM_OUTPUT" ]; then
                echo "    [CMS CHECK] FAIL — no output on port 8889"
                CORRECTNESS_FAILED=1
            else
                CMS=$(echo "$PROM_OUTPUT" | awk '/^countmin_sketch/ && !/^#/{found=1; exit} END{print found+0}')
                if [ "$CMS" = "1" ]; then
                    echo "    [CMS CHECK] PASS  countmin_sketch metrics present"
                else
                    echo "    [CMS CHECK] FAIL  missing countmin_sketch metrics"
                    CORRECTNESS_FAILED=1
                fi
            fi
        else
            echo "    [CMS CHECK] SKIPPED — curl not available"
        fi
    fi

    # For HLL benchmarks, verify that hll_cardinality metrics are present
    # on the Prometheus endpoint.
    if [[ "$PROCESSOR" == hllcol-batch ]] || [[ "$PROCESSOR" == hllcol-window ]] || \
       [[ "$PROCESSOR" == hllcol-sdk-batch ]] || [[ "$PROCESSOR" == hllcol-sdk-window ]]; then
        if command -v curl >/dev/null 2>&1; then
            PROM_OUTPUT=$(curl -s "http://localhost:8889/metrics")
            if [ -z "$PROM_OUTPUT" ]; then
                echo "    [HLL CHECK] FAIL — no output on port 8889"
                CORRECTNESS_FAILED=1
            else
                HLL=$(echo "$PROM_OUTPUT" | awk '/_hll_cardinality/ && !/^#/{found=1; exit} END{print found+0}')
                if [ "$HLL" = "1" ]; then
                    echo "    [HLL CHECK] PASS  hll_cardinality metrics present"
                else
                    echo "    [HLL CHECK] FAIL  missing hll_cardinality metrics"
                    CORRECTNESS_FAILED=1
                fi
            fi
        else
            echo "    [HLL CHECK] SKIPPED — curl not available"
        fi
    fi

    # RECORD END METRICS (for delta calculations)
    CPU_END=$(get_metric_value "otelcol_process_cpu_seconds_total" "$TELEMETRY_URL")
    RECEIVER_END=$(get_metric_value "otelcol_receiver_accepted_metric_points_total" "$TELEMETRY_URL")
    EXPORTER_END=$(get_metric_value "otelcol_exporter_sent_metric_points_total" "$TELEMETRY_URL")

    [ -z "$CPU_END" ] && CPU_END=0
    [ -z "$RECEIVER_END" ] && RECEIVER_END=0
    [ -z "$EXPORTER_END" ] && EXPORTER_END=0

    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        BYTES_SENT_END=$(get_metric_value "serf_exporter_bytes_sent_bytes_total" "$TELEMETRY_URL")
        BYTES_RECV_END=$(get_metric_value "serf_receiver_bytes_received_bytes_total" "$BACKEND_TELEMETRY_URL")
        BACKEND_CPU_END=$(get_metric_value "otelcol_process_cpu_seconds_total" "$BACKEND_TELEMETRY_URL")
        BACKEND_RECV_END=$(get_metric_value "otelcol_receiver_accepted_metric_points_total" "$BACKEND_TELEMETRY_URL")
        [ -z "$BYTES_SENT_END" ] && BYTES_SENT_END=0
        [ -z "$BYTES_RECV_END" ] && BYTES_RECV_END=0
        [ -z "$BACKEND_CPU_END" ] && BACKEND_CPU_END=0
        [ -z "$BACKEND_RECV_START" ] && BACKEND_RECV_START=0
        [ -z "$BACKEND_RECV_END" ] && BACKEND_RECV_END=0
    fi

    # STOP EVERYTHING
    kill $MONITOR_PID 2>/dev/null
    kill $MEMORY_MONITOR_PID 2>/dev/null
    kill $BACKEND_MEMORY_MONITOR_PID 2>/dev/null
    kill $LATENCY_PID 2>/dev/null
    kill $COLLECTOR_PID 2>/dev/null
    wait $COLLECTOR_PID 2>/dev/null
    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        kill $BACKEND_PID 2>/dev/null
        wait $BACKEND_PID 2>/dev/null
    fi

    # CALCULATE RESULTS
    
    # A. CPU Usage (Delta calculation for cumulative counter)
    CPU_DELTA=$(echo "scale=4; $CPU_END - $CPU_START" | bc)
    CPU_PERCENT=$(echo "scale=2; ($CPU_DELTA / $DURATION_SEC) * 100" | bc)
    
    # B. Throughput (Delta calculation for cumulative counters)
    METRICS_RECEIVED=$(echo "$RECEIVER_END - $RECEIVER_START" | bc)
    METRICS_SENT=$(echo "$EXPORTER_END - $EXPORTER_START" | bc)
    
    if [ "$METRICS_RECEIVED" == "" ] || [ "$METRICS_RECEIVED" == "0" ]; then
        ACTUAL_MPS=0
        THROUGHPUT_LABEL="Data Loss Rate"
        THROUGHPUT_RESULT="N/A"
    else
        ACTUAL_MPS=$(echo "scale=0; $METRICS_RECEIVED / $DURATION_SEC" | bc)
        if awk "BEGIN{exit !($METRICS_SENT > $METRICS_RECEIVED)}"; then
            # Expansion processor (e.g. ddsketch batch appends quantile metrics)
            RATIO=$(echo "scale=2; $METRICS_SENT / $METRICS_RECEIVED" | bc)
            THROUGHPUT_LABEL="Output/Input Ratio"
            THROUGHPUT_RESULT="${RATIO}x (expansion)"
        elif [ "$METRICS_RECEIVED" -gt 0 ]; then
            THROUGHPUT_LABEL="Data Loss Rate"
            THROUGHPUT_RESULT=$(echo "scale=4; (($METRICS_RECEIVED - $METRICS_SENT) / $METRICS_RECEIVED) * 100" | bc)"%"
        else
            THROUGHPUT_LABEL="Data Loss Rate"
            THROUGHPUT_RESULT="0%"
        fi
    fi

    # C. CPU/RAM from ps monitoring
    AVG_CPU=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if (count>0) printf "%.2f", sum/count; else print "0"}' "$METRICS_FILE")
    MAX_MEM_PS=$(awk -F',' 'NR>1 {if ($3>max) max=$3} END {print max+0}' "$METRICS_FILE")
    
    # D. Memory peak from Prometheus metrics
    MAX_MEM_PROM=$(awk -F',' 'NR>1 {if ($2>max) max=$2} END {printf "%.2f", max+0}' "$MEMORY_FILE")

    # E. Latency
    tail -n +2 "$LATENCY_FILE" | cut -d',' -f2 | sort -n > sorted_lat.tmp
    LATENCY_STATS=$(awk '
    BEGIN {count=0; sum=0}
    {val=$1+0; data[count]=val; sum+=val; count++}
    END {
        if(count==0) {printf "0 0 0 0"; exit}
        avg=sum/count;
        p95=data[int(count*0.95)];
        p99=data[int(count*0.99)];
        printf "%.2f %.2f %.2f %.2f", avg, p95, p99, count
    }' sorted_lat.tmp)
    rm -f sorted_lat.tmp
    read LAT_AVG LAT_P95 LAT_P99 LAT_COUNT <<< "$LATENCY_STATS"

    # PRINT SUMMARY
    echo "    --------------------------------------------------"
    echo "    [SUMMARY RESULTS]"
    echo "    Target Rate        : $RATE MPS"
    echo "    -----------------"
    echo "    CPU (Delta Calc)   : ${CPU_DELTA}s total, ${CPU_PERCENT}% avg"
    echo "    Avg CPU (ps)       : ${AVG_CPU}%"
    echo "    Peak RAM (ps)      : ${MAX_MEM_PS} MB"
    echo "    Peak RAM (prom)    : ${MAX_MEM_PROM} MB"
    echo "    -----------------"
    echo "    Metrics Received   : ${METRICS_RECEIVED}"
    echo "    Metrics Sent       : ${METRICS_SENT}"
    echo "    Actual Throughput  : ${ACTUAL_MPS} MPS"
    echo "    ${THROUGHPUT_LABEL}  : ${THROUGHPUT_RESULT}"
    if [ "${SERF_TRANSMISSION_MODE:-0}" = "1" ]; then
        BYTES_SENT_DELTA=$(echo "$BYTES_SENT_END - $BYTES_SENT_START" | bc)
        BYTES_RECV_DELTA=$(echo "$BYTES_RECV_END - $BYTES_RECV_START" | bc)
        BACKEND_CPU_DELTA=$(echo "scale=4; $BACKEND_CPU_END - ${BACKEND_CPU_START:-0}" | bc)
        BACKEND_RECV_DELTA=$(echo "$BACKEND_RECV_END - $BACKEND_RECV_START" | bc)
        BACKEND_CPU_PERCENT=$(echo "scale=2; ($BACKEND_CPU_DELTA / $DURATION_SEC) * 100" | bc)
        BACKEND_MPS=$(echo "scale=0; $BACKEND_RECV_DELTA / $DURATION_SEC" | bc)
        BW_BPS=$(echo "scale=0; $BYTES_SENT_DELTA / $DURATION_SEC" | bc)
        BW_KBPS=$(echo "scale=2; $BYTES_SENT_DELTA / ($DURATION_SEC * 1024)" | bc)
        echo "    -----------------"
        echo "    [TRANSMISSION METRICS]"
        echo "    Compressed Bytes   : ${BYTES_SENT_DELTA} B  (${BW_KBPS} KB/s)"
        echo "    Bytes Received     : ${BYTES_RECV_DELTA} B"
        echo "    Backend Points In  : ${BACKEND_RECV_DELTA}"
        echo "    Backend Throughput : ${BACKEND_MPS} MPS"
        echo "    Backend CPU        : ${BACKEND_CPU_DELTA}s total, ${BACKEND_CPU_PERCENT}% avg"
    fi
    echo "    -----------------"
    echo "    Query Latency (Avg): ${LAT_AVG} ms"
    echo "    Query Latency (P95): ${LAT_P95} ms"
    echo "    Query Latency (P99): ${LAT_P99} ms"
    echo "    Total Queries      : ${LAT_COUNT}"
    echo "    --------------------------------------------------"

    sleep 2

done

echo ""
echo "=========================================================="
echo "All benchmarks completed. Results in $RESULT_DIR"
echo "=========================================================="
if [ "${CORRECTNESS_FAILED:-0}" = "1" ]; then
    echo "One or more correctness checks failed. Exiting with code 1."
    exit 1
fi
