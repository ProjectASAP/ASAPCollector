#!/usr/bin/env bash
# run_gorilla_local.sh — Capture resource CSVs for the Telegraf+Gorilla
# local-disk benchmark. No AWS dependency.
#
# Pairs the existing `max-throughput-gorilla-local.conf` Telegraf config
# with the `send_firehose.py` line-protocol generator. Samples telegraf's
# CPU% and RSS once per second; afterward summarises via
# `summarize_telegraf_metrics.py`.
#
# Prerequisites:
#   - A local telegraf binary built with the gorilla plugin from
#     DataCollector/telegraf-patch/. Path to it must be exported as
#     TELEGRAF_BIN, otherwise we look in PATH.
#   - python3 with the existing send_firehose.py deps (asyncio).
#
# Usage:
#   TELEGRAF_BIN=/path/to/telegraf ./run_gorilla_local.sh [duration_sec] [rate_lps]
# Example:
#   TELEGRAF_BIN=/tmp/telegraf ./run_gorilla_local.sh 300 50000

set -euo pipefail

DURATION_SEC="${1:-300}"
RATE_LPS="${2:-50000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/max-throughput-gorilla-local.conf"
OUT_DIR="$SCRIPT_DIR/results/gorilla_local_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

TELEGRAF_BIN="${TELEGRAF_BIN:-$(command -v telegraf 2>/dev/null || true)}"
if [ -z "$TELEGRAF_BIN" ] || [ ! -x "$TELEGRAF_BIN" ]; then
    cat >&2 <<EOF
Telegraf binary not found.

Build the patched telegraf first:
    cd /home/zeying/repos/DataCollector/telegraf-patch
    go build -o /tmp/telegraf ./cmd/telegraf

Then re-run:
    TELEGRAF_BIN=/tmp/telegraf $0 $@
EOF
    exit 1
fi

# Ensure the local output dir exists (the gorilla-local config writes here).
LOCAL_DIR="$(awk -F'=' '/^[[:space:]]*local_dir[[:space:]]*=/ {gsub(/[" ]/,"",$2); print $2; exit}' "$CONFIG")"
[ -n "$LOCAL_DIR" ] && mkdir -p "$LOCAL_DIR"

echo "==> Telegraf+Gorilla bench: ${DURATION_SEC}s @ ${RATE_LPS} lps"
echo "    output: $OUT_DIR"
echo "    binary: $TELEGRAF_BIN"
echo "    config: $CONFIG"

"$TELEGRAF_BIN" --config "$CONFIG" --pprof-addr localhost:6062 \
    > "$OUT_DIR/telegraf.log" 2>&1 &
TG_PID=$!
trap "kill $TG_PID 2>/dev/null || true" EXIT
sleep 5  # warmup

# Resource sampler @ 1 Hz
echo "ts,cpu_pct,rss_mb,fd_count" > "$OUT_DIR/resource.csv"
START=$(date +%s)
(
    while kill -0 $TG_PID 2>/dev/null; do
        NOW=$(date +%s)
        [ $((NOW - START)) -ge $DURATION_SEC ] && break
        CPU=$(ps -p $TG_PID -o %cpu= 2>/dev/null | tr -d ' ' || echo 0)
        RSS_KB=$(ps -p $TG_PID -o rss= 2>/dev/null | tr -d ' ' || echo 0)
        RSS_MB=$(echo "scale=1; $RSS_KB / 1024" | bc)
        FD=$(ls /proc/$TG_PID/fd 2>/dev/null | wc -l || echo 0)
        echo "$NOW,$CPU,$RSS_MB,$FD" >> "$OUT_DIR/resource.csv"
        sleep 1
    done
) &
SAMPLER=$!

# Drive the firehose
python3 "$SCRIPT_DIR/send_firehose.py" \
    --target tcp://localhost:8094 \
    --rate "$RATE_LPS" \
    --duration "$DURATION_SEC" \
    > "$OUT_DIR/firehose.log" 2>&1 || true

wait $SAMPLER 2>/dev/null || true
sleep 2

# Summarise via the existing tool
python3 "$SCRIPT_DIR/summarize_telegraf_metrics.py" \
    --telegraf-log "$OUT_DIR/telegraf.log" \
    --resource-csv "$OUT_DIR/resource.csv" \
    > "$OUT_DIR/summary.md" 2>&1 || \
    echo "(summarize_telegraf_metrics.py argv may differ — see telegraf.log)" \
    > "$OUT_DIR/summary.md"

# Disk-bytes written by gorilla output
if [ -n "$LOCAL_DIR" ] && [ -d "$LOCAL_DIR" ]; then
    DU=$(du -sb "$LOCAL_DIR" 2>/dev/null | awk '{print $1}')
    echo "" >> "$OUT_DIR/summary.md"
    echo "## Disk usage" >> "$OUT_DIR/summary.md"
    echo "- gorilla_local output: $DU bytes ($(du -sh "$LOCAL_DIR" | awk '{print $1}'))" >> "$OUT_DIR/summary.md"
fi

kill $TG_PID 2>/dev/null || true
echo ""
echo "==> Done. See $OUT_DIR/summary.md and $OUT_DIR/resource.csv"
