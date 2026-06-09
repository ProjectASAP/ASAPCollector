#!/usr/bin/env bash
# A/B CPU comparison for otel-app across SDK aggregation modes.
# Identical workload; only -agg varies. Measures producer CPU-seconds
# (/usr/bin/time), max RSS, exported datapoints/bytes (from the discard
# sink), and a CPU pprof profile per mode.
set -u
cd "$(dirname "$0")/.."        # otel-app/
OUT="abtest/results"
mkdir -p "$OUT"
APP=./otel-app
SINK=./sink/sink

FREQ=200
CARD=500
WINDOW=15s
DUR=60          # seconds
PROF_DELAY=12   # start profiling after warmup
PROF_SECS=40    # profile duration
PPROF_PORT=6060
SINK_ADDR=:4317

: > "$OUT/summary.txt"
echo "params: freq_hz=$FREQ cardinality=$CARD sdk_window=$WINDOW duration=${DUR}s" | tee -a "$OUT/summary.txt"
echo "nproc=$(nproc) GOMAXPROCS=${GOMAXPROCS:-default}" | tee -a "$OUT/summary.txt"
echo "" | tee -a "$OUT/summary.txt"

for AGG in default dd-full raw-buffer; do
  echo "==================== AGG=$AGG ====================" | tee -a "$OUT/summary.txt"

  # fresh sink
  $SINK "$SINK_ADDR" > "$OUT/sink-$AGG.log" 2>&1 &
  SINK_PID=$!
  # wait for sink to be listening
  for i in $(seq 1 50); do
    grep -q "sink listening" "$OUT/sink-$AGG.log" 2>/dev/null && break
    kill -0 "$SINK_PID" 2>/dev/null || break
    bash -c 'read -t 0.2 _ < <(:) || true'
  done

  # producer under /usr/bin/time
  /usr/bin/time -v -o "$OUT/time-$AGG.txt" \
    $APP -target=localhost:4317 -agg="$AGG" -freq-hz=$FREQ -cardinality=$CARD \
         -sdk-window=$WINDOW -duration=${DUR}s -pprof-addr=:$PPROF_PORT \
         > "$OUT/app-$AGG.log" 2>&1 &
  APP_PID=$!

  # capture CPU profile mid-run
  ( bash -c "read -t $PROF_DELAY _ < <(:) || true"
    curl -s "http://localhost:$PPROF_PORT/debug/pprof/profile?seconds=$PROF_SECS" \
      -o "$OUT/cpu-$AGG.pprof" ) &
  PROF_PID=$!

  wait "$APP_PID"
  wait "$PROF_PID" 2>/dev/null

  # drain + stop sink to print stats
  kill -TERM "$SINK_PID" 2>/dev/null
  wait "$SINK_PID" 2>/dev/null

  # collect
  USER_S=$(grep "User time" "$OUT/time-$AGG.txt" | grep -oE "[0-9.]+")
  SYS_S=$(grep "System time" "$OUT/time-$AGG.txt" | grep -oE "[0-9.]+")
  PCT=$(grep "Percent of CPU" "$OUT/time-$AGG.txt" | grep -oE "[0-9]+%")
  RSS_KB=$(grep "Maximum resident" "$OUT/time-$AGG.txt" | grep -oE "[0-9]+")
  STATS=$(grep "SINK_STATS" "$OUT/sink-$AGG.log" | tail -1)
  CPU_TOTAL=$(awk "BEGIN{printf \"%.2f\", ${USER_S:-0}+${SYS_S:-0}}")
  RSS_MB=$(awk "BEGIN{printf \"%.0f\", ${RSS_KB:-0}/1024}")
  {
    echo "  CPU_total_s = $CPU_TOTAL  (user=$USER_S sys=$SYS_S, %CPU=$PCT)"
    echo "  MaxRSS      = ${RSS_MB} MB"
    echo "  sink        = $STATS"
  } | tee -a "$OUT/summary.txt"
  echo "" | tee -a "$OUT/summary.txt"
done

echo "DONE" | tee -a "$OUT/summary.txt"
