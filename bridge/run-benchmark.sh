#!/bin/bash

help() {
    echo "Benchmark the OTEL - Telegraf pipeline"
    echo "Flags:"
    echo "  -c 40000                          num metrics to send per second"
    echo "  -d 30                             duration in seconds"
    echo "  -w 10                             num workers"
    echo "  -o ./configs/otel-bench.yaml      path to otel config file"
    echo "  -t ./configs/telegraf-bench.conf  path to telegraf config file"
    echo
    echo "Total number of metrics sent = count * duration * workers"
}

_count=(40000)
_duration=(30)
_workers=(10)
_otelPath=("./configs/otel-bench.yaml")
_telegrafPath=("./configs/telegraf-bench.conf")

while getopts :c:d:o:t:w: flag; do
    case $flag in
        c) _count+=("$OPTARG");;
        d) _duration+=("$OPTARG");;
        o) _otelPath+=("$OPTARG");;
        t) _telegrafPath+=("$OPTARG");;
        w) _workers+=("$OPTARG");;
        *) {
            help
            exit 1
        };;
    esac
done

count="${_count[-1]}"
duration="${_duration[-1]}"
workers="${_workers[-1]}"
otelPath="${_otelPath[-1]}"
telegrafPath="${_telegrafPath[-1]}"

# otel and telegraf path are relative to script location, but if user provides a path we want that to be relative to where they are
base=$(pwd)
# shellcheck disable=SC2164
if [[ "${#_otelPath[@]}" -eq 1 ]]; then
    cd "$(dirname "$0")"
    otelPath="$(cd -- "$(dirname -- "$otelPath")"; pwd)/$(basename -- "$otelPath")"
    cd "$base"
else
    otelPath="$(cd -- "$(dirname -- "$otelPath")"; pwd)/$(basename -- "$otelPath")"
fi

# shellcheck disable=SC2164
if [[ "${#_telegrafPath[@]}" -eq 1 ]]; then
    cd "$(dirname "$0")"
    telegrafPath="$(cd -- "$(dirname -- "$telegrafPath")"; pwd)/$(basename -- "$telegrafPath")"
    cd "$base"
else
    telegrafPath="$(cd -- "$(dirname -- "$telegrafPath")"; pwd)/$(basename -- "$telegrafPath")"
fi

# ============================= run otel and telegraf

# script assumes we're in source directory
cd "$(dirname "$0")" || exit 1

# clear outputs
rm -rf out 2> /dev/null
mkdir out 2> /dev/null

otelOut="out/otel-out"
telegrafOut="out/telegraf-out"
telemetrygenOut="out/telemetrygen-out"

# capture both stdout and stderr into same file (this is what terminal sees; gives better sense of temporality)
(./bin/otel/OTEL --config "$otelPath" |& cat) > "$otelOut" &
otelPid=$(pgrep -f "OTEL" | head -n 1)

(./bin/telegraf/telegraf --config "$telegrafPath" |& cat) > "$telegrafOut" &
telegrafPid=$(pgrep -f "telegraf" | head -n 1)

# wait a bit to make sure neither process exits (error)
echo "Starting processes..."
sleep 5

if (! kill -0 "$otelPid" 2> /dev/null) || (! kill -0 "$telegrafPid" 2> /dev/null); then
    echo "One of OTEL or Telegraf quit prematurely, exiting..."

    # one (or both) of the pids dne, so kill will error on at least one of them (pid dne)
    kill "$otelPid" 2> /dev/null
    kill "$telegrafPid" 2> /dev/null
    exit 1
fi

echo "========== PARAMETERS ============"
echo "Rate:            $count"
echo "Duration:        ${duration}s"
echo "Workers:         $workers"
echo "Total Metrics:   $(( "$count*$duration*$workers" ))"
echo
echo "OTEL Config:     $otelPath"
echo "Telegraf Config: $telegrafPath"
echo "=================================="
echo
echo "Running..."

# total metrics = rate * duration * workers
(telemetrygen metrics \
    --otlp-insecure \
    --otlp-endpoint="localhost:4317" \
    --rate "$count" \
    --duration "$duration"s \
    --workers "$workers" \
    --unique-timeseries \
|& cat) > "$telemetrygenOut"

kill "$otelPid"
kill "$telegrafPid"

# otel stats (from opentelemetry-collector-contrib-patch/processor/kllprocessor/kll_bench.sh)
otelSent=$(grep "metrics generated" "$telemetrygenOut" | awk '{sum+=$NF} END {print sum+0}' | tr -d '"')
if [ "$otelSent" -eq 0 ]; then otelMps=0; else otelMps=$((otelSent / duration)); fi

echo
echo "OTEL Metrics/s: $otelMps"
echo

# telegraf stats
py=$(command -v python || command -v python3)
"$py" ../telegraf_benchmarks/summarize_telegraf_metrics.py --results-dir out