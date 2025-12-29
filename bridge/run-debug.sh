#!/bin/bash

# only send a small number of metrics through the OTEL - KLL pipeline
# used for testing and manual verification
# the metrics telegraf observes will be output to out/telegraf-output.json

help() {
    echo "Test OTEL - Telegraf pipeline with a small number of metrics"
    echo "Telegraf will output to out/telegraf-debug.json"
    echo "Flags:"
    echo "  -c 5                              num metrics to send"
    echo "  -o configs/otel-debug.yaml      path to otel config file"
    echo "  -t configs/telegraf-debug.conf  path to telegraf config file"
}

_count=(5)
_otelPath=("./configs/otel-debug.yaml")
_telegrafPath=("./configs/telegraf-debug.conf")

while getopts :c:o:t: flag; do
    case $flag in
        c) _count+=("$OPTARG");;
        o) _otelPath+=("$OPTARG");;
        t) _telegrafPath+=("$OPTARG");;
        *) {
            help
            exit 1
        };;
    esac
done

count="${_count[-1]}"
otelPath="${_otelPath[-1]}"
telegrafPath="${_telegrafPath[-1]}"

# shellcheck disable=SC2164
absSrc=$( (cd "$(dirname "$0")"; pwd) )

# otel and telegraf path are relative to script location, but if user provides a path we want that to be relative to where they are
# shellcheck disable=SC2164
if [[ "${#_otelPath[@]}" -eq 1 ]]; then otelPath="$absSrc/$otelPath"
else otelPath="$(cd -- "$(dirname -- "$otelPath")"; pwd)/$(basename -- "$otelPath")"
fi

# shellcheck disable=SC2164
if [[ "${#_telegrafPath[@]}" -eq 1 ]]; then telegrafPath="$absSrc/$telegrafPath"
else telegrafPath="$(cd -- "$(dirname -- "$telegrafPath")"; pwd)/$(basename -- "$telegrafPath")"
fi

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

# total metrics = rate * duration
(telemetrygen metrics \
    --otlp-insecure \
    --otlp-endpoint="0.0.0.0:4317" \
    --rate "$count" \
    --duration "1s" \
    --workers 1 \
    --unique-timeseries \
|& cat) > "$telemetrygenOut"

kill "$otelPid"
kill "$telegrafPid"

echo "Finished."