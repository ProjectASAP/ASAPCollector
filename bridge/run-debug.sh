#!/bin/bash

# only send a small number of metrics through the OTEL - KLL pipeline
# used for testing and manual verification
# the metrics telegraf observes will be output to out/telegraf-output.json

# script assumes we're in source directory
cd "$(dirname "$0")" || exit 1

mkdir out 2> /dev/null
otelOut="out/otel-out"
telegrafOut="out/telegraf-out"
telemetrygenOut="out/telemetrygen-out"

# capture both stdout and stderr into same file (this is what terminal sees; gives better sense of temporality)
(./bin/otel/OTEL --config ./configs/otel-debug.yaml |& cat) > "$otelOut" &
otelPid=$(pgrep -f "OTEL" | head -n 1)

(./bin/telegraf/telegraf --config ./configs/telegraf-debug.conf |& cat) > "$telegrafOut" &
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
(telemetrygen metrics --otlp-insecure --otlp-endpoint="localhost:4317" --rate 5 --duration 1s --workers 1 --unique-timeseries |& cat) > "$telemetrygenOut"

kill "$otelPid"
kill "$telegrafPid"
