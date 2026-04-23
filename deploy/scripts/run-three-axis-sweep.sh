#!/usr/bin/env bash
# run-three-axis-sweep.sh — iterate over the SDK-side three-axis
# `(W, L, agg_type)` grid defined in
# docs/sdk-aggregation-cost.md. Brings up the compose
# stack once per cell, soaks, measures, tears down.
#
# The paper's §6.2 sub-sweeps are each a thin wrapper around this
# script that fixes two axes and varies the third:
#
#   6.2a time-axis:     WINDOWS="1s 15s 60s 300s" PROJECTIONS="" AGGS="dd-full"
#   6.2b label-axis:    WINDOWS="60s" PROJECTIONS=": zone:rack,zone:rack,node,zone:-" AGGS="dd-full"
#   6.2c encoding-axis: WINDOWS="60s" PROJECTIONS="zone,rack" AGGS="raw-buffer dd-full dd-delta kll cms-full hll-full"
#
# Output: CSV to stdout. Each row is one cell of the grid, produced
# by measure-baseline.py. Run as:
#
#   ./run-three-axis-sweep.sh 2>run.log > sweep-YYYYMMDD.csv
#
# Env overrides (defaults in [brackets]):
#
#   WINDOWS         space-sep PeriodicReader intervals (duration strings)
#                   [15s]
#   PROJECTIONS     space-sep AttributeFilter specs. Each item is a
#                   comma-list of label keys OR the literal ":" to
#                   mean "keep all" OR "-" to mean "drop all". The
#                   ":" escape is used because bash splits on
#                   whitespace and an empty string is painful to
#                   pass through.
#                   [:] (= one run with no filter)
#   AGGS            space-sep EXPORTER_SDK_AGG values
#                   [dd-full]
#   CARDINALITY     EXPORTER_CARDINALITY            [1000]
#   FREQ_HZ         EXPORTER_FREQ_HZ                [10]
#   SCALE           agents-${SCALE}.yml overlay     [N1]
#   SOAK_S          per-cell soak seconds           [180]
#   BYTES_WIN       --bytes-sample-window seconds   [5]
#
# Pre-req: asap/fake-exporter:dev image built with three-axis knobs
# (PR #190 onwards). Earlier builds silently ignore EXPORTER_SDK_*
# and produce rows all at the SDK default, which looks like the
# sweep is broken.

set -euo pipefail

SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
COMPOSE_DIR="${SCRIPT_DIR}/../docker-compose"

# shellcheck disable=SC2206
WINDOWS_ARR=(${WINDOWS:-15s})
# shellcheck disable=SC2206
PROJECTIONS_ARR=(${PROJECTIONS:-:})
# shellcheck disable=SC2206
AGGS_ARR=(${AGGS:-dd-full})

CARDINALITY="${CARDINALITY:-1000}"
FREQ_HZ="${FREQ_HZ:-10}"
SCALE="${SCALE:-N1}"
SOAK_S="${SOAK_S:-180}"
BYTES_WIN="${BYTES_WIN:-5}"

# decode_projection ":"  → empty string (keep all)
# decode_projection "-"  → "-"            (drop all)
# decode_projection "zone,rack" → "zone,rack"
decode_projection() {
    case "$1" in
        ":") echo "" ;;
        *)   echo "$1" ;;
    esac
}

# The b0a-raw-stream overlay is the workload-shape neutral choice —
# its sketchcol config is OTLP → batch(1s) → OTLP, which keeps the
# agent from doing anything interesting on top of whatever the SDK
# already emitted. This isolates the SDK-side axis under study.
BASELINE_OVERLAY="baseline-b0a-raw-stream.yml"
AGENT_CONFIG="sketchcol-agent-b0a-raw-stream.yaml"

head=1
for window in "${WINDOWS_ARR[@]}"; do
    for proj_raw in "${PROJECTIONS_ARR[@]}"; do
        for agg in "${AGGS_ARR[@]}"; do
            proj=$(decode_projection "$proj_raw")
            tag="w${window}-l${proj_raw}-a${agg}"

            echo "# === W=${window} L=${proj_raw} agg=${agg} ===" >&2

            cd "$COMPOSE_DIR"
            docker compose \
                -f base.yml -f "agents-${SCALE}.yml" -f "${BASELINE_OVERLAY}" \
                down >/dev/null 2>&1 || true

            env \
                EXPORTER_SDK_WINDOW="$window" \
                EXPORTER_SDK_PROJECTION="$proj" \
                EXPORTER_SDK_AGG="$agg" \
                EXPORTER_CARDINALITY="$CARDINALITY" \
                EXPORTER_FREQ_HZ="$FREQ_HZ" \
                AGENT_CONFIG="$AGENT_CONFIG" \
                docker compose \
                -f base.yml -f "agents-${SCALE}.yml" -f "${BASELINE_OVERLAY}" \
                up -d >/dev/null 2>&1

            echo "# soaking ${SOAK_S}s..." >&2
            sleep "$SOAK_S"

            # measure-baseline.py tag columns:
            #   --baseline        tag (W, L, agg combined)
            #   --rate            empty — freq_hz isn't a workload rate in
            #                     the old sense; keep the column but skip
            #                     filling it
            #   --cardinality     EXPORTER_CARDINALITY
            if (( head == 1 )); then
                python3 "${SCRIPT_DIR}/measure-baseline.py" \
                    --baseline "$tag" --scale "$SCALE" \
                    --rate "" --cardinality "$CARDINALITY" \
                    --bytes-sample-window "$BYTES_WIN"
                head=0
            else
                python3 "${SCRIPT_DIR}/measure-baseline.py" \
                    --baseline "$tag" --scale "$SCALE" \
                    --rate "" --cardinality "$CARDINALITY" \
                    --bytes-sample-window "$BYTES_WIN" \
                    | tail -n +2
            fi
        done
    done
done

cd "$COMPOSE_DIR"
docker compose \
    -f base.yml -f "agents-${SCALE}.yml" -f "${BASELINE_OVERLAY}" \
    down >/dev/null 2>&1 || true

echo "# three-axis sweep complete." >&2
