#!/usr/bin/env bash
# run_freshness_phase.sh — drive the three MVP demo freshness probe
# paths sequentially, one CSV per path.
#
# Each path's freshness is measured by polling a Prometheus-style
# /api/v1/query endpoint for the corresponding probe metric emitted
# by the fake-exporter (see deploy/fake-exporter/probes.go and
# deploy/configs/mvp-freshness-probes.yaml). Per-path delta math
# is in deploy/scripts/measure_freshness.py.
#
# Three paths:
#
#   raw      → http://prometheus-b0:9090   (B0 Prometheus)
#   warm     → http://backend:8080         (sketch warm tier)
#   archive  → http://backend:8080         (Gorilla-archive)
#
# Outputs:
#
#   $OUT_DIR/freshness/raw.csv
#   $OUT_DIR/freshness/warm.csv
#   $OUT_DIR/freshness/archive.csv
#
# The driver intentionally runs the three paths sequentially rather
# than in parallel — each call to measure_freshness.py is poll-only
# (the producer side is the fake-exporter, which has been emitting
# all three probes the whole time), and serializing makes the
# stderr summary lines easy to read in the demo log.
#
# Usage:
#
#   run_freshness_phase.sh \
#       --out-dir /tmp/mvp-run-$(date +%s) \
#       --duration 60 \
#       --poll-interval-ms 100 \
#       --raw-endpoint http://prometheus-b0:9090 \
#       --warm-endpoint http://backend:8080 \
#       --archive-endpoint http://backend:8080
#
# All flags have defaults that match the demo's compose stack;
# typical invocation is just:
#
#   run_freshness_phase.sh --out-dir /tmp/run

set -euo pipefail

# ── arg parsing ─────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MEASURE_SCRIPT="${SCRIPT_DIR}/measure_freshness.py"

OUT_DIR=""
DURATION=60
POLL_INTERVAL_MS=100
# Defaults match the docker-compose service names used by the
# multi-stage overlay (see deploy/docker-compose/mvp-multi-stage.yml).
# Override with the flags below if you're running outside the compose
# stack (e.g. host-mode against published ports).
RAW_ENDPOINT="${ASAP_FRESHNESS_RAW_ENDPOINT:-http://prometheus-b0:9090}"
WARM_ENDPOINT="${ASAP_FRESHNESS_WARM_ENDPOINT:-http://backend:8080}"
ARCHIVE_ENDPOINT="${ASAP_FRESHNESS_ARCHIVE_ENDPOINT:-http://backend:8080}"

usage() {
    sed -n '/^# Usage:/,/^set -euo/{/^set -euo/!p}' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --poll-interval-ms) POLL_INTERVAL_MS="$2"; shift 2 ;;
        --raw-endpoint) RAW_ENDPOINT="$2"; shift 2 ;;
        --warm-endpoint) WARM_ENDPOINT="$2"; shift 2 ;;
        --archive-endpoint) ARCHIVE_ENDPOINT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "${OUT_DIR}" ]]; then
    echo "error: --out-dir is required" >&2
    usage
    exit 1
fi
if [[ ! -f "${MEASURE_SCRIPT}" ]]; then
    echo "error: measure_freshness.py not found at ${MEASURE_SCRIPT}" >&2
    exit 1
fi

FRESH_DIR="${OUT_DIR}/freshness"
mkdir -p "${FRESH_DIR}"

# ── per-path runner ─────────────────────────────────────────────────

# fire <path-label> <probe-metric> <query-endpoint>
fire() {
    local path_label="$1"
    local probe="$2"
    local endpoint="$3"
    local out_csv="${FRESH_DIR}/${path_label}.csv"

    echo "[$(date -Is)] freshness phase: path=${path_label} probe=${probe} endpoint=${endpoint} out=${out_csv}"
    python3 "${MEASURE_SCRIPT}" \
        --query-endpoint "${endpoint}" \
        --probe "${probe}" \
        --path-label "${path_label}" \
        --duration "${DURATION}" \
        --poll-interval-ms "${POLL_INTERVAL_MS}" \
        --output "${out_csv}"
}

# Sequential — see header comment for rationale.
fire raw     http_freshness_probe_raw     "${RAW_ENDPOINT}"
fire warm    http_freshness_probe_warm    "${WARM_ENDPOINT}"
fire archive http_freshness_probe_archive "${ARCHIVE_ENDPOINT}"

echo "[$(date -Is)] freshness phase complete; CSVs in ${FRESH_DIR}"
