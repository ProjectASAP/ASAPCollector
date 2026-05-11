#!/usr/bin/env bash
# verify_thanos_sidecar.sh — sanity-check the MVP archive-tier sidecar.
#
# Run after `docker compose ... -f mvp-thanos-archive.yml up -d`. Waits
# for thanos-query to become reachable on its host-published port,
# then issues a no-op PromQL query to confirm thanos-query can talk
# to thanos-store-gateway and the latter can speak to MinIO.
#
# An empty result vector is fine — it means the chain is wired but no
# blocks are present yet (which is the expected state until Step 2.1's
# gorillas3processor lands TSDB block writes). A non-success status,
# or a curl error, indicates a real wiring break.
#
# Exits non-zero on any failure. Intended for CI / make-target use.

set -euo pipefail

THANOS_QUERY_HOST="${THANOS_QUERY_HOST:-localhost}"
THANOS_QUERY_PORT="${THANOS_QUERY_PORT:-19092}"
THANOS_QUERY_BASE="http://${THANOS_QUERY_HOST}:${THANOS_QUERY_PORT}"

# Ceiling on the readiness wait so a wedged stack fails the script
# instead of looping forever. 60 × 2s = 120s — comfortably above the
# typical 10-15s container start-up.
MAX_TRIES="${MAX_TRIES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"

echo "verify_thanos_sidecar: waiting for ${THANOS_QUERY_BASE}/-/healthy"
tries=0
until curl -fs "${THANOS_QUERY_BASE}/-/healthy" >/dev/null; do
    tries=$((tries + 1))
    if [ "${tries}" -ge "${MAX_TRIES}" ]; then
        echo "verify_thanos_sidecar: thanos-query did not become healthy after ${MAX_TRIES} tries" >&2
        exit 1
    fi
    sleep "${SLEEP_SECONDS}"
done
echo "verify_thanos_sidecar: thanos-query healthy"

# No-op query: `up` is a Prometheus convention; against an empty
# bucket Thanos returns success with an empty `result` array, which
# is the wire-up signal we want.
echo "verify_thanos_sidecar: probing /api/v1/query?query=up"
response=$(curl -fs "${THANOS_QUERY_BASE}/api/v1/query?query=up")

if ! printf '%s' "${response}" | jq -e '.status == "success"' >/dev/null; then
    echo "verify_thanos_sidecar: query did not return status=success" >&2
    printf '%s\n' "${response}" >&2
    exit 1
fi

echo "verify_thanos_sidecar: ok — thanos-query <-> thanos-store-gateway <-> MinIO chain is wired"
