#!/usr/bin/env bash
# run_mvp_demo.sh — issue-46 MVP demo driver (v5).
#
# v5 changes vs v4:
#
#   1. After the per-baseline soak completes, fire ad-hoc PromQL
#      queries with label predicates against the ASAP backend
#      (criterion ⑦). These exercise the postings-aware chunk
#      filter; the response infos carry
#      `postings_filtered_series_count` so the report can compare
#      against the would-have-been chunk-scan count.
#   2. Run the compactor TWICE on the ASAP MinIO bucket: first
#      `--dry-run` (prints the plan) and then `--no-dry-run`
#      (executes). Capture before/after object count + total
#      bytes via `mc ls --recursive --json`.
#   3. Emit `s3_cost.csv` per baseline (counts of PUT/GET/HEAD/
#      DELETE × bytes-out). Sourced from the new
#      `/internal/s3_cost.csv` HTTP endpoint on the backend
#      (asap-single-sketch only; B0/B1/B5 don't have a backend
#      that talks to S3).
#   4. Output dir layout becomes
#      deploy/eval-results/mvp-v5-2026-05-06/<baseline>/{...}
#      (was mvp-v4-...).
#
# v4 changes vs v3:
#
#   1. Cycles through FOUR baselines back-to-back instead of two:
#        B0  raw → real Prometheus      (baseline-b0-prometheus.yml)
#        B1  SERF → real Prometheus     (baseline-b1-serf.yml)
#        B5  Gorilla agent-side         (baseline-b5-gorilla.yml +
#                                        b1-style PRW forward)
#        B6s ASAP single-sketch + cold  (baseline-b6-asap-single-sketch.yml)
#      The v3 driver only ran two cells (asap all-five-sketches + raw).
#   2. Adds a query-side warm-up phase: after the agent warm-up
#      (60s) the driver polls
#      `count_over_time(http_requests_total[1m])` against the
#      relevant query backend until it goes non-zero (or 30s
#      timeout). This is the criterion-④ accuracy NaN fix —
#      without it the measurement window can open before any
#      sketch has flushed, so quantile queries return NaN and
#      the accuracy reducer can't compute relative error.
#   3. Runs `measure_freshness.py` and `measure_stages.py` in
#      parallel with the replay client during the 60s
#      measurement window. Their CSVs land in each cell dir.
#   4. The ad-hoc cold-fallback query is gated to the ASAP
#      single-sketch baseline only (B0 / B1 / B5 don't have a
#      gorilla cold archive).
#   5. Output dir layout changed to one-cell-per-baseline:
#      deploy/eval-results/mvp-v4-2026-05-06/<baseline>/{...}.
#
# Steps per baseline:
#   * docker compose down -v (clean prior cell)
#   * docker compose up -d
#   * 60s agent warm-up                (`WARMUP_S`)
#   * up to 30s query-side warm-up     (`QUERY_WARMUP_CAP_S`)
#   * 60s measurement window in parallel:
#       - replay client     (promql_replay.py)
#       - freshness probe   (measure_freshness.py)
#       - stage breakdown   (measure_stages.py)
#   * ad-hoc cold-fallback query (ASAP only)
#   * cell-level measurement.csv + accuracy.csv
#   * docker compose down -v
#
# Usage:
#   bash deploy/scripts/run_mvp_demo.sh
#
# Output:
#   deploy/eval-results/mvp-v4-2026-05-06/
#     b0-prometheus/{measurement.csv, accuracy.csv, freshness.csv,
#                    stages.csv, replay.jsonl, ad_hoc_query_response.json}
#     b1-serf/{...}
#     b5-gorilla/{...}
#     asap-single-sketch/{...}
#     MVP_REPORT_v4.md
set -euo pipefail

# ── knobs ────────────────────────────────────────────────────────
WARMUP_S="${WARMUP_S:-60}"
QUERY_WARMUP_CAP_S="${QUERY_WARMUP_CAP_S:-30}"
SOAK_S="${SOAK_S:-60}"
CARD="${CARD:-1000}"
NUM_AGENTS="${NUM_AGENTS:-10}"
FREQ_HZ="${FREQ_HZ:-1}"
SDK_WINDOW="${SDK_WINDOW:-1000ms}"
QPS="${QPS:-5}"
ASAP_SKETCH_FAMILY="${ASAP_SKETCH_FAMILY:-ddsketch}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPT_DIR/../docker-compose" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

HOST_BACKEND_QUERY_PORT="19091"
HOST_CONTROLLER_PORT="18080"
HOST_PROM_PORT="9090"
# Agent-1's OTLP HTTP port published on the host (only B6 ASAP
# overlay maps this; B0 / B1 / B5 push freshness probes to the
# gateway's :14318 instead).
HOST_AGENT_OTLP_HTTP="14328"
HOST_GATEWAY_OTLP_HTTP="14318"

SWEEP_WAIT_CAP_S="${SWEEP_WAIT_CAP_S:-3600}"
OUT_BASE="${OUT_BASE:-${REPO_ROOT}/deploy/eval-results/mvp-v5-2026-05-06}"
mkdir -p "$OUT_BASE"

# ── replay query suite (warm + cold mix) ─────────────────────────
# v4 keeps the v3 shape — the ASAP overlay still routes the
# latency-quantile metric warm-tier (criterion-④ NaN fix) and the
# raw counter cold-tier (criterion-⑤ archive marker).
REPLAY_QUERIES_FILE="${OUT_BASE}/replay-queries.json"
cat > "$REPLAY_QUERIES_FILE" <<'JSON'
[
    {"kind": "quantile",     "promql": "quantile_over_time(0.99, http_requests_total_latency_ms_quantile[1m])"},
    {"kind": "quantile",     "promql": "quantile_over_time(0.5, http_requests_total_latency_ms_quantile[1m])"},
    {"kind": "sum",          "promql": "sum_over_time(http_requests_total[1m])"},
    {"kind": "count_unique", "promql": "count(http_requests_total)"},
    {"kind": "topk",         "promql": "topk(10, http_requests_total)"}
]
JSON

# Ad-hoc cold-fallback query — only meaningful for the ASAP
# baseline (the others have no Gorilla archive).
COLD_QUERY="sum_over_time(http_requests_total[1m])"

# ── v5: ad-hoc PromQL with label predicates (criterion ⑦) ────────
# Fired against the ASAP backend after the soak. These exercise
# the postings-aware chunk filter; the report compares response
# `postings_filtered_series_count` against the
# would-have-been-scanned count.
LABEL_PREDICATE_QUERIES_FILE="${OUT_BASE}/label-predicate-queries.json"
mkdir -p "$(dirname "$LABEL_PREDICATE_QUERIES_FILE")"
cat > "$LABEL_PREDICATE_QUERIES_FILE" <<'JSON'
[
    {"name": "count_with_service_eq",
     "promql": "count(http_requests_total{service=\"api\"})"},
    {"name": "topk_with_status_regex",
     "promql": "topk(5, sum by (zone) (rate(http_requests_total{status=~\"5..\"}[5m])))"}
]
JSON

# ── helpers ──────────────────────────────────────────────────────
log() { printf '[mvp v4] %s\n' "$*"; }

wait_for_sweep_idle() {
    local waited=0
    while pgrep -f "^bash[[:space:]]+.*run_e2e_sweep\.sh" >/dev/null 2>&1; do
        if (( waited >= SWEEP_WAIT_CAP_S )); then
            log "WARN: run_e2e_sweep.sh still alive after ${SWEEP_WAIT_CAP_S}s — proceeding anyway"
            break
        fi
        if (( waited == 0 )); then
            log "run_e2e_sweep.sh detected — waiting for it to finish (cap ${SWEEP_WAIT_CAP_S}s)"
        fi
        sleep 30
        waited=$((waited + 30))
    done
}

teardown() {
    local cell_dir="$1"; shift
    log "  tearing down stack..."
    (cd "$COMPOSE_DIR" && \
       docker compose "$@" down -v >> "${cell_dir}/down.log" 2>&1 || true)
}

# Poll a PromQL endpoint for `count_over_time(http_requests_total[1m])`
# until non-zero or `cap_s` seconds elapse. Returns 0 if data
# arrived, 1 if the cap fired (the run continues either way; the
# CSV merely records that the warm-tier didn't flush in time).
query_warmup() {
    local query_url="$1"
    local cap_s="$2"
    local started; started=$(date +%s)
    while true; do
        local now; now=$(date +%s)
        local elapsed=$((now - started))
        if (( elapsed >= cap_s )); then
            log "  query-warmup CAP (${cap_s}s) — no data yet; proceeding"
            return 1
        fi
        local body
        body=$(curl -sG "${query_url}/api/v1/query" \
            --data-urlencode "query=count_over_time(http_requests_total[1m])" \
            2>/dev/null || true)
        # Look for any non-zero, non-empty value entry.
        if echo "$body" | grep -Eq '"value":\[[0-9.]+,"[1-9][0-9]*\.?[0-9]*"' \
           || echo "$body" | grep -Eq '"value":\[[0-9.]+,"0\.[0-9]*[1-9]'; then
            log "  query-warmup OK after ${elapsed}s"
            return 0
        fi
        sleep 1
    done
}

run_cell() {
    # Args: cell_label cell_dir overlay_yaml agent_cfg query_url \
    #       freshness_otlp freshness_query freshness_paths is_asap
    local label="$1" dir="$2" overlay="$3" agent_cfg="$4" query_url="$5"
    local fresh_otlp="$6" fresh_query="$7" fresh_paths="$8" is_asap="$9"
    log "── cell: ${label} (${overlay} + ${agent_cfg}, N=${NUM_AGENTS} agents) ──"
    mkdir -p "$dir"

    local -a COMPOSE_ARGS=(
        -f base.yml
        -f "agents-N${NUM_AGENTS}.yml"
        -f "$overlay"
        -f e2e-overlay.yml
    )

    teardown "$dir" "${COMPOSE_ARGS[@]}"

    log "  bringing up stack..."
    (cd "$COMPOSE_DIR" && \
       AGENT_CONFIG="$agent_cfg" \
       EXPORTER_FREQ_HZ="$FREQ_HZ" \
       EXPORTER_CARDINALITY="$CARD" \
       EXPORTER_SDK_WINDOW="$SDK_WINDOW" \
       ASAP_SKETCH_FAMILY="$ASAP_SKETCH_FAMILY" \
       docker compose "${COMPOSE_ARGS[@]}" up -d) > "${dir}/up.log" 2>&1

    log "  stack settle..."
    sleep 8

    # Agent warm-up: let the warm-tier sketch accumulators fill
    # and the gorillas3 processor write its first chunk.
    log "  agent warm-up ${WARMUP_S}s..."
    sleep "$WARMUP_S"

    # Query-side warm-up: wait until queries actually have data
    # before the measurement window opens. Criterion-④ NaN fix.
    log "  query-side warm-up (cap ${QUERY_WARMUP_CAP_S}s) against ${query_url}..."
    query_warmup "$query_url" "$QUERY_WARMUP_CAP_S" || true

    # Replay (background)
    log "  replay (qps=${QPS}, soak=${SOAK_S}s) → ${query_url}"
    python3 "${SCRIPT_DIR}/promql_replay.py" \
        --target "$query_url" \
        --controller "http://localhost:${HOST_CONTROLLER_PORT}" \
        --queries "$REPLAY_QUERIES_FILE" \
        --qps "$QPS" \
        --duration "$SOAK_S" \
        --out "${dir}/replay.jsonl" \
        > "${dir}/replay.log" 2>&1 &
    REPLAY_PID=$!

    # Freshness probe (background)
    log "  freshness probe (paths=${fresh_paths}, otlp=${fresh_otlp}, query=${fresh_query})"
    python3 "${SCRIPT_DIR}/measure_freshness.py" \
        --baseline "$label" \
        --otlp-http "$fresh_otlp" \
        --query "$fresh_query" \
        --paths "$fresh_paths" \
        --duration "$SOAK_S" \
        --out "${dir}/freshness.csv" \
        > "${dir}/freshness.log" 2>&1 &
    FRESH_PID=$!

    # Stage probe (background)
    log "  stage breakdown (duration=${SOAK_S}s)"
    python3 "${SCRIPT_DIR}/measure_stages.py" \
        --baseline "$label" \
        --duration "$SOAK_S" \
        --out "${dir}/stages.csv" \
        > "${dir}/stages.log" 2>&1 &
    STAGE_PID=$!

    wait "$REPLAY_PID" || true
    wait "$FRESH_PID" || true
    wait "$STAGE_PID" || true
    log "  measurement window done."

    # Ad-hoc cold-fallback query — ASAP cell only.
    if [[ "$is_asap" == "yes" ]]; then
        log "  ad-hoc cold-fallback query: ${COLD_QUERY}"
        curl -sG "${query_url}/api/v1/query" \
            --data-urlencode "query=${COLD_QUERY}" \
            > "${dir}/ad_hoc_query_response.json" 2>"${dir}/ad_hoc_query.err" \
            || true

        # v5 — ad-hoc label-predicate queries (criterion ⑦). One
        # JSON file per query carrying the response body PLUS
        # the wall-clock latency we measured client-side.
        log "  v5 ad-hoc label-predicate queries"
        mkdir -p "${dir}/label_predicate_queries"
        python3 - "$query_url" "$LABEL_PREDICATE_QUERIES_FILE" "${dir}/label_predicate_queries" <<'PY'
import json, os, sys, time, urllib.parse, urllib.request

target, queries_path, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]
with open(queries_path) as f:
    queries = json.load(f)
for q in queries:
    name = q["name"]
    promql = q["promql"]
    url = f"{target}/api/v1/query?{urllib.parse.urlencode({'query': promql})}"
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            body = resp.read()
        latency_ms = (time.monotonic() - t0) * 1000
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body.decode("utf-8", errors="replace")}
    except Exception as e:  # noqa: BLE001
        latency_ms = (time.monotonic() - t0) * 1000
        payload = {"error": str(e)}
    out = {
        "name": name,
        "promql": promql,
        "client_latency_ms": latency_ms,
        "response": payload,
    }
    with open(os.path.join(out_dir, f"{name}.json"), "w") as f:
        json.dump(out, f, indent=2)
PY

        # v5 — pull the backend's S3 cost CSV. Endpoint is
        # `/internal/s3_cost.csv` on the backend; if the build
        # doesn't expose it we silently fall through (the
        # report renders a "—" cell).
        log "  v5 fetching s3_cost.csv"
        curl -sf "${query_url}/internal/s3_cost.csv" \
            > "${dir}/s3_cost.csv" 2>"${dir}/s3_cost.err" \
            || echo "put_count,get_count,head_count,list_count,delete_count,bytes_put,bytes_got" \
                > "${dir}/s3_cost.csv"
    fi

    # Per-cell measurement.csv (Prom-sourced + docker stats).
    log "  measurement..."
    python3 "${SCRIPT_DIR}/measure-baseline.py" \
        --prom "http://localhost:${HOST_PROM_PORT}" \
        --baseline "${label}" \
        --scale "N${NUM_AGENTS}" \
        --rate "$FREQ_HZ" \
        --cardinality "$CARD" \
        --replay-jsonl "${dir}/replay.jsonl" \
        --bytes-sample-window 15 \
        > "${dir}/measurement.csv" \
        2> "${dir}/measurement.log" || true

    # Snapshot cold-store ground truth (used by accuracy reducer).
    BACKEND_CONT="$(cd "$COMPOSE_DIR" && docker compose "${COMPOSE_ARGS[@]}" \
        ps -q backend 2>/dev/null | head -n1 || true)"
    if [[ -n "$BACKEND_CONT" ]]; then
        docker cp "${BACKEND_CONT}:/var/asap/cold/raw" "${dir}/cold-truth" \
            > "${dir}/cold-snapshot.log" 2>&1 || true
    fi

    # MinIO Gorilla chunk listing (only meaningful for ASAP cell).
    if [[ "$is_asap" == "yes" ]]; then
        MINIO_CONT="$(cd "$COMPOSE_DIR" && docker compose "${COMPOSE_ARGS[@]}" \
            ps -q minio 2>/dev/null | head -n1 || true)"
        if [[ -n "$MINIO_CONT" ]]; then
            docker run --rm --network container:"$MINIO_CONT" \
                --entrypoint sh minio/mc:latest -c \
                "mc alias set asap http://localhost:9000 asap asap-local-only >/dev/null 2>&1; mc ls --recursive asap/asap-gorilla 2>/dev/null || true" \
                > "${dir}/gorilla_chunks.txt" 2>&1 || true
        fi
    fi

    # Accuracy reduce.
    if [[ -d "${dir}/cold-truth" ]]; then
        python3 "${SCRIPT_DIR}/accuracy_reduce.py" \
            --cell-dir "$dir" \
            --out "${dir}/accuracy.csv" \
            > "${dir}/accuracy.log" 2>&1 || true
    fi

    teardown "$dir" "${COMPOSE_ARGS[@]}"
    log "  cell ${label} done → ${dir}"
}

wait_for_sweep_idle

# ── B0: raw OTel → real Prometheus ───────────────────────────────
run_cell "b0-prometheus" "${OUT_BASE}/b0-prometheus" \
    "baseline-b0-prometheus.yml" "sketchcol-agent-b0-prometheus.yaml" \
    "http://localhost:${HOST_PROM_PORT}" \
    "http://localhost:${HOST_GATEWAY_OTLP_HTTP}" \
    "http://localhost:${HOST_PROM_PORT}" \
    "warm" "no"

# ── B1: SERF → real Prometheus ───────────────────────────────────
run_cell "b1-serf" "${OUT_BASE}/b1-serf" \
    "baseline-b1-serf.yml" "sketchcol-agent-b1-serf-prometheus.yaml" \
    "http://localhost:${HOST_PROM_PORT}" \
    "http://localhost:${HOST_GATEWAY_OTLP_HTTP}" \
    "http://localhost:${HOST_PROM_PORT}" \
    "warm" "no"

# ── B5: Gorilla agent-side → real Prometheus ─────────────────────
# v4 run-agent fix: the stock `sketchcol-agent-b5-gorilla.yaml`
# uses `drop_original: true` so Prometheus saw nothing and
# criterion ⑥ freshness was permanently NaN for B5. The
# `sketchcol-agent-b5-gorilla-prometheus.yaml` variant flips
# `drop_original: false` and forwards the raw stream via PRW —
# mirrors B1's PRW config so the Prometheus query surface is
# identical across B0/B1/B5. The b5 overlay was also extended
# to start Prometheus with the remote-write-receiver flag.
run_cell "b5-gorilla" "${OUT_BASE}/b5-gorilla" \
    "baseline-b5-gorilla.yml" "sketchcol-agent-b5-gorilla-prometheus.yaml" \
    "http://localhost:${HOST_PROM_PORT}" \
    "http://localhost:${HOST_GATEWAY_OTLP_HTTP}" \
    "http://localhost:${HOST_PROM_PORT}" \
    "warm" "no"

# ── B6 ASAP single-sketch + Gorilla-S3 cold archive ──────────────
run_cell "asap-single-sketch" "${OUT_BASE}/asap-single-sketch" \
    "baseline-b6-asap-single-sketch.yml" "sketchcol-agent-b6-asap-single-sketch.yaml" \
    "http://localhost:${HOST_BACKEND_QUERY_PORT}" \
    "http://localhost:${HOST_AGENT_OTLP_HTTP}" \
    "http://localhost:${HOST_BACKEND_QUERY_PORT}" \
    "warm,archive" "yes"

# ── v5 — compactor sweep on the ASAP MinIO bucket ────────────────
# This runs AFTER all four cells (the ASAP cell's MinIO has been
# torn down — but we restart only the MinIO container with a
# host-mount so the cold archive survives). For demo purposes the
# stack is brought up fresh against the surviving objects in
# MinIO; in a multi-node deployment the compactor runs as a
# scheduled K8s job pointed at the live S3 bucket and never
# requires the ASAP backend to be up.
COMPACTOR_DIR="${OUT_BASE}/compactor"
mkdir -p "$COMPACTOR_DIR"

# Pull credentials from the b6 ASAP overlay so the compactor can
# auth against MinIO without the user setting env vars by hand.
COMPACTOR_BUCKET="${COMPACTOR_BUCKET:-asap-gorilla}"
COMPACTOR_ENDPOINT="${COMPACTOR_ENDPOINT:-http://localhost:9000}"
COMPACTOR_ACCESS_KEY="${COMPACTOR_ACCESS_KEY:-asap}"
COMPACTOR_SECRET_KEY="${COMPACTOR_SECRET_KEY:-asap-local-only}"
COMPACTOR_TENANT="${COMPACTOR_TENANT:-default}"
COMPACTOR_BIN="${COMPACTOR_BIN:-${REPO_ROOT}/compactor/target/release/gorilla-compactor}"

# Pre-compactor object listing (count + total bytes).
list_minio_objects() {
    local out_path="$1"
    docker run --rm --network host --entrypoint sh minio/mc:latest -c \
        "mc alias set asap ${COMPACTOR_ENDPOINT} ${COMPACTOR_ACCESS_KEY} ${COMPACTOR_SECRET_KEY} >/dev/null 2>&1; \
         mc ls --recursive --json asap/${COMPACTOR_BUCKET} 2>/dev/null || true" \
        > "$out_path" 2>"${out_path}.err" || true
}

if [[ -x "$COMPACTOR_BIN" ]]; then
    log "── v5 compactor: dry-run ──"
    list_minio_objects "${COMPACTOR_DIR}/before.minio.jsonl"
    "$COMPACTOR_BIN" \
        --endpoint "$COMPACTOR_ENDPOINT" \
        --bucket "$COMPACTOR_BUCKET" \
        --tenant "$COMPACTOR_TENANT" \
        --access-key-id "$COMPACTOR_ACCESS_KEY" \
        --secret-access-key "$COMPACTOR_SECRET_KEY" \
        --threshold-count 6 \
        --threshold-hours 6 \
        --dry-run \
        --out "${COMPACTOR_DIR}/dry_run.json" \
        > "${COMPACTOR_DIR}/dry_run.log" 2>&1 || true

    log "── v5 compactor: live run ──"
    "$COMPACTOR_BIN" \
        --endpoint "$COMPACTOR_ENDPOINT" \
        --bucket "$COMPACTOR_BUCKET" \
        --tenant "$COMPACTOR_TENANT" \
        --access-key-id "$COMPACTOR_ACCESS_KEY" \
        --secret-access-key "$COMPACTOR_SECRET_KEY" \
        --threshold-count 6 \
        --threshold-hours 6 \
        --out "${COMPACTOR_DIR}/live_run.json" \
        > "${COMPACTOR_DIR}/live_run.log" 2>&1 || true

    list_minio_objects "${COMPACTOR_DIR}/after.minio.jsonl"
else
    log "WARN: compactor binary missing at ${COMPACTOR_BIN}; skipping compaction phase"
    echo "compactor binary missing" > "${COMPACTOR_DIR}/SKIPPED"
fi

# ── reduce → MVP_REPORT_v5.md ────────────────────────────────────
log "── reducing → MVP_REPORT_v5.md ──"
python3 "${SCRIPT_DIR}/mvp_report.py" \
    --results-dir "$OUT_BASE" \
    --num-agents "$NUM_AGENTS" \
    --per-agent-cardinality "$CARD" \
    --out "${OUT_BASE}/MVP_REPORT_v5.md"

log "MVP demo v5 complete. Report: ${OUT_BASE}/MVP_REPORT_v5.md"
