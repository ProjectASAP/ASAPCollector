#!/usr/bin/env bash
# run_mvp_demo.sh — MVP demo driver (controller-driven multi-stage).
#
# What this driver does:
#
#   1. Topology is fan-in: 10 producers → 2 agents → 1 gateway → 1
#      backend (+ optional B0 Prometheus). The compose overlay is
#      `deploy/docker-compose/mvp-multi-stage.yml`.
#
#   2. Agent + gateway runtime configs are emitted by the controller
#      (typed-stage-split path, behind `USE_TYPED_STAGE_SPLIT=1`)
#      rather than mounted from a fixed file. The driver waits for
#      OpAMP push to settle, then captures whatever the controller
#      has on its `/api/v1/collector-config/{agent,backend}`
#      introspection endpoints. If the typed-stage-split path doesn't
#      fire (Phase B/C wiring caveats), the driver logs the
#      capture failure clearly and continues with the placeholder
#      configs that mvp-multi-stage.yml mounts as fallback.
#
#   3. Three canonical query classes from
#      `deploy/configs/mvp-workload.yaml` exercise:
#        - window-per-series   (DDSketch p99 over 1m)
#        - label-at-instant    (sum by zone, gateway fan-in)
#        - combined            (rate over 5m + sum by zone)
#      plus a fourth ad-hoc cold-fallback probe
#      (`http_requests_total{service="payments"}` — assigned
#      to role "archive").
#
#   4. Freshness phase calls `run_freshness_phase.sh` which emits
#      `freshness/{raw,warm,archive}.csv`.
#
#   5. Compactor phase invokes the `gorilla-compactor` binary at
#      `${COMPACTOR_BIN}` with `--threshold-hours 0 --threshold-count 0`
#      so even a 60s soak generates one merge candidate. Dry-run
#      then live-run, capturing before/after MinIO listings.
#
#   6. Per-edge bandwidth probe (`measure_per_edge_bandwidth.py`)
#      runs alongside `measure_stages.py` so criterion ① gets a
#      per-edge breakdown:
#        sdk→agent / agent→gateway / gateway→backend / gateway→s3.
#
# Usage:
#   bash deploy/scripts/run_mvp_demo.sh
#
# Output:
#   deploy/eval-results/mvp-current/{
#       stack-up.log, controller-emitted-configs/,
#       measurements/{stages.csv, per_edge_bandwidth.csv,
#                     replay.jsonl, accuracy.csv},
#       freshness/{raw.csv, warm.csv, archive.csv},
#       ad-hoc/{<query_label>.json},
#       compactor/{dry_run.json, live_run.json,
#                   before.minio.jsonl, after.minio.jsonl},
#       MVP_REPORT.md
#   }
set -euo pipefail

# ── knobs ────────────────────────────────────────────────────────
STACK_SETTLE_S="${STACK_SETTLE_S:-60}"
AGENT_WARMUP_S="${AGENT_WARMUP_S:-60}"
QUERY_WARMUP_S="${QUERY_WARMUP_S:-30}"
SOAK_S="${SOAK_S:-60}"
FRESHNESS_DURATION_S="${FRESHNESS_DURATION_S:-60}"
QPS="${QPS:-5}"
PER_AGENT_CARDINALITY="${PER_AGENT_CARDINALITY:-500}"
N_PRODUCERS="${N_PRODUCERS:-10}"
EXPORTER_FREQ_HZ="${EXPORTER_FREQ_HZ:-10}"
EXPORTER_FRESHNESS_PROBES="${EXPORTER_FRESHNESS_PROBES:-on}"
EXPORTER_FRESHNESS_PROBE_HZ="${EXPORTER_FRESHNESS_PROBE_HZ:-1.0}"
ASAP_SKETCH_FAMILY="${ASAP_SKETCH_FAMILY:-ddsketch}"
USE_TYPED_STAGE_SPLIT="${USE_TYPED_STAGE_SPLIT:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(cd "${SCRIPT_DIR}/../docker-compose" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Host-published ports (see deploy/docker-compose/base.yml).
HOST_BACKEND_QUERY_PORT="${HOST_BACKEND_QUERY_PORT:-19091}"
HOST_BACKEND_INGEST_PORT="${HOST_BACKEND_INGEST_PORT:-19090}"
HOST_CONTROLLER_PORT="${HOST_CONTROLLER_PORT:-18080}"
HOST_PROM_B0_PORT="${HOST_PROM_B0_PORT:-19090}"  # collides with backend ingest;
# The multi-stage overlay republishes Prometheus B0 on 19090 only when
# the `b0` profile is active (compose `--profile b0`). The driver does
# NOT bring up B0 in the same compose stack as the backend on this
# port; B0 mode is a separate cycle (see Phase 0 baseline_b0() below).

OUT_BASE="${OUT_BASE:-${REPO_ROOT}/deploy/eval-results/mvp-current}"
REPORT_NAME="${REPORT_NAME:-MVP_REPORT.md}"
COMPACTOR_BIN="${COMPACTOR_BIN:-${REPO_ROOT}/compactor/target/release/gorilla-compactor}"
COMPACTOR_BUCKET="${COMPACTOR_BUCKET:-asap-gorilla}"
COMPACTOR_ENDPOINT="${COMPACTOR_ENDPOINT:-http://localhost:9000}"
COMPACTOR_ACCESS_KEY="${COMPACTOR_ACCESS_KEY:-asap}"
COMPACTOR_SECRET_KEY="${COMPACTOR_SECRET_KEY:-asap-local-only}"
COMPACTOR_TENANT="${COMPACTOR_TENANT:-default}"

# ── helpers ──────────────────────────────────────────────────────
log() { printf '[mvp] %s\n' "$*"; }

ensure_out_dirs() {
    mkdir -p \
        "${OUT_BASE}" \
        "${OUT_BASE}/controller-emitted-configs" \
        "${OUT_BASE}/measurements" \
        "${OUT_BASE}/freshness" \
        "${OUT_BASE}/ad-hoc" \
        "${OUT_BASE}/compactor"
}

# Phase 0 — pre-flight checks. Bail loud, bail early.
preflight() {
    log "Phase 0 preflight"

    if ! command -v docker >/dev/null 2>&1; then
        echo "[error] docker not found on PATH" >&2; exit 2
    fi
    if ! docker compose version >/dev/null 2>&1; then
        echo "[error] 'docker compose' subcommand not available" >&2; exit 2
    fi

    if [[ ! -x "${COMPACTOR_BIN}" ]]; then
        log "WARN: compactor binary missing at ${COMPACTOR_BIN}; "
        log "      run \`cargo build --release -p gorilla-compactor\` "
        log "      OR set COMPACTOR_BIN to the location of the built "
        log "      binary. The compactor phase will be skipped with a "
        log "      clear marker file rather than crashing."
    fi

    # Best-effort: warn if the fake-exporter image is missing.
    # We do NOT fail here — the docker-compose `up` will surface
    # that directly.
    if docker image inspect asap/fake-exporter:dev >/dev/null 2>&1; then
        log "  fake-exporter:dev image present"
    else
        log "  fake-exporter:dev image NOT FOUND — run: "
        log "    docker build -t asap/fake-exporter:dev deploy/fake-exporter/"
    fi

    # Clean any stale containers from a previous run of the MVP
    # overlay. Won't disturb other compose projects on the host.
    docker compose \
        --project-directory "${COMPOSE_DIR}" \
        -f "${COMPOSE_DIR}/base.yml" \
        -f "${COMPOSE_DIR}/mvp-multi-stage.yml" \
        down -v --remove-orphans \
        > "${OUT_BASE}/preflight-down.log" 2>&1 || true
}

# Phase 1 — bring up the multi-stage controller-driven topology.
bring_up_stack() {
    log "Phase 1 stack up (controller + 10 producers + 2 agents + 1 gateway + 1 backend)"

    (
        cd "${COMPOSE_DIR}"
        USE_TYPED_STAGE_SPLIT="${USE_TYPED_STAGE_SPLIT}" \
        PER_AGENT_CARDINALITY="${PER_AGENT_CARDINALITY}" \
        EXPORTER_FREQ_HZ="${EXPORTER_FREQ_HZ}" \
        EXPORTER_FRESHNESS_PROBES="${EXPORTER_FRESHNESS_PROBES}" \
        EXPORTER_FRESHNESS_PROBE_HZ="${EXPORTER_FRESHNESS_PROBE_HZ}" \
        ASAP_SKETCH_FAMILY="${ASAP_SKETCH_FAMILY}" \
        docker compose \
            -f base.yml \
            -f mvp-multi-stage.yml \
            up -d
    ) > "${OUT_BASE}/stack-up.log" 2>&1

    log "  stack settle ${STACK_SETTLE_S}s (controller plan + OpAMP push)"
    sleep "${STACK_SETTLE_S}"

    # Trigger handle_plan() for the typed-stage-split path.
    # The startup workload-registry pre-pop loop in controller/main.rs only
    # runs `planner.plan(&wl)`; the `USE_TYPED_STAGE_SPLIT` block lives
    # inside `handle_plan()` (POST /api/v1/plan). Without an explicit POST
    # the typed path is never reached and §8 STATUS comes back
    # `not-exercised`. POST each canonical workload now that the OpAMP
    # fabric is up — this exercises the emitter + the typed-backend JSON push.
    log "  POST /api/v1/plan for each canonical workload (exercise typed-stage-split)"
    post_workload_plan() {
        local label="$1"; local promql="$2"; local accuracy="$3"; local metric="$4"
        local body
        body=$(printf '{"query_string":%s,"metric_name":%s,"accuracy_sla":%s,"aggregations":["quantile"],"time_window":"1m","latency_sla":null,"sketch_type":null}' \
            "$(printf '%s' "$promql" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" \
            "$(printf '%s' "$metric" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" \
            "$accuracy")
        local code
        code=$(curl -sS -o "${OUT_BASE}/plan-post-${label}.json" -w '%{http_code}' \
            -X POST "http://localhost:${HOST_CONTROLLER_PORT}/api/v1/plan" \
            -H 'Content-Type: application/json' \
            -d "$body" \
            2> "${OUT_BASE}/plan-post-${label}.err" || true)
        log "    POST /api/v1/plan ${label} → HTTP ${code}"
    }
    post_workload_plan window-per-series \
        'quantile_over_time(0.99, http_requests_total_latency_ms[1m])' \
        '0.01' 'http_requests_total_latency_ms'
    post_workload_plan label-at-instant \
        'sum by (zone) (http_requests_total)' \
        '0.0' 'http_requests_total'
    post_workload_plan combined-window-label \
        'sum by (zone) (rate(http_requests_total[5m]))' \
        '0.01' 'http_requests_total'
    post_workload_plan cold-fallback-payments \
        'count(http_requests_total{service="payments"})' \
        '0.0' 'http_requests_total'

    log "  agent warm-up ${AGENT_WARMUP_S}s (sketches fill)"
    sleep "${AGENT_WARMUP_S}"

    log "  query-side warm-up ${QUERY_WARMUP_S}s"
    local started; started=$(date +%s)
    while true; do
        local now; now=$(date +%s)
        local elapsed=$((now - started))
        if (( elapsed >= QUERY_WARMUP_S )); then
            log "  query warm-up window ${QUERY_WARMUP_S}s elapsed; proceeding"
            break
        fi
        local body
        body=$(curl -sG "http://localhost:${HOST_BACKEND_QUERY_PORT}/api/v1/query" \
            --data-urlencode "query=count(http_requests_total)" \
            2>/dev/null || true)
        # Look for any non-zero result.
        if echo "$body" | grep -Eq '"value":\[[0-9.]+,"[1-9]'; then
            log "  query warm-up: backend has data after ${elapsed}s"
            break
        fi
        sleep 2
    done
}

# Capture controller-emitted configs (best-effort introspection).
# If the controller isn't exposing them (typed-stage-split path
# disabled or returned None) we record that fact rather than
# crashing.
capture_emitted_configs() {
    log "  capturing controller-emitted runtime configs"
    local cdir="${OUT_BASE}/controller-emitted-configs"
    local ctrl="http://localhost:${HOST_CONTROLLER_PORT}"

    # Agent bootstrap config (controller side: see
    # /api/v1/collector-config/agent in controller/src/main.rs).
    if curl -sf "${ctrl}/api/v1/collector-config/agent" \
            -o "${cdir}/agent.bootstrap.yaml" 2> "${cdir}/agent.bootstrap.err"; then
        log "    agent bootstrap config captured"
    else
        log "    [warn] agent bootstrap config not available — see agent.bootstrap.err"
    fi

    if curl -sf "${ctrl}/api/v1/collector-config/backend" \
            -o "${cdir}/backend.bootstrap.yaml" 2> "${cdir}/backend.bootstrap.err"; then
        log "    backend bootstrap config captured"
    else
        log "    [warn] backend bootstrap config not available — see backend.bootstrap.err"
    fi

    # Per-metric typed config (one per workload entry — Phase B
    # emitter output). The mvp-workload.yaml has four entries.
    for metric in \
        http_requests_total_latency_ms \
        http_requests_total ; do
        local out="${cdir}/per-metric.${metric}.json"
        if curl -sf "${ctrl}/api/v1/config/${metric}" -o "${out}" \
                2> "${out}.err"; then
            log "    per-metric config captured: ${metric}"
        else
            log "    [warn] per-metric config missing for ${metric}"
        fi
    done

    # Connected agents (Phase C role plumbing — should list both
    # agent-a + agent-b under role=Agent and gateway under
    # role=Gateway once the AgentRole::Gateway path is exercised).
    if curl -sf "${ctrl}/api/v1/agents" -o "${cdir}/agents.json" \
            2> "${cdir}/agents.err"; then
        log "    /api/v1/agents captured"
    else
        log "    [warn] /api/v1/agents unavailable"
    fi

    # Snapshot the placeholder gateway config for diffing.
    if [[ -f "${REPO_ROOT}/deploy/configs/sketchcol-gateway-mvp-placeholder.yaml" ]]; then
        cp "${REPO_ROOT}/deploy/configs/sketchcol-gateway-mvp-placeholder.yaml" \
            "${cdir}/gateway.placeholder.yaml"
    fi

    # Detect "controller didn't actually emit a typed config" — this
    # is the failure mode the spec calls out as expected if Phase
    # B/C wiring has remaining caveats. We grep the controller logs
    # for the typed-stage-split tracing markers.
    docker logs "$(cd "${COMPOSE_DIR}" && docker compose -f base.yml -f mvp-multi-stage.yml ps -q controller 2>/dev/null | head -n1)" \
        2> "${cdir}/controller.stderr" \
        > "${cdir}/controller.stdout" || true
    # Rust's tracing default writes to stdout, so the captured
    # controller.stderr is often empty even when the typed-stage-split
    # path fires. Grep both files so STATUS reflects the true state.
    if grep -q "USE_TYPED_STAGE_SPLIT.*pushing typed" \
            "${cdir}/controller.stdout" "${cdir}/controller.stderr" 2>/dev/null; then
        log "    controller logs show typed-stage-split push events — emitter LIVE"
        echo "live" > "${cdir}/STATUS"
    elif grep -q "split_typed_three_stage returned None" \
            "${cdir}/controller.stdout" "${cdir}/controller.stderr" 2>/dev/null; then
        log "    [warn] controller's typed-stage-split returned None — falling back to placeholder"
        echo "fallback-placeholder" > "${cdir}/STATUS"
    else
        log "    [warn] no typed-stage-split tracing in controller logs — emitter not exercised"
        echo "not-exercised" > "${cdir}/STATUS"
    fi
}

# Phase 2 — measurements (replay + stages + per-edge bandwidth).
measure_phase() {
    log "Phase 2 measurement window (${SOAK_S}s)"
    local mdir="${OUT_BASE}/measurements"
    local backend_url="http://localhost:${HOST_BACKEND_QUERY_PORT}"

    # Build the replay query suite from mvp-workload.yaml.
    # We keep the JSON adjacent to the run dir for reproducibility.
    cat > "${mdir}/replay-queries.json" <<'JSON'
[
    {"kind": "quantile",     "promql": "quantile_over_time(0.99, http_requests_total_latency_ms[1m])"},
    {"kind": "sum",          "promql": "sum by (zone) (http_requests_total)"},
    {"kind": "sum",          "promql": "sum by (zone) (rate(http_requests_total[5m]))"}
]
JSON

    # Replay (background).
    log "  replay (qps=${QPS} → ${backend_url})"
    python3 "${SCRIPT_DIR}/promql_replay.py" \
        --target "${backend_url}" \
        --controller "http://localhost:${HOST_CONTROLLER_PORT}" \
        --queries "${mdir}/replay-queries.json" \
        --qps "${QPS}" \
        --duration "${SOAK_S}" \
        --out "${mdir}/replay.jsonl" \
        > "${mdir}/replay.log" 2>&1 &
    REPLAY_PID=$!

    # Stage probe (background).
    log "  measure_stages.py duration=${SOAK_S}s"
    python3 "${SCRIPT_DIR}/measure_stages.py" \
        --baseline "mvp" \
        --duration "${SOAK_S}" \
        --out "${mdir}/stages.csv" \
        > "${mdir}/stages.log" 2>&1 &
    STAGE_PID=$!

    # Per-edge bandwidth (background).
    log "  measure_per_edge_bandwidth.py duration=${SOAK_S}s"
    python3 "${SCRIPT_DIR}/measure_per_edge_bandwidth.py" \
        --duration "${SOAK_S}" \
        --out "${mdir}/per_edge_bandwidth.csv" \
        > "${mdir}/per_edge_bandwidth.log" 2>&1 &
    EDGE_PID=$!

    wait "${REPLAY_PID}" || true
    wait "${STAGE_PID}" || true
    wait "${EDGE_PID}" || true
    log "  measurement window done"

    # Accuracy reduce against the cold-store ground truth.
    log "  accuracy reduce"
    local backend_cont
    backend_cont="$(cd "${COMPOSE_DIR}" && \
        docker compose -f base.yml -f mvp-multi-stage.yml ps -q backend 2>/dev/null | head -n1)"
    if [[ -n "${backend_cont}" ]]; then
        docker cp "${backend_cont}:/var/asap/cold/raw" "${mdir}/cold-truth" \
            > "${mdir}/cold-snapshot.log" 2>&1 || true
    fi
    if [[ -d "${mdir}/cold-truth" ]]; then
        python3 "${SCRIPT_DIR}/accuracy_reduce.py" \
            --cell-dir "${mdir}" \
            --out "${mdir}/accuracy.csv" \
            > "${mdir}/accuracy.log" 2>&1 || true
    else
        log "  [warn] cold-truth snapshot not captured — accuracy.csv skipped"
    fi
}

# Phase 3 — freshness (raw / warm / archive).
freshness_phase() {
    log "Phase 3 freshness probes (raw/warm/archive)"
    # The fake-exporter has been emitting probes the whole time
    # (EXPORTER_FRESHNESS_PROBES=on); this phase is poll-only.
    bash "${SCRIPT_DIR}/run_freshness_phase.sh" \
        --out-dir "${OUT_BASE}" \
        --duration "${FRESHNESS_DURATION_S}" \
        --raw-endpoint "${ASAP_FRESHNESS_RAW_ENDPOINT:-http://localhost:${HOST_BACKEND_QUERY_PORT}}" \
        --warm-endpoint "${ASAP_FRESHNESS_WARM_ENDPOINT:-http://localhost:${HOST_BACKEND_QUERY_PORT}}" \
        --archive-endpoint "${ASAP_FRESHNESS_ARCHIVE_ENDPOINT:-http://localhost:${HOST_BACKEND_QUERY_PORT}}" \
        > "${OUT_BASE}/freshness/run.log" 2>&1 || \
            log "  [warn] freshness phase exited non-zero — see freshness/run.log"
}

# Phase 4 — ad-hoc queries that exercise postings filtering.
ad_hoc_postings_phase() {
    log "Phase 4 ad-hoc postings exercise"
    local adir="${OUT_BASE}/ad-hoc"
    local backend_url="http://localhost:${HOST_BACKEND_QUERY_PORT}"

    fire_query() {
        local label="$1"; shift
        local promql="$1"; shift
        log "  ad-hoc[${label}]: ${promql}"
        # Use --get + --data-urlencode so the PromQL is not shell-mangled.
        curl -sG -m 10 \
            "${backend_url}/api/v1/query" \
            --data-urlencode "query=${promql}" \
            -o "${adir}/${label}.json" \
            -w '{"http_code":%{http_code},"time_total":%{time_total},"size_download":%{size_download}}\n' \
            > "${adir}/${label}.curlstats" \
            2> "${adir}/${label}.curl.err" \
            || log "    [warn] curl exited non-zero for ${label}"
    }

    # The two postings-exercise queries from the spec.
    fire_query "count_api_series" \
        'count(http_requests_total{service="api"})'
    fire_query "topk_5xx_by_zone" \
        'topk(5, sum by (zone) (rate(http_requests_total{status=~"5.."}[5m])))'
}

# Phase 5 — cold-fallback verification (assigned-archive metric).
cold_fallback_phase() {
    log "Phase 5 cold-fallback verification (gorilla_archive marker)"
    local adir="${OUT_BASE}/ad-hoc"
    local backend_url="http://localhost:${HOST_BACKEND_QUERY_PORT}"

    log "  cold[payments]: count(http_requests_total{service=\"payments\"})"
    curl -sG -m 10 \
        "${backend_url}/api/v1/query" \
        --data-urlencode 'query=count(http_requests_total{service="payments"})' \
        -o "${adir}/cold_payments.json" \
        -w '{"http_code":%{http_code},"time_total":%{time_total}}\n' \
        > "${adir}/cold_payments.curlstats" \
        2> "${adir}/cold_payments.curl.err" \
        || log "    [warn] curl exited non-zero for cold_payments"

    if grep -q '"data_source":"gorilla_archive"\|gorilla_archive' \
            "${adir}/cold_payments.json" 2>/dev/null; then
        log "  cold-fallback marker present (data_source: gorilla_archive)"
        echo "PASS" > "${adir}/cold_payments.verdict"
    else
        log "  [warn] no gorilla_archive marker in response — see cold_payments.json"
        echo "MISSING" > "${adir}/cold_payments.verdict"
    fi
}

# Phase 6 — compactor (dry-run + live, concat-only).
compactor_phase() {
    log "Phase 6 compactor (concat-only)"
    local cdir="${OUT_BASE}/compactor"

    if [[ ! -x "${COMPACTOR_BIN}" ]]; then
        log "  [skip] compactor binary missing at ${COMPACTOR_BIN}"
        echo "compactor binary missing at ${COMPACTOR_BIN}" \
            > "${cdir}/SKIPPED"
        return 0
    fi

    # MinIO listing helper (before / after).
    list_minio_objects() {
        local out_path="$1"
        docker run --rm --network host --entrypoint sh minio/mc:latest -c \
            "mc alias set asap ${COMPACTOR_ENDPOINT} ${COMPACTOR_ACCESS_KEY} ${COMPACTOR_SECRET_KEY} >/dev/null 2>&1; \
             mc ls --recursive --json asap/${COMPACTOR_BUCKET} 2>/dev/null || true" \
            > "${out_path}" 2>"${out_path}.err" || true
    }

    list_minio_objects "${cdir}/before.minio.jsonl"

    log "  compactor dry-run"
    "${COMPACTOR_BIN}" \
        --endpoint "${COMPACTOR_ENDPOINT}" \
        --bucket "${COMPACTOR_BUCKET}" \
        --tenant "${COMPACTOR_TENANT}" \
        --access-key-id "${COMPACTOR_ACCESS_KEY}" \
        --secret-access-key "${COMPACTOR_SECRET_KEY}" \
        --threshold-count 0 \
        --threshold-hours 0 \
        --dry-run \
        --out "${cdir}/dry_run.json" \
        > "${cdir}/dry_run.log" 2>&1 || \
            log "  [warn] compactor dry-run exited non-zero"

    log "  compactor live run"
    "${COMPACTOR_BIN}" \
        --endpoint "${COMPACTOR_ENDPOINT}" \
        --bucket "${COMPACTOR_BUCKET}" \
        --tenant "${COMPACTOR_TENANT}" \
        --access-key-id "${COMPACTOR_ACCESS_KEY}" \
        --secret-access-key "${COMPACTOR_SECRET_KEY}" \
        --threshold-count 0 \
        --threshold-hours 0 \
        --out "${cdir}/live_run.json" \
        > "${cdir}/live_run.log" 2>&1 || \
            log "  [warn] compactor live run exited non-zero"

    list_minio_objects "${cdir}/after.minio.jsonl"
}

# Phase 6.5 — fetch s3 cost tracker if backend exposes it.
fetch_s3_cost_csv() {
    log "Phase 6.5 fetch s3_cost.csv"
    local cdir="${OUT_BASE}/measurements"
    local backend_url="http://localhost:${HOST_BACKEND_QUERY_PORT}"
    if curl -sf "${backend_url}/internal/s3_cost.csv" \
            > "${cdir}/s3_cost.csv" 2> "${cdir}/s3_cost.err"; then
        log "  s3_cost.csv captured"
    else
        log "  [warn] /internal/s3_cost.csv unavailable — see s3_cost.err"
    fi
}

# Phase 7 — tear down.
teardown() {
    log "Phase 7 tear down"
    (
        cd "${COMPOSE_DIR}"
        docker compose \
            -f base.yml \
            -f mvp-multi-stage.yml \
            down -v --remove-orphans
    ) > "${OUT_BASE}/teardown.log" 2>&1 || true
}

# Phase 8 — generate the MVP report.
generate_report() {
    local report_name="${REPORT_NAME:-MVP_REPORT.md}"
    log "Phase 8 generate ${report_name}"
    python3 "${SCRIPT_DIR}/mvp_report.py" \
        --results-dir "${OUT_BASE}" \
        --num-producers "${N_PRODUCERS}" \
        --per-agent-cardinality "${PER_AGENT_CARDINALITY}" \
        --out "${OUT_BASE}/${report_name}" \
        > "${OUT_BASE}/report.log" 2>&1 || \
            log "  [warn] mvp_report.py exited non-zero — see report.log"
}

# ── main ─────────────────────────────────────────────────────────
main() {
    ensure_out_dirs
    preflight
    bring_up_stack
    capture_emitted_configs
    measure_phase
    freshness_phase
    ad_hoc_postings_phase
    cold_fallback_phase
    compactor_phase
    fetch_s3_cost_csv
    teardown
    generate_report

    local report_name="${REPORT_NAME:-MVP_REPORT.md}"
    log "MVP demo complete. Report: ${OUT_BASE}/${report_name}"
}

main "$@"
