# ASAP multi-agent deployment

Stack that backs the evaluation. Lives in two flavours —
**docker-compose** for single-machine dev + small/mid-scale
runs (up to ~50 agents on a beefy box), and a **Helm chart**
for real K8s scale points (templates still pending).

_Last updated: 2026-04-23 (post N=10 sweep)._

## Scale dials

```
N ∈ {1, 10, 100}   # number of edge agents
```

All other components (backend, gateway, controller, MinIO,
Prometheus, Grafana) stay at 1 replica — paper's claim is "one
controller coordinates N agents".

## Compose (dev / small & mid scale)

```bash
# N=1
docker compose -f deploy/mvp-singlenode/docker-compose/base.yml \
               -f deploy/mvp-singlenode/docker-compose/agents-N1.yml up

# N=10
docker compose -f deploy/mvp-singlenode/docker-compose/base.yml \
               -f deploy/mvp-singlenode/docker-compose/agents-N10.yml up

# N=100 (requires ≥ ~60 GB RAM, 50 CPU cores — fine on a 40-core /
# 187 GB workstation, not on a laptop; Helm path once templates land)
docker compose -f deploy/mvp-singlenode/docker-compose/base.yml \
               -f deploy/mvp-singlenode/docker-compose/agents-N100.yml up
```

Arbitrary N:

```bash
./deploy/mvp-singlenode/docker-compose/gen-agents.sh 42 > agents-N42.yml
```

### What comes up

`base.yml` defines 8 services; the agent overlays add N × asap-otel
agent containers. Host ports:

| Port (host) | Service | Why |
|---|---|---|
| 8080 | controller API | `/api/v1/plan`, `/api/v1/runtime-samples` |
| 4320 | controller OpAMP | websocket for agent config push |
| 19090 | backend PromQL | Grafana datasource |
| 19465 | backend /metrics | Prometheus scrape |
| 9090 | Prometheus | |
| 3000 | Grafana | (admin/admin, anon viewer also allowed) |
| 9000 / 9001 | MinIO S3 / console | raw-sample cold store (local disk via named volume) |
| 4317 / 4318 | gateway OTLP | where agents ship metrics |

MinIO is configured as a local cold store (data in the
`minio-data` named volume under `/mydata/...`). Byte-layout
matches real S3 so the `s3_adapter.rs` fallback in
ASAPQuery-backend works as-is; swap credentials + endpoint when
a real bucket is available.

### Baselines

Seven baseline overlays (paper reports B0/B1/B2/B3; b4/b5 are
extra sweep axes):

| Overlay | Description |
|---|---|
| `baseline-b0a-raw-stream.yml` | Raw streaming — no sketch, per-sample OTLP forwarding |
| `baseline-b0b-raw-batched.yml` | Raw batched — OTLP batch processor only |
| `baseline-b1-serf.yml` | Serf-style gossip summarization reference |
| `baseline-b2-full.yml` | Full-sketch transmission (CMS/KLL/DDSketch snapshots) |
| `baseline-b3-delta.yml` | Delta-sketch transmission (the paper's ASAP) |
| `baseline-b4-tunable.yml` | Same as b3, parametrized over `WINDOWS` env var |
| `baseline-b5-gorilla.yml` | Gorilla-style float compression reference |

Combine with the agents overlay:

```bash
docker compose -f deploy/mvp-singlenode/docker-compose/base.yml \
               -f deploy/mvp-singlenode/docker-compose/agents-N10.yml \
               -f deploy/mvp-singlenode/docker-compose/baseline-b3-delta.yml up
```

### Inference dispatch (warm-tier query coverage)

The backend's "warm tier" (precomputed-sketch path) only answers
queries whose canonical PromQL string exact-matches an entry in the
mounted inference YAML. Five overlays live in `deploy/mvp-singlenode/configs/`:

| YAML | Mounted by | Covers (PromQL families × ranges) | Entries |
|---|---|---|---|
| `backend-inference.yaml` | `e2e-overlay.yml` (default) | All 33 patterns from `ASAPQuery-backend` PR #79: spatial multi-quantile, `quantile_over_time(φ ∈ {0.5, 0.9, 0.95, 0.99}, …[1m\|2m\|5m])`, `sum_over_time` / `count_over_time` × wider ranges, `rate` / `increase`, spatial `count` / `sum` / `avg`, `topk(5\|10\|50, …)`. | 33 |
| `backend-inference-cms.yaml` | `e2e-overlay-family.yml` (with `FAMILY=cms`) | CountMinSketch families: `{sum, count, avg}`, `{sum_over_time, count_over_time, rate, increase}` × `[1m, 2m, 5m]`. | 14 |
| `backend-inference-cs.yaml` | `e2e-overlay-family.yml` (with `FAMILY=cs`) | CountSketch families: same as CMS plus `topk(5\|10\|50, …)`. | 16 |
| `backend-inference-hll.yaml` | `e2e-overlay-family.yml` (with `FAMILY=hll`) | HLL cardinality families: spatial `count(metric_hll)` and `count_over_time(metric_hll[1m\|2m\|5m])` for both the counter (`http_requests_total_hll`) and gauge (`http_requests_total_latency_ms_hll`) flavours. | 8 |
| `backend-inference-kll.yaml` | `e2e-overlay-family.yml` (with `FAMILY=kll`) | KLL rank-quantile families: spatial `quantile by (zone) (φ, …)` and `quantile_over_time(φ, metric_kll[1m\|2m\|5m])` × `φ ∈ {0.5, 0.9, 0.95, 0.99}`. | 16 |

This is the canonical paper-experiment pattern set (PR #79
`tests/inference_yaml_pattern_coverage.rs` is the runtime contract).
Adding a new query family here without a matching entry in PR #79's
canonical YAML risks shipping warm-tier "promises" the engine can't
keep — the YAML is checked exact-string at request time by
`find_query_config`.

#### Metric-name conventions per overlay

| Overlay | Backend-side metric name | Why |
|---|---|---|
| `backend-inference.yaml` | `http_requests_total_latency_ms` (DDSketch / KLL quantile patterns), `http_requests_total` (CMS / CountSketch / HLL Sum / Count / Topk patterns) | Refactor-2026-05: sketch processors preserve the input metric name on the wire. Sketch encoding is identified by the OTLP `pdata.Metric` variant tag (DDSketch / KLLSketch / HLLSketch / CountSketch / CountMinSketch), so the backend ingests sketches under the raw input name and PromQL queries against the bare metric name resolve directly against the stored sketch state. |
| `backend-inference-cms.yaml`, `-cs.yaml` | `http_requests_total` | Same — name preserved end-to-end. |
| `backend-inference-hll.yaml` | `http_requests_total`, `http_requests_total_latency_ms` | Same. |
| `backend-inference-kll.yaml` | `http_requests_total_latency_ms` | Same. |

#### Open gap: `histogram_quantile(φ, …)` is NOT covered

The engine pattern matcher
(`asap-query-engine/src/engines/simple_engine.rs::controller_patterns`)
includes `quantile_over_time` and the spatial `quantile by (…)` ops
but does **not** include a `histogram_quantile` pattern block.
`histogram_quantile(0.99, sum by (le) (http_requests_total_latency_ms))`
fails the engine matcher before reaching `find_query_config`, even
when an entry of that exact string is present in the YAML.

Two paths forward (in priority order):

1. **Today (this PR's choice for `queries-e2e.json`):** use the
   pre-aggregated `quantile_over_time(φ, *_quantile[…])` shape. The
   gateway / agent path produces `http_requests_total_latency_ms_quantile`
   (DDSketch / KLL backed) which the engine matches and answers.
   PROGRESS.md "Single-pipeline multi-sketch + delta + queryable
   warm tier (2026-05-01)" verified this path live (`q=0.5 →
   19.49`).
2. **Tomorrow (separate engine PR):** extend `controller_patterns`
   with a `histogram_quantile` block. Out of scope for E0; tracked
   alongside PR #79 follow-ups.

### E0: single-cell smoke (P5–P9 end-to-end against a live stack)

Smallest cell that exercises the whole P1–P9 path. Use this
before kicking off the 60-cell sweep to catch wiring regressions.

```bash
# 1. Stack up. The e2e overlay is what flips controller workload
#    registry on, wires CONTROLLER_BACKEND_ENDPOINT, mounts the
#    cold-store volume into both fake-exporter (writer) and
#    backend (reader), and points the gateway at the OTLP
#    forwarder (gateway-otlp-forward.yaml). Pin cardinality and
#    frequency low — the b3-delta overlay's 100k×100Hz default
#    can blow past the OTLP exporter's 64 MiB max message size on
#    the first window, even with delta_transmission=true.
AGENT_CONFIG=asap-otel-agent-b3-delta.yaml \
EXPORTER_CARDINALITY=1000 EXPORTER_FREQ_HZ=10 \
docker compose \
    -f deploy/mvp-singlenode/docker-compose/base.yml \
    -f deploy/mvp-singlenode/docker-compose/agents-N1.yml \
    -f deploy/mvp-singlenode/docker-compose/baseline-b3-delta.yml \
    -f deploy/mvp-singlenode/docker-compose/e2e-overlay.yml \
    up -d

# 2. Wait for first sketch ingest at the backend (one agent window
#    = 60 s for b3-delta).
until docker logs docker-compose-backend-1 2>&1 \
        | grep -q "OTLP modified-proto sketch ingest"; do sleep 3; done

# 3. Drive workload + plan-transition observability concurrently.
mkdir -p /tmp/cell-smoke-e0
python3 deploy/mvp-singlenode/scripts/metricsql_replay.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --queries deploy/mvp-singlenode/scripts/queries-e2e.json \
    --qps 5 --duration 60 \
    --out /tmp/cell-smoke-e0/replay.jsonl &
python3 deploy/mvp-singlenode/scripts/plan_transition.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --transition-query 'histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))' \
    --transition-out /tmp/cell-smoke-e0/transition.jsonl \
    --sample-out /tmp/cell-smoke-e0/sample.jsonl \
    --soak-secs 60 --pre-transition-secs 20 &
# Note: the `histogram_quantile(...)` shape above is INTENTIONALLY
# unmatched by the engine — it's the capability-miss probe used by
# `plan_transition.py` to drive a fresh plan publish. Replay-side
# queries (`queries-e2e.json`) deliberately use shapes that DO match
# (see "Inference dispatch" above).
wait

# 4. Snapshot ground truth from the cold-store volume (the volume
#    goes away on `down -v`, so this has to happen before teardown).
docker cp $(docker compose \
    -f deploy/mvp-singlenode/docker-compose/base.yml \
    -f deploy/mvp-singlenode/docker-compose/e2e-overlay.yml ps -q backend):/var/asap/cold/raw \
    /tmp/cell-smoke-e0/cold-truth

# 5. Reduce + plot.
python3 deploy/mvp-singlenode/scripts/accuracy_reduce.py \
    --cell-dir /tmp/cell-smoke-e0 --out /tmp/cell-smoke-e0/accuracy.csv
python3 deploy/mvp-singlenode/scripts/e2e_plots.py \
    --sweep-root /tmp --accuracy /tmp/cell-smoke-e0/accuracy.csv \
    --out-dir /tmp/cell-smoke-e0/plots
```

Expected: `replay.jsonl` rows tagged with a non-null `plan_id`,
`transition.jsonl` with non-null `before_plan` / `after_plan`
(populated by `controller/src/metrics_exposer.rs`'s
`asap_active_plan_id` gauge), `cold-truth/<metric>/YYYY/MM/DD/HH/`
hour-bucketed JSONL, `accuracy.csv` with one row per query, and
at least `query_latency_cdf.png` + `pareto_acc_vs_thru.png` under
`plots/`. The full 5-sketch coverage in `accuracy.csv` requires
the 60-cell sweep (E3, `run_e2e_sweep.sh`); the b3-delta cell
alone only exercises DDSketch + HLL.

### Sweep driver

```bash
SOAK_S=180 ./deploy/mvp-singlenode/scripts/run-baseline-sweep.sh N=10
```

Outputs one CSV per sweep to `deploy/eval-results/`. The
measurement script (`measure-baseline.py`) scrapes Prometheus at
steady state and records CPU / RSS / throughput for agent /
gateway / backend per baseline.

## Helm (K8s / scale)

```bash
helm install asap deploy/helm/asap \
  --set agents.count=100 \
  --set backend.cold.endpoint=https://s3.amazonaws.com \
  --set backend.cold.bucket=my-raw-bucket
```

The chart defaults (see `values.yaml`) match the compose stack
byte-for-byte — same image tags, same endpoints, same resource
envelope (0.5 CPU / 512 Mi per agent, matching the paper's
"realistic edge" sizing).

**Templates are not yet written** — `values.yaml` and
`Chart.yaml` land here; the `templates/` directory is empty.
See the top-level [`PROGRESS.md`](../PROGRESS.md) "Future work"
section for the template landing order. Until templates land,
Helm is values-only; use the compose path for actual runs.

## Current known issues (read before running a sweep)

### N=10 throughput collapse (PR #185, 2026-04-22)

All six reporting baselines collapse to a universal ~2,000 pts/s
per-agent floor at N=10, vs 130k–326k at N=1. Gateway aggregate
= 10 × 2k = 20k/s. Agents are near-idle (0.01c, ~220 MiB RSS),
so this is a producer / transport / kernel bottleneck, not
agent-side saturation. Diagnosis and fix tracked top-level
in `DataCollector/PROGRESS.md`.

Until fixed, **treat N=10 as a stack-stability test, not a
scale-quality datapoint**. N=1 rows in `sweep-N1-*.csv` are the
current load-quality datapoints.

### `nan` cells in the sweep CSV

The current `measure-baseline.py` does not collect every metric
for every baseline:

- Bytes in/out only emitted by b2 / b3 — tracked in the
  top-level `PROGRESS.md` under instrumentation.
- Gateway points/s and backend samples/s only emitted by the
  raw baselines (b0a / b0b).
- `backend_query_p99_ms` is `nan` everywhere because there's
  no query-side driver in the sweep yet — tracked as the
  query-side follow-up in top-level `PROGRESS.md`.

### Grafana dashboards not yet authored

`configs/grafana-datasources.yml` provisions the Prometheus
datasource, but no dashboard JSONs are checked in. Paper figures
should be exported from dashboards; writing them is tracked
in the top-level `PROGRESS.md`.

## Evaluation → metric mapping

| Evaluation axis | Metric (Prometheus) | Scope |
|---|---|---|
| Agent CPU reduction | `container_cpu_usage_seconds_total{name=~"agent-.*"}` | B1 vs B3 |
| Bandwidth reduction | `gateway_forwarded_bytes_total` | B1 vs B3 |
| Query P99 latency | `queryengine_query_duration_seconds` | B0 vs B3 |
| Accuracy vs resource Pareto | `queryengine_cold_bytes_served_total` + `accuracy.epsilon` field on PromQL responses | all N |
| Workload drift response | `time_to_plan_ready` from `/api/v1/plan` timestamps | any N |
| N-scale | above metrics × `agents.count` | N ∈ {1, 10, 100} |
