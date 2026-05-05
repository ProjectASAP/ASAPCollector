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
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N1.yml up

# N=10
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N10.yml up

# N=100 (requires ≥ ~60 GB RAM, 50 CPU cores — fine on a 40-core /
# 187 GB workstation, not on a laptop; Helm path once templates land)
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N100.yml up
```

Arbitrary N:

```bash
./deploy/docker-compose/gen-agents.sh 42 > agents-N42.yml
```

### What comes up

`base.yml` defines 8 services; the agent overlays add N × sketchcol
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
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N10.yml \
               -f deploy/docker-compose/baseline-b3-delta.yml up
```

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
AGENT_CONFIG=sketchcol-agent-b3-delta.yaml \
EXPORTER_CARDINALITY=1000 EXPORTER_FREQ_HZ=10 \
docker compose \
    -f deploy/docker-compose/base.yml \
    -f deploy/docker-compose/agents-N1.yml \
    -f deploy/docker-compose/baseline-b3-delta.yml \
    -f deploy/docker-compose/e2e-overlay.yml \
    up -d

# 2. Wait for first sketch ingest at the backend (one agent window
#    = 60 s for b3-delta).
until docker logs docker-compose-backend-1 2>&1 \
        | grep -q "OTLP modified-proto sketch ingest"; do sleep 3; done

# 3. Drive workload + plan-transition observability concurrently.
mkdir -p /tmp/cell-smoke-e0
python3 deploy/scripts/promql_replay.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --queries deploy/scripts/queries-e2e.json \
    --qps 5 --duration 60 \
    --out /tmp/cell-smoke-e0/replay.jsonl &
python3 deploy/scripts/plan_transition.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --transition-query 'histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))' \
    --transition-out /tmp/cell-smoke-e0/transition.jsonl \
    --sample-out /tmp/cell-smoke-e0/sample.jsonl \
    --soak-secs 60 --pre-transition-secs 20 &
wait

# 4. Snapshot ground truth from the cold-store volume (the volume
#    goes away on `down -v`, so this has to happen before teardown).
docker cp $(docker compose \
    -f deploy/docker-compose/base.yml \
    -f deploy/docker-compose/e2e-overlay.yml ps -q backend):/var/asap/cold/raw \
    /tmp/cell-smoke-e0/cold-truth

# 5. Reduce + plot.
python3 deploy/scripts/accuracy_reduce.py \
    --cell-dir /tmp/cell-smoke-e0 --out /tmp/cell-smoke-e0/accuracy.csv
python3 deploy/scripts/e2e_plots.py \
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
SOAK_S=180 ./deploy/scripts/run-baseline-sweep.sh N=10
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
