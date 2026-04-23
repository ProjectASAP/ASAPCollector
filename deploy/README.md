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
