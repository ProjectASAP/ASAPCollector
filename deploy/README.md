# ASAP multi-agent deployment

Stack that backs the paper's §6 eval (DataCollector `TODO.md`
blocker #1). Lives in two flavours — **docker-compose** for
single-machine dev + small-scale runs, and a **Helm chart** for
the real K8s scale points.

## Scale dials (the paper's x-axis)

```
N ∈ {1, 10, 100}   # number of edge agents
```

All other components (backend, gateway, controller, MinIO,
Prometheus, Grafana) stay at 1 replica — paper's claim is "one
controller coordinates N agents".

## Compose (dev / small scale)

```bash
# N=1
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N1.yml up

# N=10
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N10.yml up

# N=100 (requires ≥ ~60 GB RAM, 50 CPU cores — not realistic on
# a laptop; use the Helm path for real 100-agent runs)
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/agents-N100.yml up
```

Arbitrary N:

```bash
./deploy/docker-compose/gen-agents.sh 42 > agents-N42.yml
```

### What comes up

| Port (host) | Service | Why |
|---|---|---|
| 8080 | controller API | `/api/v1/plan`, `/api/v1/runtime-samples` |
| 4320 | controller OpAMP | websocket for agent config push |
| 19090 | backend PromQL | Grafana datasource |
| 19465 | backend /metrics | Prometheus scrape |
| 9090 | Prometheus | |
| 3000 | Grafana | (admin/admin, anon viewer also allowed) |
| 9000 / 9001 | MinIO S3 / console | raw-sample cold store |
| 4317 / 4318 | gateway OTLP | where agents ship metrics |

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

## What still needs to be built

This PR lands the **topology scaffold** — compose files, Helm
values, Prometheus / Grafana provisioning, controller Dockerfile.
Three pieces of the Docker-image supply chain are still TODO:

1. **`asap/sketchcol:dev`** — the edge agent image. Builds from
   a patched `otel-collector-contrib` + ASAP's sketch processors.
   Dockerfile lives in `deploy/docker/Dockerfile.sketchcol`
   (not yet written); the build script that stitches the upstream
   collector with our processor binaries lives in
   `build_sketchcollector.sh` at the repo root.
2. **`asap/query-backend:dev`** — backend image built from
   `ASAPQuery-backend/main.rs`. A Dockerfile for the backend
   repo is tracked in `ASAPQuery-backend/TODO.md`.
3. **`asap/fake-exporter:dev`** — synthetic metrics producer
   that replays the Google cluster 2011/2019 trace.
   `datasets_eval/` has the scaffolding but no image build yet.

Until those three images exist, only the **controller** +
**MinIO** + **Prometheus** + **Grafana** services come up clean
from `docker compose`. The rest fail to pull and you'll see
`pull access denied for asap/...:dev`. That's expected for this
scaffold PR.

## Paper §6 mapping

| Experiment | Metric (Prometheus) | Scope |
|---|---|---|
| 6.2 CPU reduction | `container_cpu_usage_seconds_total{name=~"agent-.*"}` | B1 vs B3 |
| 6.2 bandwidth reduction | `gateway_forwarded_bytes_total` | B1 vs B3 |
| 6.3 query P99 latency | `queryengine_query_duration_seconds` | B0 vs B3 |
| 6.4 ε vs resource Pareto | `queryengine_cold_bytes_served_total` + `accuracy.epsilon` field on PromQL responses | all N |
| 6.5 workload drift response | `time_to_plan_ready` from `/api/v1/plan` timestamps | any N |
| 6.7 N × scale | above metrics × `agents.count` | N ∈ {1, 10, 100} |

See `docs/paper-outline.md` for the full eval matrix.
