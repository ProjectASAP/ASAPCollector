# TODO — DataCollector + controller for paper submission

This doc lists what's left to get a VLDB / SIGMOD submission out
the door. For the v1 paper we keep updating `controller/`
directly in this repo — the [ASAPController](https://github.com/ProjectASAP/ASAPController)
consolidation is post-paper.

See also:
- [`docs/paper-outline.md`](docs/paper-outline.md) — paper
  sections, claims, experiment matrix.
- ASAPQuery-backend [`TODO.md`](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/TODO.md)
  for sketchDB-side work.

## For paper submission (blocker)

### 1. Multi-agent deployment at scale

Today the demo is single-node Docker compose. Paper experiments
need multi-agent for bandwidth and CPU/mem-per-node claims.

- K8s manifests: `N` edge agents, 1 gateway, 1 backend, 1
  controller. Parameterize `N ∈ {1, 10, 100}` via Helm values.
- S3-backed raw storage — MinIO for local eval, real S3 for
  scale runs.
- Agent-side resource requests/limits tuned to "realistic edge"
  (0.5 CPU / 512Mi).

### 2. Instrumentation

Every node exports the following so we can make "reduce by N%"
claims:

**Agents** (edge OTel collector):
- CPU (`container_cpu_usage_seconds_total`)
- Memory RSS (`container_memory_rss`)
- Network egress (`container_network_transmit_bytes_total`,
  filtered to agent→gateway and/or agent→backend links)
- Per-processor overhead (sketchcol exposes `bytes_in` /
  `bytes_out` / `processor_samples_total` / `processor_cpu_seconds`)

**Gateway**:
- Same as agents
- Plus `gateway_forwarded_bytes_total` for downstream link

**Backend** (ASAPQuery-backend):
- Query latency histogram (HTTP handler)
- Per-query CPU time + memory peak (per-request span)
- Hot vs cold bytes served (from ASAPQuery-backend TODO #1)
- Existing `queryengine_ingest_samples_blocked_by_schema_barrier_total`
  stays

**Controller**:
- Plan count / replan count
- Cost-model eval latency
- OpAMP push latency
- Query-miss-notification rate

Deliverable: Prometheus scrape config + Grafana dashboards
covering all of the above. Dashboards become paper figures.

### 3. Baselines

Four separately-deployable configs:

| Baseline | What it exercises |
|---|---|
| **B0. Prometheus native** | No DC, no sketches. Direct Prometheus scrape + query. "Do nothing fancy." |
| **B1. DC + raw-forward** | DC agents with sketchcol replaced by a passthrough processor. Tests "controller + backend minus sketches." |
| **B2. DC + hand-tuned sketches (no controller)** | Sketchcol processors configured by hand to a reasonable workload. Tests "sketches minus controller planning." |
| **B3. Full ASAP** | DC + controller + sketches + backend + cold S3 fallback. Our system. |

Each deployable via a single `docker-compose-<baseline>.yml`
and a single K8s `values-<baseline>.yaml`.

### 4. Workload

Observability traces only (no TPC-H / NYC Taxi — out of scope,
see `paper-outline.md` §non-goals).

Candidates:
- **Google cluster usage 2011 + 2019** — canonical: per-job
  CPU / memory / I/O time-series at minute granularity
- **Alibaba cluster trace 2017 / 2018** — similar shape,
  different scale / topology
- **Robinhood / Uber / Grafana-published Prometheus query logs**
  where available — for the query-side workload

Ingestion workload: replay the trace through a synthetic
"exporter" that faithfully reproduces the cardinality and
update rate.

Query workload: replay the matching PromQL query log (if we
have it) OR hand-author a representative query suite
(`N × {avg, p99, rate, topK} × {1m, 5m, 1h, 24h}`).

### 5. Controller feedback loop — e2e on the workload

Across-lifecycle claim in the paper hinges on "controller sees
a query miss, replans, next query hits." Measure this on the
Google cluster workload:

- Seed: initial controller plan has CMS for metric X but no
  `by (zone)` grouping.
- Issue: query `p99 of X by zone` → miss.
- Measure:
  - time-to-plan-ready (controller's replan latency)
  - time-to-first-hit (end-to-end wall clock from query issue
    to successful same-query hit)
  - bandwidth/CPU trace during the transition
- Assert: bounded regression (e.g. ≤2× baseline during the
  transition window).

### 6. Fault injection

Reviewers ask: "what if the controller fails?"

- **Controller kill**: backend keeps serving from last-known
  plan; queries still answered from existing sketches.
- **Agent kill**: controller detects (agent status monitor),
  marks it degraded / missing, re-plans without that agent's
  contribution.
- **Network partition** (agent ⇄ controller): agent keeps
  running its last config; controller marks config "stale"
  until link heals; reconciliation at heal time.

Each of these gets a test in `fault-injection/` (ChaosMesh or
simple `tc` rules + docker network disconnect).

### 7. Reproducibility archive

VLDB / SIGMOD Reproducibility (badge):

- `reproduce/` with one entrypoint `make reproduce`
- `Dockerfile.reproduce` baking the full stack
- `reproduce/workload/` — fetcher + anonymizer for the Google
  cluster trace
- `reproduce/expected/` — table of expected numbers (with
  tolerance bands for variance)
- README linking to the archive from the paper

## Future work (post-paper)

### F1. ASAPController split

Per the [ASAPController design](https://github.com/ProjectASAP/ASAPController):
consolidate `DataCollector/controller/` + `ASAPQuery-backend/asap-planner-rs/`
+ `asap-fusion/` into one repo. Deferred until after the
paper — the controller code keeps evolving here for v1.

### F2. OpAMP stress-test for hundreds of agents

OpAMP server handles N agents today, tested with ~dozen. Need
proper stress test at N=100+ for confidence in the scalability
story.

### F3. Sketch processor CPU offload

Edge sketchcol processors today update sketches on the
data-plane thread. Move to a worker-pool + lock-free ring
buffer for higher ingest rates on resource-constrained edge
nodes. Would help B3 CPU numbers.

### F4. Controller HA (active-passive)

Today single controller. Add an active-passive pair with a
simple leader election (etcd or similar) so a controller kill
isn't a "manual restart" event.
