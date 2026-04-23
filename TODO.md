# TODO — DataCollector + controller for paper submission

_Last updated: 2026-04-23 — N=10 "collapse" diagnosed + new 3-axis SDK design landed_

This doc lists what's left to get a VLDB / SIGMOD submission out
the door. For the v1 paper we keep updating `controller/`
directly in this repo — the [ASAPController](https://github.com/ProjectASAP/ASAPController)
consolidation is post-paper.

See also:
- [`docs/paper-outline.md`](docs/paper-outline.md) — paper
  sections, claims, experiment matrix.
- [`deploy/README.md`](deploy/README.md) / [`deploy/TODO.md`](deploy/TODO.md)
  — multi-agent stack status and compose/Helm knobs.
- ASAPQuery-backend [`TODO.md`](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/TODO.md)
  for sketchDB-side work.

## Current state (what's landed)

The multi-agent scaffold + baseline matrix + sweep harness
landed over PRs #168–#185. Briefly:

- `deploy/docker-compose/` — `base.yml` (8 services: controller,
  backend, gateway, fake-exporter, minio, minio-setup, prometheus,
  grafana) + `agents-N{1,10,100}.yml` overlays + `gen-agents.sh`
  for arbitrary N.
- 7 baselines as separate overlays: b0a-raw-stream, b0b-raw-batched,
  b1-serf, b2-full, b3-delta, b4-tunable, b5-gorilla. (The paper
  presents B0/B1/B2/B3; b4 is the window-duration sweep axis and
  b5 is a Gorilla reference point.)
- `deploy/helm/asap/` — `Chart.yaml` + `values.yaml` with the
  paper's resource envelope (0.5 CPU / 512 Mi per agent).
  Templates not yet written.
- Five Dockerfiles in `deploy/docker/` (sketchcol, sketchcol-stock,
  backend, controller, fake-exporter).
- Sweep driver (`deploy/scripts/run-baseline-sweep.sh`) +
  measurement (`deploy/scripts/measure-baseline.py`).
- Controller `/metrics` Prometheus exposer + gRPC
  `runtime-samples` receiver (PRs #169, #170).
- N=1 baseline sweep results in `deploy/eval-results/` — clean,
  per-agent throughput 130k–326k pts/s across baselines, usable
  as the load-quality datapoint.

## For paper submission (blocker)

### 1. SDK cost evaluation (P0) — supersedes the "N=10 throughput collapse" blocker

The N=10 "collapse" (#185) turned out not to be a bottleneck:
the 2 k pts/s floor was the OTel SDK's correct pre-aggregation
output at `interval=1 s, cardinality=1000, 2 instruments`,
independent of input rate. That finding reframed the bandwidth
claim as a **three-independent-factor product** across time
window `W`, label projection `L`, and encoding `agg_type`; see
[`docs/sdk-cost-evaluation.md`](docs/sdk-cost-evaluation.md)
for the design + methodology.

Concrete work items (P0 because the cost sweeps can't run without
them):

- [x] ~~`AggregationRawBuffer`~~ — merged. Contract test in
      `deploy/fake-exporter/sdk_emit_test.go`.
- [x] ~~`Aggregation<X>Delta` × 5~~ — four of five (DDSketch /
      CS / CMS / HLL) already have `DeltaTransmission` as a
      flag on the `*-full` aggregator. `kll-delta` isn't
      needed (see
      [`docs/sdk-cost-evaluation.md`](docs/sdk-cost-evaluation.md)
      for rationale).
- [x] ~~`fake-exporter` three-axis knobs~~ (`deploy/fake-exporter/main.go`)
      — merged (`EXPORTER_SDK_WINDOW`, `EXPORTER_SDK_PROJECTION`,
      `EXPORTER_SDK_AGG`; 4-dim label schema).
- [x] ~~`measure-baseline.py` producer-side columns~~
      (`producer_cpu_cores`, `producer_rss_mib`,
      `producer_bytes_out_per_s`) — merged.
- [x] ~~First-pass cost sweeps at N=1~~ — CSVs + findings in
      `deploy/eval-results/sdk-cost/`. Known methodology
      caveat (`BYTES_WIN < W` under-reports bytes on some
      cells) called out in the findings doc.
- [ ] **V2 cost sweep** with `BYTES_WIN ≥ 2×W` so the absolute
      bandwidth numbers are trustworthy on every row, not only
      ratios within a sub-experiment. ~90 min.
- [ ] **N-scale sweep rerun** at fixed representative
      `(W=60s, L=subset, agg=dd-delta)` across `N ∈ {1, 10, 100}`.
      This is now the honest scalability test — the 2 k floor
      from #185 is expected; we're looking for whether gateway /
      backend hold up as aggregate ingress grows.
- [ ] **Profile the label-axis CPU climb** — producer CPU
      increases ~4× when the `AttributeFilter` is active;
      root-cause before the numbers go into a paper figure.

### 2. Instrumentation — fill the `nan` columns (P1)

`sweep-N10-20260422.csv` has missing measurements:

- `agent_in_kib_per_s` / `agent_out_kib_per_s` — `nan` on
  b0a, b0b, b1, b5. Only b2 (full sketch) and b3 (delta) have
  these. Needed for the cross-baseline bandwidth comparison
  across **all** baselines, not just the two with byte
  counters.
- `gateway_points_per_s` / `gateway_out_series_per_s` — `nan`
  on sketch baselines (b1, b2, b3, b5). Only raw baselines
  (b0a, b0b) have them.
- `backend_samples_per_s` — `nan` on all sketch baselines.
- `backend_query_p99_ms` — `nan` everywhere. Query side isn't
  driven during the sweep; see the query-side item below.

Deliverable: `measure-baseline.py` + Prometheus scrape covers
every cell of the matrix for every baseline.

Also still-TODO:

- **Grafana dashboards.** `configs/grafana-datasources.yml`
  provisions the datasource; no dashboard JSONs exist yet.
  Plot sources for each evaluation axis: producer CPU,
  producer bandwidth, query latency, workload drift, N-scale.
- **Per-processor overhead** — sketchcol exposes `bytes_in /
  bytes_out / processor_samples_total / processor_cpu_seconds`
  but the sweep script doesn't collect them per-processor today.

### 3. Query side of the sweep (P1)

The current sweep only drives ingest; it does not issue any
PromQL queries while the stack is warm. Query-latency and
accuracy-vs-resource Pareto evaluation both need:

- A query replay process co-located with the load generator,
  issuing a query suite (avg / p99 / rate / topK × {1m, 5m, 1h}
  windows) at a steady rate for the SOAK window.
- Per-query attributes captured:
  `queryengine_query_duration_seconds`,
  `queryengine_cold_bytes_served_total`,
  `queryengine_ingest_samples_blocked_by_schema_barrier_total`,
  PromQL response's `accuracy.epsilon` field (once exposed —
  see ASAPQuery-backend TODO §2).
- Sweep output adds: `query_p50/p99_ms`, `cold_bytes_served`,
  `barrier_drops`.

### 4. Real workload — Google cluster trace wiring (P2)

fake-exporter already has trace-replay mode and a demo dataset
(PR #182). Still needed:

- Fetcher for Google cluster 2011 **and** 2019 traces
  (`datasets_eval/` has scaffolding, no fetcher yet).
- Mapping from trace rows to OTLP series (preserve cardinality +
  update rate).
- Matching PromQL query log if Robinhood/Uber/Grafana-published
  logs are usable; otherwise the hand-authored query suite from
  §3 is the fallback. Paper uses at minimum one synthetic +
  one trace workload.

### 5. Controller feedback loop end-to-end on real workload (P2)

HTTP-layer round-trip is done (see ASAPQuery-backend
`capability_miss_http_e2e_tests.rs`). What's missing is the
cross-process story with real ingest:

- Seed: initial plan has CMS for metric X, no `by (zone)`.
- Issue: `p99 of X by zone` → capability miss.
- Measure: time-to-plan-ready, time-to-first-hit,
  bandwidth/CPU during the transition.
- Assert: bounded regression (≤ 2× baseline during the
  transition window).

Depends on §1 (throughput) + §3 (query side) + §4 (real data).

### 6. Fault injection (P3)

Reviewer-facing "what if the controller fails?"

- Controller kill — backend keeps serving from last-known plan.
- Agent kill — controller detects, marks degraded, replans.
- Network partition (agent ⇄ controller) — agent keeps running
  last config; reconciliation on heal.

Implement each as a test under `fault-injection/` using
ChaosMesh if on K8s, or `docker network disconnect` + `tc`
rules on compose.

### 7. Reproducibility archive (P3)

VLDB / SIGMOD Reproducibility (badge):

- `reproduce/` with `make reproduce` entry point.
- `Dockerfile.reproduce` baking the full stack.
- `reproduce/workload/` — fetcher + anonymizer for the Google
  cluster trace (ties to §4).
- `reproduce/expected/` — expected-number table with tolerance
  bands.

## Future work (post-paper)

### F1. ASAPController split

Per the [ASAPController design](https://github.com/ProjectASAP/ASAPController):
consolidate `DataCollector/controller/` + `ASAPQuery-backend/asap-planner-rs/`
+ `asap-fusion/` into one repo. Deferred until after the
paper — the controller code keeps evolving here for v1.

### F2. OpAMP stress-test for hundreds of agents

OpAMP server handles N agents today, tested with ~dozen. Need
proper stress test at N=100+ for confidence in the scalability
story. (Partially reached once §1 is fixed and N=100 sweep
runs clean.)

### F3. Sketch processor CPU offload

Edge sketchcol processors today update sketches on the
data-plane thread. Move to a worker-pool + lock-free ring
buffer for higher ingest rates on resource-constrained edge
nodes.

### F4. Controller HA (active-passive)

Today single controller. Add an active-passive pair with a
simple leader election (etcd or similar) so a controller kill
isn't a "manual restart" event.

### F5. Helm templates

`deploy/helm/asap/` has values.yaml + Chart.yaml but no
templates. Writing them properly wants an initial pass on a
real cluster to validate readiness probes / resource
requests / network policies. Out of scope for the paper (compose
covers N ≤ ~50 on a single beefy box); needed for the
reproducibility archive if we promise K8s replay.
