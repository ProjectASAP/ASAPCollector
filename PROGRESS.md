# DataCollector progress

_Last updated: 2026-04-23._

Single source of truth for where DataCollector stands: what's
implemented, what's outstanding, what's out of scope for this
paper. Scoped siblings:

- [`docs/paper-outline.md`](docs/paper-outline.md) — paper
  sections, claims, experiment matrix.
- [`docs/sdk-cost-evaluation.md`](docs/sdk-cost-evaluation.md)
  — SDK-side aggregation knobs + cost-evaluation design.
- [`deploy/README.md`](deploy/README.md) — multi-agent stack
  setup + sweep driver.
- ASAPQuery-backend [`TODO.md`](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/TODO.md)
  — sketch-DB-side work.

## Implemented

### SDK-side aggregators — `opentelemetry-go-patch/sdk/metric`

Drives the encoding axis of the SDK cost evaluation. Status per
slot:

| `agg_type` | Status | Notes |
|---|---|---|
| `sum`, `last-value`, `explicit-bucket-histogram`, `exponential-histogram` | ✅ upstream | `go.opentelemetry.io/otel/sdk/metric` |
| `dd-full`, `dd-delta` | ✅ | `AggregationDDSketch{DeltaTransmission?: bool}` |
| `kll-full` | ✅ | `AggregationKLLSketch{}` |
| `cs-full`, `cs-delta` | ✅ | `AggregationCountSketch{DeltaTransmission?: bool}` |
| `cms-full`, `cms-delta` | ✅ | `AggregationCountMinSketch{DeltaTransmission?: bool}` |
| `hll-full`, `hll-delta` | ✅ | `AggregationHLLSketch{DeltaTransmission?: bool}` |
| `raw-buffer` | ✅ | `AggregationRawBuffer`; impl `opentelemetry-go-patch/sdk/metric/internal/aggregate/rawbuffer.go`; contract test `deploy/fake-exporter/sdk_emit_test.go` |
| `kll-delta` | no need | KLL's multi-level sample buffers make a byte-diff no smaller than the full sketch. `kll-full` suffices for the encoding-axis comparison. See `docs/sdk-cost-evaluation.md`. |

### Collector-side sketch processors — `opentelemetry-collector-contrib-patch/processor/`

All five sketches use the SDK pre-aggregation path — SDK emits
a typed metric data point (`DDSketch` / `KLLSketch` /
`CountSketch` / `CountMinSketch` / `HLLSketch`), collector
deserializes the sketch bytes, merges into its own per-series
or per-window sketch, emits the aggregated result.

| Processor | Algorithm | Query | Batch | Window |
|---|---|---|---|---|
| `ddsketchprocessor` | DDSketch | Quantiles | ✅ | ✅ |
| `kllprocessor` | KLL | Quantiles | ✅ | ✅ |
| `countsketchprocessor` | CountSketch | Frequency / heavy hitters | ✅ | ✅ |
| `countminsketchprocessor` | Count-Min Sketch | Frequency | ✅ | ✅ |
| `hllprocessor` | HyperLogLog | Cardinality | ✅ | ✅ |
| `countsketchmergeprocessor`, `countminsketchmergeprocessor` | — | Backend-tier merge of per-series sketches | ✅ | ✅ |

Serialization is canonical via `sketchlib-go` (KLL / CS / HLL
use `SerializeToBytes`; CMS uses a per-snapshot gob of
`{Rows, Cols, Count, Sum, Sum2, L1, L2}`).

### Controller — `controller/`

- `/metrics` Prometheus exposer + gRPC `runtime-samples` receiver.
- 5-layer planning pipeline (query → language AST → sketch
  algebra → optimizer → physical plan). See
  [`controller/docs/query-to-sketch-translation.md`](controller/docs/query-to-sketch-translation.md).
- OpAMP config push to agents.

### Multi-agent deploy — `deploy/`

- `deploy/docker-compose/`: `base.yml` (8 services — controller,
  backend, gateway, fake-exporter, minio, minio-setup,
  prometheus, grafana) + `agents-N{1,10,100}.yml` overlays +
  `gen-agents.sh` for arbitrary `N`.
- 7 baseline overlays: `b0a-raw-stream`, `b0b-raw-batched`,
  `b1-serf`, `b2-full`, `b3-delta`, `b4-tunable`, `b5-gorilla`.
- 5 Dockerfiles under `deploy/docker/`: `sketchcol`,
  `sketchcol-stock`, `backend`, `controller`, `fake-exporter`.
- `deploy/helm/asap/`: `Chart.yaml` + `values.yaml` with the
  paper's resource envelope (0.5 CPU / 512 Mi per agent).
  Templates not yet written.

### Evaluation tooling — `deploy/scripts/`

- `run-sdk-cost-grid.sh` — engine: iterates the
  `WINDOWS × PROJECTIONS × AGGS` grid supplied via env.
- `run-sdk-cost-eval.sh` — runs the three canonical
  sub-sweeps (time axis, label axis, encoding axis) via the
  grid engine.
- `run-baseline-sweep.sh` — older per-baseline sweep driver
  (pre-cost-eval, still valid for the baseline matrix).
- `measure-baseline.py` — per-cell scrape of Prometheus + docker
  stats; producer-side columns (CPU / RSS / tx bytes) included.

### Evaluation tooling — `otel_collector_benchmark/` (eval-suite expansion, 2026-04-30)

In-process / single-host benches that don't need the deploy stack;
useful for fast iteration on sketch-internal claims and for paper
plots that only require a producer + collector pair. Landed via
[#197](https://github.com/ProjectASAP/DataCollector/pull/197),
[#198](https://github.com/ProjectASAP/DataCollector/pull/198),
[#199](https://github.com/ProjectASAP/DataCollector/pull/199).

- `matched_accuracy/` (new Go module) — DDSketch / KLL / T-Digest /
  HDR / linhist / raw at the **same** p99 error target. Full sweep
  across Zipf `s ∈ {1.01, 1.5, 2.5}` × 1M samples checked in.
  Headline: DDSketch ~0.5–1% p99 rel-err at **0.9–2 KB** vs HDR
  exact at 123 KB and raw at 8 MB.
- `cardinality_crossover/` (new Go module) — CountSketch and
  CountMinSketch sketch-bytes vs raw-bytes across `N ∈ {100, 1k,
  10k, 100k, 1M, 5M}`, default and narrowed dim configs. Full sweep
  CSVs checked in.
- `bench_delta_sweep.sh` + `delta_sweep_config_template.yaml` —
  window `{1s, 5s, 30s, 5m}` × threshold `{0, 0.1, 1.0}` matrix on
  the existing delta-transmission processors.
- `bench_2node_sim.sh` — single-host simulation of a 2-node
  deployment (port-shifted configs, per-node CPU/RSS, balance
  metric). Lifts to real two-node by swapping the binary launcher
  for ssh-spawn.
- `bench_soak.sh` — long-running steady-state with minute-resolution
  CPU/RSS/heap/fd-count + slope-based leak verdict.
- `telegraf_benchmarks/run_gorilla_local.sh` — wraps existing
  send_firehose.py + summarize_telegraf_metrics.py + 1 Hz ps
  sampler around `max-throughput-gorilla-local.conf`.
- `datasets_eval/debs/benchmark/` — `crosskey` subcommand on
  `run.py` plus `groupings.py` / `compare_crosskey.py` for the
  cross-key merging accuracy plot ([#199](https://github.com/ProjectASAP/DataCollector/pull/199)).

### Evaluation artefacts

- N=1 and N=10 baseline sweep CSVs in `deploy/eval-results/`
  (pre-cost-eval).
- First-pass SDK cost evaluation in
  `deploy/eval-results/sdk-cost/`:
  - `time-axis-20260423.csv` — varies `W`, fixes `L=keep-all, agg=dd-full`.
  - `label-axis-20260423.csv` — varies `L`, fixes `W=60s, agg=dd-full`.
  - `encoding-axis-20260423.csv` — varies `agg`, fixes `W=60s, L=zone,rack`.
  - `FINDINGS-20260423.md` — interpretation + methodology caveat.

## Outstanding — paper blockers

1. **V2 cost sweep with `BYTES_WIN ≥ 2×W`.** First pass used
   `BYTES_WIN=20s < W=60s` on most cells, so absolute bandwidth
   numbers are under-reported. Ratios within a sub-experiment
   are fine; absolute numbers need a rerun (~90 min wall).
2. **Profile the label-axis `AttributeFilter` hot path.** First
   pass showed producer CPU climbing ~4× under label
   projection. Root-cause before that number goes into a
   figure.
3. **Fill the `nan` columns in the multi-agent sweep CSV.**
   - Agent `bytes_in` / `bytes_out` on raw + Gorilla baselines
     (`b0a`, `b0b`, `b1`, `b5`).
   - Gateway `points/s` + backend `samples/s` on sketch
     baselines.
   - `backend_query_p99_ms` — needs the query-side driver
     (item 4).
   - Grafana dashboards — one per evaluation axis (producer
     CPU, producer bandwidth, query latency, workload drift,
     N-scale).
4. **Query side of the sweep.** Co-located PromQL replay
   issuing avg / p99 / rate / topK queries over {1m, 5m, 1h}
   windows at steady rate. Capture `query_p50/p99_ms`,
   `cold_bytes_served`, `barrier_drops`.
5. **N-scale sweep rerun** at a representative `(W, L, agg)`
   across `N ∈ {1, 10, 100}`. The 2 k pts/s floor from the
   earlier N=10 sweep is expected under SDK pre-aggregation;
   the question is whether gateway / backend hold up as
   aggregate ingress scales.
6. **Real workload — Google cluster trace.** Fetcher for
   2011 + 2019 traces (`datasets_eval/` has scaffolding, no
   fetcher yet); mapping from trace rows to OTLP series;
   matching PromQL query log.
7. **Controller feedback loop e2e on real workload.** HTTP
   round-trip is done in ASAPQuery-backend
   (`capability_miss_http_e2e_tests.rs`). Cross-process story
   with real ingest still needed: seed plan, inject capability
   miss, measure time-to-plan-ready / time-to-first-hit /
   bw + CPU during transition, assert bounded regression.
8. **Fault injection.** Controller kill, agent kill, network
   partition. Tests under `fault-injection/`:
   - `controller-kill.sh` — `docker kill`; assert queries keep
     serving from the last-known plan.
   - `agent-kill.sh` — `docker kill` one agent; assert the
     controller marks it degraded and replans.
   - `network-partition.sh` — `docker network disconnect`
     agent ⇄ controller; assert the agent runs its last config
     and reconciliation happens at heal.

   ChaosMesh variants on K8s go under the Helm chart.
9. **Reproducibility archive.** `reproduce/` with
   `make reproduce`, `Dockerfile.reproduce`, trace fetcher /
   anonymiser, expected-numbers table with tolerance bands.

## Outstanding — SDK runtime (not a cost-eval blocker)

- **Hot-reload of View `AttributeFilter`.** Upstream OTel Go
  SDK doesn't support replacing a View's filter after
  `MeterProvider` construction. Fine for static cost sweeps
  (each run is a fresh process), but the controller-in-loop
  scenario where the planner pushes a new `L` mid-run needs a
  hot-reload hook. Small patch in
  `opentelemetry-go-patch/sdk/metric/` to expose a swap API.

## Future work (post-paper)

- **ASAPController split.** Consolidate
  `DataCollector/controller/` +
  `ASAPQuery-backend/asap-planner-rs/` + `asap-fusion/` into
  one repo. Deferred — controller keeps evolving here for v1.
- **OpAMP stress test at `N = 100+` agents.** Currently tested
  with ~dozen; proper stress test needed for the scalability
  story.
- **Sketch-processor CPU offload.** Edge sketchcol processors
  update sketches on the data-plane thread. Worker-pool +
  lock-free ring buffer for higher ingest rates on
  resource-constrained edge nodes.
- **Controller HA.** Single controller today. Active-passive
  pair with simple leader election (etcd or similar) so a
  controller kill isn't a "manual restart" event.
- **Helm chart templates.** `deploy/helm/asap/` has
  `values.yaml` + `Chart.yaml` but no templates. Needs an
  initial pass on a real cluster to validate readiness probes /
  resource requests / network policies. Required for the
  reproducibility archive if we promise K8s replay. Landing
  order (one at a time, so each is reviewable):
  `_helpers.tpl` → `controller.yaml` → `backend.yaml` (adds
  PVC for the sketch-DB disk) → `gateway.yaml` → `agents.yaml`
  (replicas = `{{ .Values.agents.count }}` + headless Service
  for Prometheus DNS SD) → `minio.yaml` (StatefulSet + PVC,
  gated on `.Values.minio.enabled`) → `prometheus.yaml` +
  `grafana.yaml`.
- **Compose polish.**
  - Per-agent `AGENT_ID` label. The static enumeration in
    `agents-N*.yml` works for N ≤ 100 but bloats the
    Prometheus target list. Once the agent emits its own
    hostname as a label, the scrape config collapses to a
    single DNS-SD rule.
  - CI check that `base.yml + agents-N<K>.yml + baseline-*.yml`
    merge to a valid combined compose config.
  - `deploy/k8s/` plain manifests as a non-Helm alternative
    for operators who don't want Helm. Lowest priority.

## Architecture

```
Load generator — deploy/fake-exporter/ (OTel-SDK-instrumented app)
        │
        │ OTLP gRPC
        ▼
Agent OTel collector (sketchcol)
  ├── receiver/otlpreceiver
  ├── processor/{dd,kll,cs,cms,hll}sketchprocessor
  ├── processor/batchprocessor
  └── exporter/otlp
        │
        ▼
Gateway OTel collector (contrib 0.108)
        │
        ▼
ASAPQuery backend (sketchDB + PromQL surface)
        ▲
        │ cold fallback
        │
   MinIO (local) / S3 (prod) — raw JSONL parts
                                under raw/<metric>/YYYY/MM/DD/HH/


Controller — DataCollector/controller/
  observes: query workload, SLAs, agent metrics
  decides:  per-metric (W, L, agg_type) triple
  pushes:   OpAMP → agent sketchcol config
            HTTP  → backend streaming config
```
