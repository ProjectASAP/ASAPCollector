# DataCollector progress

_Last updated: 2026-04-30._

## All-five-sketch runtime e2e verification (2026-04-30)

PromQL → controller → agent (sketchcol) → backend (precompute_engine) →
PromQL response, end-to-end through the modified-OTLP wire format
(typed `Metric.data = {DDSketch | KLLSketch | HLLSketch | CountSketch
| CountMinSketch}` data points, not Gauge-with-payload). One soak
per sketch with a single configuration; this is *the* path that the
sweep harness (P5–P9) drives.

| Sketch | Query (PromQL) | Result | Accuracy envelope | Notes |
|---|---|---|---|---|
| DDSketch | `histogram_quantile(0.5,…)` etc. | q=0.5→21.12, q=0.9→47.95, q=0.99→104.60 | `relative_quantile`, ε=0.01 | Agent uses `sketchlib-go/DDSketch` (replaced DataDog impl); proto envelope encoded via `SerializePortable`. |
| KLLSketch | `histogram_quantile(0.5,…)` | q=0.5→18.26 | `rank_quantile`, ε=0.16 | sketchlib-go KLL `SerializeMsgpack` → backend `DatasketchesKLLAccumulator::from_msgpack_bytes`. |
| HLLSketch | `count(http_requests_total)` | 149.68 distinct | `relative_cardinality`, ε=0.008 | HLL accumulator's `query_statistic` accepts both `Statistic::Cardinality` and `Statistic::Count` (Count alias added). |
| CountSketch | `sum_over_time(http_requests_total[1m])` | 24266 | `additive_frequency`, ε=0.03 | CountSketch query_statistic returns row-mean total when no key is provided. |
| CountMinSketch | `sum_over_time(http_requests_total[1m])` | 145735 | `additive_frequency`, ε≈0.0027, δ=0.03125 | CMS query_statistic now mirrors CountSketch's no-key fallback: returns the min-row sum (canonical CMS total-event estimator). |

### Cross-cutting fixes that made the e2e land

- **sketchlib-go**: `SerializeMsgpack` added to HLL / CountSketch /
  CountMinSketch (parity with KLL); cross-language wire format is
  what `ASAPQuery-backend` consumes through
  `*::from_msgpack_bytes`.
- **Agent processor (`ddsketchprocessor`)**: replaced
  `github.com/DataDog/sketches-go` with `sketchlib-go/DDSketch` so
  the proto envelope is decodable by `asap_sketchlib`'s
  `DDSketchState`.
- **Controller `data_sink`** (`controller/src/types.rs` +
  `config/agent.rs`): generated agent config now picks between
  `Otlp{endpoint,…}` and `PrometheusScrape{…}` exporters via an
  `AgentDataSink` enum, instead of always emitting the
  `prometheus` exporter (architectural fix the user flagged —
  Prometheus exposition is not a controller concern).
- **OpAMP framing** (`controller/src/opamp/mod.rs`): incoming WS
  payloads have their varint header stripped before proto decode;
  outbound `ServerToAgent` frames carry the
  `ReportFullState` flag and the Accept/Offer capability bitmask
  so agents accept and apply config.
- **Backend `query_statistic`**: implemented Quantile / Sum / Count
  / Min / Max for DDSketch; Cardinality (+ Count alias) for HLL;
  Topk / Count / Sum for CountSketch; **Count / Sum (no-key) for
  CMS** with the min-row-sum estimator (this PR).
- **Build glue**: the OCB v0.141.0 builder file is now
  `cmd/sketchcollector/builder-config-sketches.yaml` (renamed from
  `builder-config-ddonly.yaml`); compiles all five sketch
  processors plus `opampextension`.

### Limitations + follow-ups

- **CMS query without a paired key aggregator returns total volume,
  not per-key frequency.** That's the right answer for `sum / count
  / sum_over_time / count_over_time` (every insert increments one
  cell per row, so the min row total is the exact insert count
  modulo CMS hashing collisions — and CMS never *under*-counts). To
  serve `topk(N, …)` over CMS-tracked frequencies the system needs
  a paired `SetAggregator` / `DeltaSetAggregator` running on the
  agent so the backend can enumerate keys in the multi-population
  dual-input path. Out of scope for this verification round.
- **`compatible_agg_types` in `capability_matching.rs` does not list
  CountMinSketch under `Statistic::Sum`** even though
  `query_logics::logics::map_statistic_to_precompute_operator`
  treats CMS as the canonical approximator for both Sum and Count.
  The exact-match `find_query_config` path bypasses
  capability_matching and made the e2e pass; reconciling the two
  tables (so capability matching also picks CMS for Sum) is a
  separate cleanup.

## e2e harness build-out (P1–P9, in progress)

Driven by the user request for a real complete e2e: PromQL →
controller → plan push → agent sketch + backend query → accuracy
+ throughput + latency + plan-transition observability.

| Step | Status | Notes |
|---|---|---|
| P1. Wire `ASAP_COLD_STORE_ROOT` in `asap-query-engine/main.rs` | ✅ 2026-04-30 | `--cold-store-root` flag (env `ASAP_COLD_STORE_ROOT`) selects `prometheus_promql_with_cold`; 4 unit tests |
| P2. Hot-reload View `AttributeFilter` (mid-run projection swap) | ✅ 2026-04-30 | `deploy/fake-exporter/swappable_filter.go` — atomic.Pointer-backed filter wired into `Stream.AttributeFilter`; `POST /control/projection` HTTP endpoint; 5 tests incl. race + e2e through ManualReader. **No SDK patch was needed**: the SDK's `aggregate.Builder.filter` closure dispatches through the function value, so atomic-state inside the filter is observable on the next measurement. |
| P3. Build deploy images + N=1 b3-delta smoke run | ✅ 2026-04-30 | All four images (`asap/{controller,query-backend,fake-exporter,sketchcol}:dev`) build cleanly and `docker compose up` stands up the full stack. Verified: backend logs cold-tier fallback enabled; raw-tee writes ground-truth JSONL with the right path layout; swappable-filter HTTP swap returns `{"applied":"zone"}`. The earlier warm-tier limitation (gateway PRW dropping typed sketches) is now resolved by the OTLP-end-to-end path landed in the deploy follow-up — see follow-up #1 below. |
| P4. Ground-truth tee from fake-exporter to MinIO raw JSONL | ✅ 2026-04-30 | `deploy/fake-exporter/raw_tee.go` — atomic.Pointer-style hour-bucketed JSONL writer matching the Rust `RawSample` wire format byte-for-byte. 8 unit tests incl. concurrent-writer race + format anchor + per-instance file naming. Wired into `runSynthetic` + `runTraceReplay`; controlled by `EXPORTER_RAW_TEE_ROOT` env. e2e overlay mounts a shared `cold-store` Docker volume into both fake-exporter (writer) and backend (reader). |
| P5. PromQL replay client with plan-id tagging | ✅ 2026-04-30 | `deploy/scripts/promql_replay.py` — fires PromQL at backend `:19091` at fixed QPS, captures p50/p99 + result vector per query, tags every line of the JSONL log with the controller's currently-published `plan_id` (1 Hz polling thread). Smoke-tested: 17 queries / 6 s, p50 2.4 ms, p99 1.3 s (cold-fallback dominated). |
| P6. Plan-transition driver + 1 Hz CPU/bandwidth sampler | ✅ 2026-04-30 | `deploy/scripts/plan_transition.py` — fires a query the active plan can't answer; tracks `t_query_in / t_plan_ready / t_first_hit / t_steady` against the controller's plan-id stream; `DockerStatsSampler` dumps 1 Hz cpu/mem/net per container to a separate JSONL. Imports + smoke-tests pass. |
| P7. Sweep runner over sketch × N × scrape × cardinality matrix | ✅ 2026-04-30 | `deploy/scripts/run_e2e_sweep.sh` — drives `{DDSketch, KLL, CS, CMS, HLL} × {N=1, 10} × {scrape=100 ms, 1 s} × {card=1e3, 1e4, 1e5}` (60 cells). Per cell: brings stack up, runs P5 + P6 concurrently for the soak window, snapshots the cold-truth volume into the cell directory, brings stack down with `-v`. |
| P8. Accuracy reducer (truth ⋈ sketch answer) | ✅ 2026-04-30 | `deploy/scripts/accuracy_reduce.py` — parses replay JSONL + the cold-truth tree per cell; computes per-row relative error for quantile / sum / count_unique and per-row top-K recall. Smoke-tested on a real cell: 4.1 M ground-truth samples, 17 query rows, output CSV produced. |
| P9. Plots — Pareto, bandwidth, transition timeline, query CDF | ✅ 2026-04-30 | `deploy/scripts/e2e_plots.py` — four figures + their underlying CSVs. Smoke-tested: produces `pareto_acc_vs_thru.png` and `query_latency_cdf.png` from real data; `bandwidth_vs_n.png` and `transition_timeline.png` skip cleanly when the corresponding sample/transition records aren't in the cell. |

### Operating the e2e harness

```bash
# 1. Pre-reqs: docker, python ≥3.10, matplotlib + pandas in the user
#    env (`pip install --user matplotlib pandas`), the four
#    `asap/*:dev` images built (see deploy/docker/Dockerfile.* —
#    backend uses --build-context backend-src=...).
# 2. Single-cell smoke run:
AGENT_CONFIG=sketchcol-agent-b3-delta.yaml docker compose \
    -f deploy/docker-compose/base.yml \
    -f deploy/docker-compose/agents-N1.yml \
    -f deploy/docker-compose/baseline-b3-delta.yml \
    -f deploy/docker-compose/e2e-overlay.yml \
    up -d

# 3. Drive the workload: replay + plan-transition concurrently:
python3 deploy/scripts/promql_replay.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --queries deploy/scripts/queries-e2e.json \
    --qps 5 --duration 60 --out /tmp/replay.jsonl &
python3 deploy/scripts/plan_transition.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --transition-query 'histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))' \
    --transition-out /tmp/transition.jsonl --sample-out /tmp/sample.jsonl \
    --soak-secs 60 --pre-transition-secs 20

# 4. Snapshot ground truth (volume goes away on -v):
docker cp $(docker compose -f .../base.yml -f .../e2e-overlay.yml ps -q backend):/var/asap/cold/raw /tmp/cell/cold-truth

# 5. Reduce + plot:
python3 deploy/scripts/accuracy_reduce.py --cell-dir /tmp/cell --out /tmp/cell/accuracy.csv
python3 deploy/scripts/e2e_plots.py \
    --sweep-root /tmp --accuracy /tmp/cell/accuracy.csv --out-dir /tmp/cell/plots

# 6. Full sweep (~hours wall):
deploy/scripts/run_e2e_sweep.sh --out-dir /tmp/sweep-$(date +%s) --soak-secs 120
```

### Open follow-ups (not e2e blockers)

1. ~~**Warm-tier sketch ingest is dropped at the gateway.**~~ **Done
   (2026-04-30, deploy follow-up).** Both options landed:
   (a) `deploy/configs/gateway.yaml` now uses an `otlp/backend`
   exporter (gRPC → `backend:4317`) instead of
   `prometheusremotewrite/backend`, and `deploy/docker-compose/base.yml`
   passes `--enable-otel-ingest --otel-grpc-port=4317
   --otel-http-port=4318` to `precompute_engine`. The OTLP receiver
   in `asap-query-engine/src/drivers/ingest/otel.rs` decodes the
   typed `Metric.data = {DDSketch | KLLSketch | HLLSketch |
   CountSketch | CountMinSketch}` payloads end-to-end (companion
   PR ASAPQuery-backend#69). (b) `deploy/docker/Dockerfile.backend.queryengine`
   builds the `query_engine_rust` binary instead, and
   `deploy/docker-compose/queryengine-overlay.yml` swaps it in
   when stack-wide controller-in-loop / query-tracker / backfill /
   schema-eviction features are wanted alongside OTLP ingest. The
   PRW exporter is retained in `gateway.yaml` as a non-active
   fallback for legacy clients.
2. **Cold reader is intolerant of torn last lines.** Under
   concurrent producer write + reader scan, the §5.2
   `parse_jsonl` path failed on a torn last line. A 5-line
   change in
   `asap-query-engine/src/drivers/query/fallback/cold_store/format.rs`
   to drop a malformed trailing line + warn would unblock soaks
   that don't pause writes before snapshotting.
3. **Reducer runs offline; doesn't need the backend live.** That's
   fine for accuracy claims, but PromQL semantics are easy to
   drift from the engine. Add a self-check that runs the same
   query against the cold truth via the engine itself, where
   feasible.

---

_Original progress notes follow._

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
- `run-prom-client-profiling-eval.sh` — runs the matching
  source-side Prometheus `client_golang` profiling matrix
  before any collector/backend work.
- `run-prom-client-profile-cell.sh` — captures one Prometheus
  client cell with pprof CPU/heap, scrape timing, and docker
  stats.
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
- Prometheus client profiling harness in
  `deploy/eval-results/prom-client/`:
  - producer-only path: app updates → `client_golang` → `/metrics`
  - profile phases: update-only, scrape-only, combined
  - update modes: cached children vs dynamic `WithLabelValues`

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

- ~~**Hot-reload of View `AttributeFilter`.**~~ **Done (P2,
  2026-04-30).** Implemented as an in-process swappable filter in
  `deploy/fake-exporter/swappable_filter.go` rather than a SDK
  patch. The SDK's `Stream.AttributeFilter` is a function value
  that the SDK invokes per measurement; an `atomic.Pointer`-backed
  closure satisfies the same interface and lets the controller
  swap the projection at runtime via `POST /control/projection`.
  Caveat: post-swap, attribute sets that previously hashed to one
  bucket may now hash differently — old buckets keep their data,
  new measurements land in new buckets. The plan-transition
  driver (P6) records the swap timestamp so the accuracy reducer
  (P8) can split before/after.

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
