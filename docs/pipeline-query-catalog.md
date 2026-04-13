# Pipeline Query Catalog

**Status:** design doc, living document.
**Audience:** anyone asking "can the pipeline answer this query today, and
where is the work done?"
**Scope:** the current implementation of the end-to-end pipeline
`workloads → OTel sketchcol → OTLP → ASAPQuery-backend precompute engine →
SimpleMapStore → query engine`.

> This is a **catalog**, not a compilation reference. For the query →
> sketch algebra mapping, see
> [`docs/sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md).
> For the controller's five-layer plan compilation, see
> [`controller/docs/query-to-sketch-translation.md`](../controller/docs/query-to-sketch-translation.md).
> This doc only describes what the *physical* pipeline, as built today,
> can answer end-to-end.

---

## 1. Pipeline overview

```
               ┌──────────────────────────────────────────┐
               │ Workloads (apps, Prometheus, Kafka, ...) │
               └──────────────────┬───────────────────────┘
                                  │  raw metric streams
                                  ▼
      ┌──────────────────────────────────────────────────────┐
      │ DataCollector OTel sketchcol                         │
      │   (ddsketchcol / kllcol / countminsketchcol /        │
      │    countsketchcol / hllcol / sketchcol / ... )       │
      │                                                      │
      │   • builds per-window, per-group sketches at edge    │
      │   • emits SketchEnvelope bytes in OTLP attributes    │
      │   • labels from input metric are preserved as        │
      │     OTLP DataPoint attributes                        │
      └──────────────────┬───────────────────────────────────┘
                         │  OTLP gRPC/HTTP
                         ▼
      ┌──────────────────────────────────────────────────────┐
      │ ASAPQuery-backend OtlpReceiver                       │
      │   (drivers/ingest/otel.rs)                           │
      │                                                      │
      │   • parses ExportMetricsServiceRequest               │
      │   • formats labels into metric{k="v"} series keys    │
      │   • matches series against StreamingConfig           │
      │     aggregation_configs                              │
      │   • raw points  → WorkerMessage::GroupSamples        │
      │   • sketches    → SketchEnvelopeAccumulator          │
      │                   → WorkerMessage::AccumulatorInput  │
      └──────────────────┬───────────────────────────────────┘
                         │  router hashes (agg_id, group_key)
                         ▼
      ┌──────────────────────────────────────────────────────┐
      │ Precompute engine worker pool                        │
      │   (precompute_engine/worker.rs)                      │
      │                                                      │
      │   • per (agg_id, group_key) GroupState               │
      │   • active_panes  — raw-sample AccumulatorUpdaters   │
      │   • sketch_panes  — pre-built AggregateCore objects  │
      │   • window_manager — tumbling + sliding windows      │
      │   • merge_with → window close → emit_batch           │
      └──────────────────┬───────────────────────────────────┘
                         │  (PrecomputedOutput, AggregateCore)
                         ▼
      ┌──────────────────────────────────────────────────────┐
      │ SimpleMapStore                                       │
      │   (stores/simple_map_store)                          │
      │                                                      │
      │   • keyed by aggregation_id + window + KeyBy labels  │
      │   • stores AggregateCore serialized payloads         │
      └──────────────────┬───────────────────────────────────┘
                         │
                         ▼
      ┌──────────────────────────────────────────────────────┐
      │ SimpleEngine                                         │
      │   (engines/simple_engine.rs)                         │
      │                                                      │
      │   • PromQL / SQL / ElasticDSL front-ends             │
      │   • dispatches by AggregationType to accumulator     │
      │     .query_statistic(...)                            │
      └──────────────────────────────────────────────────────┘
```

Labels survive two coordinate systems. The OTel sketchcol receives
full metric attributes and decides which it keeps as partition keys
(the grouping the sketch is built per). The precompute engine then
applies its own `grouping_labels` from `AggregationConfig` to produce
panes per `(agg_id, group_key)`. The second grouping is always a
projection of the first — i.e. it must be a subset of labels the OTel
processor actually emitted. The catalog below notes which queries rely
on keys being preserved at each stage.

---

## 2. Building blocks: what each stage can actually do

### 2.1 OTel-side sketch processors

Processors under `opentelemetry-collector-contrib-patch/processor/*` that
emit `SketchEnvelope` payloads, listed by wire type:

| Processor | `SketchState` variant | Output | Notes |
|---|---|---|---|
| `countminsketchprocessor` | `CountMin` | CMS matrix | Window/batch mode; supports delta transmission |
| `countsketchprocessor` | `CountSketch` | Signed-count arrays | Delta transmission with snapshots |
| `ddsketchprocessor` | `Ddsketch` | DDSketch buckets + min/max | Window-aligned, per-resource partitioning, lossless extrema |
| `kllprocessor` | `Kll` | KLL levels | Window/batch mode |
| `hllprocessor` | `Hll` | HLL registers | Supports delta/full proto encoding |
| `univmonprocessor` | `Univmon` | Universal monitoring sketch | — |
| `sketchcol` (meta) | any of the above | multiplex | Mounts multiple sketch processors in one collector image |

Additional variants exist in `sketchlib.proto::SketchEnvelope::SketchState`
(`Hydra`, `Coco`, `Elastic`) but do not have dedicated single-sketch
collector images; they are only reachable via the meta-collector.

### 2.2 ASAPQuery-backend precompute engine

Concrete accumulators in
`asap-query-engine/src/precompute_operators/`:

| `AggregationType` | Accumulator | Keyed? | Mergeable? |
|---|---|---|---|
| `Sum` | `SumAccumulator` | no | yes |
| `Increase` | `IncreaseAccumulator` | no | yes |
| `MinMax` | `MinMaxAccumulator` | no | yes |
| `DatasketchesKLL` | `DatasketchesKLLAccumulator` | no | yes |
| `MultipleSum` | `MultipleSumAccumulator` | yes | yes |
| `MultipleIncrease` | `MultipleIncreaseAccumulator` | yes | yes |
| `MultipleMinMax` | `MultipleMinMaxAccumulator` | yes | yes |
| `HydraKLL` | `HydraKllSketchAccumulator` | yes | yes |
| `CountMinSketch` | `CountMinSketchAccumulator` | yes | yes |
| `CountMinSketchWithHeap` | `CountMinSketchWithHeapAccumulator` | yes | yes |
| `SetAggregator` | `SetAggregatorAccumulator` | yes | yes |
| `DeltaSetAggregator` | `DeltaSetAggregatorAccumulator` | yes | yes |
| `HLL` | `HllAccumulator` (via `SetAggregator`) | yes | yes |

Every accumulator implements `AggregateCore::merge_with`, so the
precompute engine can window-align and merge them uniformly. Keyed
accumulators additionally track per-`KeyByLabelValues` subpopulations
for multi-dimensional queries that need per-key statistics.

`AggregationConfig` drives:

- `aggregation_type` — which accumulator the worker instantiates
- `window_size`, `slide_interval` — tumbling or sliding windows
- `grouping_labels` — labels used to form the pane's `(agg_id, group_key)`
- `aggregated_labels` — labels used for the keyed accumulator's inner
  subpopulations (keyed types only)

### 2.3 SimpleEngine (query side)

PromQL function coverage exposed by `SimpleEngine` today:

| PromQL | Routed to |
|---|---|
| `quantile_over_time(φ, m[w])` | `DatasketchesKLL` / `HydraKLL` |
| `avg_over_time(m[w])` | `DatasketchesKLL` (median proxy) |
| `min_over_time`, `max_over_time` | `MinMax` / `MultipleMinMax` or KLL extrema |
| `sum_over_time(m[w])` | `Sum` / `MultipleSum` |
| `count_over_time(m[w])` | `CountMinSketch` (with or without heap) |
| `topk(k, count_over_time(m[w]))` | `CountMinSketchWithHeap` |
| `count(count_over_time(m[w]) by (l))` | `SetAggregator` / `HLL` |
| `changes(m[w])` | `DeltaSetAggregator` |

Plus PromQL spatial aggregators (`sum`, `count`, `avg`, `min`, `max`,
`topk`, `quantile`) applied after an `_over_time` read.

SQL and ElasticDSL front-ends route through separate HTTP adapters but
ultimately dispatch to the same accumulators by `AggregationType`.

---

## 3. Query catalog

Each row is a query class supported end-to-end *today*. The "OTel op"
column is the sketch processor the DataCollector controller will pick
for this intent; the "Precompute merge" column is how the backend's
worker further merges those sketches into its window panes; "Stored"
is the accumulator the query engine reads; "Query" is the PromQL
surface.

| Class | OTel op | Precompute merge | Stored accumulator | Query (PromQL) | Accuracy |
|---|---|---|---|---|---|
| **Q-C1. Frequency per group** | `countminsketchcol` → CMS per window | window-align + merge CMS cells | `CountMinSketchAccumulator` (keyed by agg labels) | `count_over_time(m{f}[w]) by (d)` | ε=2/width, δ=1/2^depth |
| **Q-C2. Top-K groups by count** | `countminsketchcol` with heap, or `countsketchcol` | merge cells + heap | `CountMinSketchWithHeapAccumulator` | `topk(k, count_over_time(m{f}[w]) by (d))` | heap-bounded |
| **Q-C3. Distinct groups / cardinality** | `hllcol` | OR of HLL registers | `HllAccumulator` / `SetAggregator` | `count(count_over_time(m{f}[w]) by (d))` | σ ≈ 1.04/√m |
| **Q-C4. Quantiles per group** | `kll` or `ddsketchcol` | merge KLL levels (or add DD buckets) | `DatasketchesKLLAccumulator` / `HydraKllSketchAccumulator` | `quantile_over_time(φ, m{f}[w]) by (d)` | KLL: ~1% rank error; DDSketch: relative ε |
| **Q-C5. Median proxy for avg** | `kll` at φ=0.5 | merge KLL | `DatasketchesKLLAccumulator` | `avg_over_time(m{f}[w]) by (d)` | median ≠ mean; explicit opt-in |
| **Q-C6. Exact min/max per group** | `ddsketchcol` lossless extrema (or `kllprocessor` tracking extrema) | `ExactMinMax` | `MinMaxAccumulator` / `MultipleMinMaxAccumulator` | `min_over_time`/`max_over_time(m{f}[w]) by (d)` | exact |
| **Q-C7. Exact sum per group** | raw metric passthrough (no sketch) | scalar accumulate | `SumAccumulator` / `MultipleSumAccumulator` | `sum_over_time(m{f}[w]) by (d)` | exact |
| **Q-C8. Increase per group** | raw metric passthrough | increase accumulation | `IncreaseAccumulator` / `MultipleIncreaseAccumulator` | `increase(m{f}[w]) by (d)` | exact |
| **Q-C9. Set change per group** | raw metric passthrough | delta set bookkeeping | `DeltaSetAggregatorAccumulator` | `changes(m{f}[w]) by (d)` | exact |

All rows are mergeable in both stages, so a **sliding window with
window_size = N × slide_interval** is well-defined: the precompute
engine merges all panes covering the window into one output per slide.
When `window_size == slide_interval` the window is tumbling (the
cheapest and most common case).

### 3.1 Precompute engine as a sketch-aware second stage

The class that makes this pipeline interesting is not any single row
above but the interaction between *OTel-side* and *precompute-side*
windows. The precompute engine can accept a sketch that the OTel
collector already built for a short window (e.g. 10 s) and aggregate
it into a longer backend window (e.g. 5 min) by merging consecutive
sketches into the pane covering the 5 min slot:

```
  OTel sketchcol         OTLP         Precompute engine                    Store
  ─────────────          ────         ─────────────────                    ─────
  every 10 s:
    CMS[0s..10s]   ──▶                merge → sketch_panes[300s] (pane 0)
    CMS[10s..20s]  ──▶                merge → sketch_panes[300s] (pane 0)
    ...                               ...
    CMS[290s..300s]──▶                merge → sketch_panes[300s] (pane 0)
                                       window close:
                                         emit CMS[0s..300s] → Store       ──▶
```

Because every sketch in the catalog is mergeable, this second-stage
aggregation is lossless with respect to the first stage. It also
means the OTel side can be tuned for **short batches** (lower edge
memory, lower OTLP payload), while the backend accrues into **long
windows** (lower store write rate, better query accuracy) without
re-sending raw points.

### 3.2 Multi-label GROUP BY

A query with `by (k1, k2, ...)` is answerable if the OTel processor
preserved those labels on its output (via partitioning or pass-through
attributes) AND the `AggregationConfig.grouping_labels` matches or is
a subset of the preserved labels. If the second set is strictly larger
the precompute engine will see empty label values and collapse groups
together — not an error, but not the user's intent. This constraint is
the pipeline equivalent of the "push grouping labels as early as
possible" optimizer rule.

### 3.3 HAVING / filter on group values

`HAVING p(keys)` and PromQL matchers on grouping labels are handled
at the **read** stage by `SimpleEngine` filtering the result map. The
sketches themselves are built without knowing which groups the user
will ultimately want, so moving this filter into the OTel or
precompute stage would require the controller to know the query
upfront (which it does for declared queries; see the `QuerySpec`
plumbing).

---

## 4. Resource & bandwidth analysis

Why does the pipeline have *two* merge stages (agent sketch build + backend
precompute merge) instead of one? The short answer: the savings from
each stage are different. Agent-side sketching saves wire bytes between
agent and backend; the backend precompute merge saves store write rate,
store footprint, and query compute — in separate accounting. Collapsing
the two stages into one loses whichever savings you folded up.

The rest of this section walks through the analysis for a
representative workload.

### 4.1 Three scenarios

| | **(A) Raw passthrough** | **(B) Agent sketches → store direct** | **(C) Multi-stage (this pipeline)** |
|---|---|---|---|
| Agent | forwards raw samples | builds sketch per window per partition | same as B |
| Backend | builds sketches from raw samples | writes each agent sketch straight to store | precompute merge → store |
| Store | one sketch per backend window per group | one entry per agent × partition × agent-window | one entry per `(agg_id, backend_window, group_key)` |
| Query | reads one sketch | reads N agent sketches and merges per query | reads one sketch |

Scenario A has no edge sketching at all — the backend does it. Scenario
B has edge sketching but no backend merge. Scenario C is what we build.

### 4.2 Reference workload

- 1000 OTel agents, each processing 10 000 events/s × 50 bytes
- each agent builds a CMS per window per partition, 10 partitions/agent
- CMS width=2000, depth=5 → ~80 KB/sketch
- agent emits every 10 s
- `grouping_labels` projects the full label space down to ~50 unique
  backend groups (cross-agent spatial collapse)
- backend `window_size = 300 s`
- ~100 queries/s against the stored data

### 4.3 Bandwidth, link by link

| Link | Scenario A | Scenario B | Scenario C |
|---|---|---|---|
| **Events → agent** | 500 MB/s | 500 MB/s | 500 MB/s *(fundamental)* |
| **Agent → backend** | **500 MB/s** (raw) | **8 MB/s** (sketches) | **8 MB/s** (sketches) |
| **Backend → store (write)** | 13 KB/s (if backend sketches) | **8 MB/s** (direct) | **13 KB/s** (merged) |
| **Store footprint / 300 s** | ~4 MB | **~2.4 GB** (1000 × 10 × 30 × 80 KB) | **~4 MB** (50 × 80 KB) |
| **Store → query (read)** | 80 KB/query | **~2.4 MB scan + merge** | 80 KB/query |

Two separate savings are visible:

1. **Agent → backend** — Scenario A → B/C: 500 MB/s → 8 MB/s, ~63×
   reduction. This comes from **agent-side sketching**, not from
   multi-stage. B and C are identical on this link.
2. **Backend → store, and store → query** — Scenario B → C: 8 MB/s →
   13 KB/s on the write side (~600×), and ~2.4 MB → 80 KB on the read
   side (~30×). This comes from **the precompute merge**, not from
   agent sketching.

Multi-stage keeps both savings. Any single-stage alternative forfeits
one of them.

### 4.4 Resource usage

| | Scenario A | Scenario B | Scenario C |
|---|---|---|---|
| **Agent CPU** | pass-through | CMS inserts (O(events), bounded) | same as B |
| **Agent memory** | buffer | O(partitions × sketch_size) | same as B |
| **Backend ingest CPU** | **build all sketches from raw** (heavy) | none | `merge_with` per incoming sketch (cheap, O(sketch_size)) |
| **Backend ingest memory** | sketches in flight | none | `sketch_panes[pane_start]` per active group; bounded, evicted on window close |
| **Storage (persistent)** | small | **huge** (agent-emission granularity × retention) | small |
| **Query CPU** | per-query statistic | per-query **merge of 1000s of sketches** + statistic | per-query statistic |
| **Query latency** | ms | 10s–100s ms (merge dominates) | ms |

### 4.5 Where each multi-stage win comes from

- **Wire savings agent→backend are NOT from multi-stage.** They are
  from having agents sketch at all. Scenarios B and C pay the same
  agent → backend bandwidth cost.
- **Store write rate** collapses by `(backend_window / agent_window) ×
  (partition_cardinality / grouping_key_cardinality)` — the same
  factor also shows up in store footprint and cross-agent fan-in. In
  the reference workload this is `30 × (10 000 / 50) = 6 000×`, which
  matches the 8 MB/s → 13 KB/s difference on the write link.
- **Query amortization:** merge cost moves from *per query* to *per
  window close*. If there are `Q` queries per backend window, the
  effective saving is a factor of `Q`. For any workload with `Q ≫ 1`
  (the normal case for precomputed data) this dominates query-side
  economics — in the reference workload, 100 QPS × 300 s window =
  30 000 queries per window all answered from the same merged state.
- **Cross-agent spatial collapse.** When OTel partitions by a finer
  label than `grouping_labels` (e.g. agent partitions by `(server,
  service)` but `grouping_labels = [service]`), the precompute engine
  merges sketches **across agents** into a single per-service sketch.
  This is often the single biggest saving in real deployments and is
  invisible in any single-stage design because the agent cannot know
  what the other agents are emitting.

### 4.6 Summary

| Saving | Source of win | Typical magnitude |
|---|---|---|
| Agent → backend bandwidth | agent sketching (A → B/C) | 10×–100× |
| Store write rate | precompute merge (B → C) | 100×–10 000× |
| Store footprint | precompute merge (B → C) | 100×–10 000× |
| Query CPU | merge amortization (B → C) | ~Q (query rate per window) |
| Query latency | pre-merged at write time | 10×–1000× |
| Agent/backend CPU balance | sketch build pushed to edge; backend just merges | shifts the bottleneck away from the backend as the edge fleet grows |

### 4.7 When multi-stage does not pay off

- **`Q` is very low** (fewer than ~1 query per backend window). The
  merge cost has nothing to amortize over. Still helps with store
  size and query latency, but rarely decisive.
- **`backend_window == agent_window` and `grouping_labels` equals the
  agent's partition set.** Then the precompute merge is an identity,
  just an extra CPU hop. In this case the engine should be configured
  with `slide_interval == window_size` and a trivial grouping; the
  merge is cheap (a single-pane window close at each agent emission).
- **Non-mergeable operators** (`last_over_time`, `deriv`, bare
  selectors). These cannot sketch; multi-stage does not apply. See §5.
- **Extremely bursty workloads** where most agent batches are empty.
  The precompute engine's per-group state cost is per-active-group,
  not per-sample, so empty windows still cost memory. Tuning
  `allowed_lateness_ms` and watermarks keeps this bounded, but if
  *most* groups are idle most of the time, the store footprint saving
  may not outweigh the ingest memory cost.

---

## 5. Not (yet) answerable end-to-end

Patterns the physical pipeline cannot serve today, and why.

| Pattern | Blocker |
|---|---|
| `m_a{f} / m_b{f}` *(binary op across metrics)* | SimpleEngine's read-side arithmetic is implemented, but requires both operands to be present in `SimpleMapStore` at the same window — there is no cross-metric join operator and no guarantee the two metrics are aggregated on matching windows. |
| `last_over_time`, `deriv`, `delta`, `predict_linear` | require exact last-value / timestamped passthrough; no sketch covers them. Marked `exact_required` at compile time. |
| Bare selector `m{f}` at high resolution | same — exact passthrough is needed and the store does not retain raw samples at sub-window resolution. |
| Queries over sketches with variants not yet decoded (`Coco`, `Elastic`, some `Univmon` / `Hydra` shapes) | OTLP receiver wraps them in `SketchEnvelopeAccumulator` and stores them, but `SketchEnvelopeAccumulator::merge_with` is a no-op and `query_statistic` returns an error — the bytes survive ingest but no query path yet reads them. |
| Cross-sketch reinterpretation (e.g. feeding a CMS into a KLL reader) | not intended and not supported; each query must hit an accumulator of the matching `AggregationType`. |

### 5.1 The integration gap: SketchEnvelope → concrete accumulator

Today, when a DataCollector sketchcol emits a `SketchEnvelope` over
OTLP, the backend's `drivers/ingest/otel.rs` wraps the raw proto
bytes in `SketchEnvelopeAccumulator::from_proto_bytes` and routes the
wrapper through `WorkerMessage::AccumulatorInput`. The wrapper:

- preserves the opaque bytes,
- caches the sketch type string ("CountMin", "KLL", ...),
- implements `merge_with` as **self.clone()** — it does not actually
  combine two envelopes,
- implements `query_statistic` as **Err("not supported")**.

The worker therefore receives a correctly-labeled, correctly-routed,
correctly-pane-assigned message whose payload cannot be queried. This
is the deliberate "plumbing without semantics" state PR #3 landed the
backend in. Closing the gap means adding a **per-variant decoder**
that converts each `SketchState` variant into the concrete
`*Accumulator` the config asked for — e.g.

```rust
match sketch_state {
    SketchState::CountMin(inner) => CountMinSketchAccumulator::from_proto(inner),
    SketchState::Kll(inner)      => DatasketchesKLLAccumulator::from_proto(inner),
    SketchState::Hll(inner)      => HllAccumulator::from_proto(inner),
    ...
}
```

Once per-variant decoders exist, every row in the catalog becomes
hot end-to-end without further engine changes. The catalog's rows are
otherwise already supported by mergeable concrete accumulators; only
the "OTel built it, send it across the wire, install it in the
matching accumulator" step is open.

---

## 6. Worked examples

### 6.1 DEBS Q1 — EMA of last trade price

```
Query (PromQL):
  avg_over_time(financial.last_trade_price[5m]) by (symbol)

DataCollector controller decision:
  AggIntent = Quantile{φ=[0.5], accuracy=0.01}
  → ddsketchprocessor or kll processor on agents

OTel side (every 10s batch):
  ddsketchprocessor{metric=financial.last_trade_price,
                    partition_by=[symbol],
                    window=10s}
  emits: DDSketch[symbol] in SketchEnvelope::Ddsketch
         with DataPoint attributes {symbol="RDSA.NL", ...}

Wire:
  OTLP DataPoint:
    metric_name = financial.last_trade_price
    attributes:
      symbol = "RDSA.NL"
      ddsketch.sketch_payload = <bytes>
    time_unix_nano = ...

Backend ingest (otel.rs):
  StreamingConfig.agg_configs contains:
    { metric: "financial.last_trade_price",
      aggregation_type: DatasketchesKLL,
      grouping_labels: ["symbol"],
      window_size: 300s,
      slide_interval: 30s }
  → route WorkerMessage::AccumulatorInput{
        agg_id, group_key="RDSA.NL", timestamp_ms,
        accumulator = SketchEnvelopeAccumulator(Ddsketch bytes)
    }

Worker:
  GroupState[(agg_id, "RDSA.NL")]
    .sketch_panes[pane_start] = merge_with(...)
  window close at 300s:
    emit (PrecomputedOutput{window=[t, t+300s), key=["RDSA.NL"]},
          merged sketch) → SimpleMapStore

Query:
  SimpleEngine.handle_query_promql("avg_over_time(...) by (symbol)", t+300s)
    → read DatasketchesKLLAccumulator at (agg_id, "RDSA.NL", window)
    → statistic = median
```

All steps exist in code *except* the "decode `SketchEnvelope::Ddsketch`
into `DatasketchesKLLAccumulator`" step — that is the
`SketchEnvelopeAccumulator` gap (§5.1).

### 6.2 ClickBench Q17 — TopK search phrases

```
Query (SQL):
  SELECT SearchPhrase, COUNT(*) AS c FROM hits
  GROUP BY SearchPhrase
  ORDER BY c DESC LIMIT 10

AggIntent:
  Frequency{accuracy=...} with top-K absorption
  → countsketchprocessor or countminsketchprocessor with heap

OTel side:
  countminsketchprocessor(heap=true, k=10, partition_by=[SearchPhrase])
  emits: CMS + heap in SketchEnvelope::CountMin
         with DataPoint attributes {SearchPhrase=...}

Backend:
  aggregation_type = CountMinSketchWithHeap
  grouping_labels   = [] (no further group — top-K is global over SearchPhrase)

Stored:
  CountMinSketchWithHeapAccumulator at (agg_id, "", window)

Query:
  SimpleEngine reads CountMinSketchWithHeapAccumulator.query_statistic(TopK{k:10})
  → top-10 (SearchPhrase, count) pairs
```

### 6.3 Two-stage window aggregation

```
Goal:
  "emit 5-minute top-K heavy hitters, but never let agents buffer more
   than 10 seconds of samples"

Deployment:
  OTel side: countminsketchprocessor batch window = 10s, heap_k = 10
  Backend:   AggregationType = CountMinSketchWithHeap
             window_size = 300s, slide_interval = 30s

Flow:
  agent sends one CMS+heap per 10s
  backend merges 30 of them into a single pane over 300s
  backend emits one heap per 30s slide

Correctness:
  CMS is mergeable (cell-wise max); the heap is rebuildable from the
  merged CMS since the top-K query is monotone. The backend's 300s
  estimate is the exact sum (for CMS frequencies) of the 30 10s
  estimates, with the same ε bound as any single CMS of that size.
```

---

## 7. Use cases from real workloads

> The query examples in this section are sourced from open issues —
> [#47](https://github.com/ProjectASAP/DataCollector/issues/47)
> (benchmark datasets across finance, cluster telemetry, IoT, network,
> mobility, and healthcare),
> [#78](https://github.com/ProjectASAP/DataCollector/issues/78) (DEBS
> 2022 financial queries Q1–Q12),
> [#46](https://github.com/ProjectASAP/DataCollector/issues/46) (MVP
> reduction targets), and
> [#49–#52](https://github.com/ProjectASAP/DataCollector/issues/49)
> (the three aggregation patterns the collector is designed for) — and
> are picked to exercise different facets of the pipeline.
> Each query is mapped end-to-end: OTel processor + agent partition
> → backend `AggregationConfig` → stored accumulator → PromQL.

### 7.1 The three aggregation patterns

Issues [#49–#52](https://github.com/ProjectASAP/DataCollector/issues/49)
frame the design space as three orthogonal patterns. The catalog covers
all three; the multi-stage win is largest on the third.

| Pattern (issue) | Axis | Example | Where the savings live |
|---|---|---|---|
| **Window aggregation per series** ([#50](https://github.com/ProjectASAP/DataCollector/issues/50)) | temporal | "p99 latency for service `auth` over the last hour" | precompute folds N agent windows into one backend window per series |
| **Series aggregation at each timestamp** ([#51](https://github.com/ProjectASAP/DataCollector/issues/51)) | spatial (cross-series) | "median request rate across all pods at this instant" | precompute folds many series at the same time into one |
| **Matrix aggregation** ([#52](https://github.com/ProjectASAP/DataCollector/issues/52)) | both | "p99 latency per region per hour, over a fleet of 1000 hosts" | both reductions stacked — the killer multi-stage win |

§§7.2–7.6 below pick concrete queries from real datasets that fall
into each pattern. §7.7 shows complex compositions that span pipeline
features.

### 7.2 Financial workloads (DEBS 2022, NYSE TAQ, Binance)

Sourced from [#78](https://github.com/ProjectASAP/DataCollector/issues/78).
DataCollector ingest config: `metric=financial.last_trade_price`,
labels `[symbol, exchange, sectype]`, 5-minute tumbling windows.

- **DEBS Q1 — EMA per symbol** *(also worked example §6.1)*. KLL or
  DDSketch, `aggregate_by=[symbol]`, quantiles `[0.5]` as median proxy.
  Stored as `DatasketchesKLLAccumulator`.
  ```promql
  avg_over_time(financial.last_trade_price[5m]) by (symbol)
  ```
- **DEBS Q3 — Top-K most active symbols per window.** `countsketchcol`
  or `countminsketchcol` (heap=10), `aggregate_by=[symbol]`. Stored as
  `CountMinSketchWithHeapAccumulator`. Cross-agent collapse is the win
  — many gateways may stream the same symbols.
  ```promql
  topk(10, count_over_time(financial.last_trade_price[5m]) by (symbol))
  ```
- **DEBS Q4 — Per-symbol high/low/range.** `kllprocessor` quantiles
  `[0, 1]`, `aggregate_by=[symbol]`. Stored as `MultipleMinMaxAccumulator`
  (or `HydraKllSketchAccumulator` for joint range queries).
  ```promql
  max_over_time(financial.last_trade_price[5m]) by (symbol) -
  min_over_time(financial.last_trade_price[5m]) by (symbol)
  ```
- **DEBS Q5 — Realized volatility (IQR proxy).** DDSketch quantiles
  `[0.25, 0.5, 0.75]`. Query reads three quantiles → IQR → σ ≈ IQR/1.349.
- **DEBS Q6 — Distinct active symbols per window.** `hllprocessor`,
  `mode=window`. Stored as `HllAccumulator`.
  ```promql
  count(count_over_time(financial.last_trade_price[5m]) by (symbol))
  ```

### 7.3 Cluster & cloud telemetry

Datasets cited in [#47](https://github.com/ProjectASAP/DataCollector/issues/47):
Google Cluster Trace, Alibaba Cluster Trace, Datadog BOOM, MIT Supercloud.

- **Heavy-hitter HTTP routes by request count.** Each agent runs
  `countminsketchcol` with heap on `(host, route)`, 10-second batches.
  Backend `grouping_labels=[route]` collapses across the entire fleet.
  This is the §4.5 cross-agent spatial collapse story in action: 1000
  hosts × 10 routes × every 10 s become **one CMS per route per
  minute** stored.
  ```promql
  topk(10, count_over_time(http_requests_total[1m]) by (route))
  ```
- **p99 request latency per service per hour.** `kllprocessor` per
  `(host, service)`, 30-second batches. Backend
  `grouping_labels=[service]`, `window_size=3600s`,
  `slide_interval=60s` (sliding). Dashboards refreshing every 60 s
  read pre-merged state — query amortization (§4.5) over ~60 reads
  per closed window.
  ```promql
  quantile_over_time(0.99, http_request_duration_seconds[1h]) by (service)
  ```
- **Cluster-wide active container count.** `hllprocessor` per
  `(host, namespace)`, 10-second batches. Backend
  `grouping_labels=[namespace]`, 1-minute window.
  ```promql
  count(count_over_time(container_running[1m]) by (container_id))
  ```
- **Top noisy-neighbor pods by CPU per node per minute** (Google
  Cluster Trace pattern). `countminsketchcol` heap on `(pod_id)`,
  partitioned by `(node)`. Backend `grouping_labels=[node]`, 1-min
  tumbling. Useful as an alert input for scheduler eviction.

### 7.4 IoT, smart grid, and predictive maintenance

Datasets cited in [#47](https://github.com/ProjectASAP/DataCollector/issues/47):
Pecan Street, UCI household power, NASA CMAPSS, PHM Society.

- **Rolling p95 of household power per circuit.** `ddsketchprocessor`
  per `(meter_id, circuit)`, 1-minute batches. Backend
  `grouping_labels=[circuit]`, 15-min sliding window with 1-min slide.
  ```promql
  quantile_over_time(0.95, household_power_watts[15m]) by (circuit)
  ```
- **Top transformers by load per substation per hour.**
  `countminsketchcol` (heap) on `(transformer_id)`. Backend
  `grouping_labels=[substation]`, 1-hour tumbling.
  ```promql
  topk(5, sum_over_time(transformer_load_kw[1h]) by (substation))
  ```
- **Vibration percentile per turbofan engine per cycle** (NASA CMAPSS).
  `kllprocessor` per `(engine_id)`, 1-second batches. Backend
  `grouping_labels=[engine_id]`, per-cycle window. Feeds into
  remaining-useful-life prediction.

### 7.5 Network & 5G

5G high-frequency time-series dataset cited in
[#47](https://github.com/ProjectASAP/DataCollector/issues/47).

- **Top source IPs per cell per second.** `countminsketchcol` heap on
  `src_ip`, 1-second batches per cell. Backend `grouping_labels=[cell_id]`,
  1-second tumbling. Output feeds straight into per-cell rate limiters.
  ```promql
  topk(10, count_over_time(packets_total[1s]) by (src_ip))
  ```
- **Packet size distribution per BSS per minute.** `ddsketchprocessor`,
  1-second batches. Backend `grouping_labels=[bss_id]`, 60-second
  tumbling.
  ```promql
  quantile_over_time(0.5, packet_size_bytes[1m]) by (bss_id)
  ```

### 7.6 Mobility and healthcare

NYC Taxi, Uber Movement, MIMIC-IV waveforms (all from #47).

- **Top busiest pickup zones per 5 minutes** (NYC Taxi).
  `countsketchcol` heap on `pickup_zone`. 30-second agent batches,
  5-minute backend windows.
  ```promql
  topk(20, count_over_time(taxi_pickups_total[5m]) by (pickup_zone))
  ```
- **p50/p95 trip duration per zone pair per hour.** `kllprocessor`
  per `(pickup_zone, dropoff_zone)`. Backend
  `grouping_labels=[pickup_zone, dropoff_zone]`, 1-hour tumbling.
  ```promql
  quantile_over_time(0.95, trip_duration_seconds[1h]) by (pickup_zone, dropoff_zone)
  ```
- **HR distribution per ICU ward per minute** (MIMIC-IV).
  `ddsketchprocessor` per `(patient_id, ward)`. Backend
  `grouping_labels=[ward]`, 1-min tumbling. Crowdsourced ward-level
  alarms read pre-merged sketches.

### 7.7 Complex multi-stage compositions

These are queries that combine multiple pipeline features and
illustrate why the catalog wins on bandwidth + resources.

#### 7.7.1 Cross-tenant fairness — distinct active users per region per day

The killer matrix-aggregation example.

```yaml
# OTel side
hllprocessor:
  partition_by: [pod, namespace, region]
  emit_window: 5m

# Backend AggregationConfig
metric:           active_users_total
aggregation_type: HLL
grouping_labels:  [region]
window_size:      86400s   # 1 day
slide_interval:   300s     # emit every 5 min
```

Stored: **one `HllAccumulator` per region per day** (typically ≤ 5
entries per day, regardless of fleet size). Query:

```promql
count(count_over_time(active_users_total[1d]) by (user_id))
```

A naïve store without precompute would hold roughly
`288 backend buckets × N pods × 5 regions = ~2.5M entries/day` for a
1000-pod cluster. The pipeline collapses that to ~5/day losslessly via
HLL register-OR mergeability (§3.1).

#### 7.7.2 Anomaly detection by comparing a host to its cluster's p99

Two parallel aggregations on the same metric, both pre-merged:

```yaml
# Per-cluster p99 quantile sketch
- metric:           cpu_usage_seconds_total
  aggregation_type: DatasketchesKLL
  grouping_labels:  [cluster]
  window_size:      60s

# Per-host point value
- metric:           cpu_usage_seconds_total
  aggregation_type: Sum
  grouping_labels:  [host, cluster]
  window_size:      60s
```

Alert (pseudo-PromQL — the cross-metric binary op is in §5's "not yet
answerable" list, but the *underlying state* is already there):

```promql
cpu_usage_seconds_total{cluster="prod"}
  > on(cluster) group_left
    quantile_over_time(0.99, cpu_usage_seconds_total[1m]) by (cluster)
```

The pipeline keeps **two stored entries per cluster per minute** —
one KLL, one keyed Sum — both already merged across hundreds of hosts.
The remaining work is the read-side join (tracked in §9 future work).

#### 7.7.3 Bollinger bands chained on top of DEBS Q1

The OTel side never sees the band. It only emits per-tick-window
quantiles via `kllprocessor`, exactly as in §7.2. A downstream
consumer reads N consecutive 5-minute backend windows from the store,
recomputes SMA + σ from the stored quantiles, and emits breakout
signals.

The agent → backend bandwidth stays constant regardless of whether a
downstream consumer wants 5-, 15-, or 60-minute Bollinger bands. The
controller does not need to push a new sketch config when the
downstream window changes — the precompute engine's stored sketches
are reusable across temporal aggregation horizons.

#### 7.7.4 Two-stage volume-weighted top-K (NYSE TAQ × NYC Taxi pattern)

A query that needs both *frequency* (number of events) and *quantile*
(median size). Two parallel sketches on the same stream:

```yaml
- metric:           order_events_total
  aggregation_type: CountMinSketchWithHeap
  grouping_labels:  [symbol]
  window_size:      300s

- metric:           order_size_usd
  aggregation_type: DatasketchesKLL
  grouping_labels:  [symbol]
  window_size:      300s
```

Query (read-side composition):

```
top_10_by_count = topk(10, count_over_time(order_events_total[5m]) by (symbol))
median_size      = quantile_over_time(0.5, order_size_usd[5m]) by (symbol)
result           = join(top_10_by_count, median_size, on=symbol)
```

Both stored entries are merged across many trading-floor agents; the
read side joins ten symbols × two sketches = 20 lookups per query.

#### 7.7.5 Long-horizon monitoring — distinct error fingerprints per service per week

Demonstrates extreme temporal merging.

```yaml
hllprocessor:
  partition_by: [host, service, error_fingerprint]
  emit_window: 5m

backend:
  metric:           errors_total
  aggregation_type: HLL
  grouping_labels:  [service]
  window_size:      604800s   # 1 week
  slide_interval:   3600s     # emit hourly
```

Stored: one `HllAccumulator` per service per week, slid hourly. Each
backend window is the merge of `7 × 24 × 12 = 2016` 5-minute agent
emissions. Query:

```promql
count(count_over_time(errors_total[1w]) by (error_fingerprint))
```

**Why this works:** HLL is associative, so merging 2016 sketches is
identical (modulo numerical noise) to one sketch over the whole week.
The query is answered from a single sketch read.

### 7.8 Targets from #46 (MVP)

[#46](https://github.com/ProjectASAP/DataCollector/issues/46) lists
three reduction targets the design is meant to hit:

> Reducing metrics transmission cost by X
> Reducing aggregation query latency by Y
> Reducing aggregation query cost, and the e2e pipeline resource usage cost by Z

The query catalog and the analysis in §4 give concrete handles for
each:

| #46 target | How the catalog hits it | Magnitude (from §4) |
|---|---|---|
| transmission cost (X) | OTel agent sketches replace raw streams | 10×–100× on the agent → backend link |
| query latency (Y) | precompute merge happens at write time, queries are point reads | 10×–1000× tail-latency reduction |
| e2e resource cost (Z) | store write rate + footprint collapse via temporal × cross-agent merge | 100×–10 000× store reduction; query CPU amortized over Q queries/window |

---

## 8. Configuration knobs users care about

- **Window / slide at the backend.** `AggregationConfig.window_size` and
  `slide_interval` fully determine how many OTel-side batches are
  folded into one stored output. Set `slide_interval == window_size`
  for tumbling; set `slide_interval < window_size` to get overlapping
  outputs (sliding windows).
- **Grouping labels at the backend.** `grouping_labels` is the *keyed
  dimension* for the stored output; the query-engine's `by (...)` must
  be a subset (or equal). Must also be a subset of labels the OTel
  processor actually emitted.
- **Aggregated labels at the backend.** For multi-subpopulation
  accumulators (`MultipleSum`, `CountMinSketch`, `HydraKLL`, ...),
  `aggregated_labels` selects the inner dimension each accumulator
  tracks per-key. This is how `topk(k, count_over_time(m) by (d1, d2))`
  gets a single accumulator answering multi-dim top-K.
- **Allowed lateness.** `precompute_allowed_lateness_ms` — how far the
  watermark is allowed to trail. Samples older than `watermark -
  allowed_lateness` hit the late-data policy (drop or forward).
- **Late data policy.** `Drop` is the default; `ForwardToStore` emits
  a single-sample (or single-sketch) output past the closed window.
  Useful when the upstream is best-effort but you want every value
  retained.

---

## 9. Future work

1. **Per-variant `SketchEnvelope → concrete accumulator` decoders**
   (§5.1) — the one missing piece needed to make every row in the
   catalog hot end-to-end.
2. **Cross-metric binary ops** — `m_a / m_b` and similar; requires
   store-side window alignment between two `agg_id`s.
3. **Exact-required operators** (`last_over_time`, `deriv`,
   `predict_linear`, bare selectors) — would need either raw sample
   retention or a dedicated exact passthrough path through the
   precompute engine.
4. **Quality-of-approximation metadata in store.** Today the stored
   accumulator carries its own parameters but not an explicit error
   budget; adding that would let the query engine return confidence
   intervals alongside point estimates.
5. **Runtime sketch upgrade.** When a query asks for φ=0.99 on a
   sketch built for φ=0.5, there is currently no way to ask the OTel
   side to rebuild — the controller has to schedule a new aggregation.
   A feedback loop from query engine → controller → OTel would close
   this.

---

## 10. Pointers

- Existing compilation-side design:
  [`docs/sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md) — SQL/PromQL → sketch algebra IR
- Controller's five-layer plan:
  [`controller/docs/query-to-sketch-translation.md`](../controller/docs/query-to-sketch-translation.md)
- Precompute engine source of truth:
  `asap-query-engine/src/precompute_engine/` (worker.rs, engine.rs,
  series_router.rs, accumulator_factory.rs)
- OTLP ingest source of truth:
  `asap-query-engine/src/drivers/ingest/otel.rs`
- Accumulator set:
  `asap-query-engine/src/precompute_operators/`
- Wire protobuf:
  `asap_sketchlib::proto::sketchlib::SketchEnvelope`
