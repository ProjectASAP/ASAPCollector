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

## 4. Not (yet) answerable end-to-end

Patterns the physical pipeline cannot serve today, and why.

| Pattern | Blocker |
|---|---|
| `m_a{f} / m_b{f}` *(binary op across metrics)* | SimpleEngine's read-side arithmetic is implemented, but requires both operands to be present in `SimpleMapStore` at the same window — there is no cross-metric join operator and no guarantee the two metrics are aggregated on matching windows. |
| `last_over_time`, `deriv`, `delta`, `predict_linear` | require exact last-value / timestamped passthrough; no sketch covers them. Marked `exact_required` at compile time. |
| Bare selector `m{f}` at high resolution | same — exact passthrough is needed and the store does not retain raw samples at sub-window resolution. |
| Queries over sketches with variants not yet decoded (`Coco`, `Elastic`, some `Univmon` / `Hydra` shapes) | OTLP receiver wraps them in `SketchEnvelopeAccumulator` and stores them, but `SketchEnvelopeAccumulator::merge_with` is a no-op and `query_statistic` returns an error — the bytes survive ingest but no query path yet reads them. |
| Cross-sketch reinterpretation (e.g. feeding a CMS into a KLL reader) | not intended and not supported; each query must hit an accumulator of the matching `AggregationType`. |

### 4.1 The integration gap: SketchEnvelope → concrete accumulator

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

## 5. Worked examples

### 5.1 DEBS Q1 — EMA of last trade price

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
`SketchEnvelopeAccumulator` gap (§4.1).

### 5.2 ClickBench Q17 — TopK search phrases

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

### 5.3 Two-stage window aggregation

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

## 6. Configuration knobs users care about

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

## 7. Future work

1. **Per-variant `SketchEnvelope → concrete accumulator` decoders**
   (§4.1) — the one missing piece needed to make every row in the
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

## 8. Pointers

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
