# SDK cost evaluation

## Why this doc exists

The ASAP pipeline has three data-plane decision points —
SDK / agent collector / backend collector — and the paper's
claim is that the controller plans each of them based on the
current query workload. This document pins down **what exactly
the controller decides for the SDK**, what knobs the SDK
exposes to express that decision, and how the paper's §6
ablations map onto those knobs.

It is not a full architecture doc: it only covers the SDK
decision point. The agent / backend decision points are covered
in:

- [`controller-optimization-problem.md`](controller-optimization-problem.md)
  — full lifecycle + cost model
- [`delta-transmission-design.md`](delta-transmission-design.md)
  — Mode 1 / 2 / 3 terminology used throughout the collector
  side
- [`sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md)
  — how queries compile to the algebra the planner consumes

## The three orthogonal knobs

For any given metric, the SDK's aggregation behaviour is fully
specified by a triple:

```
(W,   L,              agg_type)
 time axis            label axis    encoding axis
```

| Knob | Meaning | Example values |
|---|---|---|
| `W` — **time-agg window** | How long the aggregator accumulates before emitting. Implemented as `PeriodicReader.WithInterval(W)`. | `1s`, `15s`, `60s`, `300s` |
| `L` — **label projection** | The subset of label keys preserved inside the aggregator key. Everything not in `L` is dropped before bucketing. Implemented as the SDK `View`'s `AttributeFilter`. | `{}` (all dropped) … `{zone}` … `{zone, endpoint}` … full label set |
| `agg_type` — **encoding** | What the aggregator emits per `(W, L)` bucket at each tick. Implemented as a SDK `Aggregation` impl. | `raw-buffer`, `sum`, `dd-full`, `dd-delta`, `kll-full`, `kll-delta`, `cms-full`, `hll-full` |

**The three axes are independent** — any triple is valid. The
controller's planner output for each metric is exactly this
triple.

## The cardinality of what's emitted per tick

Given ground-truth `orig_cardinality` (distinct full-label
attribute sets in the app) and the controller-chosen `L`, define:

```
reduced_cardinality(L) = number of distinct combinations of label values
                         when projected onto L
```

(`reduced_cardinality({}) = 1`, `reduced_cardinality(full) = orig_cardinality`.)

Per-tick data-point count on the wire:

| `agg_type` | Data points per tick | Bytes per data point (typical) |
|---|---|---|
| `raw-buffer` | `events_per_interval(W) × orig_cardinality` | ≈ label-bytes + 8 (value) |
| `sum` / `last-value` | `reduced_cardinality(L)` | ≈ label-bytes + 8 |
| `*-full` sketch | `reduced_cardinality(L)` | ≈ label-bytes + sketch-size (e.g., 1–5 KiB for DDSketch) |
| `*-delta` sketch | `reduced_cardinality(L)` where the delta is non-trivial | ≈ label-bytes + delta-bytes (often an order of magnitude smaller than full sketch) |

Bytes-per-second = data points × bytes-per-datapoint × (1 / W).

## Cross-reference to Mode 1 / 2 / 3 in `delta-transmission-design.md`

The collector-side delta-transmission doc talks about three
aggregation "Modes" with slightly different emphasis. The
mapping:

| This doc's knobs | `delta-transmission-design.md` Mode |
|---|---|
| `L = full`, `W = none` (cumulative) | **Mode 1** — by series labels |
| `L = full`, `W = W_tumble` | **Mode 2** — window per series |
| `L = subset`, `W = W_tumble` | **Mode 3** — selected labels × window |
| `L = subset`, `W = 1s` (short) | (not separately named; = per-tick cross-label) |
| `L = full`, `W = 1s` (short) | (not separately named; = near-raw per-series) |
| `agg_type = raw-buffer`, any `W` / `L` | (not separately named; the paper's "raw baseline" slot) |

The three-axis framing in this doc is strictly more general and
is the one the planner emits. The Mode 1/2/3 vocabulary is kept
in the collector-side doc because it matches historical PR /
issue numbering; treat it as shorthand for three common cells
of the `(W, L, agg_type)` grid.

## How the controller produces `(W, L, agg_type)` from queries

For each metric `M` with observed query workload `Q_M`:

```
L        = ⋃ { labels referenced in `by (...)` / SQL GROUP BY
               of q  :  q ∈ Q_M }
W        = min { q.window  :  q ∈ Q_M }   subject to freshness SLA
agg_type = pick_by_statistic({ q.statistic  :  q ∈ Q_M })
             # p99 / quantile → DDSketch or KLL
             # count-distinct → HLL
             # top-K / heavy-hitters → CMS / CountSketch
             # exact sum/count on small domain → sum
             # exact needed + no summary suffices → raw-buffer
```

The planner owns this mapping. Its output is pushed through
OpAMP to the SDK, which hot-reloads the corresponding View
config. Details of the planner's internals (layered IR,
optimizer rewrites) live in
[`sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md)
and `controller/docs/query-to-sketch-translation.md`.

## SDK implementation surface

Per-axis:

- **`W`** — `sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(W))`.
  Already supported by upstream OTel Go SDK. No patching needed.
- **`L`** — a `sdkmetric.View` whose `Stream.AttributeFilter`
  keeps only the labels in `L`. Already supported by upstream
  OTel Go SDK.
- **`agg_type`** — SDK `Aggregation` implementation. Current
  status:

  | `agg_type` | Status | Where |
  |---|---|---|
  | `sum`, `last-value`, `explicit-bucket-histogram`, `exponential-histogram` | ✅ upstream | `go.opentelemetry.io/otel/sdk/metric` |
  | `dd-full` (`AggregationDDSketch{}`) | ✅ | `opentelemetry-go-patch/sdk/metric/aggregation.go` |
  | `dd-delta` (`AggregationDDSketch{DeltaTransmission: true}`) | ✅ | same — flag on the `*-full` type |
  | `kll-full` (`AggregationKLLSketch{}`) | ✅ | same |
  | `cms-full` (`AggregationCountMinSketch{}`) | ✅ | same |
  | `cms-delta` (`…{DeltaTransmission: true}`) | ✅ | flag |
  | `cs-full` (`AggregationCountSketch{}`) | ✅ | same |
  | `cs-delta` (`…{DeltaTransmission: true}`) | ✅ | flag |
  | `hll-full` (`AggregationHLLSketch{}`) | ✅ | same |
  | `hll-delta` (`…{DeltaTransmission: true}`) | ✅ | flag |
  | `raw-buffer` (`AggregationRawBuffer`) | ✅ | `opentelemetry-go-patch/sdk/metric/internal/aggregate/rawbuffer.go` |
  | **`kll-delta`** | **no need** | KLL's sample-buffer structure makes delta-vs-last nontrivial, and `kll-full` suffices for the encoding-axis comparison. See note below if/when this changes. |

**Note on `kll-delta`**: the other four sketches (DDSketch / CMS /
CountSketch / HLL) have sparse internal state (buckets / cells /
registers) where "what changed" is naturally expressible as a
list of `(index, new_value)` pairs. KLL keeps sorted sample
buffers at multiple compaction levels; the sample set can shift
every tick via compaction, so a naive byte-diff would be no
smaller than the full sketch. Two options for adding delta
support:

1. **Incremental add-only**: transmit only samples observed
   since last export, let the receiver re-apply; changes the
   algorithm invariant (receiver has to run compaction).
2. **Hierarchical diff**: transmit per-level diffs of the
   compactor sample arrays. Smaller payload, but requires
   exposing KLL internal state through `sketchlib-go`.

Not a §6.2 blocker — the `kll-full` row is sufficient for a
three-way comparison with `raw-buffer` and `dd-delta` on the
encoding axis.

### `AggregationRawBuffer` design

**Why this baseline exists.** Every other `Aggregation` the
upstream OTel SDK ships is lossy by design — `Sum` discards
individual events, `Histogram` quantises values into fixed
buckets, the sketch aggregators keep bounded-error summaries.
`raw-buffer` is the one aggregator that preserves the full
observation stream unchanged. It exists to answer the question
the other encodings can't answer on their own: **what does
making an SDK-side aggregation decision save compared to not
making one at all**? Without `raw-buffer` as the reference
point, every bandwidth / CPU / RSS number reported by a sketch
encoding is a ratio against something unmeasured, and the
encoding-axis ablation (sketch vs raw) collapses into
"sketch vs nothing".

It's also the one encoding that preserves enough to serve
queries no summary can — exact events for cold-fallback
replay, per-sample audit trails, or downstream sketch
computation that the SDK policy didn't anticipate. That's
why the three-axis framework keeps it as a first-class slot
rather than a "bypass the SDK" escape hatch.

**Semantics.**

- On every `Counter.Add(value, attrs)` / `Gauge.Record(value,
  attrs)` call, append `(now(), attrs, value)` to the per-
  reduced-attribute-key buffer.
- On each `PeriodicReader(W)` collect, emit one
  `metricdata.DataPoint[N]` per buffered tuple (grouped into
  a `Gauge[N]` — OTLP allows multiple data points per
  attribute set at different timestamps), then clear all
  buffers.
- Both delta and cumulative temporality paths call the same
  `collect()` and always clear. Raw-buffer has no meaningful
  cumulative interpretation — re-emitting every historical
  sample on every tick would shadow the whole point of
  having an interval.

**Overflow policy.** Per-series buffer capped at
`MaxEventsPerSeries` (default 10 000). Once full, new
measurements on that attribute key are dropped and the
per-series drop counter increments. Deliberately **not**
backpressure: blocking `Add` / `Record` would entangle "SDK
can't keep up" with "application slow path" in the experiment
numbers. The drop counter is in-memory for v1 — exposing it
as a side-channel metric so the processor or operator can see
drops in flight is a follow-up tracked in `PROGRESS.md`.

**Contract.** Given `N` events emitted on `K` distinct
attribute sets during one `W`, `collect()` returns `N` data
points (bounded by `K × MaxEventsPerSeries`). Verified by
`deploy/fake-exporter/sdk_emit_test.go` via a `ManualReader`
harness — the wiring-level complement to the aggregator unit
tests in `opentelemetry-go-patch/sdk/metric/internal/aggregate/`.

**Memory cost.** `O(W × event_rate × per_series_cardinality)`
— growing linearly with the time window and the event volume,
unlike the sketch encodings whose size is bounded by their
respective parameters regardless of event count. This is the
tradeoff the encoding axis measures.

Adding `Aggregation<X>Delta` (×5):
- Semantics: keep last emitted sketch bytes per reduced-attribute-key;
  on tick, diff against current sketch and emit delta. If `L` or
  sketch params changed since last tick, emit full sketch (not a
  delta) and reset the reference.
- Encoding: byte-level XOR + zstd at first (simple, works for all
  5 sketch types with one codepath). Semantic delta (e.g., CMS cell
  changes, DDSketch bucket changes) is a possible paper follow-up.
- Expected size: ~100 LOC each × 5 = 500 LOC + tests.

Hot-reload of `L` at runtime:
- OTel Go SDK doesn't currently support replacing a View's
  `AttributeFilter` after `MeterProvider` construction. For the
  static §6 sweeps this is fine — each run is a fresh process.
- For the controller-in-loop §6.5 "planner pushes `L` change mid-run"
  scenario, the SDK needs a hot-reload hook. This is tracked as a
  separate implementation item; **not** a §6.2 blocker.

## Paper §6 mapping (three-axis ablation)

§6.2 splits into four sub-sweeps, each sweeping one axis while
holding the other two fixed at a representative operating point:

| Sub-sweep | Fixed | Swept | Claim |
|---|---|---|---|
| **6.2a Time axis** | `L = full`, `agg = dd-full` | `W ∈ {1s, 15s, 60s, 300s}` | "Longer windows reduce bw / weaken freshness SLA" |
| **6.2b Label axis** | `W = 60s`, `agg = dd-full` | `\|L\| ∈ {0, 1, 2, 3, 4} dims` | "Projecting compatible label dims reduces bw by `orig/reduced`" |
| **6.2c Encoding axis** | `W = 60s`, `L = typical projection` | `agg ∈ {raw-buffer, dd-full, dd-delta, kll-full, kll-delta}` | "Sketch vs raw reduces per-point bytes; delta reduces further" |
| **6.2d End-to-end** | — | best `(W, L, agg)` per metric chosen by planner, vs `raw-buffer` at full label set + `W = 15s` | "Total bw reduction = time-factor × label-factor × encoding-factor" |

§6.5 becomes a **planner-quality** experiment independent of the
SDK emit cost: given query sets `Q_1, …, Q_k`, inspect the
planner's `(W, L, agg)` output and compare against hand-tuned
ground truth.

## Non-goals of this doc

- How the agent collector further aggregates across SDK windows
  — covered by `delta-transmission-design.md` and
  `serf-compression-architecture.md`.
- Cost-model formulation — covered by
  `controller-optimization-problem.md`.
- Query-side algebra and the algebra → physical-plan rewrite
  rules — covered by `sketch-algebra-query-mapping.md` and
  `controller/docs/query-to-sketch-translation.md`.

## Implementation order (follow-up PRs)

1. ~~`AggregationRawBuffer` + unit tests + `Aggregation` enum wire-up.~~ — merged.
2. ~~`AggregationDelta<X>Sketch` ×5~~ — already present as
   `DeltaTransmission: true` on the four sparse-state sketches
   (DDSketch / CountSketch / CountMinSketch / HLLSketch).
   Only `kll-delta` is a follow-up, and it's optional for §6.2.
3. `fake-exporter` knobs: drop `EXPORTER_RATE`; add
   `EXPORTER_SDK_WINDOW`, `EXPORTER_SDK_PROJECTION`,
   `EXPORTER_SDK_AGG`. Widen the synthetic label schema from
   `{zone, pod}` (2 dims) to `{zone, rack, node, pod}` (4 dims)
   so the `L`-axis sweep has range.
4. `measure-baseline.py`: add producer-side `producer_cpu_cores`,
   `producer_rss_mib`, `producer_bytes_out_per_s` columns.
5. Run the four §6.2 sub-sweeps at `N = 1` on the 40-core dev
   box. Produce four CSVs + four figures.
6. (Separate track) OpAMP hot-reload of `L` for §6.5.
