# SDK cost evaluation

## Why this doc exists

The ASAP pipeline has three data-plane decision points —
SDK / agent collector / backend collector — and the controller's
job is to plan each of them based on the current query workload.
This document pins down **what exactly the controller decides
for the SDK**, what knobs the SDK exposes to express that
decision, and how those knobs are evaluated.

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

Not a blocker for this evaluation — the `kll-full` row is
sufficient for a three-way comparison with `raw-buffer` and
`dd-delta` on the encoding axis.

### `AggregationRawBuffer` design

**Why this baseline exists.** Two observations drive it:

1. **The upstream OTel SDK aggregators are lossy _over the emit
   period `W`_.** Within a single `W`, `Sum` collapses every
   `Add` call into one running total, `Histogram` collapses into
   bucket counts, the sketch aggregators collapse into their
   bounded-error summaries. By the time anything hits the wire
   at the end of `W`, individual observations are gone.
2. **The experiment we want to run** is: at the _same_ emit
   period `W` that the sketch encodings use, what does the
   producer pay in CPU, RSS, and wire bandwidth if it doesn't
   aggregate at all and just batch-ships the raw events
   accumulated during `W`?

`raw-buffer` is the one aggregator that preserves every
observation during `W` and ships the full batch on tick. That
gives the encoding-axis an apples-to-apples reference point —
same `W`, same `L`, only the encoding differs. Without it,
every bandwidth / CPU / RSS number a sketch encoding reports
is a ratio against something unmeasured and the encoding
ablation collapses into "sketch vs nothing".

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

### `Aggregation<X>Delta` — what it is and why it's a separate slot

For the four sparse-state sketches (DDSketch, CountSketch,
CountMinSketch, HLL) the SDK can ship one of two wire payloads
per tick:

- **Full state** (`*-full`, the default): every bucket / cell /
  register the aggregator holds.
- **Delta** (`*-delta`, enabled by `DeltaTransmission: true` on
  the same `Aggregation`): only the cells that changed since the
  last tick. The aggregator keeps the last-emitted state in
  memory, computes a sparse diff on collect, and ships just the
  diff; the receiver reconstructs the full state by accumulating
  successive deltas. When the projection `L` or sketch params
  change, the aggregator emits a full state once (not a delta)
  and resets the reference.

Delta has two independent reasons to live as a distinct encoding
in this evaluation, not just a free-win optimization:

1. **The bandwidth claim needs both halves measured separately.**
   "Sketch uses less wire than raw" has two independent sources:
   the sketch payload is smaller than the raw buffer, _and_ the
   sparse delta is smaller than the full sketch. If the
   encoding-axis sweep only had `*-full` rows, every reported
   bandwidth reduction conflates those two factors and you
   can't attribute the savings. `*-delta` vs `*-full` vs
   `raw-buffer` as three points on the same axis lets each
   factor be read off directly.
2. **Delta is a memory-for-bandwidth trade, not a free win.**
   The aggregator has to hold the previous-tick state alongside
   the current state to compute the diff, so `cms-delta` RSS is
   ≈ 2× `cms-full` RSS in practice. Readers need that number
   next to the bandwidth savings to make a meaningful choice;
   the encoding-axis row is where they sit side by side.

KLL doesn't get a delta variant: its multi-level sample buffers
are rewritten by compaction on most ticks, so a naive byte-diff
is no smaller than the full sketch. See the note on `kll-delta`
above; for the evaluation the `kll-full` row plus `dd-full` /
`dd-delta` covers the quantile encoding cost adequately.

Hot-reload of `L` at runtime:
- OTel Go SDK doesn't currently support replacing a View's
  `AttributeFilter` after `MeterProvider` construction. For
  the static cost sweeps in this doc that's fine — each run
  is a fresh process.
- For the controller-in-loop scenario where the planner pushes
  a new `L` mid-run, the SDK needs a hot-reload hook. That's
  a separate implementation track, not a blocker here.

## Evaluation design

Each sub-experiment sweeps one axis of the `(W, L, agg_type)`
triple and holds the other two fixed at a representative
operating point.

**The workload driver is
[`deploy/fake-exporter/`](../deploy/fake-exporter/) — a
synthetic *instrumented application*, not an OTel `Exporter` and
not a Prometheus client.** The misleading name is historical.
Concretely it imports the ASAP-patched OTel Go SDK, stands up
a `MeterProvider` with a user-configured `View` (the `L` and
`agg_type` knobs live there) and a `PeriodicReader(W)`, and
drives `Counter.Add` / `Gauge.Record` from per-series
goroutines firing at `freq_hz`. That stack is identical to
what any OTel-instrumented Go service does in production — no
bespoke wire format, no bypass of the SDK. The SDK's own OTLP
gRPC exporter ships the aggregated data downstream.

Defaults: `cardinality=1000`, `freq_hz=10` per series, counter
+ gauge instruments, `SOAK_S=180s`. `measure-baseline.py` then
reads producer CPU / RSS / tx-bytes from `docker stats` and
agent / gateway / backend stats from Prometheus.

| Axis | Fixed | Swept | What the sweep measures |
|---|---|---|---|
| **Time** | `L = keep-all`, `agg = dd-full` | `W ∈ {1s, 15s, 60s, 300s}` | How producer bw / CPU / RSS change with flush interval. Longer `W` should reduce wire traffic at the cost of freshness. |
| **Label** | `W = 60s`, `agg = dd-full` | `\|L\| ∈ {0, 1, 2, 3, 4}` dims kept | How attribute-set folding under `AttributeFilter` trades producer RSS against filter CPU. Expected: fewer sketch instances → lower RSS; filter-per-measure → higher CPU. |
| **Encoding** | `W = 60s`, `L = typical projection` | `agg ∈ {raw-buffer, dd-full, dd-delta, kll, cms-full, cms-delta, hll-full, hll-delta}` | What each encoding costs at a fixed `(W, L)`. Sketches vs raw-buffer for bandwidth + RSS; full vs delta for the memory/bw tradeoff. |
| **Combined** | — | best `(W, L, agg)` picked for each metric, vs. a baseline at `W=15s`, full `L`, `agg=raw-buffer` | End-to-end bandwidth reduction as the product of the three per-axis factors. |

The driver is
[`deploy/scripts/run-three-axis-sweep.sh`](../deploy/scripts/run-three-axis-sweep.sh);
the wrapper that runs all four above is
[`deploy/scripts/run-sdk-cost-sweeps.sh`](../deploy/scripts/run-sdk-cost-sweeps.sh).
CSVs and a findings write-up live under
`deploy/eval-results/sdk-cost/`.

**Related evaluation (separate harness).** Given a set of query
workloads, check that the controller's chosen `(W, L, agg)`
matches the hand-tuned ideal for that workload. Measures
planner quality independent of SDK emit cost; doesn't need the
cost sweeps above.

