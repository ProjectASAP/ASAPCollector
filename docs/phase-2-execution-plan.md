# Phase 2 execution plan — extract `asap-precompute-go`

_Companion to [ADR-0002](adr/adr-0002-extract-precompute-runtime.md).
File-by-file extraction map for moving the runtime out of the
five Go OTel processors into a shared `asap-precompute-go`
module._

## Inventory of source files

```
opentelemetry-collector-contrib-patch/processor/
├── ddsketchprocessor/processor.go          942 LoC
├── kllprocessor/processor.go                720 LoC
├── hllprocessor/processor.go                785 LoC
├── countsketchprocessor/processor.go        663 LoC
└── countminsketchprocessor/processor.go     739 LoC
                                          --------
                                            3849 LoC total
```

The processors fall into two structural patterns that need to be
harmonized in the extracted runtime:

- **Pattern A** (DDSketch / KLL / HLL): nested
  `resourceWindow → scopeWindow → metricWindow → sketchSeries`
  hierarchy. Per-`pmetric.ScopeMetrics` aggregation. Explicit
  `accumulateIntoWindow` / `flushWindow` pair.
- **Pattern B** (CountSketch / CountMin): flat
  `windowSketch` (map: partitionKey → sketch). Per-metric
  aggregation. Timer-driven `startWindowLoop` /
  `emitWindowAndReset`.

Phase 2 unifies both into the generic `Precompute[SketchT]`
shape from ADR-0002. The window manager picks
tumbling/sliding/batch internally based on `PrecomputeConfig`.

## Target layout

```
asap-precompute-go/
├── go.mod                     // module github.com/ProjectASAP/asap-precompute-go
├── observation.go              // Observation + ObservationValue (~50 LoC)
├── envelope.go                 // SketchEnvelope view of the sketchlib-go proto (~30 LoC)
├── precompute.go               // Precompute interface + generic impl (~250 LoC)
├── window.go                   // tumbling / sliding / batch logic (~250 LoC)
├── snapshot_cache.go           // outbound + inbound caches; ComputeDelta (~200 LoC)
├── matchers.go                 // LabelMatcher, seriesKey, seriesAttrs (~150 LoC)
├── config.go                   // PrecomputeConfig, AggregationMode, OnOverflow (~120 LoC)
├── adapter.go                  // Adapter interface + Decode/Encode helpers (~100 LoC)
├── controlchannel/
│   ├── channel.go              // ControlChannel interface (~30 LoC)
│   ├── http_poll.go            // HttpPollChannel impl (~80 LoC)
│   └── opamp.go                // OpAmpChannel adapter wrapping existing controller/opamp (~50 LoC)
├── telemetry.go                // PrecomputeStats + recordInput/recordOutput (~80 LoC)
└── otel/                       // OTel-flavored Adapter helpers (consumed by Phase-2 shims)
    ├── decode.go               // pmetric.Metrics → []Observation
    ├── encode.go               // []SketchEnvelope → pmetric.Metrics
    └── seriesattrs.go          // attribute key construction
```

Approximate total: 1500–1800 LoC. Each existing OTel processor
shrinks to ~50–80 LoC shim that constructs a
`Precompute[<sketch type>]` and delegates `ConsumeMetrics`.

## Function-level extraction map

For each existing function, where it goes after Phase 2:

### Pattern A (DDSketch, KLL, HLL) — same map applies to all three

| Today | Layer | Becomes |
|---|---|---|
| `type resourceWindow / scopeWindow / metricWindow / sketchSeries` | 3 | `precompute.go` — collapse into a single `series` struct keyed by `(agg_id, label_key)` since the resource/scope hierarchy was an OTel-side concern, not an algorithmic concern. |
| `func newProcessor` | 4 | stays in shim; constructs `Precompute[*ddsketch.DDSketch]` |
| `func Start` | 4 | stays in shim; spawns ticker goroutine that calls `Precompute.Tick` and emits via `Adapter.Encode` + `next.ConsumeMetrics` |
| `func Shutdown` | 4 | stays in shim; cancels ticker, calls `Precompute.Shutdown` |
| `func ConsumeMetrics` | 4 | stays in shim; calls `Adapter.Decode(md)` → `for _, o := range obs { p.pc.Observe(o) }` → `next.ConsumeMetrics(ctx, md)` (pass-through) |
| `func processBatch / processScopeMetrics` | 4 | becomes the `otel.Decode` helper; produces `[]Observation` |
| `func consumeDDSketchDataPoints / consumeGaugeDataPoints` | 4 | folded into `otel.Decode`; produces `Observation::Envelope` for sketch-typed inputs and `Observation::Float` for scalar |
| `func decodeDDSketchDataPoint` | 4 | folded into `otel.Decode` envelope path |
| `func cacheInboundSnapshot` | 3 | `snapshot_cache.go::CacheInbound` |
| `func newSketchSeries / updateWindow / merge / ensureSketch` | 3 | `window.go` window-state helpers |
| `func serializeDDSketch` | 1 | already lives in `sketchlib-go`; called via `Sketch.Snapshot()` |
| `func computeDDSketchDelta` | 3 | `snapshot_cache.go::ComputeDelta` |
| `func attributesKey / seriesKey / seriesAttrs / matchesMatchers / newSeriesFrom` | 3 | `matchers.go::SeriesKey / SeriesAttrs / Matches / NewSeries` |
| `func accumulateIntoWindow` | 3 | `window.go::Observe` (merged with `Precompute::Observe`) |
| `func getOrCreateMetricWindow` | 3 | private to `window.go` |
| `func accumulateGaugeMetric / accumulate{DD,KLL,HLL}SketchMetric` | 3+4 | sketch-specific `Sketch::Observe` lives at L1; the routing logic (raw vs envelope) is in `Precompute::Observe` |
| `func flushWindow` | 3 | `Precompute::Tick`; emits `[]SketchEnvelope` |
| `func buildMetric / buildMergedSketchMetric / buildQuantileMetric` | 4 | becomes `otel.Encode`; produces `pmetric.Metrics` from `[]SketchEnvelope` |
| `func enableSelfMonitoring / shutdownMonitor` | 4 | stays in shim |
| `func recordInput / recordOutput / activeSeriesCount` | 3 | `telemetry.go` |

### Pattern B (CountSketch, CountMin) — same map

| Today | Layer | Becomes |
|---|---|---|
| `type windowSketch` | 3 | absorbed into `series` struct in `precompute.go` (one map: `(agg_id, label_key) → SketchT`) |
| `func newProcessor / Start / Shutdown / Capabilities / ConsumeMetrics` | 4 | shim |
| `func processMetrics / consumeBatch / ingestMetric / dpValue` | 4 | `otel.Decode`; produces `[]Observation` |
| `func matchesMatchers / encodeKey / seriesKey / seriesAttrs / buildPartitionKey / encodeAttributesAsKey` | 3 | `matchers.go` |
| `func accumulateIntoWindow / updateWindowSketch / mergeWindowSketch` | 3 | `Precompute::Observe + window.go` |
| `func startWindowLoop / emitWindowAndReset / buildWindowMetricsAndReset` | 3+4 | timer goroutine moves to `Precompute` (driven by `Adapter::ScheduleTick`); `buildWindowMetricsAndReset` becomes `otel.Encode` |
| `func inboundDecode{CS,CMS} / mergeWindow{CS,CMS}` | 3 | `snapshot_cache.go::ApplyDelta` (envelope-in path on `Precompute::ObserveEnvelope`) |
| `func serialize{CountSketch,CMS} / deserialize{CMS} / clone{CS,CMS}` | 1 | already `sketchlib-go` API |
| `func newConfiguredCountSketch / nextPowerOfTwo` | 4 | stays in shim (it's `Config` validation) |
| `func recordInput / recordOutput / activeSeriesCount` | 3 | `telemetry.go` |

### Cross-cutting

- All five processors have an `enableSelfMonitoring` /
  `shutdownMonitor` pair that emits OTel-shaped runtime metrics.
  These become two pieces:
  - `telemetry.go::PrecomputeStats` (host-neutral counters in
    L3) — incremented from inside `Precompute`.
  - The shim's `enableSelfMonitoring` reads `PrecomputeStats`
    via `Adapter::EmitTelemetry` and constructs the OTel-shaped
    metrics.

## Per-processor shim shape (post-Phase-2)

Every existing processor file becomes ~50-80 LoC of this shape:

```go
package ddsketchprocessor

import (
    precompute "github.com/ProjectASAP/asap-precompute-go"
    otelhost  "github.com/ProjectASAP/asap-precompute-go/otel"
    "github.com/ProjectASAP/asap-precompute-go/controlchannel"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/pdata/pmetric"
)

type ddsketchProcessor struct {
    pc       precompute.Precompute  // generic over the sketch type bound by Config.SketchType
    adapter  *otelhost.Adapter
    cc       controlchannel.ControlChannel
    cfg      *Config
    next     consumer.Metrics
    logger   *zap.Logger
    monitor  *otelhost.SelfMonitor   // wraps PrecomputeStats
    shutdown chan struct{}
}

func (p *ddsketchProcessor) Capabilities() consumer.Capabilities {
    return consumer.Capabilities{MutatesData: false}
}

func (p *ddsketchProcessor) Start(ctx context.Context, host component.Host) error {
    if err := p.pc.Start(ctx); err != nil { return err }
    go p.controlChannelLoop(ctx)
    go p.tickLoop(ctx)
    return p.monitor.Start(ctx, host)
}

func (p *ddsketchProcessor) Shutdown(ctx context.Context) error {
    close(p.shutdown)
    return p.pc.Shutdown(ctx)
}

func (p *ddsketchProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
    obs, err := p.adapter.Decode(md)
    if err != nil { return err }
    for _, o := range obs {
        if err := p.pc.Observe(&o); err != nil { /* OnOverflow */ }
    }
    return p.next.ConsumeMetrics(ctx, md)
}

func (p *ddsketchProcessor) tickLoop(ctx context.Context) {
    t := time.NewTicker(p.cfg.WindowSize)
    defer t.Stop()
    for {
        select {
        case <-p.shutdown: return
        case now := <-t.C:
            envelopes := p.pc.Tick(now.UnixMilli())
            md := p.adapter.Encode(envelopes)
            if err := p.next.ConsumeMetrics(ctx, md); err != nil { /* log */ }
        }
    }
}

func (p *ddsketchProcessor) controlChannelLoop(ctx context.Context) {
    t := time.NewTicker(p.cfg.ControlPollInterval)
    defer t.Stop()
    for {
        select {
        case <-p.shutdown: return
        case <-t.C:
            if cs := p.cc.Poll(); cs != nil {
                p.pc.UpdateConfig(cs)  // atomic swap inside Precompute
            }
        }
    }
}
```

The five processors share this scaffolding; the only per-processor
differences are:

- The generic `Precompute` type parameter (`*ddsketch.DDSketch`
  vs `*kll.KllSketch` vs ...).
- The `otel.Adapter` decode path — which `Metric.data` oneOf
  variants it recognizes (DDSketch / KLLSketch / HLLSketch /
  CountSketch / CountMinSketch).
- Default config values (`alpha`, `k`, `precision`, `width`,
  `depth`).

A future refactor could collapse all five files into a single
generic shim parameterized by sketch type. Phase 2 keeps them
separate for OCB build-config compatibility (`builder-config.yaml`
references each processor's package path).

## Phase 2 work breakdown

Sequenced for incremental verification — each step is shippable
and reversible:

| Step | Scope | Verification |
|---|---|---|
| **2.1** Bootstrap `asap-precompute-go` module + types | `observation.go`, `envelope.go`, `config.go`, `adapter.go` (interface only). No logic, just type definitions matching ADR-0002 contracts. | `go vet ./...` clean. |
| **2.2** Implement `matchers.go` and `snapshot_cache.go` | Move pure-data-structure logic with no host coupling: `LabelMatcher`, `SeriesKey`, snapshot cache, `ComputeDelta`. | Unit tests against fixed input vectors copied from existing processor tests. |
| **2.3** Implement `window.go` and `precompute.go` | Generic Precompute implementation. Drives matchers + snapshot cache. Generic over `Sketch` interface. | Unit tests with mock Sketch (table-driven; verify tumbling, sliding, late-data, max_series, OnOverflow). |
| **2.4** Implement `otel/{decode,encode}.go` | Pmetric decoders for DDSketch + Gauge + Sum. Encoders for `Metric.data = DDSketch{...}`. | Unit tests with fixed `pmetric.Metrics` fixtures. |
| **2.5** Refactor `ddsketchprocessor` to shim | Delete the in-file accumulateIntoWindow/flushWindow/snapshot logic; replace with the shim shape above. Existing tests must still pass. | b3-delta e2e: same `19.49` value at offset −90s. |
| **2.6** Refactor `kllprocessor` | Same as 2.5 but for KLL. | Existing KLL accuracy tests pass. |
| **2.7** Refactor `hllprocessor` | Same as 2.5 but for HLL. | Existing HLL accuracy tests pass. |
| **2.8** Refactor `countsketchprocessor` | Pattern B — verify flat-window collapse to (agg_id, label_key) keying preserves behavior. | CountSketch top-K accuracy reducer (`P8`) matches pre-extraction. |
| **2.9** Refactor `countminsketchprocessor` | Same as 2.8 for CMS. | P8 accuracy reducer matches. |
| **2.10** `controlchannel/http_poll.go` + adapter wiring | First non-OpAMP control channel. Backward-compat: existing OpAMP-driven deploys keep using `OpAmpChannel`; new deploys can opt into `HttpPollChannel`. | b3-delta e2e survives a runtime config push (sketch_type unchanged, window_size changed) without state loss. |
| **2.11** Performance gate | Per-observation latency p99 within 10% of pre-refactor (R2). | Bench against the existing fake-exporter throughput harness. |

Steps 2.1–2.4 can run in parallel (no inter-dependencies).
Steps 2.5–2.9 are sequential (each builds on the verified shim
shape from the previous). 2.10–2.11 gate the phase exit.

## Risks during the migration

- **R-Phase-2-A: Pattern-B → unified series-map collapse hides
  partition semantics.** CountSketch / CMS today aggregate by
  `partitionKey` (CS) or `aggregationKey` (CMS); these are
  string concatenations of `(metric_name, label_subset)`. The
  unified `(agg_id, label_key)` schema must preserve the same
  string. Mitigation: in 2.8 / 2.9, write a key-equivalence test
  before refactoring.
- **R-Phase-2-B: Tick goroutine races with ConsumeMetrics.** Today
  each processor uses an internal mutex. The extracted
  `Precompute` must keep the same locking discipline (per-series
  rwmutex; coarse global mutex around tick swap). Mitigation:
  keep mutex shape identical in 2.3; race detector run on
  refactored shims.
- **R-Phase-2-C: OCB build manifest drift.** ASAPCollector's
  `builder-config.yaml` references each processor's Go package
  path. After Phase 2, those paths still work (the processor
  packages still exist; they just call a new module). The new
  `asap-precompute-go` module needs to be added to OCB's `gomod`
  list. Mitigation: 2.5 includes `build_sketchcollector.sh`
  smoke run before merge.

## Phase exit criterion (blocking)

1. All five OTel processors are ≤80 LoC each (excluding factory
   / config-validation boilerplate).
2. b3-delta e2e produces the observed `19.49` value at offset
   −90s, identical to pre-extraction.
3. Per-observation `Observe` latency p99 within 10% of
   pre-refactor.
4. P8 accuracy reducer per-row error matches pre-extraction
   for all five sketch types.
5. `cargo test` / `go test` clean across all touched packages.
6. `controlchannel.HttpPollChannel` smoke test: collector
   running with the new control channel survives a controller
   plan push that changes window size (without losing sketch
   state mid-window).

If any of (1)–(6) fails, Phase 2 does not merge.
