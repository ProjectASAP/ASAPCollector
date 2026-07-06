# ADR-0002: Extract the precompute runtime to `asap-precompute-{go,rs}`

| | |
|---|---|
| Status | **Proposed** (gates Phase 2 / Phase 3 of the edge-framework migration) |
| Date | 2026-05-02 |
| Deciders | Project ASAP maintainers |
| Supersedes | n/a |
| Superseded by | n/a |

## Context

Today the windowing / delta / scheduler runtime logic for the
**edge** path lives inside the Go OTel processors (~3850 LoC
across
`opentelemetry-collector-contrib-patch/processor/{ddsketch,kll,hll,countsketch,countminsketch}processor/processor.go`)
and the analogous envelope-parsing / delta-apply / sketch
reconstruction logic on the **backend ingest** side lives inside
`ASAPQuery-backend/asap-query-engine/src/precompute_operators/*.rs`
and `drivers/ingest/otel.rs::apply_modified_otlp_delta_bytes`.
Both implement the same wire-format contract; both are in scope
for this ADR and converge onto `asap-precompute-{go,rs}`. The
backend's query-side engine (PromQL aggregation, storage, query
planning) is a separate concern with its own design and is **not**
the subject of this ADR — see the backend's own design docs.

Each of the edge-side files conflates four concerns:

1. **OTel binding** — implementing `processor.Metrics`, accepting
   `pmetric.Metrics`, calling `nextConsumer.ConsumeMetrics`.
2. **Data shape adapter** — extracting `(timestamp, attrs, value)`
   tuples out of `pmetric.Gauge | Sum | DDSketchDataPoint | …`.
3. **Runtime** — `accumulateIntoWindow`,
   `flushWindow`, snapshot caches, label matchers, scheduler.
4. **Output binding** — emitting `pmetric.Metrics` of the right
   typed variant, calling `nextConsumer.ConsumeMetrics`.

(1, 2, 4) are host-specific. (3) is host-neutral. The framework
refactor (see `design-asap-edge-framework.md` §6) extracts (3) so
adapters provide (1, 2, 4) only.

## Decision

### What gets extracted

Two new artifacts, one per language:

- **`asap-precompute-go`** — Go module living under
  `ASAPCollector/asap-precompute-go/` (subdirectory, not a
  separate repo for now; promotion to a separate repo is a
  Phase-7 / repo-rename concern).
- **`asap-precompute-rs`** — Rust crate living under
  `ASAPCollector/asap-precompute-rs/`. Mirrors
  `asap-precompute-go`'s runtime bit-identically. The crate
  source lives in this repo and is consumed by
  (a) future Rust-based edge agents (Vector adapter, OTAP-Rust
  receiver), AND (b) `ASAPQuery-backend`'s ingest path — which
  uses the crate's envelope parsing, delta apply, and sketch
  reconstruction logic, while keeping its own query-side engine
  (PromQL aggregation, storage, query planning) separate. The
  backend's QUERY-side engine is out of scope for this ADR — its
  design is governed by `ASAPQuery-backend`'s own docs — but the
  backend's INGEST path explicitly depends on this crate, the
  same way it depends on `asap_sketchlib` today (consumed via
  git URL).

### Public API

The trait, types, and config surface are pinned by the design
doc (§5.1, §6.2, §6.3). The summary contract:

- `Observation` — host-neutral input. Includes timestamp, metric
  name, labels, and a `value` (`Float | Hash | Bytes |
  Envelope`).
- `Precompute` trait — generic over `SketchT: Sketch`.
  - `observe(&Observation) -> Result<(), Overflow>`
  - `observe_envelope(SketchEnvelope) -> Result<(), Error>`
  - `tick(now_ms) -> Vec<SketchEnvelope>`
- `PrecomputeConfig` — exactly today's processor config knobs
  (sketch type, window size, matchers, delta thresholds, sketch
  params) plus two new fields needed once the runtime is no
  longer behind OTel's pipeline backpressure: `max_series` +
  `OnOverflow` (Drop | Block | EvictOldest).
- `Sketch` trait family — `Sketch + QuantileSketch +
  CardinalitySketch + FrequencySketch` (split). Each in-process
  sketch impls only the queries it supports.
- Crash recovery is intentionally **not** in the trait. A future
  `PersistentPrecompute: Precompute` extension trait can add
  `snapshot()` / `restore()` later.

### Behavior preservation

- Wire format unchanged. `SketchEnvelope.payload` bytes are
  byte-identical pre/post-extraction.
- Window semantics unchanged. Tumbling-vs-sliding-vs-batch logic
  is moved verbatim from each processor file into
  `asap-precompute-go::window.go` (Go) /
  `asap-precompute-rs::window.rs` (Rust).
- Snapshot cache invariants unchanged. The
  `snapshots map[string][]byte` (Go) and
  `IngestState.sketch_snapshots` (Rust) maps move into
  `snapshot_cache.go` / `snapshot_cache.rs` with the same
  per-series-key contract — every `ComputeDelta` call updates the
  cached previous snapshot to the current one (always-refresh),
  matching the legacy processors' "snapshot-update-after-every-emit"
  behavior. There is no configurable "refresh-only-on-full" mode;
  successive sub-threshold deltas are each computed against the
  immediately preceding window.
- Backwards-compat for the Go OTel processors during Phase 2:
  each existing `processor/{ddsketch,kll,hll,countsketch,countminsketch}processor/processor.go`
  reduces to a ~50-line shim that delegates to
  `asap-precompute-go`. The shim's `ConsumeMetrics` signature,
  metric output schema, and config keys remain identical;
  config-file changes are not required for existing deployments.

### Test API contract — promote private methods to public on the shim

Today's per-processor tests directly call private methods that
the shim model would otherwise hide:

- `proc.processBatch(ctx, md) (pmetric.Metrics, error)` — DDSketch tests
- `proc.processMetrics(ctx, md) (pmetric.Metrics, error)` — CountSketch / CMS tests
- `proc.flushWindow(ctx) error` — DDSketch / KLL / HLL tests

To avoid either rewriting all 5 processor test files or adding
private wrapper methods that conflict with `MutatesData: false`,
the shim **promotes these to public methods** with the same
semantics:

| Public method | Semantics |
| --- | --- |
| `Shim.ProcessBatch(ctx, md) (pmetric.Metrics, error)` | Decode input → `Precompute.Observe` each → `Precompute.Tick` (batch flushes per call) → `Adapter.Encode` → return synthesized output. Does NOT touch `nextConsumer`. Caller decides what to do with the output. |
| `Shim.ProcessMetrics(ctx, md) (pmetric.Metrics, error)` | CountSketch / CMS naming variant of `ProcessBatch`. Same semantics. |
| `Shim.FlushWindow(ctx) error` | Force `Precompute.Tick(now)`, encode envelopes, and forward via `nextConsumer.ConsumeMetrics`. No-op if no closed windows have data. |

Each is ~10 LoC of delegation. The methods are explicitly
documented as "test-friendly hooks; production callers should use
`ConsumeMetrics`." Tests adapt by capitalizing the method name
(sed-style rename); no test logic changes.

This keeps `Capabilities() = {MutatesData: false}` honest because
`ProcessBatch` returns a fresh `pmetric.Metrics` rather than
mutating input md in place.

### Performance contract

- **Go (Phase 2):** per-observation `Observe` latency p99 must
  stay within 10% of the pre-refactor in-line implementation.
  Verified via the existing otel-app b3-delta benchmark.
- **Rust (Phase 3) — edge side:** `asap-precompute-rs` mirrors
  `asap-precompute-go`'s runtime bit-identically (same
  `SeriesKey` format, same `SnapshotCache` always-refresh
  semantics, same `Drain` rotation). This is the contract for
  Rust-based edge agents (Vector, OTAP-Rust).
- **Rust (Phase 3) — backend ingest side:** the contract is
  bit-identical envelope parsing across languages — bytes
  produced by `sketchlib-go` (Go agents) must reconstruct
  correctly via `asap_sketchlib` (the Rust backend ingest path
  that `asap-precompute-rs` calls into). Cross-language
  byte-format harmonization is tracked in
  [issue #243](https://github.com/ProjectASAP/ASAPCollector/issues/243)
  and is a hard prerequisite for backend integration: until #243
  closes, the agent → backend wire path can lose information
  between encode (Go) and reconstruct (Rust). Phase 3 step 3
  (backend ingest cutover to `asap-precompute-rs`) cannot land
  before #243 closes.

### Repo / module layout

```
ASAPCollector/
├── asap-precompute-go/
│   ├── go.mod                          // module github.com/ProjectASAP/asap-precompute-go
│   ├── observation.go                  // Observation type
│   ├── envelope.go                     // SketchEnvelope type (Go view of the proto)
│   ├── precompute.go                   // Precompute interface + impl
│   ├── window.go                       // tumbling / sliding / batch logic
│   ├── snapshot_cache.go               // outbound + inbound snapshot caches; ComputeDelta
│   ├── matchers.go                     // LabelMatcher / aggregate_by / seriesKey
│   ├── config.go                       // PrecomputeConfig + AggregationMode + OnOverflow
│   ├── adapter.go                      // Adapter trait + helpers
│   └── controlchannel/                 // ControlChannel trait + HttpPollChannel impl
├── asap-precompute-rs/
│   ├── Cargo.toml                      // crate name asap-precompute-rs
│   └── src/
│       ├── lib.rs
│       ├── observation.rs
│       ├── envelope.rs                 // re-exports asap_sketchlib::proto::sketchlib::SketchEnvelope
│       ├── precompute.rs               // Precompute trait + generic impl
│       ├── window.rs
│       ├── snapshot_cache.rs
│       ├── matchers.rs
│       ├── config.rs
│       ├── adapter.rs
│       └── control_channel.rs
└── opentelemetry-collector-contrib-patch/processor/
    ├── ddsketchprocessor/processor.go      # Phase 2: ~50 LoC shim delegating to asap-precompute-go
    ├── kllprocessor/processor.go            # Phase 2: ~50 LoC shim
    ├── hllprocessor/processor.go            # Phase 2: ~50 LoC shim
    ├── countsketchprocessor/processor.go    # Phase 2: ~50 LoC shim
    └── countminsketchprocessor/processor.go # Phase 2: ~50 LoC shim
```

### Sequencing

- Phase 2 (Go) and Phase 3 (Rust) can proceed in parallel
  because they touch different repos. Both must merge before
  Phase 4 (Telegraf adapter) starts, since Telegraf reuses
  `asap-precompute-go` and the OTel adapter shim must be a
  proven shape before generalizing it.

## Consequences

### Positive

- Each existing processor shrinks from ~700–950 LoC to ~50 LoC
  shim, removing the same conflated-concern logic five times.
- Telegraf / OTAP / Vector adapters become viable — they reuse
  `asap-precompute-{go,rs}` rather than re-implementing window /
  snapshot / matcher logic.
- Future Rust-based edge agents (Vector, OTAP-Rust, Arrow-backed
  shims) become viable — they reuse `asap-precompute-rs` rather
  than re-implementing window / snapshot / matcher logic.
- `ASAPQuery-backend`'s ingest path becomes a thin adapter
  calling the same Rust crate the Rust edge agents would use,
  eliminating the agent / backend duplication for the SHARED
  ingest pieces — envelope parsing, delta apply, sketch state
  reconstruction, and merge logic. (The backend's query-side
  engine — PromQL aggregation, storage, query planning — is a
  separate design and is unaffected by this dedup.)
- Future Sketch trait additions (e.g., `observe_batch` for
  columnar Arrow ingest) become single-crate changes.

### Negative

- Cross-repo dependency added: Go OTel patches now pull in
  `github.com/ProjectASAP/asap-precompute-go`. The build
  pipeline (`build_asap_otel.sh`) needs to handle two
  module sources.
- Test surface doubles temporarily during the migration: each
  function moves through a "duplicated, behavior-verified, then
  delete the original" sequence to catch divergence. Phase 2
  exit criterion (b3-delta produces same value, p99 within 10%)
  is the gate.

### Compatibility

- No wire-format changes.
- No config-file changes for existing OTel collector
  deployments.
- `SketchEnvelope.payload` bytes stay byte-identical
  pre/post-extraction (R4 mitigation), preserving compatibility
  with any consumer of the wire format. The backend's PromQL
  output is governed by its own ADR; this ADR only commits to the
  wire format.

## Phase-2 / Phase-3 execution plan

See [`docs/phase-2-execution-plan.md`](../phase-2.md)
for the file-by-file extraction map covering all 5 OTel
processors.

## References

- [`docs/design-asap-edge-framework.md`](../design-asap-edge-framework.md) §3, §6, §9 (Phases 2–3)
- ADR-0001 (sketch-core retirement — prerequisite that simplified the Rust algorithm crate before this extraction)
- ADR-0003 (adapter trait + control channel — defines what the OTel processor shims look like after extraction)
