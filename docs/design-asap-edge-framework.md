# ASAP Edge Precompute Framework — Design

_Status: **draft** — Authored 2026-05-01. Not yet implementation-bound;
gates the host-adapter work tracked in
[`PROGRESS.md`](../PROGRESS.md)._

## 1. Motivation

The repo is named `ASAPCollector`, the binary is `sketchcollector`,
and the bulk of the code today lives under
`opentelemetry-collector-contrib-patch/`. That framing misleads
both contributors and downstream users:

- **For contributors:** the reusable thing isn't "an OTel Collector
  with sketch processors"; it's an *edge precompute runtime* — a
  state machine that takes per-event observations, maintains
  windowed sketch state, emits compact summaries on a schedule,
  applies inbound sketch deltas against cached snapshots, and
  hands off control-plane configs from a planner. The OTel
  Collector is *one host* for that runtime.

  On the Rust side, `asap_sketchlib` is the single algorithm
  crate (mirroring `sketchlib-go` on the Go side); the former
  `sketch-core` wrapper is retired (see §3.3 + §8 Phase 0.5).

- **For downstream users:** the same runtime should be deployable
  inside any modern edge data-plane — not just OTel Collector.
  Telegraf, Vector, and OpenTelemetry's Rust/Arrow successor
  ([OTAP Dataflow](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/README.md))
  all have plugin/processor surfaces that map cleanly onto the
  same runtime; today none of them are reachable because the
  runtime is fused into Go-side OTel `processor.Processor`
  implementations.

The goal of this doc is to define the framework, the host
adapters, the wire format invariants, and a phased migration
path that doesn't break the working e2e (b3-delta with #210 +
#211 + ASAPQuery-backend#70 / #71).

## 2. Non-goals

- Replacing the controller, query backend, or storage layer.
- Reimplementing sketches in every host language. Algorithm
  implementations stay in `sketchlib-go` (Go) and `asap_sketchlib`
  (Rust); host adapters call into those.
- Forking pdata / Arrow / Telegraf metric / Vector event models —
  each host adapter converts to/from its native data model at
  the edge, the runtime never sees those types.
- Multi-tenant isolation, AuthN/AuthZ, network/QoS — orthogonal.
- Replacing OpAMP. OpAMP stays the OTel-native control-plane
  channel; non-OTel hosts get an alternative (HTTP poll or local
  file-watch) covered in §7.

## 3. Current state — what's factored vs what's coupled

### 3.1 Already factored out of OTel

| Layer | Crate / package | Status |
|---|---|---|
| Sketch algorithms (Go) | `sketchlib-go/sketches/{DDSketch,KLL,HLL,CountSketch,CountMinSketch}` | **Done.** Algorithms host-independent. Each sketch ships its own `delta.go` with `ComputeDelta` / `ApplyDelta`. |
| Sketch algorithms (Rust) | `asap_sketchlib/src/sketches/*.rs` | **Done.** All sketches (`ddsketch`, `countmin`, `count`, `hll`, `kll`, `cms_heap`, `hydra_kll`, `set_aggregator`, `delta_set_aggregator`) ship in-crate alongside their wire-format types and (where applicable) `apply_delta`. The former `sketch-core` wrapper is retired — see §3.3. |
| Sketch wire format | `asap_otel_proto::sketchlib::v1::*` (Rust), `sketchlib-go/proto/{ddsketch,countminsketch,...}` (Go) | **Done.** Byte-identical envelopes across languages; cross-validated by the e2e (#210). |
| Delta proto | `asap_otel_proto::sketchlib::v1::{DDSketchDelta,...}` mirrors `sketchlib-go/proto/*Delta` | **Done.** |
| Controller plan generator | `controller/src/config/agent.rs` `generate_agent_config(...)` | **Coupled to OTel YAML.** Emits an OTel-Collector config string today; needs a host-neutral form (see §6). |

### 3.2 Coupled to the OTel Collector today

The runtime state machine — *windowing, snapshot cache, delta
encode/apply, batching, output dispatch* — lives inside
`opentelemetry-collector-contrib-patch/processor/{ddsketch,kll,hll,countsketch,countminsketch}processor`
as Go OTel `processor.Processor` implementations. Each of those
files is ~600–1000 lines and conflates four concerns:

1. **OTel binding** — implementing `processor.Metrics`, accepting
   `pmetric.Metrics`, calling `nextConsumer.ConsumeMetrics(...)`.
2. **Data shape adapter** — extracting `(timestamp, attrs, value)`
   tuples from `pmetric.Gauge|Sum|*SketchDataPoint`.
3. **Runtime** — `accumulateIntoWindow`, `flushWindow`,
   `snapshots map[string][]byte`, `inboundSnapshots`,
   `closed_windows` time-keeping, the
   `mode = batch|window` switch.
4. **Output binding** — emitting `pmetric.Metrics` of the right
   typed variant and calling `nextConsumer.ConsumeMetrics`.

Concerns 1, 2, and 4 are host-specific. Concern 3 is the
runtime — host-neutral. The framework refactor extracts (3) so
host adapters provide (1, 2, 4) only.

The Rust side has the symmetric story: `apply_modified_otlp_delta_bytes`
and the per-accumulator `apply_proto_delta_bytes` methods in
`asap-query-engine/src/precompute_operators/*.rs` are the
*backend-side* runtime piece (delta-apply, snapshot cache).
On the agent side the Rust runtime doesn't yet exist because
all current ASAP agent deployments use the Go OTel Collector
binary; the OTAP Dataflow adapter (Phase 4) is what motivates
extracting it.

### 3.3 Layer 1: `asap_sketchlib` (Rust) + `sketchlib-go` (Go)

`asap_sketchlib` is the single Rust algorithm crate; every sketch
(`ddsketch`, `countmin`, `count`, `hll`, `kll`, `cms_heap`,
`hydra_kll`, `set_aggregator`, `delta_set_aggregator`) lives in
`asap_sketchlib/src/sketches/<name>.rs` alongside its wire-format
type and (where applicable) its `apply_delta` — mirroring
`sketchlib-go`'s per-sketch `delta.go` layout. The former
`sketch-core` wrapper is retired (see §8 Phase 0.5).

## 4. Target architecture — five layers

```
┌─────────────────────────────────────────────────────────┐
│  Layer 5  Control plane (controller, plan, OpAMP)        │
│           controller/, ASAPController repo, OpAMP server │
├─────────────────────────────────────────────────────────┤
│  Layer 4  Host adapter (per-platform thin shim)          │
│           asap-host-otelcol, -telegraf, -vector, -otap   │
├─────────────────────────────────────────────────────────┤
│  Layer 3  Operator runtime (windowing, delta, scheduler) │
│           asap-operator-go, asap-operator-rs             │
├─────────────────────────────────────────────────────────┤
│  Layer 2  Wire (envelope, versioning)                    │
│           asap_otel_proto::sketchlib::v1, sketchlib-go/proto│
├─────────────────────────────────────────────────────────┤
│  Layer 1  Sketch algorithms (one crate per language)     │
│           sketchlib-go (Go), asap_sketchlib (Rust)       │
└─────────────────────────────────────────────────────────┘
```

- **Layer 1** is host- and language-independent. Reused.
- **Layer 2** is host-independent today; this refactor only
  needs to ensure no OTel-specific tags leak into the envelope
  (e.g. `Metric.data` OneOf tag numbers are an OTel transport
  concern, not a sketch-wire concern).
- **Layer 3** is *new as a standalone artifact* — today's
  windowing/delta logic lives inside Go OTel processors and
  Rust ingest paths; this doc proposes promoting it to its own
  crate/package.
- **Layer 4** is *new* — host adapters call Layer 3 and convert
  to/from the host's native event model.
- **Layer 5** is mostly stable. The only change is that the
  controller's `generate_agent_config` outputs a host-neutral
  Operator config + a host selector, and per-host adapters
  format that into their native config (yaml, hcl, lua, ...).

## 5. Key abstractions

### 5.1 `asap-core::Sample`

The runtime's input is a stream of `Sample`. One canonical struct,
no inheritance, no host types:

```rust
pub struct Sample {
    pub timestamp_ms: u64,
    pub labels: SmallVec<[(StringId, StringId); 8]>, // interned key=value
    pub value: SampleValue,
    pub agg_id: u64,    // resolved upstream by host adapter from metric_name + plan
}

pub enum SampleValue {
    Float(f64),
    Hash(u64),     // for cardinality / topk inputs
    Bytes(Bytes),  // for opaque keys (set aggregator)
    SketchFrame(SketchFrame),  // already-aggregated input from upstream tier
}
```

The `agg_id` field is the join key with the controller's plan.
Today the plan-id resolution lives inside each Go OTel
processor's `seriesKey` / `attributesKey` helpers; in the
framework, host adapters do this lookup and tag the Sample.

### 5.2 `asap-core::Sketch`

```rust
pub trait Sketch {
    fn add(&mut self, sample: &Sample);
    fn merge(&mut self, other: &dyn Sketch) -> Result<(), Error>;
    fn snapshot(&self) -> SketchSnapshot;
    fn quantile(&self, q: f64) -> Option<f64>;
    fn sum(&self) -> f64;
    fn count(&self) -> u64;
    fn cardinality(&self) -> Option<f64>;
    // …
}
```

Each implementation lives in `sketchlib-go` / `asap_sketchlib`
already — this trait is a thin facade over what's there.

### 5.3 `asap-operator::Operator`

The state machine. Host-neutral:

```rust
pub trait Operator: Send {
    fn observe(&mut self, sample: Sample) -> Result<(), Error>;
    fn observe_frame(&mut self, frame: SketchFrame) -> Result<(), Error>;
    fn tick(&mut self, now_ms: u64) -> Result<Vec<SketchFrame>, Error>;
    fn snapshot(&self) -> OperatorSnapshot;       // for crash recovery
    fn restore(&mut self, snap: OperatorSnapshot) -> Result<(), Error>;
}
```

Internally the Operator owns:

- Per-`(agg_id, label_key)` series map of Sketch state.
- Window manager (tumbling / sliding / batch).
- Snapshot cache for delta encoding (matches today's
  `snapshots map[string][]byte` in
  `ddsketchprocessor/processor.go`).
- Inbound snapshot cache for delta apply (matches today's
  Rust `IngestState.sketch_snapshots` —
  [residual #2 in `PROGRESS.md`](../PROGRESS.md) calls out making
  this disk-backed).

Configured by:

```rust
pub struct OperatorConfig {
    pub sketch_type: SketchType,           // DDSketch / KLL / HLL / CMS / CountSketch
    pub mode: WindowMode,                  // Batch | Window | Sliding
    pub window: WindowSpec,                // size, slide, allowed_lateness
    pub label_matchers: Vec<LabelMatcher>,
    pub aggregate_by: Vec<String>,         // grouping keys
    pub transmit_sketch: bool,
    pub delta_transmission: bool,
    pub delta_threshold: u64,              // sketch-specific
    pub sketch_params: SketchParams,       // alpha, k, p, w, d, ...
}
```

This is **the same set of options the OTel processors take
today** — no new knobs, just relocated.

### 5.4 `asap-host::Host` trait

Each host adapter implements:

```rust
pub trait Host {
    type Event;     // pmetric.Metrics, telegraf.Metric, vector.Event, arrow.RecordBatch

    fn decode(&self, ev: Self::Event) -> Result<Vec<Sample>, Error>;
    fn encode(&self, frames: Vec<SketchFrame>) -> Result<Self::Event, Error>;
    fn schedule_tick(&self, period: Duration, callback: TickCallback);
    fn emit_telemetry(&self, stats: &OperatorStats);
}
```

The host wraps an Operator and a `nextConsumer`-equivalent.
Pseudocode for the OTel Collector adapter:

```go
func (p *otelHostAdapter) ConsumeMetrics(ctx, md pmetric.Metrics) error {
    samples := p.host.Decode(md)
    for _, s := range samples {
        p.op.Observe(s)
    }
    return p.next.ConsumeMetrics(ctx, md)  // pass-through (#211)
}

// timer goroutine:
frames := p.op.Tick(time.Now().UnixMilli())
out := p.host.Encode(frames)
p.next.ConsumeMetrics(ctx, out)
```

### 5.5 Wire format — `SketchFrame`

The on-the-wire envelope. Already exists as
`SketchEnvelope { sketch_state: oneof {...}, ... }` in
`sketchlib-go/proto/sketch_envelope` and the Rust mirror; this
doc proposes one rename + clarification:

```proto
message SketchFrame {
    string schema_version = 1;            // semver-string, not OTel proto version
    SketchType sketch_type = 2;
    string agg_id = 3;
    repeated KeyValue labels = 4;          // string-string, not OTel AnyValue
    Window window = 5;                     // [start_ms, end_ms)
    Encoding encoding = 6;                 // PROTO_FULL | PROTO_DELTA | MSGPACK
    bytes payload = 7;
    HashSpec hash_spec = 8;                // determinism contract
}
```

Important: `SketchFrame` is **transport-agnostic**. When carried
over OTLP today, it's wrapped in
`Metric.data = oneof { DDSketch | KLLSketch | ... }` — that's an
OTel-transport concern. When carried over Vector's `Event`, it'd
be a structured-log payload. Over Arrow / OTAP, a column. The
`SketchFrame` itself doesn't change.

### 5.6 Operator config — host-neutral form

Today's controller `generate_agent_config` emits OTel YAML. In
the framework, it emits:

```yaml
asap_operators:
  - id: agg-1
    sketch_type: ddsketch
    mode: window
    window: { size: 60s }
    matchers: [ "metric=http_requests_total" ]
    transmit_sketch: true
    delta: { enabled: true, threshold: 1 }
    sketch_params: { relative_accuracy: 0.01 }

  - id: agg-2
    sketch_type: hll
    mode: window
    window: { size: 60s }
    matchers: [ "metric=http_requests_total" ]
    transmit_sketch: true
    delta: { enabled: true }

asap_data_sink:
  kind: otlp
  endpoint: gateway:4317

asap_host: otel_collector  # | telegraf | vector | otap_dataflow
```

Each host adapter has a translator that lowers this into the
host's native config. For OTel Collector that produces today's
yaml. For Telegraf it's `[[aggregators.asap]]` blocks. For
Vector it's a `[transforms.asap]` block. For OTAP it's a
`[[processors]]` entry in the dataflow manifest.

## 6. Host adapters — concrete plans

### 6.1 OTel Collector (Go)

**Status:** today, `processor/{ddsketch,kll,hll,countsketch,countminsketch}processor` are the implementations. After Phase 1 (§8) each becomes a ~50-line shim:

```go
package ddsketchprocessor

import "github.com/ProjectASAP/asap-operator-go"

type processor struct {
    op asapoperator.Operator
    cfg *Config
    next consumer.Metrics
}

func (p *processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
    samples := otelhost.Decode(md, p.cfg.AggID)
    for _, s := range samples {
        if err := p.op.Observe(s); err != nil {
            return err
        }
    }
    return p.next.ConsumeMetrics(ctx, md)
}
```

The OCB builder manifest already compiles all five processors
into one binary (`builder-config.yaml`); after the refactor it
just gets thinner.

### 6.2 Telegraf (Go)

Telegraf's `processors.aggregator` plugin model is a near-1:1
match for `Operator`:

| Telegraf method | ASAP equivalent |
|---|---|
| `Add(in telegraf.Metric)` | `Operator.Observe(Sample)` |
| `Push(acc telegraf.Accumulator)` | `Operator.Tick(now)` → emit frames |
| `Reset()` | `Operator.tick` already swaps the active window |

Adapter package: `asap-host-telegraf`. Reuses the same
`asap-operator-go` runtime as the OTel adapter. Estimated 300
lines.

This is the **first non-OTel adapter** to land because Telegraf
is widely deployed at the edge and the plugin API is simple
enough to validate the abstraction without a heavy lift.

### 6.3 Vector (Rust)

Vector exposes `function_transform` and `task_transform` Rust
traits. ASAP becomes a `task_transform` (because we need an
internal scheduler / ticker, which `function_transform` doesn't
support):

```rust
impl TaskTransform<Event> for AsapTransform {
    fn transform(self: Box<Self>, input: BoxStream<Event>) -> BoxStream<Event> { … }
}
```

Reuses `asap-operator-rs`. The `Event → Sample` mapping uses
Vector's metric event variant (Vector has dedicated metric
events with name + tags + value, similar to Telegraf's metric
shape). Estimated 500 lines.

### 6.4 OTAP Dataflow (Rust)

OTAP Dataflow is Rust + Arrow. The processor node trait operates
on Arrow `RecordBatch`. The adapter benefits from columnar
batched input:

```rust
impl Processor for AsapNode {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        let samples = otap::decode_batch(&batch)?;
        for s in samples {
            self.op.observe(s)?;
        }
        // Output frames inline if any window closed:
        let frames = self.op.tick(now_ms())?;
        otap::encode_frames(&frames, &batch.schema())
    }
}
```

This is the **most architecturally interesting adapter** because
it lets the operator runtime exploit Arrow's columnar layout for
batched insertion paths (today's Go runtime processes one
DataPoint at a time inside `accumulateIntoWindow`). A future
Rust-side batched-add API on `Sketch` would multiply throughput
here.

### 6.5 Comparison table

| Platform | Lang | Adapter complexity | Throughput ceiling | Maturity (sketchlib-side) |
|---|---|---|---|---|
| OTel Collector | Go | 50 LoC (post-refactor) | OTel pdata | Reuses `sketchlib-go`, fully validated |
| Telegraf | Go | ~300 LoC | telegraf.Metric | Reuses `sketchlib-go` |
| Vector | Rust | ~500 LoC | vector.Event | Reuses `asap_sketchlib` |
| OTAP Dataflow | Rust | ~500 LoC, Arrow-aware | Arrow batch | Reuses `asap_sketchlib`; opens batched-add path |

## 7. Control plane

OpAMP is OTel-Collector-specific. For multi-host deployments the
controller needs an alternative.

Three reasonable channels, ranked by how mature they are in the
existing ASAP codebase:

1. **OpAMP** (existing, OTel only). `controller/src/opamp/mod.rs`.
   Continue to use for OTel Collector hosts.
2. **HTTP poll** (simple, host-agnostic). Each host adapter
   periodically `GET /api/v1/plan?host_id=<id>` against the
   controller, applies any delta. Telegraf's `[[inputs.http]]`
   already supports this kind of pull; Vector and OTAP need
   ~50 lines to wire.
3. **File watch + asap-control-sidecar** (most decoupled). A tiny
   sidecar binary tails an HTTP/OpAMP feed and rewrites a local
   file the host watches. Host adapter only needs file-watch
   support, which all of OTel/Telegraf/Vector/OTAP already have.

Recommendation: start with HTTP-poll for non-OpAMP hosts, since
the controller already exposes the plan via REST
(`GET /api/v1/plan`). Sidecar mode is a hedge if a particular
host can't poll for some reason.

## 8. Migration plan

Phased so each phase is independently shippable and reverts to
the prior behavior if rolled back.

### Phase 0 — Lock the design (this doc)

- This doc + ADR.
- Sample / SketchFrame / Operator API frozen.
- No code changes.
- **Exit criterion:** doc reviewed + ADR opened.

### Phase 0.5 — Retire `sketch-core` — DONE

Done in `asap_sketchlib#36` + `ASAPQuery-backend#73` +
`ASAPQuery#307`. `sketch-core` is gone; every sketch (algorithm
type, wire-format type, and `apply_delta` where applicable) now
lives in `asap_sketchlib/src/sketches/<name>.rs`. The former
`*_sketchlib.rs` FFI helpers were inlined into the same file
(e.g. `SketchlibCms` lives directly in `countmin.rs`); new
sibling files were added where no existing home existed
(`hydra_kll.rs`, `set_aggregator.rs`, `delta_set_aggregator.rs`).
Backends (`ASAPQuery-backend`, `ASAPQuery`, `sketchlib-bench`)
depend on `asap_sketchlib` directly; the three on-disk
`sketch-core` copies are deleted.

### Phase 1 — Extract the Go runtime

- New repo or directory: `asap-operator-go`.
- Move from each `processor/*processor/processor.go`:
  `accumulateIntoWindow`, `flushWindow`, snapshot caches, `closed_windows`
  scheduling, label-matcher logic.
- Each existing processor reduces to its OTel-specific shim
  (decode `pmetric.Metrics` → `[]Sample`, encode
  `[]SketchFrame` → `pmetric.Metrics`).
- **Crucially: no behavior change.** Same wire format, same
  config knobs, same window semantics. The b3-delta e2e
  (#210/#211/ASAPQuery-backend#71) must still produce the
  observed `19.49` value at offset `−90s`.
- **Exit criterion:** existing test suite + `accuracy_reduce.py` /
  P9 plot show identical numbers pre/post-extraction.

### Phase 2 — Extract the Rust runtime

- New crate `asap-operator-rs` in `asap-common/crates/`.
- Move from `asap-query-engine/src/precompute_operators/*`:
  `apply_proto_delta_bytes`, the snapshot cache infrastructure,
  the per-accumulator `from_*_bytes` constructors that today
  live across `dd_sketch_accumulator.rs` /
  `count_min_sketch_accumulator.rs` / etc.
- Backend ingest path becomes a thin host adapter calling the
  same crate.
- **Exit criterion:** backend e2e unchanged; no PromQL output
  drift.

### Phase 3 — Telegraf adapter

- New repo / directory: `asap-host-telegraf`.
- Reuses `asap-operator-go`.
- Smoke test: route the same fake-exporter raw stream through
  Telegraf instead of OTel Collector; confirm warm-tier query
  returns the same value within the sketch's `AccuracyEnvelope`.
- **Exit criterion:** P8 accuracy reducer runs against
  Telegraf-emitted data; per-row error matches OTel-emitted.

### Phase 4 — OTAP Dataflow adapter

- New crate / directory: `asap-host-otap`.
- Reuses `asap-operator-rs`.
- Validates Arrow-batched ingestion path.
- Opportunity to add a batched `Sketch::add_batch(samples)` API
  and benchmark vs scalar `add(sample)` loop.
- **Exit criterion:** OTAP Dataflow processor node passes the
  same accuracy gate.

### Phase 5 — Vector adapter

- New crate: `asap-host-vector`.
- Reuses `asap-operator-rs`.
- Rounds out the reach to all four edge platforms.

### Phase 6 — Repo restructure (optional, breaking)

If the project decides to commit to "ASAP is a framework, not a
collector":

```
asap/                        (rename ASAPCollector → asap)
├── operator-go/             (Phase 1)
├── operator-rs/             (Phase 2)
├── host-otelcol/            (existing, refactored Phase 1)
├── host-telegraf/           (Phase 3)
├── host-vector/             (Phase 5)
├── host-otap/               (Phase 4)
├── controller/              (today's controller/, unchanged)
├── deploy/                  (unchanged)
└── docs/
```

`ASAPController` and `ASAPQuery-backend` stay separate repos.

Phase 6 is *optional* — Phases 1–5 deliver the technical
benefit without renaming anything. The rename is a marketing /
clarity decision that can lag the engineering by months without
cost.

## 9. Risks and mitigations

### R1 — Sketch algorithm divergence between Go and Rust

**Risk:** `sketchlib-go` (Go) and `asap_sketchlib` (Rust) are
two implementations of the same algorithms. We've already had
wire mismatches —
[ASAPCollector#210](https://github.com/ProjectASAP/ASAPCollector/pull/210)
caught a typed-encoding mismatch in DDSketch delta. Adding more
host adapters in more languages multiplies this risk.

**Mitigation:**

- Cross-language byte-corpus test: a fixed set of input streams
  → expected `SketchFrame.payload` bytes, run by both runtimes
  in CI.
- `HashSpec` field in the envelope (already in
  `sketchlib-go::common::storage::HashSpec`) — if hash seeds
  diverge, the cross-test fails.
- Limit to two reference implementations long-term:
  `sketchlib-go` (Go) and `asap_sketchlib` (Rust). Vector + OTAP
  both use Rust; OTel Collector + Telegraf both use Go. No
  third language allowed.
- **Reduced as of `asap_sketchlib#36`** — `sketch-core` is gone,
  so the Rust side has exactly one algorithm crate. What's left
  is establishing per-sketch parity tests between `sketchlib-go`
  and `asap_sketchlib` (still future work).

### R2 — Performance regression in Phase 1

**Risk:** extracting the runtime out of the OTel processor
introduces an extra function call per sample / per tick. At 1k
cardinality × 60s emit cadence, the loop matters.

**Mitigation:**

- The current processor inlines `accumulateIntoWindow` /
  `flushWindow`; extraction must use generics or trait dispatch
  with `inline` hints.
- Benchmark: per-sample observe latency p99 must stay within
  10% of the pre-refactor value. Ship Phase 1 only if that
  holds.

### R3 — Controller already emits OTel-specific YAML

**Risk:** `generate_agent_config` builds a yaml string that the
collector parses. Telegraf / Vector / OTAP can't read that yaml.

**Mitigation:**

- Phase 1 changes nothing on the controller side — OTel-yaml
  still works.
- Phase 3+ introduces a host-neutral `OperatorConfig` JSON the
  controller emits; per-host adapters lower it to native config.
  The collector keeps reading OTel yaml; new hosts read the
  neutral form.

### R4 — Two delta implementations diverging

**Risk:** Phase 2 extracts Rust delta apply. If the extracted
runtime's `apply_proto_delta_bytes` semantically differs from
today's per-accumulator versions, every windowed sketch on the
backend miscomputes.

**Mitigation:**

- Test corpus from R1 is the same regression catcher.
- Phase 2 is a pure code move; the entry point stays bit-identical.

### R5 — OpAMP is the only control channel today

**Risk:** non-OTel hosts can't subscribe to OpAMP (it's OTel-specific).

**Mitigation:** §7 — start with HTTP-poll for non-OTel hosts.
The controller already exposes the plan via REST; new hosts add
~30 lines to poll it.

## 10. Open questions

1. **`asap-operator-go` repo or directory?**
   - Option A: separate repo `github.com/ProjectASAP/asap-operator-go`.
     Cleanest, but adds a Go-module dependency for ASAPCollector.
   - Option B: subdirectory in this repo. Simpler for now, harder
     to reuse from a hypothetical future repo.
   - **Recommendation:** Option B, with Option A as a future
     promotion if Telegraf adapter wants to live elsewhere.

2. **Do we need a Rust sketch trait abstraction at Layer 1 too?**
   - Today `Sketch` is implicit (each accumulator is its own
     type). A `trait Sketch` would let Layer 3 be generic over
     sketch type. Cost: a vtable call per `add`. Benefit:
     Operator becomes ~1/5 the size.
   - **Recommendation:** measure first; don't speculate.

3. **Does the controller plan need to specify the host?**
   - Today the controller's plan is implicitly OTel-Collector-shaped.
   - If a deploy mixes hosts (OTel agents alongside Telegraf
     agents) the plan must say which sketch goes to which host.
   - **Recommendation:** add `host: { otel | telegraf | vector | otap }`
     to `AgentCollectorConfig`; plumb through in Phase 3.

4. **Operator-side cold-tier?**
   - Backend has a cold-store fallback (#69 P1). Should host
     adapters also be able to write raw samples to a cold tier
     (similar to today's `raw_tee.go` in fake-exporter)?
   - **Recommendation:** out of scope for the framework; the
     workload generator's responsibility, not the operator's.

5. **Versioning policy for `SketchFrame.schema_version`?**
   - Semver (`1.0`, `1.1`)? Just a monotonic int? The
     [persist-format-versioning tests](https://github.com/ProjectASAP/ASAPQuery-backend/pull/65)
     use `PERSIST_FORMAT_VERSION + 1` — same approach for wire?
   - **Recommendation:** monotonic `u32`, same forward-compat
     contract as the persist tests (load → fall back +
     warn → operator continues at the older version it knows).

## 11. Out of scope

- The **planner** (sketch placement decisions) stays in the
  controller. ASAP-Edge is the *executor* of the plan, not the
  decider.
- The **query backend** (sketchDB, ASAPQuery-backend) stays
  separate. Only its OTLP-receiving ingest path counts as a
  "host adapter" by this framework's definition; the rest is
  query/storage layer.
- **Multi-tenancy / authn / authz** — orthogonal.
- **Replacing OTLP** — OTLP is one transport. Operator output is
  `SketchFrame`, which can ride OTLP, raw HTTP/JSON, Kafka,
  Arrow Flight, anything. Choosing transport is a deploy
  decision per host adapter.

## 12. Proposed deliverable layout

End-state of Phase 1–5 (without the optional Phase 6 rename;
Phase 0.5 already landed):

```
ASAPCollector/
├── asap-operator-go/                     # NEW (Phase 1)
│   ├── go.mod
│   ├── operator.go                        # trait + impl
│   ├── window.go                          # tumbling/sliding/batch logic
│   ├── snapshot_cache.go                  # delta encode/apply
│   ├── matchers.go                        # label_matchers / aggregate_by
│   └── config.go                          # host-neutral config
├── asap-operator-rs/                      # NEW (Phase 2)
│   ├── Cargo.toml
│   └── src/{operator,window,snapshot}.rs
├── asap-host-telegraf/                    # NEW (Phase 3)
├── asap-host-vector/                      # NEW (Phase 5, Rust)
├── asap-host-otap/                        # NEW (Phase 4, Rust)
├── opentelemetry-collector-contrib-patch/
│   └── processor/
│       ├── ddsketchprocessor/             # Phase 1: thin shim ~50 LoC
│       ├── kllprocessor/                  # Phase 1: thin shim
│       ├── hllprocessor/                  # Phase 1: thin shim
│       ├── countsketchprocessor/          # Phase 1: thin shim
│       ├── countminsketchprocessor/       # Phase 1: thin shim
│       ├── countminsketchmergeprocessor/  # legacy, deprecated
│       └── countsketchmergeprocessor/     # legacy, deprecated
├── controller/                            # unchanged through Phase 2
├── deploy/                                # unchanged
├── docs/
│   ├── design-asap-edge-framework.md      # this doc
│   ├── adr-0001-retire-sketch-core.md     # ADR for Phase 0.5
│   ├── adr-0002-extract-runtime.md        # ADR for Phase 1
│   ├── adr-0003-host-adapter-trait.md     # ADR for §5.4
│   └── … (existing docs)
└── … (submodules)
```

Out-of-tree state in sibling repos (post Phase 0.5):

```
asap_sketchlib/                            # single Rust algorithm crate
└── src/sketches/
    ├── ddsketch.rs                        # DDSketch + DdSketch + DdSketchDelta + apply_delta
    ├── count.rs                           # Count + CountSketch + CountSketchDelta + apply_delta
    ├── countmin.rs                        # CountMin + CountMinSketch + CountMinDelta + SketchlibCms (FFI inlined)
    ├── hll.rs                             # HLL + HllSketch + HllSketchDelta + HllVariant + apply_delta
    ├── kll.rs                             # KLL + KllSketch + KllSketchData
    ├── cms_heap.rs                        # CMSHeap + CountMinSketchWithHeap + CmsHeapItem
    ├── hydra_kll.rs
    ├── set_aggregator.rs
    └── delta_set_aggregator.rs

ASAPQuery-backend/, ASAPQuery/, sketchlib-bench/
└── all depend on asap_sketchlib directly; the three sketch-core copies are deleted.
```

## 13. Decision-required to proceed

This doc is gating the host-adapter work. Specifically:

- [ ] Approve §5 abstractions (Sample, Sketch, Operator,
      OperatorConfig, SketchFrame, Host).
- [ ] Approve §8 phasing (Phase 0 = this doc; Phases 1–6 =
      runtime extraction + host adapters; Phase 0.5 already
      landed).
- [ ] Pick R1 mitigation strategy: cross-language test corpus —
      who owns the corpus, where does it live (sketchlib-bench
      seems like the right home).
- [ ] Decide repo layout for `asap-operator-go` and
      `asap-operator-rs`: separate repos vs. subdirectories
      (Open Q1).
- [ ] Decide control-plane channel for non-OTel hosts: HTTP poll
      vs. sidecar (§7).

Once those land, Phase 1 is ~2 weeks of focused work (extract
runtime + shim five OTel processors), and Phase 3 (Telegraf) is
~1 week after that.

## Appendix A — Concrete mapping table

How today's code maps onto the proposed layers, for orientation
during implementation:

| Today | Layer | Becomes |
|---|---|---|
| `sketchlib-go/sketches/DDSketch/DDSketch.go` | 1 | unchanged |
| `sketchlib-go/sketches/DDSketch/delta.go` `ComputeDelta`, `ApplyDelta` | 1 | unchanged |
| `asap_sketchlib/src/sketches/{ddsketch,countmin,count,hll}.rs::apply_delta` | 1 | unchanged (Phase 0.5 done) |
| `asap_otel_proto::sketchlib::v1::DdSketchDelta` | 2 | unchanged |
| `sketchlib-go/proto/sketch_envelope/SketchEnvelope` | 2 | renamed `SketchFrame` (see §5.5) |
| `processor/ddsketchprocessor/processor.go::accumulateIntoWindow` | 3 | moves to `asap-operator-go::operator.go::observe` |
| `processor/ddsketchprocessor/processor.go::flushWindow` | 3 | moves to `asap-operator-go::operator.go::tick` |
| `processor/ddsketchprocessor/processor.go::snapshots` | 3 | moves to `asap-operator-go::snapshot_cache.go` |
| `processor/ddsketchprocessor/processor.go::ConsumeMetrics` | 4 | stays in `processor/ddsketchprocessor/`, becomes shim |
| `processor/ddsketchprocessor/processor.go::computeDDSketchDelta` | 3 | moves to `asap-operator-go::snapshot_cache.go::compute_delta` |
| `apply_modified_otlp_delta_bytes` (Rust, `drivers/ingest/otel.rs`) | 3 | moves to `asap-operator-rs::operator.rs::observe_frame` |
| `controller/src/config/agent.rs::generate_agent_config` | 4/5 | adds host-neutral `OperatorConfig` output mode (§5.6) |
| `controller/src/opamp/mod.rs` | 5 | unchanged for OTel; new HTTP-poll endpoint for non-OTel hosts |

## Appendix B — Why now

Three reasons this is the right time, not in 6 months:

1. **The runtime just got correct.** #210 + #211 + #71
   represent the first time the agent → wire → backend loop
   produces real PromQL answers with deltas. Extracting the
   runtime now means the test harness already exists to
   regression-check the extraction.

2. **OTAP Dataflow is shipping.** Once OTAP is the recommended
   OTel agent, the cost of being "an OTel Collector mod" rises.
   Better to be a runtime that works with both old (Go OTel) and
   new (Rust OTAP) than a fork-of-collector that has to be
   re-forked.

3. **External users are asking.** The thread that spawned this
   doc (Telegraf, Vector, OTAP fits) shows the question is in
   the air. If the answer is "it's coupled, but we have a plan",
   the plan should be writable today, not invented in a panic
   later.
