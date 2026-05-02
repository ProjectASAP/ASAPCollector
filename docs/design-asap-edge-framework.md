# ASAP Edge Precompute Framework — Design

_Status: **draft** — 2026-05-01. Forward-looking; gates the adapter
work tracked in [`PROGRESS.md`](../PROGRESS.md)._

## 1. Motivation

The repo is named `ASAPCollector`, the binary is `sketchcollector`,
and the bulk of the code today lives under
`opentelemetry-collector-contrib-patch/`. That framing misleads
contributors and users alike: the reusable artifact isn't "an OTel
Collector with sketch processors", it's an *edge precompute
runtime* — a state machine that observes per-event samples,
maintains windowed sketch state, transmits compact summaries on a
schedule, and applies inbound deltas against cached snapshots.
The OTel Collector is *one host* for that runtime.

The same runtime should fit into any modern edge data plane —
OTel Collector, Telegraf, Vector, or OpenTelemetry's
[OTAP Dataflow](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/README.md)
— but today the runtime is fused into Go OTel `processor.Processor`
implementations, so none of those are reachable without a
fork-and-rewrite. This doc defines the framework, the wire
contract, the per-platform encoding choices, and a phased
migration that doesn't break the working e2e (b3-delta with #210
+ #211 + ASAPQuery-backend#71).

## 2. Non-goals

- Replacing the controller, query backend, or storage layer.
- Reimplementing sketches in every host language. Algorithms stay
  in `sketchlib-go` (Go) and `asap_sketchlib` (Rust); host
  adapters call into those.
- Forking pdata / Arrow / Telegraf / Vector data models — every
  adapter converts to/from its native model at the edge; the
  runtime never sees host types.
- Multi-tenancy, AuthN/AuthZ, network/QoS — orthogonal.

## 3. Architecture — five layers, one narrow waist

```
┌──────────────────────────────────────────────────────────────┐
│  Layer 5  Control plane (controller, plan, ControlChannel)    │
│           controller/, ASAPController repo, OpAMP / HTTP-poll │
├──────────────────────────────────────────────────────────────┤
│  Layer 4  Adapter (per-platform thin shim)                    │
│           asap-otelcol, asap-telegraf, asap-vector, asap-otap │
├──────────────────────────────────────────────────────────────┤
│  Layer 3  Precompute runtime (windowing, delta, scheduler)    │
│           asap-precompute-go, asap-precompute-rs              │
├──────────────────────────────────────────────────────────────┤
│  Layer 2  Wire envelope                                       │
│           asap_sketchlib::proto::sketchlib (Rust),            │
│           sketchlib-go/proto (Go)                             │
├──────────────────────────────────────────────────────────────┤
│  Layer 1  Sketch algorithms                                   │
│           sketchlib-go (Go), asap_sketchlib (Rust)            │
└──────────────────────────────────────────────────────────────┘
```

**Narrow waist**: every adapter decodes its native event into the
host-neutral `Observation` struct, hands it to the runtime, and
encodes runtime-emitted `SketchEnvelope`s back to its native
event. The runtime sees only `Observation` and `SketchEnvelope`;
adapters see only the host's types and those two. That's the
entire cross-platform contract.

```
host event ─► Adapter::decode ─► Observation ─► Precompute::observe
                                                       │
                                                       ▼ Precompute::tick
host event ◄─ Adapter::encode ◄─ SketchEnvelope ◄──────┘
```

**What's already factored** — Layer 1 (algorithms in
`sketchlib-go` / `asap_sketchlib`) and Layer 2 (`SketchEnvelope`
proto, byte-identical across languages) are host-independent
today and reused as-is. **What's coupled** — Layer 3 lives inside
each Go OTel `processor.Processor` and inside the Rust ingest
path (`apply_modified_otlp_delta_bytes`, the per-accumulator
delta apply); this is what gets extracted. **What's new** —
Layer 4 (adapter shims) and the `ControlChannel` abstraction in
Layer 5.

## 4. Layer 1 — Sketch algorithms

Two algorithm crates, one per language, each ships its full set
of sketches plus their wire-format types and `apply_delta` where
applicable:

- `sketchlib-go/sketches/{DDSketch,KLL,HLL,CountSketch,CountMinSketch}`
- `asap_sketchlib/src/sketches/{ddsketch,countmin,count,hll,kll,cms_heap,hydra_kll,set_aggregator,delta_set_aggregator}.rs`

The Rust side exposes a small trait family so Layer 3 can be
generic over sketch type and Layer 4 can pick the right query
verb at compile time:

```rust
pub trait Sketch: Send {
    fn observe(&mut self, obs: &Observation);
    fn merge(&mut self, other: &Self) -> Result<(), Error>;
    fn snapshot(&self) -> SketchEnvelope;
    fn count(&self) -> u64;
}

pub trait QuantileSketch: Sketch     { fn quantile(&self, q: f64) -> f64; }
pub trait CardinalitySketch: Sketch  { fn cardinality(&self) -> f64; }
pub trait FrequencySketch: Sketch    { fn estimate(&self, key: &str) -> f64; }
```

DDSketch / KLL implement `QuantileSketch`; HLL implements
`CardinalitySketch`; CountMin / CountSketch / CMSHeap implement
`FrequencySketch`. Each sketch only exposes the queries it can
actually answer.

The Go side mirrors this with idiomatic naming
(`Sketch.Observe`, `QuantileSketch.Quantile`, etc.).

## 5. Layer 2 — Wire envelope and the bandwidth invariant

### 5.1 `SketchEnvelope`

```protobuf
message SketchEnvelope {
    uint32     schema_version = 1;     // monotonic, forward-compat
    SketchType sketch_type    = 2;     // DDSketch / KLL / HLL / CMS / CountSketch
    uint64     agg_id         = 3;     // join key with controller plan
    repeated KeyValue labels  = 4;     // string-string, host-neutral
    Window     window         = 5;     // [start_ms, end_ms)
    Encoding   encoding       = 6;     // PROTO_FULL | PROTO_DELTA | MSGPACK
    bytes      payload        = 7;     // sketch state or delta bytes
    HashSpec   hash_spec      = 8;     // determinism contract
}
```

Lives in `asap_sketchlib::proto::sketchlib::*` (Rust) and
`sketchlib-go/proto/sketch_envelope` (Go). The state proto types
(`DDSketchState`, `KllState`, …) live in `asap_sketchlib`
already; the delta proto types (`DDSketchDelta`, `HllDelta`, …)
currently live in the vendored `asap_otel_proto::sketchlib::v1`
under `ASAPQuery-backend/asap-common/dependencies/rs/`. Folding
the deltas back into `asap_sketchlib::proto::sketchlib` is a
small follow-up cleanup so state and delta share a home; it's a
file move with no semantic change.

`SketchEnvelope.payload` (the proto-encoded sketch state or
delta) is what actually travels across the wire. How the payload
is *carried* by each platform varies (§7.2 covers this in detail
with per-platform tables): Strategy-A platforms (OTel today, OTAP
maybe later) wrap it in a typed sketch oneOf in their native
event schema; Strategy-B platforms (Telegraf, Vector, OTAP today,
all future hosts) wrap it in the platform's nearest binary-clean
field carrier (a string-typed Telegraf field, a `Value::Bytes`
log field on Vector, an OTAP attribute) with the well-known
`_asap_envelope` key alongside metadata keys identifying it as
ASAP. The payload bytes themselves are identical across all
strategies — adapters only differ in how they wrap and unwrap.

### 5.2 The bandwidth invariant

The whole point of edge precompute is that the wire carries
sub-linear sketches instead of raw samples. That saving has to
survive every hop in the pipeline (agent → optional gateway →
backend → cold store) and any third-party middleware in the
path. The framework imposes a hard contract:

> **From the moment the controller's plan selects a sketch type
> for an `agg_id`, until the data is reconstructed into a query
> result on the backend or frozen into the cold store, the data
> flows in `SketchEnvelope` byte form. No ASAP node — adapter,
> runtime, or storage layer — is allowed to "explode" the
> envelope back to per-sample observations and re-aggregate
> downstream.**

Five enforcement points:

1. **`Adapter::encode`** wraps `SketchEnvelope.payload` into the
   platform's typed sketch carrier (§7 Strategy A) or into a
   binary field with the well-known markers (§7 Strategy B).
   It must NOT serialize sketch state as a list of scalar data
   points.
2. **`Adapter::decode`** recognizes incoming sketch-typed events
   (or events carrying `_asap_envelope` per Strategy B) and
   produces `Observation::Envelope(SketchEnvelope)` — never a
   list of `Observation::Float`.
3. **`Precompute::observe_envelope`** is the only path that
   accepts inbound sketches; it does `merge` or `apply_delta`
   (sketch in → sketch out). There is no inverse "envelope →
   samples" anywhere in the runtime.
4. **`schema_version` / `sketch_type` / `hash_spec`** mismatches
   are hard errors at decode time. No silent fall-through to a
   raw-sample path.
5. **Capability-miss / cold-store** stores envelope bytes
   verbatim. Even when the receiver has no plan for an incoming
   `agg_id`, the bytes go to disk as-is rather than being
   rehydrated.

Multi-hop chains preserve the invariant the same way: gateway's
`decode` produces `Envelope(...)`, runtime's `observe_envelope`
runs merge or apply_delta, gateway's `encode` re-wraps the merged
envelope. Wire between every pair of hops carries
`SketchEnvelope` bytes, not expanded scalars.

CI signal: for any adapter, the byte-count ratio of
`encode(envelopes)` output vs. raw-sample output should match the
sketch's expected compression ratio (e.g., 100×–1000× for
DDSketch on a typical distribution). A surprise drop means an
adapter is leaking samples.

## 6. Layer 3 — Precompute runtime

The host-neutral state machine. Each `Precompute` instance owns
one sketch type; multiple sketch types in a deployment = multiple
`Precompute` instances side-by-side.

### 6.1 Input — `Observation`

```rust
pub struct Observation<'a> {
    pub timestamp_ms: u64,
    pub metric:       &'a str,
    pub labels:       &'a [(&'a str, &'a str)],
    pub value:        ObservationValue<'a>,
}

pub enum ObservationValue<'a> {
    Float(f64),
    Hash(u64),                         // cardinality / topk inputs
    Bytes(&'a [u8]),                   // opaque keys (set aggregator)
    Envelope(SketchEnvelope),          // pre-aggregated input from upstream
}
```

Adapters emit raw `Observation`s. Resolving a metric to an
`agg_id` (the join key with the plan) is the runtime's job, done
internally via the matchers in `PrecomputeConfig` — adapters
don't need plan-aware lookup logic.

### 6.2 Trait

```rust
pub trait Precompute: Send {
    type SketchT: Sketch;

    fn observe(&mut self, obs: &Observation) -> Result<(), Overflow>;
    fn observe_envelope(&mut self, env: SketchEnvelope) -> Result<(), Error>;
    fn tick(&mut self, now_ms: u64) -> Vec<SketchEnvelope>;
}

pub enum Overflow {
    SeriesCapExceeded,    // see PrecomputeConfig::max_series
    LateData,             // sample timestamp outside allowed_lateness
}
```

Internally a `Precompute` owns a per-`(agg_id, label_key)` series
map, a window manager (tumbling / sliding / batch), an outbound
snapshot cache for delta encoding, and an inbound snapshot cache
for delta apply.

Crash recovery is intentionally out of the trait. Today's
collector is stateless across restarts; the backend persists via
`SimpleMapStore`. A future `PersistentPrecompute: Precompute`
trait can add `snapshot()` / `restore()` later without breaking
this interface.

### 6.3 Configuration

```rust
pub struct PrecomputeConfig {
    pub agg_id:              AggId,
    pub sketch_type:         SketchType,
    pub mode:                AggregationMode,    // Tumbling | Sliding | Batch
    pub window:              WindowSpec,
    pub matchers:            Vec<LabelMatcher>,
    pub aggregate_by:        Vec<String>,
    pub transmit_sketch:     bool,
    pub delta_transmission:  bool,
    pub delta_threshold:     u64,
    pub sketch_params:       SketchParams,       // alpha, k, p, w, d, ...
    pub max_series:          usize,
    pub on_overflow:         OnOverflow,         // Drop | Block | EvictOldest
}
```

The first ten fields are exactly what today's OTel processors
take — relocated, not new. The last two are necessary because
extracting the runtime out of the OTel pipeline removes the
implicit channel-based backpressure.

The controller emits this config in a host-neutral YAML/JSON form
(below); each adapter has a translator that lowers it to the
host's native config (OTel YAML, Telegraf TOML, Vector VRL, OTAP
manifest):

```yaml
asap_precompute:
  - agg_id: 1
    sketch_type: ddsketch
    mode: tumbling
    window: { size: 60s }
    matchers: [ "metric=http_requests_total" ]
    transmit_sketch: true
    delta: { enabled: true, threshold: 1 }
    sketch_params: { relative_accuracy: 0.01 }
    max_series: 100000
    on_overflow: evict_oldest

asap_data_sink:
  kind: otlp
  endpoint: gateway:4317

asap_adapter: otelcol      # | telegraf | vector | otap
```

## 7. Layer 4 — Adapters and per-platform encoding

### 7.1 The `Adapter` trait

```rust
pub trait Adapter {
    type Event;     // pmetric.Metrics, telegraf.Metric, vector.Event, arrow.RecordBatch

    fn decode<'a>(&self, ev: &'a Self::Event) -> Result<Vec<Observation<'a>>, Error>;
    fn encode(&self, envelopes: Vec<SketchEnvelope>) -> Result<Self::Event, Error>;
    fn schedule_tick(&self, period: Duration, cb: TickCallback);
    fn emit_telemetry(&self, stats: &PrecomputeStats);
}
```

Pseudocode for the OTel adapter (every adapter follows this
shape):

```go
func (p *otelAdapter) ConsumeMetrics(ctx, md pmetric.Metrics) error {
    obs := p.adapter.Decode(md)
    for _, o := range obs { p.precompute.Observe(o) }
    return p.next.ConsumeMetrics(ctx, md)        // pass-through (#211)
}
// timer goroutine:
envelopes := p.precompute.Tick(now)
out := p.adapter.Encode(envelopes)
p.next.ConsumeMetrics(ctx, out)
```

### 7.2 Two encoding strategies

`SketchEnvelope` is the canonical Layer 2 wire format, but only
OTel's `pmetric` schema has been extended with typed sketch
variants (modified-OTLP). For every other platform, the adapter
fits the envelope into a schema that wasn't designed for
sketches. Two strategies, picked per platform:

**Strategy A — Native typed variant.** Extend the platform's
schema with a sketch-typed oneOf / variant. The native event
carries the sketch as itself.

```protobuf
// modified-OTLP (already shipped):
message Metric {
  oneof data {
    Gauge      gauge        = 5;
    Sum        sum          = 7;
    Histogram  histogram    = 9;
    DDSketch   ddsketch     = 13;     // ← ASAP extension
    KLLSketch  kll          = 14;
    HLLSketch  hll          = 15;
    CountSketch    count_sketch = 16;
    CountMinSketch cms          = 17;
  }
}
```

Pro: type-safe at platform level, native tooling recognizes
sketches, smallest serialization overhead. Con: requires forking
the platform schema or upstream PR. Realistic only for
OTel-family platforms.

**Strategy B — Opaque bytes in the platform's nearest
binary-clean carrier.** Don't touch the platform schema. Use
whatever the native event already exposes that can carry 8-bit-
clean bytes, with **standardized key names** so receivers can
identify the payload as a SketchEnvelope. The keys are
project-level standardized; how each platform realizes them is
adapter-specific (and, as the per-platform feasibility audits
showed, varies more than expected).

The standardized keys (every Strategy-B adapter uses exactly
these spellings, case-sensitive, prefix included):

| Key | Logical type | Required | Meaning |
|---|---|---|---|
| `_asap_envelope` | bytes | yes | `SketchEnvelope.payload` proto bytes |
| `_asap_sketch_type` | string | yes | `"DDSketch"` / `"KLLSketch"` / … |
| `_asap_agg_id` | uint64 | yes | matches controller plan |
| `_asap_schema_version` | uint32 | yes | matches `SketchEnvelope.schema_version` |
| `_asap_window_start_ms` | uint64 | yes | window lower bound |
| `_asap_window_end_ms` | uint64 | yes | window upper bound |
| `_asap_encoding` | string | optional | `PROTO_FULL` (default) / `PROTO_DELTA` / `MSGPACK` |

Per-platform realization of these keys (verified against current
upstream source):

| Platform | Carrier for `_asap_envelope` | Notes |
|---|---|---|
| **Telegraf** | A `string`-typed field on `telegraf.Metric` holding 8-bit-clean envelope bytes (i.e. `string(envelopeBytes)`). | `telegraf.Metric` has no `[]byte` field type — `metric/metric.go::convertField()` coerces `[]byte` → `string`. Bytes are not lost (Go strings are 8-bit-clean) but type tag is. **A custom `asap` serializer is required** for HTTP / Kafka / File sinks; InfluxDB line-protocol and Prometheus remote-write are not supported (would force base64 +33%). |
| **Vector** | A `Value::Bytes(Bytes)` field on a `LogEvent`. | NOT `MetricValue::Sketch` — that variant is hard-typed to `AgentDDSketch` and won't carry our payload. Required sinks: `vector` native protocol (Protobuf), Kafka with `encoding=native\|raw_message`, S3 with `encoding=native`. JSON-shaped sinks (Elasticsearch, Loki, Splunk default) corrupt non-UTF-8 bytes — base64 with +33% if those are required. |
| **OTAP Dataflow** | A Bytes-typed value (`AttributeValueType::Bytes`) on the per-row attribute child batch of an `OtapArrowRecords`. **Not** a sibling top-level Binary column — `crates/pdata/src/schema/payloads.rs::check_match` returns `Error::ExtraneousField` for any extension column on Logs/Metrics/Traces RecordBatches. Encoding as an attribute is OTLP-round-trippable (`AnyValue.bytes_value` exists). |
| **Future platforms** (Fluent Bit, Beats, …) | Whatever the native binary-clean field is. | If the platform's native event has no binary-clean field at all (rare), base64 in a string field is the universal fallback at +33%. |

Pro of Strategy B: no schema fork; works on any platform with a
binary-clean carrier; third-party intermediaries (Kafka brokers,
HTTP LBs) pass it through unchanged. Con: native tooling sees
opaque bytes (can't introspect); ~2–5% overhead from key names;
per-platform carrier choice is real engineering, not a uniform
recipe.

Regardless of strategy, `SketchEnvelope.payload` bytes are
identical. Strategy A wraps them in a typed oneOf; Strategy B
wraps them in the platform's binary-clean carrier with the
standardized keys above. Multi-hop chains where each hop uses a
different strategy are fine — bytes-in = bytes-out at every hop
as long as decode/encode are pure inverses.

### 7.3 Per-platform integration

LoC estimates below are post-feasibility-audit (verified against
each platform's current upstream source); they include the
adapter package itself, the controller-pull goroutine/task (every
non-OTel adapter ships its own — see §7.5), the platform-specific
serializer/encoder, and basic tests. Original "casual" estimates
in earlier drafts of this doc undercounted by 25–65%.

| Platform | Strategy | Adapter LoC | Native event | Notes |
|---|---|---|---|---|
| **OTel Collector** (`asap-otelcol`, Go) | A — modified-OTLP `Metric.data` oneOf | ~50 (post-extract) | `pmetric.Metrics` | Already shipped (#206 / #210 / #211). Each existing per-sketch processor reduces to a thin shim. |
| **Telegraf** (`asap-telegraf`, Go) | B | **350–450** | `telegraf.Metric` (string-typed `_asap_envelope` field) | Plugin model maps 1:1: `Add → Observe`, `Push → Tick`, `Reset` is a no-op. **A custom `asap` Telegraf `Serializer` is required** (~80 LoC) — the `[]byte` → `string` coercion in `convertField` means HTTP / Kafka / File sinks need a serializer that emits raw bytes. InfluxDB / Prometheus-RW sinks not supported. Reuses `asap-precompute-go`. |
| **Vector** (`asap-vector`, Rust) | B | **800–1200** | `vector::Event::Log` with `Value::Bytes` | Implemented as `TaskTransform<EventArray>` (NOT `<Event>` — per-event mpsc kills throughput). Use `vector_lib::stream::expiration_map::map_with_expiration` helper for timer + flush + drain (the same pattern `reduce` uses). `MetricValue::Sketch` is hard-typed to `AgentDDSketch` and unusable. Reuses `asap-precompute-rs`. Required sinks: `vector` native, Kafka `native`/`raw_message`, S3 `native`. |
| **OTAP Dataflow** (`asap-otap`, Rust + Arrow) | B | **~500** | `OtapArrowRecords` with envelope as Bytes attribute | Implemented as `local::Processor<OtapPdata>`. Use `NodeControlMsg::Wakeup` for window-aligned ticks; emit synthesized records via `effect_handler.send_message`. **`_asap_envelope` rides as an `AttributeValueType::Bytes` attribute, NOT a sibling Binary column** — OTAP's strict schema validator rejects extension columns. The in-tree `temporal_reaggregation_processor` is a structural template. Reuses `asap-precompute-rs`. |
| **Future platforms** (Fluent Bit, Beats, custom) | B | ~400–600 | platform-specific | Strategy B is the universal fallback. Pick the platform's nearest binary-clean carrier; if none exists, base64 in a string field at +33% overhead. |

### 7.4 Integration model — none of the four hosts have a drop-in plugin ABI

A finding from the per-platform feasibility audits that's worth
calling out as its own subsection: **the "ASAP ships a separate
`asap-{platform}` crate that users can drop in" framing is
aspirational at best.** None of the four target platforms exposes
a stable dynamic-library or external-process plugin interface for
the kind of stateful, scheduled, custom-emitting plugin ASAP
needs. Realistic integration model per platform:

| Platform | Integration mode | Implication |
|---|---|---|
| **OTel Collector** | OCB (OpenTelemetry Collector Builder) compiles in custom processors at build time. ASAPCollector already does this via `builder-config.yaml`. | ASAP ships an OCB manifest snippet; users build a custom collector binary that includes `asap-otelcol`. Same workflow ASAP already uses today. |
| **Telegraf** | Compiled-in only (`plugins/aggregators/all/asap.go` + build tag in `plugins/aggregators/all/aggregators.go`). No `aggregators.execd`. | ASAP ships a forked or vendored Telegraf binary. Users replace stock `telegraf` with `asap-telegraf` binary. |
| **Vector** | In-tree feature flag + `inventory::submit!` registration. No plugin ABI. | ASAP ships a forked or vendored Vector binary (or path-dependency the `asap-vector` crate into a custom Vector build). |
| **OTAP Dataflow** | `linkme` distributed-slice compile-time registration. Project README explicitly states "current system is compile-time only." | ASAP ships its own binary depending on `otap-df-engine` + `asap-otap` processor crate, OR a fork of `df_engine` that includes ASAP. |

Net consequence: ASAP's deployment story across non-OTel
platforms is "we ship a binary," not "drop a plugin into your
existing install." The LoC estimates in §7.3 reflect this — they
include build/feature plumbing (Cargo workspace edits, OCB
manifest entries, `linkme` registration boilerplate, etc.) that
naive estimates miss.

### 7.5 Third-party intermediaries

Kafka brokers, HTTP load balancers, generic routing tools that
sit between ASAP nodes never decode the envelope:

| Intermediary | What it does |
|---|---|
| Kafka broker / HTTP LB | Forwards bytes unchanged |
| Routing rule (Vector `route`, Telegraf `processors.route`) | Routes by tag / metric name; doesn't decode binary fields |
| Aggregation middleware (Telegraf `aggregators.basicstats`) | Skips — these aggregators only operate on numeric fields, so the binary `_asap_envelope` is naturally ignored. **This is why Strategy B uses a binary field**: a numeric carrier would let naive aggregators silently corrupt sketches |
| Schema-aware visualization | Opaque bytes; no decoder registered means no display, but no corruption |

## 8. Layer 5 — Control plane

Plan delivery is abstracted behind a trait so adapters can swap
transports at deploy time:

```rust
pub trait ControlChannel: Send {
    /// Returns Some when the plan has changed since the last poll.
    fn poll(&mut self) -> Option<PrecomputeConfigSet>;
    fn ack(&mut self, plan_version: u64);
}
```

Three implementations:

| Impl | Hosts | Status |
|---|---|---|
| `OpAmpChannel` | OTel Collector | Existing; `controller/src/opamp/mod.rs`. |
| `HttpPollChannel` | Telegraf, Vector, OTAP, anything else | New, ~30–50 LoC per adapter. The controller already serves `GET /api/v1/plan?host_id=<id>`. |
| `FileWatchChannel` | sidecar fallback | A tiny `asap-control-sidecar` binary tails an HTTP feed and writes a local file the host watches. All four adapters support file-watch natively. |

Recommendation: ship `HttpPollChannel` alongside `OpAmpChannel`
in Phase 4 (the first non-OTel adapter); `FileWatchChannel` is
held in reserve.

**Critical implementation note — applies to ALL four adapters,
including the OTel one.** Every host platform's reload machinery
treats config change as "rebuild the plugin instance," which
would clobber the in-memory sketch state on every plan push.
Verified for each platform:

| Platform | Reload behavior | ASAP-state survives? |
|---|---|---|
| OTel Collector | SIGHUP and OpAMP supervisor restart both call `service.Shutdown()` → `setupConfigurationComponents()` (`otelcol/collector.go::reloadConfiguration`); processors are reconstructed from factories. `component.Component` has only `Start`/`Shutdown` — no `OnConfigChange` hook. `ConfigWatcher` fires for extensions only. Issues #5966 / #6226 confirm no hot-reload path is planned. | **No.** |
| Telegraf | `--watch-config` triggers SIGHUP → `reloadLoop` → `loadConfiguration` rebuilds plugins. `StatefulPlugin` interface can save/restore but is itself opt-in and lossy for live sketch maps. | **No.** |
| Vector | `reload_config_and_respawn` computes a `ConfigDiff`; components whose configs changed are shut down and rebuilt. | **No.** |
| OTAP Dataflow | `NodeControlMsg::Config { config }` arrives on the same inbox as data; the processor *can* in principle update in place, but the canonical pattern is the same internal-poll pattern as the others for cross-platform consistency. | Yes (in principle), but use internal poll for uniformity. |

Pattern across all four:

```text
asap-{otelcol,telegraf,vector,otap}:
  on Start / Init:
    spawn a goroutine / tokio task running ControlChannel::poll
    in a loop; on plan change, atomically swap an
    atomic.Pointer[PrecomputeConfig] (Go) /
    Arc<ArcSwap<PrecomputeConfig>> (Rust) that the hot path
    reads on each tick / observe. On Stop / Shutdown, cancel the
    goroutine via context.Context / CancellationToken.

  Plugin / processor / transform / node config in the host's
  config file:
    bootstrap-only — contains controller_url, agent_id, auth.
    Never includes sketch_type, window size, matchers, or any
    runtime-mutable parameter. SIGHUP / host-side reload is a
    no-op for ASAP semantics.
```

There is precedent for this pattern across the contrib ecosystem:
OTel's `tailsamplingprocessor` spawns its own `loop()` goroutine
in `Start()` and updates policy via channels, fully bypassing the
collector's reload machinery. ASAP's adapter does the same.

This makes ASAP's runtime config completely independent of the
host's config-management story. The host config file specifies
only how to reach the controller; everything actually controlling
sketch behavior arrives via `ControlChannel::poll`. The
controller plan and the host config file are two separate
delivery channels with non-overlapping responsibility.

## 9. Migration plan

Phased so each phase is independently shippable. `sketch-core`
retirement (the first cleanup that enabled this design) already
landed in `asap_sketchlib#36` + `ASAPQuery-backend#73` +
`ASAPQuery#309`; recorded in `adr-0001-retire-sketch-core.md`.

| Phase | Scope | Exit criterion |
|---|---|---|
| **1.** Lock the design | This doc + ADRs (`adr-0002` for runtime extraction, `adr-0003` for adapter trait). API frozen, no code. | Doc reviewed; ADRs opened. |
| **2.** Extract Go runtime | New crate/dir `asap-precompute-go`. Move `accumulateIntoWindow` / `flushWindow` / snapshot caches / matchers out of each `processor/*/processor.go`. Each processor becomes a ~50-line shim. | b3-delta e2e produces same value (`19.49` at offset −90s); per-observation latency p99 within 10% of pre-refactor. |
| **3.** Extract Rust runtime | New crate `asap-precompute-rs`. Move `apply_proto_delta_bytes`, snapshot cache, per-accumulator constructors. Backend ingest becomes a thin adapter. | Backend e2e unchanged; no PromQL output drift. |
| **4.** Telegraf adapter (first non-OTel host) | New repo/dir `asap-telegraf`. Reuses `asap-precompute-go`. First `HttpPollChannel` wiring. | P8 accuracy reducer runs against Telegraf-emitted data; per-row error matches OTel-emitted. |
| **5.** OTAP Dataflow adapter | New crate `asap-otap`. Reuses `asap-precompute-rs`. Validates Arrow-batched ingestion. | OTAP processor node passes the same accuracy gate. |
| **6.** Vector adapter | New crate `asap-vector`. Reuses `asap-precompute-rs`. | Same accuracy gate. |

Phase 7 (optional, breaking): rename `ASAPCollector` → `asap-edge`
once the framework framing is settled. The repo layout end-state:

```
asap-edge/                        # ← Phase 7 rename
├── asap-precompute-go/           # Phase 2
├── asap-precompute-rs/           # Phase 3
├── asap-otelcol/                 # existing, refactored Phase 2
├── asap-telegraf/                # Phase 4
├── asap-otap/                    # Phase 5
├── asap-vector/                  # Phase 6
├── controller/                   # unchanged
├── deploy/                       # unchanged
└── docs/
```

Phase 7 is a marketing decision that can lag the engineering by
months without cost.

## 10. Risks

**R1 — Sketch-algorithm divergence between Go and Rust.** Two
implementations of the same algorithms are a permanent risk; we
caught a typed-encoding mismatch in #210. Mitigation: a
cross-language **statistical-output** test (not byte corpus —
hash seeds, storage growth, and protobuf field ordering
legitimately differ across runtimes). Same input stream into both
runtimes; compare P50/P90/P99/count/sum/cardinality/top-K within
each sketch's published error bound (DDSketch α, KLL rank error,
HLL standard error). Harness in `sketchlib-bench` (the natural
cross-language home; already runs Go vs Rust microbenchmarks).
Limit to two reference impls long-term: Go and Rust, no third
language.

**R2 — Performance regression in Phase 2.** Extracting the
runtime out of the OTel processor adds a function call per
observation. Mitigation: extraction uses generics or trait
dispatch with `inline` hints; benchmark p99 must stay within 10%
of pre-refactor.

**R3 — Controller emits OTel-specific YAML today.** Telegraf /
Vector / OTAP can't read it. Mitigation: Phase 2 changes nothing
controller-side. Phase 4+ introduces host-neutral
`PrecomputeConfig`; per-adapter translators lower it to native
config. Collector keeps reading OTel yaml; new adapters read the
neutral form.

**R4 — Two delta implementations diverging.** Phase 3 moves Rust
delta apply into a new crate. If the move is anything but a pure
code move, every windowed sketch on the backend miscomputes.
Mitigation: R1's harness is the regression catcher; Phase 3 entry
point stays bit-identical.

**R5 — None of the four host platforms have a state-preserving
config-push mechanism.** Verified by feasibility audits against
Telegraf, Vector, OTAP, AND OTel — every native reload path
(SIGHUP, OpAMP supervisor restart, `--watch-config`,
`reload_config_and_respawn`) destroys processor state. OpAMP is
not a viable transport for ASAP's runtime config either, because
it ultimately drives the same Shutdown→Setup cycle on the
collector side. Mitigation: §8 — `HttpPollChannel` runs inside
the adapter (precedent: `tailsamplingprocessor`), and the host's
config file carries only bootstrap parameters. The
`OpAmpChannel` slot in the trait stays as a delivery option for
deployments that want OpAMP for *transport*, but the receiving
adapter still atomically swaps `PrecomputeConfig` in-place rather
than letting the collector framework rebuild itself. ASAP plan
pushes never trigger a host reload.

**R6 — OTAP Dataflow project pre-1.0.** As of 2026-05, the
`otap-dataflow` crates are at workspace version `0.1.0` with
`publish = false` (consume by git rev only), ROADMAP labelled
"Work-In-Progress," and ~4 commits/day to the dataflow tree
including breaking changes (Extension System / capability
registry / schema validators). Mitigation:
- pin a specific commit SHA;
- plan a quarterly upgrade cadence with regression tests
  against the OTAP processor surface;
- isolate ASAP's runtime from OTAP API churn via a thin trait
  shim in `asap-precompute-rs` so only the `asap-otap` crate
  takes the upgrade hit when OTAP refactors;
- defer Phase 5 (OTAP adapter) until the project announces a
  stability commitment, OR accept the maintenance overhead as
  the price of getting Arrow-batched throughput early.

**R7 — Host platforms have no drop-in plugin ABI.** §7.4
documents this in detail: Telegraf / Vector / OTAP all require
fork-or-vendor builds. Mitigation: frame ASAP's deployment story
as "we ship a binary per platform" from day one. Document the
build/distribution model in each adapter's README. Don't promise
"`cargo add asap-vector`" or "drop the .so into your Telegraf" —
that's not how any of these platforms work.

## 11. Decisions required

- [ ] Approve §3–§8 abstractions and naming.
- [ ] Confirm R1 mitigation: statistical-output harness in
      `sketchlib-bench`.
- [ ] Decide repo layout for `asap-precompute-{go,rs}`: separate
      repos or subdirectories of `ASAPCollector`. Recommendation:
      subdirectories until a second consumer wants the runtime.
- [ ] Confirm `HttpPollChannel` as the first non-OTel
      `ControlChannel` impl.
- [ ] Confirm the Strategy B well-known field names (§7.2) as a
      project-level standard.

Once those land, Phase 2 is ~2 weeks of focused work; Phase 4
(Telegraf) is ~1 week after Phase 2/3 ship.

## Appendix — Concrete code mapping

How today's code maps onto the proposed layers, for orientation
during implementation:

| Today | Layer | Becomes |
|---|---|---|
| `sketchlib-go/sketches/*.go` (sketch + delta) | 1 | unchanged |
| `asap_sketchlib/src/sketches/*.rs` (sketch + apply_delta) | 1 | unchanged |
| `asap_sketchlib::proto::sketchlib::*` (state types) | 2 | unchanged |
| `asap_otel_proto::sketchlib::v1::*` (delta types, vendored) | 2 | moves into `asap_sketchlib::proto::sketchlib`; pure file move |
| `processor/*/processor.go::accumulateIntoWindow` | 3 | `asap-precompute-go::precompute.go::Observe` |
| `processor/*/processor.go::flushWindow` | 3 | `asap-precompute-go::precompute.go::Tick` |
| `processor/*/processor.go::snapshots`, `inboundSnapshots` | 3 | `asap-precompute-go::snapshot_cache.go` |
| `processor/*/processor.go::computeDDSketchDelta` | 3 | `asap-precompute-go::snapshot_cache.go::ComputeDelta` |
| `processor/*/processor.go::ConsumeMetrics` | 4 | stays in `processor/`, becomes a shim that delegates to `asap-precompute-go` |
| `apply_modified_otlp_delta_bytes` (`drivers/ingest/otel.rs`) | 3 | `asap-precompute-rs::precompute.rs::observe_envelope` |
| `controller/src/config/agent.rs::generate_agent_config` | 4/5 | adds host-neutral `PrecomputeConfig` output mode |
| `controller/src/opamp/mod.rs` | 5 | becomes one `ControlChannel` impl alongside new `HttpPollChannel` |
