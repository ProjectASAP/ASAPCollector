# ASAP Vector Integration — Design

_Status: **draft** — 2026-05-02. Doc-only; gates the Vector adapter
work that becomes the Phase 5 sister of the OTAP-Rust adapter in
the edge-framework migration._

This document is a focused supplement to
[`design-asap-edge-framework.md`](./design-asap-edge-framework.md)
and the runtime / adapter ADRs
([ADR-0002](./adr/adr-0002-extract-precompute-runtime.md),
[ADR-0003](./adr/adr-0003-adapter-trait-and-control-channel.md)),
and is the sister of
[`design-asap-otap-rust-integration.md`](./design-asap-otap-rust-integration.md)
(merged in #248). It covers only what is specifically new for the
Vector host. The five-layer model, the bandwidth invariant, the
`Adapter` / `ControlChannel` traits, the Strategy A / Strategy B
encoding split, and the `SketchEnvelope` wire format are defined
there; this doc references them and does not restate them.

## 1. Goal

Ship a single Vector binary `sketchvector` that includes all five
sketch types (DDSketch, KLL, HLL, CountSketch, CountMinSketch) as a
unified `asap_sketches` Transform plugin, using the same
`asap-precompute-rs` runtime that the `sketchotap` binary already
shares. Same wire format (`SketchEnvelope`), same backend ingest
path, same controller plan delivery (HTTP poll), same Strategy-B
carrier on egress.

Vector's plugin model is *compile-time* in-tree — there is no
dynamic loading. Plugins are discovered at startup via `inventory`
distributed-slice registration; sketches are built into the binary
at link time behind a feature flag. End-state deployment story per
[edge-framework §7.4](./design-asap-edge-framework.md#74-integration-model):

```
host operator picks:
  ─── existing OTel pipeline ─────► sketchcollector
  ─── existing Telegraf pipeline ─► sketchtelegraf
  ─── existing Vector pipeline ───► sketchvector       ← THIS DOC
  ─── Arrow-native pipeline ──────► sketchotap
                                       ↓
                             same SketchEnvelope bytes
                                       ↓
                             same gateway / backend
```

Operators choose between the agents based on the telemetry pipeline
they already operate; ASAP is indifferent. `sketchvector` is the
right choice when downstream consumers are already running Vector
for their broader telemetry pipeline (logs + metrics + traces) and
forcing a redundant Telegraf or OTel agent alongside Vector would
double the agent footprint.

## 2. Architecture — the two-layer split

Mirror the OTAP-Rust and Telegraf structure exactly. Two new
directories, no other moving parts:

```
                   ┌──────────────────────────────────────────────────┐
                   │  vector-patch/src/transforms/asap_sketches/       │
                   │     ── Layer 4 lifecycle PLUGIN                   │
                   │     - inventory::submit! registration             │
                   │     - TaskTransform<EventArray> consumer          │
                   │     - Tokio interval timer for tick               │
                   │     - graceful-shutdown drain                     │
                   │     - sample.toml, README                         │
                   │     - patches into the Vector submodule at build  │
                   └────────────────────┬─────────────────────────────┘
                                        │  uses
                                        ▼
                   ┌──────────────────────────────────────────────────┐
                   │  asap-precompute-rs/src/vector/                   │
                   │     ── Layer 4 CODEC                              │
                   │     - decode_event: Event::Metric → Observation   │
                   │     - encode_envelope: &SketchEnvelope → Event    │
                   │     - tag-key extraction / metric-value match     │
                   │     - no Vector lifecycle, pure transformation    │
                   └────────────────────┬─────────────────────────────┘
                                        │  uses
                                        ▼
                   ┌──────────────────────────────────────────────────┐
                   │  asap-precompute-rs/src/                          │
                   │  asap-precompute-rs/src/sketches/                 │
                   │  asap-precompute-rs/src/control_channel.rs        │
                   │     (REUSED UNCHANGED from runtime crate)         │
                   └──────────────────────────────────────────────────┘
```

The split is the same one ADR-0002 / ADR-0003 already pinned for
the OTel, Telegraf, and OTAP sides — the only difference is the
host: `pmetric.Metrics` / `telegraf.Metric` / `arrow::RecordBatch`
→ `vector::Event`. The runtime, sketch wrappers, snapshot caches,
matchers, window manager, and `ControlChannel` impls move zero
bytes.

Concretely:

- `asap-precompute-rs/src/vector/` — the Vector **codec**. Pure
  data-shape translation, no lifecycle. Owns no Tokio tasks, no
  timers, no control-message inbox. Mirrors the existing
  [`asap-precompute-go/otel/`](../asap-precompute-go/otel/),
  [`asap-precompute-go/telegraf/`](../asap-precompute-go/telegraf/),
  and the planned `asap-precompute-rs/src/otap/` directories in
  shape (`adapter.rs`, `config.rs`, `decode.rs`, `encode.rs`,
  `seriesattrs.rs` plus tests).
- `vector-patch/src/transforms/asap_sketches/` — the Vector
  **plugin**. Implements Vector's `TaskTransform<EventArray>`
  trait, owns the flush ticker, the `Precompute` instance, the
  control-channel task, and the config-block translation.
- Everything below the codec — the runtime, sketches wrappers,
  control_channel, `SketchEnvelope` proto types, the
  `asap_sketchlib` algorithm crate, the backend ingest path — is
  reused unchanged.

## 3. Why Vector specifically (vs OTAP-Rust)

Vector is Datadog's general-purpose Rust telemetry pipeline,
covering logs / metrics / traces with a broad input/output
ecosystem and mature operator tooling. Per
[edge-framework §7.3](./design-asap-edge-framework.md#73-per-platform-integration),
Vector's data model is a tagged-union `Event` with `Metric`,
`Log`, and `Trace` variants flowing through a graph of Sources,
Transforms, and Sinks.

Compared to the other Rust-side option, **OTAP-Rust** — the
sister adapter in
[`design-asap-otap-rust-integration.md`](./design-asap-otap-rust-integration.md)
— is targeted at OpenTelemetry's next-gen Arrow-native pipeline.
The two adapters differ in three ways: (a) **data shape** —
Vector's per-event tagged union vs OTAP-Rust's column-major Arrow
`RecordBatch`; (b) **plugin registration** — Vector's
`inventory::submit!` in a path-dependency Cargo workspace with
feature flags vs OTAP-Rust's `linkme` distributed-slices (both
compile-time); (c) **ecosystem maturity** — Vector is v0.x but
production-stable since 2019 with weekly releases, OTAP-Rust is
pre-1.0 with daily breaking changes (per
[edge-framework R6](./design-asap-edge-framework.md#10-risks)).

Frame this honestly: **Vector wins when** the operator already
runs Vector for a broader telemetry pipeline (logs + metrics +
traces), wants mature operator tooling, or needs broad
input/output coverage OTel-Arrow doesn't have today;
**OTAP-Rust wins when** downstream consumers need Arrow-native
ingest, the operator is on the OTel-canonical pipeline, or
future-facing OTel-Arrow alignment dominates.

Both reuse `asap-precompute-rs`. The codec and lifecycle differ;
the runtime, sketches, and wire format are identical. Operators
choose based on existing fleet — ASAP is indifferent.

## 4. Data model mapping — Vector `Event::Metric` ↔ `Observation`

Vector's `Event` enum has variants `Metric`, `Log`, and `Trace`;
for ASAP precompute input we only care about `Event::Metric`. A
Vector `Metric` is a `MetricSeries` (`name + namespace + tags`),
a `MetricKind` (`Absolute` / `Incremental`), a `MetricValue`
variant, an optional `timestamp: DateTime<Utc>`, and an optional
`interval_ms`. The codec's job is exactly the same as the
Telegraf and OTel codecs: walk the host event, extract
`(timestamp, name, labels, value)`, emit `Observation`. Unlike
OTAP, Vector is per-event row-major; unlike Telegraf, Vector
has typed value variants.

| Concept | OTel pmetric | Telegraf Metric | Vector Event::Metric | → `Observation` field |
|---|---|---|---|---|
| metric name | `pmetric::Metric::Name()` | `metric.Name()` | `series.name.name` (`+ namespace` if configured) | `Observation::metric` |
| labels | `dp.Attributes()` | `metric.Tags()` | `series.tags` (`MetricTags`) | `Observation::labels` |
| resource attrs | `ResourceMetrics::Resource()` | (none — flat) | (none — Vector is flat, no resource scope) | `Observation::resource_labels = empty` |
| timestamp | `dp.Timestamp()` | `metric.Time()` | `Metric.timestamp` (`Option<DateTime<Utc>>`) | `Observation::timestamp_ms` |
| value | `dp.DoubleValue()` / `IntValue()` | `metric.Fields()[<value_field>]` | `MetricValue::Counter { value }` / `Gauge { value }` | `Observation::value::float` |
| pre-aggregated sketch | typed DP variant | `_asap_envelope` string field | `_asap_envelope` tag (base64 string) — see below | `Observation::value::envelope` |

Four points need addressing explicitly.

**Vector value variant coverage.** `MetricValue` has seven
variants; only `Counter { value: f64 }` and `Gauge { value: f64 }`
are unambiguous f64 sources for sketch ingestion. The rest are
deferred: `Set` is string-cardinality (wrong type for the
`Float` path); `Distribution` is pre-binned (rarely matches
DDSketch / KLL, which want samples); `AggregatedHistogram` /
`AggregatedSummary` are already aggregated; `Sketch` is
hard-typed to Datadog's `AgentDDSketch` and cannot carry our
`SketchEnvelope::payload` bytes (per
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies)).
Scope for v1: **`Counter` and `Gauge` only**, log-and-skip the
others with a warn-once-per-series counter. Revisit
`Distribution` in v2 if a real workload needs pre-binned input.

**Resource handling — Vector is flat.** Vector has no
resource-scope analogue. Tags on `MetricSeries` are the only
attribute carrier; the codec emits `Observation::resource_labels
= empty` and the runtime's `OmitResourceAttrs=true` is the
natural default, mirroring the Telegraf side rather than the
OTel / OTAP sides.

**Pre-aggregated sketch input (KindEnvelope path).** Vector has
no `Bytes`-typed metadata channel on `Metric` (`MetricTags`
values are `String`); the egress path uses `Value::Bytes` on
`Event::Log` (per §5 and edge-framework §7.2), but ingest of
pre-aggregated envelopes via an upstream Vector transform needs
a `Metric`-carrier. Convention v1: a config-named tag (default
`_asap_envelope`) carrying base64-encoded envelope bytes, with
companion tags from the standardized key set
(`_asap_sketch_type`, `_asap_agg_id`, `_asap_schema_version`,
`_asap_window_start_ms`, `_asap_window_end_ms`,
`_asap_encoding`). The codec recognizes the well-known tag and
routes through `Precompute::observe_envelope` instead of
`observe`, paying the +33% base64 overhead on this path. Pure
Vector→Vector edge→gateway works; mixed-host multi-hop should
use the Strategy-B `Event::Log` egress form.

**`MetricKind` and namespace.** Both `MetricKind::Absolute` and
`Incremental` map to the same `Observation` shape on ingest (the
runtime treats `value` as a sample, not a counter). On emit,
synthesized envelopes default to `Absolute` — sketch-derived
quantile / cardinality readings are absolute window states, not
deltas; flag for §12 confirmation. `MetricSeries` is two-level
(`namespace.name`); the codec keys by the unqualified `name` by
default, with an `include_namespace` flag (off by default) for
operators needing cross-namespace disambiguation.

## 5. Plugin lifecycle — Vector `TaskTransform`

Vector's plugin model has three categories — **Source** (ingest),
**Transform** (consume + optionally emit), **Sink** (egress); the
ASAP adapter is a **Transform**. Transforms come in two flavors:
`FunctionTransform` (stateless `Vec<Event> -> Vec<Event>`) and
`TaskTransform<T>` (streaming async with full Tokio lifecycle,
where `T = EventArray` — not `Event` — to avoid the per-event
`mpsc` throughput bottleneck). Use **`TaskTransform<EventArray>`**
since we need a windowing ticker and a control-channel task. This
is what
[edge-framework §7.3](./design-asap-edge-framework.md#73-per-platform-integration)
already pinned.

Mapping Vector's lifecycle hooks onto the runtime contract:

| Vector method / event | What the plugin does |
|---|---|
| `TransformConfig::build()` (factory call from the `inventory` slice at startup) | Validate config; resolve `sketch_type` to one of the five `asap-precompute-rs/src/sketches/<type>` factories; build a `PrecomputeConfig` from the config block; construct the `Precompute` instance. Spawn the control-channel poll task (`HttpPollChannel`). Return a boxed `TaskTransform`. |
| `TaskTransform::transform(self: Box<Self>, input: Pin<Box<dyn Stream<Item = EventArray>>>) -> Pin<Box<dyn Stream<Item = EventArray>>>` | Consume the input stream; for each `Event::Metric` in each `EventArray`: `codec::decode_event(ev) -> Observation` → route to `Precompute::observe` (Float / Hash / Bytes path) or `Precompute::observe_envelope` (Envelope path). Drop the input metric (transducer behavior — sketch envelopes are fresh). |
| Tokio `interval` timer (driven internally inside the transform task) | `Precompute::tick(now_ms)` → `Vec<SketchEnvelope>` → `codec::encode_envelope` per envelope → emit `Event::Log` with `Value::Bytes` carrier (per [edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies)) into the output stream. |
| Tokio `select!` shutdown branch (input stream closes) | Cancel the control-channel task and flush ticker. Drain pending windows: one final `tick()` to emit any in-flight window state. Drop `Precompute`. |
| Vector's `reload_config_and_respawn` (when host config changes) | Treated as "shutdown + rebuild" per [edge-framework §8](./design-asap-edge-framework.md#8-layer-5--control-plane); state survival is the controller's responsibility via `ControlChannel::poll` swapping the `Arc<ArcSwap<PrecomputeConfig>>` on the live instance, never via Vector reload. |

Use `vector_lib::stream::expiration_map::map_with_expiration`
(the helper Vector's `reduce` transform uses for the same
timer + flush + drain pattern) to wire the interval ticker into
the output stream without a hand-rolled `tokio::select!`. Per
[edge-framework §7.3](./design-asap-edge-framework.md#73-per-platform-integration).

The plugin emits **`Event::Log` with `Value::Bytes`** for
envelope output — *not* `Event::Metric` with `MetricValue::Sketch`.
Per
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies),
`MetricValue::Sketch` is hard-typed to Datadog's `AgentDDSketch`
and cannot carry our `SketchEnvelope::payload` bytes;
`Value::Bytes` on a `LogEvent` is the canonical binary-clean
carrier. Required downstream sinks: `vector` native (Protobuf),
Kafka `native`/`raw_message`, S3 `native`; JSON sinks
(Elasticsearch, Loki, default Splunk) corrupt non-UTF-8 bytes
and need an opt-in base64 fallback at +33% overhead.

Same pattern as OTel's `tailsamplingprocessor`, Telegraf's
`allsketches` processor, and OTAP-Rust's `asap_sketches` (per
ADR-0003 §3): host owns nothing about scheduling; plugin spawns
its own task; reload is "shutdown + rebuild" with
state-preservation via atomic `PrecomputeConfig` swap on the
live instance, never via Vector's reload path.

## 6. Plugin file layout

```
vector-patch/
├── src/
│   └── transforms/
│       └── asap_sketches/
│           ├── Cargo.toml         // path = "../../../../asap-precompute-rs"
│           ├── mod.rs             // pub use; module entry point
│           ├── config.rs          // TransformConfig impl + Deserialize TOML
│           ├── transform.rs       // TaskTransform<EventArray> impl;
│           │                      //   decode → observe → tick → emit
│           ├── tests.rs           // end-to-end Tokio harness test
│           ├── README.md          // user-facing config docs
│           └── sample.toml        // canonical [transforms.asap_sketches]
└── src/
    └── transforms/
        └── mod.rs                 // patches Vector's transform module
                                   //   to bring asap_sketches into the
                                   //   inventory::submit! scope under
                                   //   feature = "transforms-asap-sketches"
```

The `vector-patch/` directory is a "patch overlay" applied onto
the upstream Vector submodule at build time, the same pattern
`telegraf-patch/` uses against the upstream `telegraf/` submodule
and `otap-patch/` will use against the OTAP submodule. A new
`restore_vector_patches.sh` script (paralleling the existing
`restore_telegraf_patches.sh`) implements the copy-overlay
mechanic; the layout deliberately mirrors the Telegraf / OTAP /
OTel sides so contributors moving between platforms see the same
shape.

## 7. Build pipeline — `build_sketchvector.sh`

Mirror `build_sketchcollector.sh`, `build_sketchtelegraf.sh`,
and `build_sketchotap.sh`. Steps: (1) apply patches via
`restore_vector_patches.sh` (registers `asap_sketches` in
Vector's `transforms/mod.rs` under feature
`transforms-asap-sketches`); (2) resolve `[patch.crates-io]` in
Vector's workspace `Cargo.toml` for `asap-precompute-rs` and
`asap_sketchlib` to local checkouts, the same pattern
`build_sketchcollector.sh` uses for `sketchlib-go`; (3) `cargo
build --release --bin vector --features
transforms-asap-sketches` (Vector uses Cargo features for
optional plugins; our flag follows the `transforms-<name>`
convention); (4) output `vector/target/release/vector`
copied / symlinked to `vector/sketchvector` for parity with
`telegraf/sketchtelegraf`.

**Key build-system decision.** Vector's plugin enumeration is
generated at link time by `inventory`'s distributed-slice
machinery (gated by feature flags) rather than by an external
tool like OCB or the Telegraf custom-builder. Adding our plugin
is (a) module declaration under `src/transforms/`, (b) feature
gate `transforms-asap-sketches` in Vector's root `Cargo.toml`,
(c) feature-on at build time. Small patches into upstream Vector
files. **No OCB equivalent needed** — `inventory`'s compile-time
discovery is the mechanism, the same way `linkme` works for
OTAP-Rust.

Estimated final script ~80 LoC including error handling and the
`--skip-patches` flag the OTel and Telegraf scripts already
support.

## 8. Config example

Vector's config language is TOML (with optional YAML / JSON).
Operators write one `[transforms.<id>]` block per ASAP precompute
(multiple blocks for multiple sketch types, distinguished by
their transform id):

```toml
[transforms.asap_ddsketch]
type        = "asap_sketches"
inputs      = ["my_metrics_source"]

  ## Sketch: ddsketch | kll | hll | countsketch | countminsketch
  sketch_type = "ddsketch"
  window_size = "10s"          # Vector duration string

  # value_field    = "value"   # Optional override; default reads
                               #   MetricValue::Counter/Gauge directly.
  include_namespace = false    # Prepend series.name.namespace if true.

  ## Sketch-specific parameters; unused keys are ignored.
  [transforms.asap_ddsketch.params]
  alpha = 0.01           # ddsketch — relative accuracy
  # k = 200              # kll — buffer size
  # precision = 14       # hll — register count exponent
  # width = 2048         # countsketch / countminsketch — width
  # depth = 5            # countsketch / countminsketch — depth
  # heavy_hitters = 100  # countsketch — top-K heap size

  ## Output metric name. If unset, codec appends a sketch-typed
  ## suffix (e.g. "_ddsketch") matching OTel / Telegraf / OTAP.
  output_metric_name = "http_request_duration_ms"

  ## Bootstrap — controller for plan delivery via HttpPollChannel.
  ## Per ADR-0003 §3, runtime sketch params are push-overridable
  ## from the plan; values above are bootstrap defaults.
  controller_url = "http://controller:8080"
  agent_id       = "vector-host-01"
```

Field reference: `inputs` is Vector-standard transform-graph
wiring; `sketch_type` selects the sketch wrapper from
`asap-precompute-rs/src/sketches/<type>`; `window_size` is a
Vector duration passed to `PrecomputeConfig::window`;
`value_field` is a rare Vector-specific override (default reads
`MetricValue::Counter`/`Gauge` directly); `include_namespace` is
Vector-specific (per §4); `params.*` passes verbatim to
`SketchParams`; `output_metric_name` is the Vector-codec
equivalent of the other adapters' same knob; `controller_url` /
`agent_id` are bootstrap-only per ADR-0003 §3.

## 9. Reused vs new code

**Reused unchanged from existing runtime crate:**

- `asap-precompute-rs/src/` — Layer 3 runtime (windowing,
  snapshot caches, scheduler abstractions, matchers).
- `asap-precompute-rs/src/sketches/<ddsketch,kll,hll,countsketch,cms>/`
  — sketch wrappers implementing the `Sketch` /
  `QuantileSketch` / `CardinalitySketch` / `FrequencySketch`
  trait family.
- `asap-precompute-rs/src/control_channel.rs` — `ControlChannel`
  trait; the plugin uses an `HttpPollChannel` impl, reused
  unchanged from the OTAP-Rust adapter (already shipped in
  Phase 5 step A — #248).
- `asap_sketchlib/` — Layer 1 sketch algorithms.
- Wire format — `SketchEnvelope` proto, byte-identical across
  all adapters per the bandwidth invariant
  ([edge-framework §5.2](./design-asap-edge-framework.md#52-the-bandwidth-invariant)).
- Backend ingest path — already accepts envelopes from any
  source.

**New (Vector-specific):**

| Component | Path | LoC estimate |
|---|---|---|
| Vector codec | `asap-precompute-rs/src/vector/` | ~400 (decode_event, encode_envelope, config, seriesattrs, MetricValue dispatch, tag-key extraction, tests) |
| `asap_sketches` Transform | `vector-patch/src/transforms/asap_sketches/` | ~700 (Tokio async lifecycle on `TaskTransform<EventArray>`, config translation, ticker wiring via `map_with_expiration`, control-channel task, factory + `inventory::submit!`, tests) |
| Vector registry overlay | `vector-patch/src/transforms/mod.rs` | ~10 (module + feature gate) |
| Build script | `build_sketchvector.sh` | ~80 |
| **Total new** | | **~1200** |

Compare to ~1500 LoC for the Telegraf side (per
[telegraf integration §9](./design-asap-telegraf-integration.md#9-reused-vs-new-code)),
~1200 for OTAP-Rust (per
[otap-rust integration §9](./design-asap-otap-rust-integration.md#9-reused-vs-new-code)),
and ~50 LoC per processor on the OTel side post-extraction. The
Vector side matches OTAP-Rust closely: less than Telegraf because
both Rust adapters share the `asap-precompute-rs` HttpPollChannel
already shipped in #248, and because `TaskTransform` + Tokio
async is a lighter-weight scheduling model than Telegraf's
`StreamingProcessor` + custom serializer.

The Strategy-B wire format work (the well-known `_asap_envelope`
key on tags / the egress carrier on `Event::Log`, sibling
metadata keys, sink compatibility list) is **inherited from the
framework spec** rather than designed here —
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies)
already pinned the carrier to `Value::Bytes` on `Event::Log` for
the egress side.

## 10. Open questions and risks

**Vector submodule pinning.** Vector is more stable than OTAP —
per [edge-framework R6](./design-asap-edge-framework.md#10-risks),
OTAP is pre-1.0 with daily breaking changes; Vector is at v0.x
but production-stable since 2019 with a weekly-ish release
cadence. Mitigation: pin a specific stable Vector release tag
(not a SHA) in `.gitmodules` and document it in
`restore_vector_patches.sh`; plan a quarterly upgrade cadence
with regression tests; isolate ASAP's runtime from Vector API
churn so only `vector-patch/` takes the upgrade hit; document
the upgrade workflow (pin new tag → `cargo build` → lifecycle
harness → cross-host parity → update tag) in the plugin's
README. Upgrade pain should be lower than OTAP-Rust's because
Vector's public Transform trait surface is more stable.

**`MetricValue` variant coverage.** v1 supports `Counter` and
`Gauge` only; `Distribution`, `AggregatedHistogram`,
`AggregatedSummary`, `Set`, and `Sketch` are deferred (per §4).
Log-and-skip unsupported variants with a warn-once-per-series
counter; document prominently in the plugin README. v2 revisits
`Distribution` if a real workload needs pre-binned input feeding
KLL / DDSketch (would require a "sample-from-distribution"
expansion in the codec).

**Vector's metric model is opinionated.** Beyond variant
coverage, two edge cases: (a) `MetricKind::Absolute` vs
`Incremental` — both ingest the same way; emit defaults to
`Absolute` (flag for §12); (b) `MetricSeries::namespace` — the
two-level `namespace.name` doesn't match the runtime's flat name
model, bridged by the `include_namespace` flag from §8 (default
off, matching OTel's unqualified `metric.Name()`).

**Cross-language byte parity (issue #243).** Vector uses
`asap_sketchlib` (Rust); the existing OTel and Telegraf agents
use `sketchlib-go` (Go). The `SketchEnvelope::payload` bytes
must match across both implementations or a mixed fleet produces
divergent backend results. Issue #243 tracks this work; it is a
**hard prerequisite** for production fleet mixing. Until #243
closes, `sketchvector` deployments must be homogeneous (all
hosts on Vector / OTAP-Rust, or all on Telegraf / OTel), and
cross-host parity tests pin to one runtime. Same constraint
OTAP-Rust faces (per
[otap-rust integration §10](./design-asap-otap-rust-integration.md#10-open-questions-and-risks)).

**No legacy parity baseline.** Like Telegraf and OTAP-Rust, this
is greenfield. Correctness is established by (1) unit tests on
the codec's `decode_event` / `encode_envelope` round trip, (2)
plugin lifecycle tests against Vector's in-tree
`vector_lib::test_util::components` harness, and (3) **cross-host
envelope parity**: a `sketchvector` agent and a `sketchotap` /
`sketchcollector` / `sketchtelegraf` agent fed the same input
stream MUST emit byte-identical `SketchEnvelope::payload` bytes.
Phase E covers this — same shape as Phase 4 step E
(`integration/parity/golden_test.go`).

**`inventory` crate stability.** `inventory`'s distributed-slice
macros must work across the
`asap-precompute-rs` ↔ `vector-patch/` boundary. `inventory` is a
stable Rust crate (Vector uses it itself), but cross-crate
registration can be fragile. Two failure modes to verify: (a)
the `inventory::submit!` entry is reachable from the binary's
main crate (linker doesn't dead-code-eliminate) — mitigated by
explicit module declaration in `vector-patch/src/transforms/mod.rs`,
matching Vector's in-tree transform pattern; (b) `inventory`
versions must align — mitigated by keeping it confined to
`vector-patch/`, not pulled into `asap-precompute-rs`. Same
shape as OTAP-Rust's `linkme` constraint.

**Sink compatibility.** Per §5 and
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies),
egress `Event::Log` with `Value::Bytes` only round-trips
losslessly through binary-clean sinks (`vector` native, Kafka
native/raw_message, S3 native). JSON-shaped sinks corrupt
non-UTF-8 bytes; document the supported sink list and offer a
`base64_egress = true` knob for operators forced onto a JSON
sink (+33% overhead).

**No drop-in plugin ABI.** Per
[edge-framework §7.4](./design-asap-edge-framework.md#74-integration-model)
and R7. Distribution is via a custom-built binary
(`sketchvector`), not by dropping a `.so` into a stock Vector
install. Same model as `sketchcollector`, `sketchtelegraf`, and
`sketchotap`.

## 11. Phase plan

| Phase | Scope | Exit criterion |
|---|---|---|
| **A** | This doc — design alignment, no code. | Reviewed; section §11 of the framework doc updated to point at this doc as the Phase-5 sister. |
| **B** | Codec implementation: `asap-precompute-rs/src/vector/` + minimal plugin shell that wires `decode_event` / `encode_envelope` against a stub `Precompute`. | `cargo test -p asap-precompute-rs --features vector` passes; plugin compiles. |
| **C** | Full `asap_sketches` Transform: all five sketch types via `sketch_type` dispatch, control-channel Tokio task, `map_with_expiration`-driven flush, lifecycle. | Vector-harness lifecycle tests pass for each `sketch_type`; round-trip raw input → envelope output preserves expected sketch counts. |
| **D** | Build script (`build_sketchvector.sh`) + Vector submodule patch (`vector-patch/src/transforms/mod.rs` registration + feature flag). | `bash build_sketchvector.sh` produces a `sketchvector` binary that lists `asap_sketches` in its transforms registry. |
| **E** _(optional)_ | Cross-host envelope parity test — `sketchvector` agent and `sketchotap` / `sketchcollector` / `sketchtelegraf` agents fed identical input emit byte-identical `SketchEnvelope::payload`s. | E2E test passes; backend PromQL output is identical regardless of which agent produced the data. **Gated on issue #243** for the cross-language case (Go vs Rust payload bytes); the homogeneous-Rust case (sketchvector vs sketchotap) does not need #243. |

Phase A is this PR. Phases B–D are sized at roughly 1 week each
for an engineer familiar with the runtime + Tokio async; the
runtime extraction (ADR-0002), the Telegraf adapter (Phase 4),
and the OTAP-Rust adapter (Phase 5 step A — #248) having already
shipped is what makes this fit in 3–4 weeks rather than 6+. Phase
E is gated on #243 for the cross-language portion.

## 12. Decisions required

- [ ] Approve the unified `asap_sketches` Transform shape (single
      transform parameterized by `sketch_type`, mirroring
      Telegraf's `allsketches` and OTAP-Rust's `asap_sketches`).
      §3 above is the rationale; it's the same argument as
      [telegraf integration §3](./design-asap-telegraf-integration.md#3-why-one-unified-allsketches-plugin-not-five).
- [ ] Confirm the Vector version to pin — recommend the latest
      stable tag at implementation start (Phase B), with quarterly
      upgrade cadence (§10).
- [ ] Confirm the `MetricKind::Absolute` default for emit-side
      synthesized envelopes (§4, §10), versus `Incremental` per
      window.
- [ ] Confirm pre-aggregated envelope input via tag-encoded base64
      (the multi-hop Vector→Vector edge→gateway path) versus
      deferring KindEnvelope ingest entirely on Vector to v2.
      §4 leans toward the tag-base64 convention; v2 might add a
      richer `Distribution` interpretation if a real workload
      needs it.
- [ ] Confirm the Strategy-B egress carrier choice (`Value::Bytes`
      on `Event::Log`) — already pinned by edge-framework §7.2 but
      flagged for re-confirmation now that the codec is being
      designed.
- [ ] Co-existence: do we ship one or both Rust adapters at
      release? The framework supports both; operator chooses.
      Recommend shipping both — the LoC cost is bounded
      (~1200 each), and the operator audience is non-overlapping.
- [ ] Confirm phase B–E sequencing; in particular, whether phase
      E (cross-host parity) is a release-gate or a post-release
      regression test, and how it sequences against #243.

Once those land, phase B can start immediately; the runtime
dependency (`asap-precompute-rs`) is already on `main` (Phase 3
shipped in #241 / #242), and the `HttpPollChannel` impl is
already on `main` (Phase 5 step A — #248).

## References

- [`docs/design-asap-edge-framework.md`](./design-asap-edge-framework.md)
  — five-layer model, bandwidth invariant, Strategy A/B,
  per-platform encoding, `Adapter` / `ControlChannel` traits,
  R6 / R7, §7.2 (per-platform Strategy-B carriers — Vector row),
  §7.3 (per-platform integration — Vector
  `TaskTransform<EventArray>`), §7.4 (integration model).
- [`docs/design-asap-otap-rust-integration.md`](./design-asap-otap-rust-integration.md)
  — Phase-5 sister adapter design that this doc mirrors
  structurally (two-layer split, unified-plugin shape, build
  pipeline, cross-language parity reasoning).
- [`docs/design-asap-telegraf-integration.md`](./design-asap-telegraf-integration.md)
  — Phase-4 adapter design that this doc inherits the
  unified-plugin and patch-overlay shape from.
- [`docs/adr/adr-0002-extract-precompute-runtime.md`](./adr/adr-0002-extract-precompute-runtime.md)
  — runtime contract that the Vector plugin reuses.
- [`docs/adr/adr-0003-adapter-trait-and-control-channel.md`](./adr/adr-0003-adapter-trait-and-control-channel.md)
  — adapter shape and control-channel rule that this design
  follows.
- [`asap-precompute-rs/src/`](../asap-precompute-rs/src/) —
  runtime crate (Phase 3, shipped) that the Vector plugin
  depends on.
- [`asap-precompute-go/otel/`](../asap-precompute-go/otel/) and
  [`asap-precompute-go/telegraf/`](../asap-precompute-go/telegraf/)
  — reference codec shapes that `asap-precompute-rs/src/vector/`
  mirrors.
- [`telegraf-patch/`](../telegraf-patch/) — patch-overlay
  structure that `vector-patch/` mirrors.
- [`build_sketchcollector.sh`](../build_sketchcollector.sh) and
  [`build_sketchtelegraf.sh`](../build_sketchtelegraf.sh) —
  build pipelines that `build_sketchvector.sh` mirrors.
- Issue #243 — cross-language byte-parity tracker; hard
  prerequisite for production Vector ↔ Telegraf / OTel fleet
  mixing.
- PR #248 — OTAP-Rust adapter design (sister Phase-5 effort).
