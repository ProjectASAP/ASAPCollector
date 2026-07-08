# ASAP OTAP-Rust Integration — Design

> **DORMANT.** This integration is not active on the current branch — the `otel-arrow` path/submodule is uninitialized and no build or eval arm exercises it. Kept for design reference; revisit before reactivating.


_Status: **draft** — 2026-05-02. Doc-only; gates the OTAP-Rust
adapter work that becomes Phase 5 of the edge-framework migration._

This document is a focused supplement to
[`design-asap-edge-framework.md`](./design-asap-edge-framework.md)
and the runtime / adapter ADRs
([ADR-0002](./adr/adr-0002-extract-precompute-runtime.md),
[ADR-0003](./adr/adr-0003-adapter-trait-and-control-channel.md)),
and is the Phase-5 mirror of
[`design-asap-telegraf-integration.md`](./design-asap-telegraf-integration.md)
(Phase 4, merged in #233). It covers only what is specifically new
for the OTAP Dataflow host. The five-layer model, the bandwidth
invariant, the `Adapter` / `ControlChannel` traits, the Strategy A /
Strategy B encoding split, and the `SketchEnvelope` wire format are
defined there; this doc references them and does not restate them.

## 1. Goal

Ship a single OTAP-Rust binary `asap-otap` that includes all five
sketch types (DDSketch, KLL, HLL, CountSketch, CountMinSketch) as a
unified `asap_sketches` receiver / processor plugin, using the same
`asap-precompute-rs` runtime that the eventual `sketchvector`
binary will share. Same wire format (`SketchEnvelope`), same
backend ingest path, same controller plan delivery (HTTP poll),
same Strategy-B carrier on egress.

OTAP Dataflow is a *compile-time* plugin model — there is no
dynamic loading. Plugins are discovered at startup via `linkme`
distributed-slice registration; sketches are built into the binary
at link time. End-state deployment story per
[edge-framework §7.4](./design-asap-edge-framework.md#74-integration-model):

```
host operator picks:
  ─── existing OTel pipeline ─────► asap-otel
  ─── existing Telegraf pipeline ─► asap-telegraf
  ─── Arrow-native pipeline ──────► asap-otap
                                       ↓
                             same SketchEnvelope bytes
                                       ↓
                             same gateway / backend
```

Operators choose between the agents based on the telemetry pipeline
they already operate; ASAP is indifferent. `asap-otap` is the right
choice when downstream consumers need Arrow-native ingest and
`asap-telegraf` / `asap-otel` would force redundant
serialization round-trips through OTLP-proto or Telegraf line
protocol.

## 2. Architecture — the two-layer split

Mirror the Telegraf structure exactly. Two new directories, no other
moving parts:

```
                   ┌──────────────────────────────────────────────────┐
                   │  otap-patch/plugins/asap_sketches/                │
                   │     ── Layer 4 lifecycle PLUGIN                   │
                   │     - linkme distributed-slice registration       │
                   │     - async Stream<RecordBatch> consumer          │
                   │     - Tokio interval timer for tick               │
                   │     - graceful-shutdown drain                     │
                   │     - sample.toml, README                         │
                   │     - patches into the OTAP submodule at build    │
                   └────────────────────┬─────────────────────────────┘
                                        │  uses
                                        ▼
                   ┌──────────────────────────────────────────────────┐
                   │  asap-precompute-rs/src/otap/                     │
                   │     ── Layer 4 CODEC                              │
                   │     - decode_batch: RecordBatch → Vec<Observation>│
                   │     - encode_batch: &[SketchEnvelope] → RecordBatch│
                   │     - schema discovery / column mapping           │
                   │     - no OTAP lifecycle, pure transformation      │
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

The split is the same one ADR-0002 / ADR-0003 already pinned for the
OTel and Telegraf sides — the only difference is the host:
`pmetric.Metrics` / `telegraf.Metric` → `arrow::RecordBatch`. The
runtime, sketch wrappers, snapshot caches, matchers, window
manager, and `ControlChannel` impls move zero bytes.

Concretely:

- `asap-precompute-rs/src/otap/` — the OTAP **codec**. Pure
  data-shape translation, no lifecycle. Owns no Tokio tasks, no
  timers, no control-message inbox. Mirrors the existing
  [`asap-precompute-go/otel/`](../asap-precompute-go/otel/) and
  [`asap-precompute-go/telegraf/`](../asap-precompute-go/telegraf/)
  directories in shape (`config.rs`, `decode.rs`, `encode.rs`,
  `lifecycle.rs`, `records.rs`, `schema.rs` plus tests). The
  `records.rs` module carries the local `OtapMetricRecords` model
  and the `flatten()` / `lift()` Strategy-B projection that bridges
  upstream OTAP's sibling-batch family to the codec's flat
  per-row `RecordBatch` shape — Phase D's binding seam to the
  upstream `OtapPdata` type lives here as a thin `From` / `Into`
  adapter.
- `otap-patch/plugins/asap_sketches/` — the OTAP **plugin**.
  Implements OTAP's receiver / processor trait, owns the flush
  ticker, the `Precompute` instance, the control-channel task, and
  the config-block translation.
- Everything below the codec — the runtime, sketches wrappers,
  control_channel, `SketchEnvelope` proto types, the
  `asap_sketchlib` algorithm crate, the backend ingest path — is
  reused unchanged.

## 3. Why OTAP-Rust specifically (vs another Rust agent)

OTAP Dataflow is the Rust-native streaming framework targeted by
OpenTelemetry's next-gen high-throughput pipeline (per
[edge-framework §7.3](./design-asap-edge-framework.md#73-per-platform-integration)).
It uses Apache Arrow as the in-memory data shape — `RecordBatch`
values flow through a graph of `Receiver` / `Processor` /
`Exporter` nodes. The codec encode/decode operates over
`arrow::RecordBatch` (consistent with the
[ADR-0002 §4 hint about future Arrow ingest](./adr/adr-0002-extract-precompute-runtime.md)
on the backend side).

Compared to the other Rust-side option, **Vector** — also Rust,
also pre-existing — is covered by a separate Phase 5 effort
(`sketchvector`, `asap-precompute-rs`'s other downstream consumer).
OTAP-Rust differs in three ways: (a) Arrow-native column-oriented
data shape vs Vector's tagged-union per-event `Event`; (b)
compile-time plugin registration via `linkme` vs Vector's
`inventory::submit!`; (c) targets the OTel project's official
next-gen pipeline (in-tree successor to today's collector) rather
than Datadog's broader-remit Vector.

Frame this honestly: **OTAP-Rust is the right choice when
downstream consumers need Arrow-native ingest.** Vector is the
right choice for plain telemetry pipelines that don't need
columnar batching. Both ride the same `asap-precompute-rs`
runtime; the codec and lifecycle differ.

## 4. Data model mapping — Arrow `RecordBatch` ↔ `Observation`

OTAP uses Arrow record batches with a known schema (per the
OpenTelemetry Arrow protocol — `OtapArrowRecords` for
metrics / logs / traces, with sibling RecordBatches for resource
attributes, scope attributes, and per-row attributes). The codec's
job is exactly the same as
[`asap-precompute-go/telegraf/decode.go`](../asap-precompute-go/telegraf/decode.go):
walk the host event, extract `(timestamp, name, labels, value)`
tuples, emit `Vec<Observation>`. OTAP's data model is column-major
where Telegraf's was row-major, which complicates batch-shape
handling and simplifies field extraction.

| Concept | OTel pmetric | Telegraf Metric | OTAP RecordBatch | → `Observation` field |
|---|---|---|---|---|
| metric name | `pmetric::Metric::Name()` | `metric.Name()` | name column on the metrics RecordBatch (or per-batch metadata) | `Observation::metric` |
| labels | `dp.Attributes()` | `metric.Tags()` | column-encoded attribute group on the per-row attribute child batch | `Observation::labels` |
| resource attrs | `ResourceMetrics::Resource()` | (none — Telegraf is flatter) | resource child batch (one row per resource scope, joined on ID) | `Observation::resource_labels` |
| timestamp | `dp.Timestamp()` | `metric.Time()` | `time_unix_nano` timestamp column | `Observation::timestamp_ms` |
| value | `dp.DoubleValue()` / `IntValue()` | `metric.Fields()[<value_field>]` | value column (`Float64Array` for gauge / sum) | `Observation::value::float` |
| pre-aggregated sketch | typed DP variant | `_asap_envelope` string field | `_asap_envelope` Bytes-typed value on the per-row attribute child batch (per [edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies)) | `Observation::value::envelope` |

Four points need addressing explicitly:

**Batch-shaped decode.** A single `RecordBatch` carries N rows;
decode produces N `Observation`s. The codec exposes
`decode_batch(&RecordBatch) -> Vec<Observation>` rather than the
per-event shape used by OTel and Telegraf. Internally it walks the
columns once (resolving `(name, labels, timestamp, value)` per row
index) and pushes one `Observation` per row. Throughput benefits
from batch-amortized column lookups vs per-row attribute map
traversal.

**Schema discovery.** The codec needs to know which columns hold
name / labels / value. v1: pin to OTel-Arrow's well-known schema
(the `OtapArrowRecords` schema in
`otap-dataflow/crates/pdata/src/schema/`) and rely on OTAP's own
validators to reject non-conforming batches. A config that names
columns (for non-OTAP-shaped `RecordBatch` inputs from custom Arrow
producers) is deferred to v2; rare in practice because the plugin
sits inside an OTAP pipeline.

**Resource handling — OTAP's resource scope is at batch
granularity.** Each `OtapArrowRecords` carries a sibling resource
RecordBatch joined to the metrics batch by an integer resource_id
column. The codec resolves resource attributes per-row by looking
up the resource row and attaching its attributes to
`Observation::resource_labels`. `OmitResourceAttrs=false` is the
reasonable default — unlike Telegraf (no resource-scope analogue,
runtime defaults `OmitResourceAttrs=true`), OTAP carries real
resource attributes that the runtime should include in series keys.

**Pre-aggregated sketch input (KindEnvelope path).** When an OTAP
upstream sends an already-aggregated sketch (typical multi-hop
case: edge `asap-otap` flushes envelopes to a gateway
`asap-otap` for re-aggregation), the envelope rides as a
Strategy-B field on the per-row attribute child batch. Per the
audit in
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies),
`_asap_envelope` rides as an `AttributeValueType::Bytes`
attribute, **not** as a sibling top-level Binary column — OTAP's
`crates/pdata/src/schema/payloads.rs::check_match` returns
`Error::ExtraneousField` for any extension column on Logs /
Metrics / Traces RecordBatches. Companion keys
(`_asap_sketch_type`, `_asap_agg_id`, `_asap_schema_version`,
`_asap_window_start_ms`, `_asap_window_end_ms`, `_asap_encoding`)
ride as same-row attributes. The codec recognizes the well-known
attribute key and routes the observation through
`Precompute::observe_envelope` instead of `observe`.

## 5. Plugin lifecycle — OTAP receiver / processor

OTAP Dataflow's plugin model uses `linkme` distributed slices: a
receiver / processor / exporter registers itself via a `static`
slice declaration; the runtime discovers all registered nodes at
startup. There are no `Start` / `Stop` callbacks like the OTel
collector — instead, async `Stream<Item = RecordBatch>` patterns
plus a `NodeControlMsg` inbox that delivers shutdown / config /
wakeup events. The canonical structural template in-tree is
`crates/processors/temporal_reaggregation_processor/`, called out
in [edge-framework §7.3](./design-asap-edge-framework.md#73-per-platform-integration).

Mapping OTAP's lifecycle hooks onto the runtime contract:

| OTAP method / event | What the plugin does |
|---|---|
| Plugin factory (called once at startup from the `linkme` slice) | Validate config; resolve `sketch_type` to one of the five `asap-precompute-rs/src/sketches/<type>` factories; build a `PrecomputeConfig` from the config block; construct the `Precompute` instance. Spawn the control-channel poll task (`HttpPollChannel`). |
| `Stream<Item = OtapPdata>` consumer loop (or `Processor::process` per OTAP's processor trait) | For each incoming `RecordBatch`: `codec::decode_batch(rb)` → `Vec<Observation>`; route each obs to `Precompute::observe` (Float / Hash / Bytes path) or `Precompute::observe_envelope` (Envelope path). Drop the input batch (or pass through, depending on whether the plugin sits as a receiver or as a processor). |
| `NodeControlMsg::Wakeup` (Tokio interval-timer-driven, `flush_interval`-aligned) | `Precompute::tick(now_ms)` → `Vec<SketchEnvelope>` → `codec::encode_batch` → emit a synthesized `OtapPdata` via `effect_handler.send_message`. |
| `NodeControlMsg::Config { config }` | Apply atomically via `ArcSwap<PrecomputeConfig>` (per [edge-framework §8](./design-asap-edge-framework.md#8-layer-5--control-plane)). The control-channel poll task and OTAP's native config-push deliver the same shape. |
| `NodeControlMsg::Shutdown` (graceful stop) | Cancel the control-channel task and flush ticker. Drain pending windows: one final `tick()` to emit any in-flight window state. Drop `Precompute`. |

This is the same pattern OTel's `tailsamplingprocessor` and
Telegraf's `allsketches` processor use (per ADR-0003 §3): host owns
nothing about the runtime's scheduling; the plugin spawns its own
task / goroutine and the host's reload machinery is treated as
"shutdown + rebuild," with state-preservation handled internally
by atomically swapping the `PrecomputeConfig` pointer the
control-channel task maintains.

The plugin emits via `effect_handler.send_message` (the OTAP
canonical out-channel), not as a return value from `process`, so
the plugin is honest about being a transducer: input batches are
consumed, output batches are fresh allocations of synthesized
sketch envelopes.

## 6. Plugin file layout

```
otap-patch/
├── plugins/
│   └── asap_sketches/
│       ├── Cargo.toml          // path = "../../../asap-precompute-rs"
│       ├── src/
│       │   ├── lib.rs           // OTAP plugin registration
│       │   │                    //   (linkme distributed-slice entry)
│       │   ├── receiver.rs      // OTAP receiver / processor impl;
│       │   │                    //   decode → observe → tick → emit
│       │   ├── config.rs        // plugin Config (mirrors PrecomputeConfig
│       │   │                    //   + AdapterConfig); TOML deserialize
│       │   └── stream.rs        // async Stream wrapper for emit-side
│       │                        //   RecordBatches
│       ├── tests/
│       │   └── lifecycle.rs     // end-to-end Tokio harness test
│       ├── README.md            // user-facing config docs
│       └── sample.toml          // canonical sample [asap_sketches] block
└── all/
    └── mod.rs                   // patches OTAP's submodule's plugin
                                 //   registry to bring asap_sketches
                                 //   into the linkme slice scope
```

The `otap-patch/` directory is a "patch overlay" applied onto the
upstream OTAP Dataflow submodule at build time, the same pattern
`telegraf-patch/` uses against the upstream `telegraf/` submodule
and `opentelemetry-collector-contrib-patch/` uses against the
upstream `opentelemetry-collector-contrib/` submodule. A new
`restore_otap_patches.sh` script (paralleling the existing
`restore_telegraf_patches.sh`) implements the copy-overlay
mechanic; the layout deliberately mirrors the Telegraf / OTel
sides so contributors moving between platforms see the same
shape.

## 7. Build pipeline — `build_asap_otap.sh`

Mirror `build_asap_otel.sh` and `build_asap_telegraf.sh`.
Steps:

1. Apply patches to the OTAP Dataflow submodule via
   `restore_otap_patches.sh` (registers `asap_sketches` plugin
   into the OTAP submodule's `linkme` registry module).
2. Resolve `[patch.crates-io]` / `[replace]` equivalent
   directives in OTAP's `Cargo.toml` (workspace root) for
   `asap-precompute-rs` and `asap_sketchlib` to local checkouts
   (sibling repos), the same pattern `build_asap_otel.sh`
   uses for `sketchlib-go` and `build_asap_telegraf.sh` uses
   for `asap-precompute-go`.
3. `cargo build --release` from the patched OTAP source tree.
4. Output: `otap-dataflow/target/release/asap-otap` binary
   (copied / symlinked to `otap/asap-otap` for parity with
   `telegraf/asap-telegraf`).

**Key build-system decision.** OTAP Dataflow is a Cargo workspace;
its plugin enumeration is generated at link time by `linkme`'s
distributed-slice machinery rather than by an external tool like
OCB or the Telegraf custom-builder. Adding our plugin to the
binary is a matter of (a) declaring the `asap_sketches` crate as
a workspace member and (b) ensuring the binary's main crate has
an `extern crate asap_sketches;` (or equivalent `use`) so the
linker pulls in the plugin's `linkme::distributed_slice` entry.
Both are one-line patches into upstream files. **No equivalent
of OCB needed** — `linkme`'s compile-time discovery is the
mechanism.

Pseudo-script (illustrative; not the actual file):

```bash
#!/usr/bin/env bash
# build_asap_otap.sh — Build the asap-otap OTAP-Rust distribution.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Apply patches via restore_otap_patches.sh (parses --skip-patches).
# 2. Wire local checkouts via [patch.crates-io] in OTAP's workspace
#    Cargo.toml (Cargo's equivalent of Go's replace directives) for
#    asap-precompute-rs and asap_sketchlib.
# 3. cargo build --release --bin asap-otap from the OTAP submodule.
# 4. Output: otap-dataflow/target/release/asap-otap.
```

Estimated final length ~80 LoC including error handling and the
patch-skip flag the OTel and Telegraf scripts already support.

## 8. Config example

OTAP config is TOML (matching the OTAP Dataflow project's existing
TOML conventions for pipeline definitions). Operators write one
`[plugins.asap_sketches]` block per ASAP precompute (multiple
blocks for multiple sketch types, distinguished by `id`):

```toml
[[pipelines.metrics.processors]]
id        = "asap_ddsketch"
plugin    = "asap_sketches"

  [pipelines.metrics.processors.config]
  ## Sketch algorithm — one of:
  ##   "ddsketch"         (quantiles, relative-error)
  ##   "kll"              (quantiles, rank-error)
  ##   "hll"              (cardinality)
  ##   "countsketch"      (frequency / top-k)
  ##   "countminsketch"   (frequency, biased upward)
  sketch_type = "ddsketch"

  ## Window size. Rust duration string (matches OTAP's existing
  ## duration parsing).
  window_size = "10s"

  ## Column name on the input RecordBatch carrying the observation
  ## value. For OTAP's standard metrics schema, this is the value
  ## column on the gauge / sum / histogram batch.
  ## Default: well-known OTel-Arrow value column name.
  value_column = "value"

  ## Sketch-specific parameters. Only the keys relevant to
  ## sketch_type are read; others are ignored.
  [pipelines.metrics.processors.config.params]
  alpha = 0.01           # ddsketch — relative accuracy
  # k = 200              # kll — buffer size
  # precision = 14       # hll — register count exponent
  # width = 2048         # countsketch / countminsketch — width
  # depth = 5            # countsketch / countminsketch — depth
  # heavy_hitters = 100  # countsketch — top-K heap size

  ## Output metric name. If unset, the codec uses the input metric
  ## name with a sketch-typed suffix (e.g. "_ddsketch") matching the
  ## OTel and Telegraf adapters' MetricSuffix default.
  output_metric_name = "http_request_duration_ms"

  ## Bootstrap — controller for plan delivery via HttpPollChannel.
  ## Per ADR-0003 §3, sketch-runtime params (sketch_type, window,
  ## matchers, sketch_params) above are also push-overridable from
  ## the controller plan; the values in this config are the
  ## bootstrap defaults, used until the first plan arrives.
  controller_url = "http://controller:8080"
  agent_id       = "otap-host-01"
```

Field reference (one line each):
- `sketch_type` — selects which sketch wrapper from
  `asap-precompute-rs/src/sketches/<type>` the plugin
  instantiates.
- `window_size` — Rust duration; passed to
  `PrecomputeConfig::window`.
- `value_column` — OTAP-specific; the codec reads this Arrow
  column for the float value.
- `params.*` — passed verbatim into `SketchParams`; the sketch
  wrapper validates which keys it requires.
- `output_metric_name` — passed into `AdapterConfig::metric_name`
  (OTAP-codec equivalent of the OTel and Telegraf codecs' same
  knob).
- `controller_url` / `agent_id` — bootstrap-only, never includes
  any runtime-mutable parameter, per ADR-0003 §3.

## 9. Reused vs new code

**Reused unchanged from existing runtime crate:**

- `asap-precompute-rs/src/` — Layer 3 runtime (windowing,
  snapshot caches, scheduler abstractions, matchers).
- `asap-precompute-rs/src/sketches/<ddsketch,kll,hll,countsketch,cms>/`
  — sketch wrappers implementing the `Sketch` /
  `QuantileSketch` / `CardinalitySketch` / `FrequencySketch`
  trait family.
- `asap-precompute-rs/src/control_channel.rs` — `ControlChannel`
  trait; the plugin uses an `HttpPollChannel` impl (shipped
  alongside the Vector adapter, paralleling the Telegraf side's
  Go `HttpPollChannel`).
- `asap_sketchlib/` — Layer 1 sketch algorithms.
- Wire format — `SketchEnvelope` proto, byte-identical across
  all adapters per the bandwidth invariant
  ([edge-framework §5.2](./design-asap-edge-framework.md#52-the-bandwidth-invariant)).
- Backend ingest path — already accepts envelopes from any source.

**New (OTAP-Rust-specific):**

| Component | Path | LoC estimate |
|---|---|---|
| OTAP codec | `asap-precompute-rs/src/otap/` | ~400 (decode_batch, encode_batch, config, seriesattrs, schema lookup, tests) |
| `asap_sketches` plugin | `otap-patch/plugins/asap_sketches/` | ~700 (Tokio async lifecycle, config translation, ticker wiring, control-channel task, factory + linkme entry, tests) |
| `linkme` registration patch | `otap-patch/all/mod.rs` | ~10 |
| Build script | `build_asap_otap.sh` | ~80 |
| **Total new** | | **~1200** |

Compare to ~1500 LoC for the Telegraf side (per
[telegraf integration §9](./design-asap-telegraf-integration.md#9-reused-vs-new-code))
and ~50 LoC per processor on the OTel side post-extraction. The
OTAP side sits between: less than Telegraf because the codec is
column-major (one walk of N columns) rather than per-metric
attribute-map traversal, and because OTAP's `Wakeup`-driven
scheduling replaces the explicit flush-ticker goroutine.

The Strategy-B wire format work (the well-known `_asap_envelope`
attribute key, sibling metadata keys, schema-validator handling) is
**inherited from the framework spec** rather than designed here —
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies)
already pinned the carrier to `AttributeValueType::Bytes` on the
per-row attribute child batch.

## 10. Open questions and risks

**OTAP submodule pinning.** OTAP Dataflow is pre-1.0, per
[edge-framework R6](./design-asap-edge-framework.md#10-risks):
workspace version `0.1.0`, `publish = false`, ~4 commits/day to
the dataflow tree, breaking changes on the Extension System /
capability registry / schema validators. Mitigation: pin a
specific commit SHA in `.gitmodules` and document it in
`build_asap_otap.sh`; plan a quarterly upgrade cadence with
regression tests; isolate ASAP's runtime from OTAP API churn so
only the `otap-patch/plugins/asap_sketches/` plugin takes the
upgrade hit when OTAP refactors; document the upgrade workflow
(pin new commit → `cargo build` → lifecycle harness → cross-host
parity → update SHA) in the plugin's README. **Current pin
(Phase D, 2026-05-05):**
[`29de46bb4dbff6e48b595459188f912b49373eed`](https://github.com/open-telemetry/otel-arrow/commit/29de46bb4dbff6e48b595459188f912b49373eed)
on `main`, recorded in `.gitmodules` and inlined into
`build_asap_otap.sh`'s header.

**Arrow schema stability.** OTel-Arrow's `OtapArrowRecords` schema
is settled but evolving (per the upstream `otel-arrow` repo's
SCHEMA-STABILITY.md). Pin to a specific Arrow column layout for
v1; document the update workflow alongside the OTAP submodule
pinning. Schema drift is a strictly upstream concern — when
OTel-Arrow tags a stable v1, the codec's column lookups are
frozen; until then, every OTAP submodule bump triggers a codec
audit.

**No legacy parity baseline.** Like Telegraf, this is greenfield.
OTAP-Rust has no existing ASAP implementation to verify
byte-equivalence against (unlike the OTel side, where ADR-0002's
behavior-preservation rule provided a strict gate). Correctness is
established by (1) direct unit tests on the codec's `decode_batch`
/ `encode_batch` round trip, (2) plugin lifecycle tests against the
in-tree OTAP `effect_handler` test utilities, and (3) **cross-host
envelope parity**: a `asap-otap` agent and a `asap-otel` /
`asap-telegraf` agent fed the same input stream MUST emit
byte-identical `SketchEnvelope::payload` bytes. Phase E covers
this — same shape as Phase 4 step E
(`integration/parity/golden_test.go`); feed the same input, hash
the payloads, assert equal.

**Cross-language byte parity (issue #243).** OTAP-Rust uses
`asap_sketchlib` (Rust); the existing OTel and Telegraf agents use
`sketchlib-go` (Go). The `SketchEnvelope::payload` byte format
must match across both implementations or a mixed fleet (some
hosts emitting via Rust, some via Go) will produce divergent
backend results. Issue #243 tracks the cross-language byte-parity
work; it is a **hard prerequisite** for production fleet mixing.
Until #243 closes, `asap-otap` deployments must be homogeneous
(all hosts in a controller plan on OTAP-Rust, or all on Telegraf /
OTel), and cross-host parity tests must pin to one runtime rather
than mix Go-encoded and Rust-encoded payloads in the same
assertion.

**Linkme + cross-crate registration.** `linkme`'s
distributed-slice macros must work across the
`asap-precompute-rs` ↔ `otap-patch/plugins/asap_sketches/`
boundary. Two failure modes to verify: (a) the plugin's `static`
slice entry must be reachable from the binary's main crate (linker
doesn't dead-code-eliminate the plugin module) — mitigated by
explicit `extern crate asap_sketches;` in `otap-patch/all/mod.rs`,
the same pattern OTAP's in-tree plugins use; (b) `linkme`'s
build-script behavior must be consistent across release profiles
and `cargo` versions — mitigated by pinning the `linkme` version
in the plugin's `Cargo.toml` to the exact version OTAP itself uses.

**OTAP's strict schema validator.** OTAP's
`crates/pdata/src/schema/payloads.rs::check_match` rejects any
extension column on Logs / Metrics / Traces RecordBatches —
documented at length in
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies).
The codec's encode side carries `_asap_envelope` and companion
metadata keys as **per-row attributes** (not as sibling
top-level columns); attribute-as-bytes round-trips through OTLP
proto's `AnyValue.bytes_value`. This is the path the audit
verified; the design doc references it but does not redesign it.

**No drop-in plugin ABI.** Per
[edge-framework §7.4](./design-asap-edge-framework.md#74-integration-model)
and R7. Distribution is via a custom-built binary (`asap-otap`),
not by dropping a `.so` into a stock OTAP Dataflow install. No
runtime plugin loader; users install the ASAP-flavored distro of
OTAP. Same model as `asap-otel` and `asap-telegraf` already
follow.

## 11. Phase plan

| Phase | Scope | Exit criterion |
|---|---|---|
| **A** | This doc — design alignment, no code. | Reviewed; section §11 of the framework doc updated to point at this doc as the Phase-5 source. |
| **B** | Codec implementation: `asap-precompute-rs/src/otap/` + minimal plugin shell that wires `decode_batch` / `encode_batch` against a stub `Precompute`. | `cargo test -p asap-precompute-rs --features otap` passes; plugin compiles. |
| **C** | Full `asap_sketches` plugin: all five sketch types via `sketch_type` dispatch, control-channel Tokio task, `Wakeup`-driven flush, lifecycle. | OTAP-harness lifecycle tests pass for each `sketch_type`; round-trip raw input → envelope output preserves expected sketch counts. |
| **D** | Build script (`build_asap_otap.sh`) + OTAP submodule patch (`otap-patch/all/mod.rs` registration). | `bash build_asap_otap.sh` produces a `asap-otap` binary that lists `asap_sketches` in its plugin registry. |
| **E** _(optional)_ | Cross-host envelope parity test — `asap-otap` agent and `asap-otel` / `asap-telegraf` agents fed identical input emit byte-identical `SketchEnvelope::payload`s. | E2E test passes; backend PromQL output is identical regardless of which agent produced the data. **Gated on issue #243** for the cross-language case (Go vs Rust payload bytes); the homogeneous-Rust case (asap-otap vs asap-otap, varied input sources) does not need #243. |

Phase A is this PR. Phases B–D are sized at roughly 1 week each
for an engineer familiar with the runtime + Tokio async; the
runtime extraction (ADR-0002) and the Telegraf adapter (Phase 4)
having already shipped is what makes this fit in 3–4 weeks rather
than 6+. Phase E is gated on #243 for the cross-language portion.

## 12. Decisions required

- [ ] Approve the unified `asap_sketches` plugin shape (single
      plugin parameterized by `sketch_type`, mirroring Telegraf's
      `allsketches`). §3 above is the rationale; it's the same
      argument as
      [telegraf integration §3](./design-asap-telegraf-integration.md#3-why-one-unified-allsketches-plugin-not-five).
- [ ] Confirm the OTAP submodule pinning + quarterly upgrade
      cadence (§10).
- [ ] Confirm the Strategy-B carrier choice
      (`AttributeValueType::Bytes` on per-row attribute child
      batch) — already pinned by edge-framework §7.2 but flagged
      for re-confirmation now that the codec is being designed.
- [ ] Confirm phase B–E sequencing; in particular, whether phase
      E (cross-host parity) is a release-gate or a post-release
      regression test, and how it sequences against #243.

Once those land, phase B can start immediately; the runtime
dependency (`asap-precompute-rs`) is already on `main` (Phase 3
shipped in #241 / #242).

## References

- [`docs/design-asap-edge-framework.md`](./design-asap-edge-framework.md)
  — five-layer model, bandwidth invariant, Strategy A/B,
  per-platform encoding, `Adapter` / `ControlChannel` traits,
  R6 (OTAP pre-1.0), §7.4 (integration model).
- [`design-asap-telegraf-integration.md`](./design-asap-telegraf-integration.md)
  — Phase-4 adapter design that this Phase-5 design mirrors
  structurally (two-layer split, unified-plugin shape, build
  pipeline).
- [`docs/adr/adr-0002-extract-precompute-runtime.md`](./adr/adr-0002-extract-precompute-runtime.md)
  — runtime contract that the OTAP plugin reuses.
- [`docs/adr/adr-0003-adapter-trait-and-control-channel.md`](./adr/adr-0003-adapter-trait-and-control-channel.md)
  — adapter shape and control-channel rule that this design
  follows.
- [`asap-precompute-rs/src/`](../asap-precompute-rs/src/) —
  runtime crate (Phase 3, shipped) that the OTAP plugin
  depends on.
- [`asap-precompute-go/otel/`](../asap-precompute-go/otel/) and
  [`asap-precompute-go/telegraf/`](../asap-precompute-go/telegraf/)
  — reference codec shapes that `asap-precompute-rs/src/otap/`
  mirrors.
- [`telegraf-patch/`](../telegraf-patch/) — patch-overlay
  structure that `otap-patch/` mirrors.
- [`build_asap_otel.sh`](../build_asap_otel.sh) and
  [`build_asap_telegraf.sh`](../build_asap_telegraf.sh) —
  build pipelines that `build_asap_otap.sh` mirrors.
- Issue #243 — cross-language byte-parity tracker; hard
  prerequisite for production OTAP-Rust ↔ Telegraf / OTel
  fleet mixing.
