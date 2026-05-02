# ADR-0003: `Adapter` trait, `ControlChannel` trait, and Strategy A/B encoding

| | |
|---|---|
| Status | **Proposed** (gates Phase 4 / Phase 5 / Phase 6 of the edge-framework migration) |
| Date | 2026-05-02 |
| Deciders | Project ASAP maintainers |
| Supersedes | n/a |
| Superseded by | n/a |

## Context

ASAP's edge precompute runtime needs to ride into multiple host
runtimes (OTel Collector, Telegraf, Vector, OTAP Dataflow). Each
host has its own data model (`pmetric.Metrics` / `telegraf.Metric`
/ `vector::Event` / `arrow.RecordBatch`), config format, and
reload semantics. Without explicit contracts for how the runtime
plugs into each host, every adapter would diverge in subtle ways
and the cross-platform invariants the framework promises (single
sketch source-of-truth, stable wire format, plan-driven
configuration) would erode.

Three contracts need pinning before adapter implementation
starts:

1. The **`Adapter` trait** that every Layer-4 shim implements.
2. The **`ControlChannel` trait** that delivers
   `PrecomputeConfig` from the controller to the runtime.
3. The **Strategy A vs Strategy B encoding choice** for how
   `SketchEnvelope` rides each host's native event model.

This ADR pins all three.

## Decision

### 1. `Adapter` trait

```rust
pub trait Adapter {
    type Event;     // pmetric.Metrics, telegraf.Metric, vector.Event, arrow.RecordBatch

    fn decode<'a>(&self, ev: &'a Self::Event) -> Result<Vec<Observation<'a>>, Error>;
    fn encode(&self, envelopes: Vec<SketchEnvelope>) -> Result<Self::Event, Error>;
    fn schedule_tick(&self, period: Duration, cb: TickCallback);
    fn emit_telemetry(&self, stats: &PrecomputeStats);
}
```

Go side has an idiomatic equivalent.

Adapters emit only raw `Observation`s. `agg_id` resolution
(matchers → agg_id) is the runtime's responsibility, not the
adapter's. This is non-negotiable: the runtime is the single
source of truth for plan binding; adapters do not duplicate
matcher logic.

### 2. `ControlChannel` trait

```rust
pub trait ControlChannel: Send {
    fn poll(&mut self) -> Option<PrecomputeConfigSet>;
    fn ack(&mut self, plan_version: u64);
}
```

Three implementations land in tree:

| Impl | Used by | Status |
|---|---|---|
| `OpAmpChannel` | OTel Collector existing deploys | already implemented in `controller/src/opamp/` |
| `HttpPollChannel` | All non-OTel adapters; new `_asap-otelcol_` deployments | new, ~30–50 LoC. Polls controller's existing `GET /api/v1/plan?host_id=<id>` endpoint. |
| `FileWatchChannel` | Sidecar fallback for environments where outbound HTTP isn't allowed | new, last-resort. |

### 3. Hard rule: `ControlChannel` runs in an internal goroutine / task

Every adapter, including OTel, runs its `ControlChannel` inside
its own owned goroutine (Go) / tokio task (Rust). The adapter
maintains an `atomic.Pointer[PrecomputeConfig]` (Go) /
`Arc<ArcSwap<PrecomputeConfig>>` (Rust) that the hot path reads
on each `Observe` / `Tick`.

This rule is forced by an audit finding: every host platform's
native reload mechanism (OTel Collector's SIGHUP + OpAMP
supervisor; Telegraf's `--watch-config`; Vector's
`reload_config_and_respawn`; OTAP's `NodeControlMsg::Config`)
**rebuilds** the plugin instance and clobbers in-memory sketch
state. ASAP cannot route plan pushes through any of those
without losing all sketches. The internal-goroutine pattern is
precedented in OTel's `tailsamplingprocessor`.

Configuration in the host's config file is **bootstrap-only**:
controller URL, agent ID, auth. Sketch parameters (sketch type,
window size, matchers, max_series, delta thresholds) are
never in host config; they arrive only via `ControlChannel::poll`.

### 4. Strategy A vs Strategy B encoding

Every adapter picks one strategy for how `SketchEnvelope.payload`
rides its native event:

- **Strategy A — Native typed variant.** Extend the host's
  schema with a sketch-typed oneOf / variant. Realistic only
  for OTel-family platforms. ASAP today uses Strategy A for
  OTel Collector via modified-OTLP `pmetric.Metric.data` oneOf.
- **Strategy B — Opaque bytes in the host's nearest binary-clean
  carrier.** Use whatever bytes/binary field the native event
  already exposes, marked with the standardized
  `_asap_envelope` + metadata keys. Required for Telegraf,
  Vector, OTAP.

Standardized well-known keys (project-level standard; every
adapter using Strategy B uses exactly these spellings):

| Key | Logical type | Required? | Meaning |
|---|---|---|---|
| `_asap_envelope` | bytes | yes | `SketchEnvelope.payload` proto bytes |
| `_asap_sketch_type` | string | yes | `"DDSketch"` / `"KLLSketch"` / etc. |
| `_asap_agg_id` | uint64 | yes | matches controller plan |
| `_asap_schema_version` | uint32 | yes | matches `SketchEnvelope.schema_version` |
| `_asap_window_start_ms` | uint64 | yes | window lower bound |
| `_asap_window_end_ms` | uint64 | yes | window upper bound |
| `_asap_encoding` | string | optional | `PROTO_FULL` (default) / `PROTO_DELTA` / `MSGPACK` |

Per-platform Strategy-B carrier (verified against current upstream
source):

| Platform | Carrier for `_asap_envelope` | Notes |
|---|---|---|
| Telegraf | `string`-typed field on `telegraf.Metric` holding 8-bit-clean envelope bytes | `[]byte` is coerced to `string` by `convertField`; bytes survive but the type tag is lost. A custom `asap` Telegraf Serializer is required for HTTP / Kafka / File sinks. InfluxDB line-protocol and Prometheus remote-write are not supported sinks. |
| Vector | `Value::Bytes(Bytes)` field on a `LogEvent` | NOT `MetricValue::Sketch` (hard-typed to `AgentDDSketch`). Required sinks: `vector` native (Protobuf), Kafka with `encoding=native\|raw_message`, S3 with `encoding=native`. |
| OTAP Dataflow | `AttributeValueType::Bytes` on the per-row attribute child batch of `OtapArrowRecords` | NOT a sibling top-level Binary column — OTAP's strict schema validator rejects extension columns. Encoding as an attribute is OTLP round-trippable. |

### 5. Bandwidth invariant — sketches stay sketches end-to-end

Every adapter MUST preserve the framework's bandwidth invariant
(see design doc §5.2): `SketchEnvelope.payload` bytes flow
end-to-end without ever being "exploded" back to per-sample
observations. Adapters that fail this invariant break the entire
framework's bandwidth promise. CI signal: encoded byte-count vs
raw-sample byte-count ratio for any adapter must match the
sketch's expected compression ratio.

## Consequences

### Positive

- Adapters get a clear, narrow contract. Changing an adapter
  doesn't touch runtime / wire / control-plane code.
- ControlChannel becomes a single abstraction shared across all
  four hosts. The same controller can serve all four adapters
  via either OpAMP or HTTP-poll without per-host customization.
- Strategy A/B explicit means future platforms have a clear
  template. "Take the project's nearest binary-clean field,
  attach the well-known keys" is the recipe.

### Negative

- Per-platform Strategy-B carrier is non-uniform. The doc
  acknowledges this; adapter implementers must read the
  per-platform table carefully.
- Internal-goroutine pattern means each adapter ships a copy of
  the same controller-poll loop. ~50 LoC duplication is
  acceptable; the alternative (a shared library with a `Send +
  Sync` poll task) adds Go/Rust async-runtime coupling that's
  worse than the duplication.

### Compatibility

- OTel adapter today already emits via Strategy A. No
  wire-format change.
- New OTel deployments using `HttpPollChannel` instead of OpAMP
  are a deploy-time choice; the controller serves both.

## References

- [`docs/design-asap-edge-framework.md`](../design-asap-edge-framework.md) §5 (envelope), §6 (Precompute trait), §7 (Adapter + Strategy A/B), §8 (ControlChannel)
- ADR-0001 (sketch-core retirement) — establishes that `SketchEnvelope` is the canonical wire type
- ADR-0002 (extract precompute runtime) — defines the runtime that adapters wrap
- OTel `tailsamplingprocessor` — precedent for internal-goroutine config update pattern
