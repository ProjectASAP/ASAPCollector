# ASAP Telegraf Integration — Design

_Status: **draft** — 2026-05-02. Doc-only; gates the Telegraf
adapter work that becomes Phase 4 of the edge-framework migration._

This document is a focused supplement to
[`design-asap-edge-framework.md`](./design-asap-edge-framework.md) and
the runtime / adapter ADRs ([ADR-0002](./adr/adr-0002-extract-precompute-runtime.md),
[ADR-0003](./adr/adr-0003-adapter-trait-and-control-channel.md)). It
covers only what is specifically new for the Telegraf host. The
five-layer model, the bandwidth invariant, the `Adapter` /
`ControlChannel` traits, the Strategy A / B encoding split, and the
`SketchEnvelope` wire format are defined there; this doc references
them and does not restate them.

## 1. Goal

Ship a single Telegraf binary `asap-telegraf` that includes all five
sketch types (DDSketch, KLL, HLL, CountSketch, CountMinSketch) as a
unified `processors.allsketches` plugin, using the same
`asap-precompute-go` runtime as the existing OTel `asap-otel`
binary. Same wire format (`SketchEnvelope`), same backend ingest path,
same controller plan delivery (HTTP poll), same Strategy-B carrier on
egress.

Operators choose between Telegraf and OTel agents based on whatever
telemetry pipeline they already operate; ASAP doesn't care which they
pick. End-state deployment story per
[edge-framework §7.4](./design-asap-edge-framework.md#74-integration-model):

```
host operator picks:
  ─── existing OTel pipeline ──► asap-otel
  ─── existing Telegraf pipeline ──► asap-telegraf
                                       ↓
                             same SketchEnvelope bytes
                                       ↓
                             same gateway / backend
```

## 2. Architecture — the two-layer split

Mirror the OTel structure exactly. Two new directories, no other
moving parts:

```
                   ┌──────────────────────────────────────────────────┐
                   │  telegraf-patch/processors/allsketches/           │
                   │     ── Layer 4 lifecycle PLUGIN                   │
                   │     - StreamingProcessor.Add / Stop               │
                   │     - flush ticker, sample.conf, registration     │
                   │     - patches into upstream Telegraf at build     │
                   └────────────────────┬─────────────────────────────┘
                                        │  uses
                                        ▼
                   ┌──────────────────────────────────────────────────┐
                   │  asap-precompute-go/telegraf/                     │
                   │     ── Layer 4 CODEC                              │
                   │     - Decode: telegraf.Metric → []Observation     │
                   │     - Encode: []SketchEnvelope → telegraf.Metric  │
                   │     - no Telegraf lifecycle, pure transformation  │
                   └────────────────────┬─────────────────────────────┘
                                        │  uses
                                        ▼
                   ┌──────────────────────────────────────────────────┐
                   │  asap-precompute-go/  ── Layer 3 RUNTIME          │
                   │  asap-precompute-go/sketches/  ── sketch wrappers │
                   │  asap-precompute-go/controlchannel/  ── plan poll │
                   │     (REUSED UNCHANGED from asap-otel build) │
                   └──────────────────────────────────────────────────┘
```

The split is the same one ADR-0002 / ADR-0003 already pinned for the
OTel side — the only difference is the host: `pmetric.Metrics` →
`telegraf.Metric`. The runtime, sketch wrappers, snapshot caches,
matchers, window manager, and `ControlChannel` impls move zero bytes.

Concretely:

- `asap-precompute-go/telegraf/` — the Telegraf **codec**. Pure
  data-shape translation, no lifecycle. Owns no goroutines, no
  tickers, no acc state. Mirrors the existing
  [`asap-precompute-go/otel/`](../asap-precompute-go/otel/) directory
  in shape (`adapter.go`, `config.go`, `decode.go`, `encode.go`,
  `seriesattrs.go` plus tests).
- `telegraf-patch/processors/allsketches/` — the Telegraf **plugin**.
  Implements `processors.StreamingProcessor`, owns the flush ticker,
  the `Precompute` instance, the control-channel goroutine, and the
  config-block translation.
- Everything below the codec — the runtime, sketches wrappers,
  controlchannel, `SketchEnvelope` proto types, the `sketchlib-go`
  algorithm crate, the backend ingest path — is reused unchanged.

## 3. Why one unified `allsketches` plugin (not five)

The OTel side ships five processors —
`ddsketchprocessor`, `kllprocessor`, `hllprocessor`,
`countsketchprocessor`, `countminsketchprocessor`. Telegraf will NOT
mirror that split. It ships **one** plugin called `allsketches` with a
`sketch_type` config field. Reasoning:

1. **Telegraf convention is capability-grouped, not type-per-plugin.**
   Existing Telegraf processors like `processors.aggregator`,
   `processors.regex`, `processors.template` are single plugins
   parameterized by a config block, not five sibling plugins by
   variant. Five `[[processors.ddsketch]]` `[[processors.kll]]` …
   blocks would feel un-Telegraf-y to operators.
2. **The five sketches share lifecycle.** Window rotation, flush
   ticker, config-reload semantics, control-channel poll, snapshot
   cache — all identical across sketch types. Splitting buys nothing;
   the per-sketch differences live entirely in the
   `asap-precompute-go/sketches/<type>` wrappers, which the unified
   plugin dispatches into via `sketch_type`.
3. **One config block per ASAP deployment is simpler.** Operators
   write one `[[processors.allsketches]]` with `sketch_type =
   "ddsketch"` rather than choosing between five spelled-out plugin
   names. Multi-sketch deployments (DDSketch on metric A, HLL on
   metric B) just declare two `[[processors.allsketches]]` blocks
   with different `sketch_type` and different `namepass` / `tagpass`
   filters — Telegraf already handles plugin-instance multiplicity
   natively.
4. **Easier to ship.** One `register()` line patches Telegraf's
   `plugins/processors/all/all.go` instead of five. One sample.conf,
   one README, one set of tests-against-host fixtures.

This **differs from OTel** because the OTel collector's existing tree
already had five separate processors at the time ADR-0002 was written,
and ADR-0002 promised "behavior preservation, ~50-line shim per
processor" — five files because there were already five processors to
preserve. **Telegraf is greenfield**: no parity-preservation
constraint, no existing five-plugin user base. Designing the right
shape from day one is cheaper than carrying five plugins and
collapsing them later.

## 4. Data model mapping — `telegraf.Metric` ↔ `Observation`

The codec's job is exactly the same as
[`asap-precompute-go/otel/decode.go`](../asap-precompute-go/otel/decode.go):
walk the host event, extract `(timestamp, name, labels, value)`
tuples, emit `[]Observation`. Telegraf's data model is flatter than
OTel's, which simplifies most fields and complicates one (multi-field
metrics).

| Concept | OTel (`pmetric`) | Telegraf (`telegraf.Metric`) | Maps to `Observation` field |
|---|---|---|---|
| metric name | `pmetric.Metric.Name()` | `metric.Name()` | `Observation.Metric` |
| labels | `dp.Attributes()` (per-data-point) | `metric.Tags()` (per-metric) | `Observation.Labels` |
| resource attrs | `ResourceMetrics.Resource()` | (none — Telegraf is flatter) | `Observation.ResourceLabels` (always empty for Telegraf) |
| timestamp | `dp.Timestamp()` | `metric.Time()` | `Observation.TimestampMs` |
| value | `dp.DoubleValue()` / `IntValue()` | `metric.Fields()[<value_field>]` | `Observation.Value.Float` |
| pre-aggregated sketch input | typed DP variants (Strategy A) | `metric.Fields["_asap_envelope"]` (8-bit-clean string per Strategy B) | `Observation.Value.Envelope` |

Four points need addressing explicitly:

**Resource attrs (gap from OTel).** Telegraf is flatter — there is no
ResourceMetrics analogue. Resource-level information (host name,
service name, region) is normally encoded as Telegraf tags, which the
codec maps to `Observation.Labels`. The codec emits `nil` for
`ResourceLabels`. The runtime's existing `OmitResourceAttrs=true`
config flag (which CountSketch / CMS already use, see
`asap-precompute-go/config.go`) is the natural default for the
Telegraf adapter's `PrecomputeConfig`. No new runtime knob needed; we
just default-true a flag the runtime already understands.

**Multi-field metrics (deferred to v2).** A single `telegraf.Metric`
can carry multiple numeric fields — a hostmetrics-style `cpu` metric
typically has `usage_user`, `usage_system`, `usage_idle`, etc. The v1
plugin config exposes a `value_field` (string) selecting **one**
field as the observation value; other fields on the same metric are
ignored. Multi-field decode (one `Observation` per matched field, with
a synthesized metric name like `cpu.usage_user`) is documented as
future work and tracked in the open-questions section.

**Pre-aggregated sketch input (KindEnvelope path).** When a Telegraf
input upstream sends an already-aggregated sketch (typical multi-hop
case: an edge `asap-telegraf` flushes envelopes to a gateway
`asap-telegraf` for re-aggregation), the envelope rides as a
Strategy-B field on the `telegraf.Metric`. The codec recognizes the
well-known field name `_asap_envelope` (verbatim per
[ADR-0003 §4](./adr/adr-0003-adapter-trait-and-control-channel.md#4-strategy-a-vs-strategy-b-encoding))
and routes it through `Precompute.ObserveEnvelope` instead of
`Observe`. Companion fields (`_asap_sketch_type`, `_asap_agg_id`,
`_asap_schema_version`, `_asap_window_start_ms`, `_asap_window_end_ms`,
`_asap_encoding`) are read alongside.

**Binary-friendly carrier.** `telegraf.Metric.Fields` is
`map[string]interface{}`; `[]byte` is silently coerced to `string` by
`metric.convertField()`. Bytes survive (Go strings are 8-bit-clean)
but the type tag is lost. This is the same constraint
[edge-framework §7.2](./design-asap-edge-framework.md#72-two-encoding-strategies)
already documented: the codec stores envelope bytes as
`string(envelopeBytes)` on encode and re-reads them via
`field.(string)` on decode. **A custom `asap` Telegraf Serializer
for HTTP / Kafka / File sinks is required** — InfluxDB line-protocol
and Prometheus remote-write sinks are not supported (they would force
base64 +33%). The serializer is a sibling concern and lives outside
the `allsketches` processor; this design doc treats it as
out-of-scope (a separate plugin under
`telegraf-patch/serializers/asap/`, not addressed here).

## 5. Plugin lifecycle — Telegraf `StreamingProcessor`

Telegraf has two processor interfaces:

- **`processors.Processor`** — synchronous, `Apply(in
  ...telegraf.Metric) []telegraf.Metric`. Stateless-friendly, no
  start/stop hooks, no scheduler access. **Not enough**: ASAP needs a
  flush ticker that emits envelopes on a schedule independent of
  inbound traffic, and we cannot get that from a pure `Apply()`.
- **`processors.StreamingProcessor`** — asynchronous, `Add(metric,
  acc)` plus `Start(acc)` / `Stop()` lifecycle. Allows scheduled
  emission via `acc.AddMetric(out)` from a goroutine the plugin
  itself owns. **This is what the plugin uses.**

Mapping Telegraf's lifecycle methods onto the runtime contract:

| Telegraf method | What the plugin does |
|---|---|
| `Init() error` | Validate config; resolve `sketch_type` to one of the five `asap-precompute-go/sketches/<type>` factories; build a `PrecomputeConfig` from the config block; **don't** start anything yet (Telegraf re-Init's on reload). |
| `Start(acc telegraf.Accumulator)` | Stash `acc`. Construct `Precompute` instance with the resolved sketch + config. Spawn the control-channel poll goroutine (`HttpPollChannel`). Spawn the flush-ticker goroutine that fires `flush_interval`-aligned ticks. |
| `Add(metric, acc)` | `codec.Decode(metric) → []Observation`; for each obs, route to `Precompute.Observe` (Float / Hash / Bytes path) or `Precompute.ObserveEnvelope` (Envelope path). Drop the input metric (Telegraf processors that emit different output drop input, like `processors.aggregator`). |
| ticker callback (every `flush_interval`) | `Precompute.Tick(now)` → `[]SketchEnvelope` → `codec.Encode` → `acc.AddMetric(out)` for each emitted metric. |
| `Stop()` | Cancel the control-channel goroutine and flush ticker. Drain pending windows: one final `Tick` to emit any in-flight window state. Close `Precompute`. |

This is the same pattern OTel's `tailsamplingprocessor` uses (per
ADR-0003 §3): host owns nothing about the runtime's scheduling; the
plugin spawns its own goroutines and the host's reload machinery is
treated as "shutdown + rebuild", with state-preservation handled
internally by atomically swapping the `PrecomputeConfig` pointer the
control-channel goroutine maintains.

The plugin emits via `acc.AddMetric` (not as `Add()`'s return value),
so `Capabilities.SupportsMetricFiltering()` and
`Capabilities.MutatesMetric()` are both honest: input metrics are
consumed (filtered out of the downstream pipeline), output metrics
are fresh allocations.

## 6. Plugin file layout

```
telegraf-patch/
├── processors/
│   └── allsketches/
│       ├── allsketches.go      // Telegraf StreamingProcessor impl;
│       │                       //   Start/Add/Stop, flush ticker,
│       │                       //   control-channel goroutine wiring
│       ├── allsketches_test.go // table-driven plugin lifecycle tests
│       ├── config.go           // plugin Config struct (TOML tags);
│       │                       //   maps to PrecomputeConfig + AdapterConfig
│       ├── factory.go          // processors.Add("allsketches", ...) registration
│       ├── README.md           // user-facing config docs
│       └── sample.conf         // canonical sample [[processors.allsketches]] block
└── all/
    └── all.go                  // patches telegraf's plugins/processors/all/all.go
                                //   to import _ "…/processors/allsketches"
```

The `telegraf-patch/` directory is a "patch overlay" applied onto the
upstream Telegraf submodule at build time, the same pattern
`opentelemetry-collector-contrib-patch/` uses against the upstream
`opentelemetry-collector-contrib/` submodule. The existing
`restore_telegraf_patches.sh` already implements the
copy-overlay mechanic for older `aggregators/` patches; the new
`processors/allsketches/` directory plugs into that same script
without changes.

## 7. Build pipeline — `build_asap_telegraf.sh`

Mirror `build_asap_otel.sh`. Steps:

1. Apply patches to the `telegraf` submodule via
   `restore_telegraf_patches.sh` (registers `allsketches` plugin
   into `plugins/processors/all/all.go`).
2. Resolve `replace` directives for `asap-precompute-go` and
   `sketchlib-go` to local checkouts (sibling repos), the same
   pattern `build_asap_otel.sh` uses for `sketchlib-go`.
3. `go build` from the patched Telegraf source tree.
4. Output: `telegraf/asap-telegraf` binary.

**Key build-system decision:** Telegraf has its own custom-build
support (the `telegraf` repo includes a `--build_tags "custom"`
option, plus the official `telegraf-custom-builder` tool published at
`tools/custom_builder/`), but the cleanest fit is a plain `go build`
from the patched fork because we control the import graph fully. The
custom-builder is designed to **subset** stock Telegraf to a smaller
set of plugins, not to **add** out-of-tree plugins; for our case,
where the new plugin is patched into upstream's `all.go`, vanilla
`go build` is simpler. **No equivalent of OCB needed** — this is
actually simpler than the OTel side, where OCB is mandatory because
the OTel collector's plugin enumeration is generated, not patched.

Pseudo-script (illustrative; not the actual file):

```bash
#!/usr/bin/env bash
# build_asap_telegraf.sh
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Apply patches
bash "${ROOT_DIR}/restore_telegraf_patches.sh"

# 2. Resolve replace directives for sibling checkouts
TELEGRAF_DIR="${ROOT_DIR}/telegraf"
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/sketchlib-go" \
       "${TELEGRAF_DIR}/go.mod"; then
  echo "replace github.com/ProjectASAP/sketchlib-go => ${ROOT_DIR}/../sketchlib-go" \
       >> "${TELEGRAF_DIR}/go.mod"
fi
if ! grep -qE "^replace[[:space:]]+github\.com/ProjectASAP/asap-precompute-go" \
       "${TELEGRAF_DIR}/go.mod"; then
  echo "replace github.com/ProjectASAP/asap-precompute-go => ${ROOT_DIR}/asap-precompute-go" \
       >> "${TELEGRAF_DIR}/go.mod"
fi

# 3. Build
cd "${TELEGRAF_DIR}"
go build -o asap-telegraf ./cmd/telegraf

echo "Build successful: ${TELEGRAF_DIR}/asap-telegraf"
```

Estimated final length ~80 LoC including error handling and the
patch-skip flag the OTel side already supports.

## 8. Config example

Telegraf config is TOML. Operators write one
`[[processors.allsketches]]` block per ASAP precompute (multiple
blocks for multiple sketch types):

```toml
[[processors.allsketches]]
  ## Sketch algorithm — one of:
  ##   "ddsketch"         (quantiles, relative-error)
  ##   "kll"              (quantiles, rank-error)
  ##   "hll"              (cardinality)
  ##   "countsketch"      (frequency / top-k)
  ##   "countminsketch"   (frequency, biased upward)
  sketch_type = "ddsketch"

  ## Window size. Telegraf uses Go duration strings (matches
  ## Telegraf's existing `flush_interval` style).
  window_size = "10s"

  ## Field on the input metric carrying the observation value.
  ## Multi-field input metrics are supported only by selecting one
  ## field via this knob; multi-field-fanout is future work (§10).
  ## Default "value".
  value_field = "value"

  ## Sketch-specific parameters. Only the keys relevant to
  ## sketch_type are read; others are ignored.
  [processors.allsketches.params]
    alpha = 0.01           # ddsketch — relative accuracy
    # k = 200              # kll — buffer size
    # precision = 14       # hll — register count exponent
    # width = 2048         # countsketch / countminsketch — width
    # depth = 5            # countsketch / countminsketch — depth
    # heavy_hitters = 100  # countsketch — top-K heap size

  ## Output metric name. If unset, the codec uses the input metric
  ## name with a sketch-typed suffix (e.g. "_ddsketch") matching the
  ## OTel adapter's MetricSuffix default.
  output_metric_name = "http_request_duration_ms"

  ## Bootstrap — controller for plan delivery via HttpPollChannel.
  ## Per ADR-0003 §3, sketch-runtime params (sketch_type, window,
  ## matchers, sketch_params) above are also push-overridable from
  ## the controller plan; the values in this config are the
  ## bootstrap defaults, used until the first plan arrives.
  controller_url = "http://controller:8080"
  agent_id       = "telegraf-host-01"
```

Field reference (one line each):
- `sketch_type` — selects which sketch wrapper from
  `asap-precompute-go/sketches/<type>` the plugin instantiates.
- `window_size` — Go duration; passed to
  `PrecomputeConfig.Window`.
- `value_field` — Telegraf-specific; the codec reads this field for
  the float value.
- `params.*` — passed verbatim into `SketchParams`; sketch wrapper
  validates which keys it requires.
- `output_metric_name` — passed into `AdapterConfig.MetricName`
  (telegraf-codec equivalent of the OTel codec's same knob).
- `controller_url` / `agent_id` — bootstrap-only, never includes
  any runtime-mutable parameter, per ADR-0003 §3.

## 9. Reused vs new code

**Reused unchanged from existing asap-otel build:**

- `asap-precompute-go/` — Layer 3 runtime (windowing, snapshot
  caches, scheduler abstractions, matchers).
- `asap-precompute-go/sketches/<ddsketch,kll,hll,countsketch,countminsketch>/`
  — sketch wrappers implementing the `Sketch` /
  `QuantileSketch` / `CardinalitySketch` / `FrequencySketch`
  interface family.
- `asap-precompute-go/controlchannel/` — `HttpPollChannel` and
  `OpAmpChannel` impls; the plugin uses `HttpPollChannel`.
- `sketchlib-go/` — Layer 1 sketch algorithms.
- Wire format — `SketchEnvelope` proto, byte-identical across all
  adapters per the bandwidth invariant
  ([edge-framework §5.2](./design-asap-edge-framework.md#52-the-bandwidth-invariant)).
- Backend ingest path — already accepts envelopes from any source.

**New (Telegraf-specific):**

| Component | Path | LoC estimate |
|---|---|---|
| Telegraf codec | `asap-precompute-go/telegraf/` | ~500 (decode, encode, config, seriesattrs, tests) |
| `allsketches` plugin | `telegraf-patch/processors/allsketches/` | ~800 (lifecycle glue, config translation, ticker wiring, factory, tests) |
| `all.go` registration patch | `telegraf-patch/all/all.go` | ~10 |
| Build script | `build_asap_telegraf.sh` | ~80 |
| **Total new** | | **~1500** |

Compare to ~4000 LoC the OTel side took before the runtime extraction
(five processors × ~800 LoC each, including duplicated windowing /
snapshot / matcher logic). Telegraf is much cheaper because the
runtime extraction of ADR-0002 is already done — the plugin is
literally just lifecycle glue + codec.

The custom `asap` Telegraf Serializer (~80 LoC) at
`telegraf-patch/serializers/asap/` is **excluded from this estimate**
because it's a sibling concern; it doesn't live inside the
`allsketches` plugin, ships in its own design / PR, and is required
only for HTTP / Kafka / File sinks (InfluxDB and Prometheus-RW sinks
are explicitly unsupported per
[edge-framework §7.3](./design-asap-edge-framework.md#73-per-platform-integration)).

## 10. Open questions and risks

**No legacy parity baseline.** Telegraf doesn't have an existing ASAP
implementation to verify byte-equivalence against (unlike the OTel
side, where ADR-0002's behavior-preservation rule provided a strict
gate). Correctness is established by:
1. Direct unit tests against the codec's `Decode` / `Encode` round
   trip.
2. Plugin lifecycle tests against an in-memory `telegraf.Accumulator`
   (Telegraf's stock test harness).
3. **Cross-host envelope parity**: a `asap-telegraf` agent and a
   `asap-otel` agent fed the same input stream (e.g.
   identical synthetic Prometheus scrape) MUST emit byte-identical
   `SketchEnvelope.payload` bytes. The backend ingest path, being
   strategy-blind, then produces identical PromQL output. This is
   the hard test; phase E covers it.

**Multi-field metrics (deferred to v2).** Telegraf's native model has
multi-field metrics first-class; v1 picks one via `value_field` and
ignores the rest. Future v2 work: a `value_fields = ["usage_user",
"usage_system"]` mode that emits one `Observation` per matched field
with a synthesized metric name. Out of scope for the initial design.

**Telegraf upstream churn.** Telegraf 1.x has been stable since 2017;
the `processors.StreamingProcessor` interface specifically has been
stable since v1.10 (2019). Risk is low but non-zero — pin the
submodule to a tagged release and bump on a quarterly cadence with a
regression test before the upgrade lands. Same model
`build_asap_otel.sh` already uses for the OTel collector
submodule.

**Config-reload semantics.** Telegraf supports `--watch-config` and
SIGHUP-driven reload, both of which rebuild the plugin instance and
clobber in-memory sketch state. This is the same problem audited
across all four hosts in
[edge-framework §8](./design-asap-edge-framework.md#8-layer-5--control-plane).
Mitigation is the same as for OTel: the host config file carries
**bootstrap-only** parameters (controller URL, agent ID, auth);
runtime parameters arrive via the internal
`HttpPollChannel`, which lives inside the plugin's own goroutine and
is not invalidated by Telegraf's reload machinery. Already in place
in `asap-precompute-go/controlchannel/`; the Telegraf plugin reuses
it identically.

**What Telegraf is NOT good for.** Two scope clarifications:

- **Native histogram / OTel histogram inputs are rare in the Telegraf
  ecosystem.** Telegraf inputs that natively produce histograms (e.g.
  the `prometheus` input scraping a Prometheus histogram) materialize
  them as a series of `_bucket` numeric fields, not as a typed
  histogram object. The codec treats those as ordinary scalar metrics
  on the `value_field`-selected field. Strategy-A-style native sketch
  variants (the way modified-OTLP carries DDSketch on the wire) have
  no Telegraf equivalent and aren't in scope.
- **Trace data is not in Telegraf's domain.** ASAP's trace processing
  (such as it is) lives entirely in the OTel side; `asap-telegraf`
  is a metrics-only artifact.

## 11. Phase plan

| Phase | Scope | Exit criterion |
|---|---|---|
| **A** | This doc — design alignment, no code. | Reviewed; section §11 of the framework doc updated to point at this doc as the Phase-4 source. |
| **B** | Codec implementation: `asap-precompute-go/telegraf/` + minimal plugin shell that wires `Decode` / `Encode` against a stub `Precompute`. | `go test ./asap-precompute-go/telegraf/...` passes; plugin compiles. |
| **C** | Full `allsketches` plugin: all five sketch types via `sketch_type` dispatch, control-channel goroutine, flush ticker, lifecycle. | Telegraf-harness unit tests pass for each `sketch_type`; round-trip raw input → envelope output preserves expected sketch counts. |
| **D** | Build script (`build_asap_telegraf.sh`) + Telegraf submodule patch (`telegraf-patch/all/all.go` registration). | `bash build_asap_telegraf.sh` produces a `asap-telegraf` binary that `--list-processors` includes `allsketches`. |
| **E** | Cross-host envelope parity test — `asap-telegraf` agent and `asap-otel` agent fed identical input emit byte-identical `SketchEnvelope.payload`s. | E2E test passes; backend PromQL output is identical regardless of which agent produced the data. |

Phase A is this PR. Phase B–E are sized at roughly 1 week each for an
engineer familiar with the runtime; the runtime extraction (ADR-0002)
having already shipped is what makes this fit in 4 weeks rather than
8.

## 12. Decisions required

- [ ] Approve the unified `allsketches` plugin shape (vs five
      sibling plugins). §3 above is the rationale.
- [ ] Confirm `value_field` (single field) is acceptable for v1;
      multi-field deferred to v2 per §10.
- [ ] Confirm that the custom `asap` Telegraf Serializer is a
      separate design / PR and not bundled into this work.
- [ ] Confirm phase B–E sequencing; in particular, whether phase E
      (cross-host parity) is a release-gate or a post-release
      regression test.

Once those land, phase B can start immediately; the runtime
dependency is already on `main`.

## References

- [`docs/design-asap-edge-framework.md`](./design-asap-edge-framework.md)
  — five-layer model, bandwidth invariant, Strategy A/B,
  per-platform encoding, `Adapter` / `ControlChannel` traits.
- [`docs/adr/adr-0002-extract-precompute-runtime.md`](./adr/adr-0002-extract-precompute-runtime.md)
  — runtime contract that the Telegraf plugin reuses.
- [`docs/adr/adr-0003-adapter-trait-and-control-channel.md`](./adr/adr-0003-adapter-trait-and-control-channel.md)
  — adapter shape and control-channel rule that this design follows.
- [`asap-precompute-go/otel/`](../asap-precompute-go/otel/) —
  reference codec shape that `asap-precompute-go/telegraf/` mirrors.
- [`opentelemetry-collector-contrib-patch/`](../opentelemetry-collector-contrib-patch/)
  — patch-overlay structure that `telegraf-patch/` mirrors.
- [`build_asap_otel.sh`](../build_asap_otel.sh) —
  build pipeline that `build_asap_telegraf.sh` mirrors.
