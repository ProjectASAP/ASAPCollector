# ASAP `allsketches` processor

Unified Telegraf `StreamingProcessor` that adapts the host-neutral
[`asap-precompute-go`](../../../asap-precompute-go) runtime to
Telegraf's metric pipeline. One plugin, one `sketch_type` knob.

The design rationale (one unified plugin instead of five sibling
plugins) is documented in the integration design doc:
[`docs/design-asap-telegraf-integration.md`](../../../docs/design-asap-telegraf-integration.md)
§3 covers the choice; §5 covers the lifecycle; §8 covers the config
shape.

## Supported sketches

| `sketch_type` | Sketch | Use case |
|---|---|---|
| `ddsketch` | DDSketch | Quantiles with relative-error guarantees |
| `kll` | KLL | Quantiles with rank-error guarantees |
| `hll` | HyperLogLog | Cardinality estimation |
| `countsketch` | CountSketch | Frequency / top-K |
| `countminsketch` | Count-Min Sketch | Frequency, biased upward |

Each `sketch_type` is dispatched through
[`asap-precompute-go/sketches/`](../../../asap-precompute-go/sketches),
which is the same wrapper the OTel `sketchcollector` build uses — so
both hosts emit byte-identical `SketchEnvelope` payloads when fed the
same input.

## Lifecycle

1. **Start** — validates the config, builds the runtime `Precompute`
   instance via `sketchFactory(sketch_type)`, constructs the
   Telegraf-side codec adapter, and spawns a window-flush ticker
   goroutine.
2. **Add** — decodes each inbound `telegraf.Metric` to a host-neutral
   `Observation` (via the Phase-B
   [`asap-precompute-go/telegraf`](../../../asap-precompute-go/telegraf)
   codec), then routes the observation into the `Precompute` window.
   Decode errors are logged and dropped, never returned, so a single
   malformed input doesn't take the pipeline down.
3. **Tick** — every `window_size` the ticker calls `Precompute.Tick`,
   encodes the closed window's `SketchEnvelope`s through the codec,
   and emits the resulting `telegraf.Metric`s on the stashed
   accumulator. One emitted metric per envelope.
4. **Stop** — cancels the ticker goroutine, performs a final drain
   so no in-flight window state is lost on shutdown, and returns.

## Configuration

```toml
[[processors.allsketches]]
  sketch_type        = "ddsketch"
  window_size        = "10s"
  value_field        = "value"
  envelope_field     = "_asap_envelope_b64"
  output_metric_name = "http_request_duration_ms"

  ## Sketch-specific tuning. Only the keys for the configured
  ## sketch_type are read; others are ignored.
  alpha     = 0.01     # ddsketch — relative accuracy
  k         = 200      # kll      — buffer size
  precision = 14       # hll      — register count exponent (currently fixed)
  width     = 2048     # countsketch / cms — width
  depth     = 4        # countsketch / cms — depth

  ## Delta transmission knobs (see ADR-0003).
  delta_transmission = false
  delta_threshold    = 0

  ## Series-key shape.
  omit_resource_attrs = false
  global_aggregation  = false
  emit_window_stats   = false
```

### Field reference

- `sketch_type` — selects the wrapper from
  `asap-precompute-go/sketches/<type>` the plugin instantiates.
- `window_size` — Go duration string. Tumbling-window cadence.
- `value_field` — Telegraf field on each input metric carrying the
  scalar observation value. Multi-field metrics are supported only
  by selecting one field (multi-field-fanout is future work, see
  design doc §10).
- `envelope_field` — well-known field name carrying a base64-encoded
  pre-aggregated upstream `SketchEnvelope`. When present, the
  decode-side scalar path is skipped; the envelope is merged
  directly into the active window (`ObserveEnvelope` path).
- `output_metric_name` — fallback metric name used on emit when the
  envelope itself doesn't carry one.

Multi-sketch deployments declare multiple
`[[processors.allsketches]]` blocks with different `sketch_type` and
different `namepass` / `tagpass` filters — Telegraf handles
plugin-instance multiplicity natively.

## Tests

```
go test -race ./...
```

Lifecycle coverage in `allsketches_test.go` includes:
- Invalid sketch_type → Start returns error.
- Invalid window_size → Start returns error.
- DDSketch happy path: Start, observe N scalars, Stop, assert
  envelope-bearing output metrics on the accumulator.
- Envelope-shortcut: feed a `_asap_envelope_b64` field, assert no
  decode error.
- Stop without Start: no panic.
- All five sketch types: each constructs and processes input
  cleanly via the dispatch table.

## See also

- [`docs/design-asap-telegraf-integration.md`](../../../docs/design-asap-telegraf-integration.md)
  — the design rationale; this README is the user-facing surface.
- [`docs/adr/adr-0002-extract-precompute-runtime.md`](../../../docs/adr/adr-0002-extract-precompute-runtime.md)
  — the runtime contract this plugin reuses.
- [`docs/adr/adr-0003-adapter-trait-and-control-channel.md`](../../../docs/adr/adr-0003-adapter-trait-and-control-channel.md)
  — the adapter shape and control-channel rule this plugin follows.
