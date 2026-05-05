# integration/cross-host-parity

Phase 4 step E: prove `asap-precompute-go` produces byte-identical
`SketchEnvelope` payloads regardless of whether observations come
from OpenTelemetry's `pmetric.Metrics` (driven through the OTel
codec) or Telegraf's `telegraf.Metric` (driven through the Telegraf
codec).

## What this checks

The runtime in `asap-precompute-go/` is host-neutral by design — it
sees only `Observation` and emits only `SketchEnvelope`. Per-host
codecs in `asap-precompute-go/otel/` and `asap-precompute-go/telegraf/`
translate between the host's native event type and these neutral
shapes. If both codecs are doing pure data-shape translation, then
identical observation streams yield identical sketch state and the
emitted envelope payload bytes match exactly.

This harness:

1. Builds one `pmetric.Metrics` and one `[]telegraf.Metric` carrying
   the same logical observation set (deterministic seeded RNG).
2. Runs each through a fresh `Precompute` per sketch via the matching
   adapter; calls `Drain()` to flush the active window.
3. Asserts byte-equality of the resulting `SketchEnvelope.Payload`s
   per sketch type.

The check is in-process; no real binaries run. The compared bytes are
the runtime's emitted envelopes — not the host-specific re-encoded
events — because that is the precise contract the Phase 4 design
asserts.

## Running

```
cd integration/cross-host-parity
go test -v ./...
go test -race ./...
```

## Layout

- `parity_test.go` — `TestCrossHostParity_AllSketches` and per-sketch
  subtests.
- `harness/input_otel.go` — deterministic `pmetric.Metrics` builder.
- `harness/input_telegraf.go` — deterministic `[]telegraf.Metric`
  builder; one Telegraf metric per OTel data point so the two inputs
  describe the same observation stream.
- `harness/runtime_otel.go` — wires the OTel adapter into per-sketch
  `Precompute` instances.
- `harness/runtime_telegraf.go` — same for Telegraf.
- `harness/sketches.go` — test-only `precompute.Sketch` wrappers
  (mirrors `integration/parity/harness/sketches.go`).
- `harness/diff.go` — envelope byte-comparison and structured diff.
