# integration/parity — three layered parity gates

Three parity tests, layered by where they pin the contract. All three
must hold for the ASAP edge framework's "swap host, swap binary, swap
codec" claim to mean anything end-to-end.

| Sub-gate | Scope | What two things it equates |
| --- | --- | --- |
| [`runtime-impl/`](runtime-impl/) | runtime ↔ legacy 5-OTel-processor envelope bytes | `asap-precompute-go` runtime emits the same `SketchEnvelope` payload bytes as the 5 legacy OTel sketch processors when fed identical input. |
| [`codec/`](codec/) | OTel codec ↔ Telegraf codec, in-process | Both per-host codecs (`asap-precompute-go/otel`, `asap-precompute-go/telegraf`) feed the runtime byte-equivalent observation streams, so the emitted envelopes match across host types. |
| [`agent-binary/`](agent-binary/) | `asap-otel` ↔ `asap-otap` agent binaries | The full agent shells (OTLP receiver + processor pipeline + OTLP exporter) preserve the runtime's envelope bytes through the receive-process-emit path. |

## Why three gates

- `runtime-impl/` proves the runtime is byte-faithful to the legacy
  per-sketch processor cluster the migration replaces.
- `codec/` proves the host-neutral runtime treats `pmetric.Metrics` and
  `[]telegraf.Metric` as wire-equivalent inputs.
- `agent-binary/` proves the agent shells (separate codebases, separate
  languages) don't perturb the bytes around the runtime.

If any link in the chain breaks, downstream guarantees (cross-language
backend equivalence, Thanos-side query parity) collapse.

## Layout

Each subdir is its own Go module — Go's nested-module rules require a
distinct `go.mod` at every package root. Run each subdir's tests
independently:

```
cd integration/parity/runtime-impl  && go test ./...
cd integration/parity/codec         && go test ./...
cd integration/parity/agent-binary  && go test ./...
```

`runtime-impl/` and `codec/` are in-process Go tests; `agent-binary/`
runs in fixture mode by default (no Docker) with a binary mode
opt-in via `run_parity.sh --mode=binary`.

## Cross-language tie-in

The Rust runtime test
[`asap-precompute-rs/tests/cross_language_parity.rs`] consumes the
`runtime-impl/golden/*.bin` fixtures: regenerating those fixtures is
how the Go-side and Rust-side runtimes ratify they emit the same
canonical bytes. Regenerate via:

```
cd integration/parity/runtime-impl && \
  GOLDEN_REGEN=1 go test -run TestGenerateGoldenFixtures ./...
```
