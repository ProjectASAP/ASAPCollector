# integration/cross_host_parity

Phase 5 step E (per `docs/design-asap-otap-rust-integration.md` §11
row E): the three ASAP-flavored agents — `sketchcol` (OTel-Go),
`sketchotap` (OTAP-Rust), `sketchtelegraf` (Telegraf-Go) — fed
identical input emit byte-identical `SketchEnvelope.Payload`s, and
the backend's PromQL output is identical regardless of which agent
produced the data.

## Scope of THIS PR — two-way (sketchcol ↔ sketchotap)

The cross-language byte-parity gate (issue #243) closed
2026-05-05 and is the prerequisite for the Go↔Rust comparison; the
homogeneous-Rust case (sketchotap vs sketchotap, varied input
sources) does not need #243.

This PR ships the **two-way** sketchcol ↔ sketchotap test as the
default. The `sketchtelegraf` third agent is a one-flag opt-in
(`CROSS_HOST_PARITY_INCLUDE_TELEGRAF=1` or `--include-telegraf`)
so the asymmetry around Telegraf's line-protocol input format
doesn't block the Go↔Rust gate this PR is built around. Phase 4
step E (`integration/cross-host-parity/`) already closes the
sketchcol ↔ sketchtelegraf comparison at the in-process codec
level; Phase 5E binary-mode coverage is a follow-up.

## How this layers on existing parity gates

| Gate | Scope | Phase |
| --- | --- | --- |
| `integration/parity/` | runtime ↔ legacy-OTel-processor envelope bytes | Phase 2 |
| `integration/cross-host-parity/` | OTel-codec ↔ Telegraf-codec, both in-process Go runtime | Phase 4E |
| `asap-precompute-rs/tests/cross_language_parity.rs` | Go runtime ↔ Rust runtime per-sketch wire format (issue #243) | Phase 5 prereq |
| `integration/cross_host_parity/` (this dir) | sketchcol ↔ sketchotap agent-binary envelope bytes + PromQL | Phase 5E |

The sketch-level cross-language gate (#243) ratifies that *each
runtime* produces the canonical envelope bytes from goldenFloats /
goldenHllKeys / goldenCsKeys / goldenCmsKeys. This directory
ratifies that *each agent shell* (the OTLP receiver + processor
pipeline + OTLP exporter wrapper) preserves those bytes through the
full receive-process-emit path.

## Modes

### Fixture mode (default)

```
bash integration/cross_host_parity/run_parity.sh
```

- Verifies the cross-language gate's golden envelope fixtures
  (`integration/parity/golden/*.bin`) are present and non-empty.
- Runs `parity_test.go` in `CROSS_HOST_PARITY_MODE=fixture`, which
  asserts byte-equality across pairs by re-loading the same canonical
  fixture for each agent (transitively valid because #243 already
  proved each runtime emits exactly that fixture).
- Verifies `deploy/scripts/queries-e2e.json` is well-formed.
- Fast; no Docker; runnable in CI.

This mode is meaningful and not tautological because the alternative
— a silent skip when fixtures are missing — was already burned-in
by #254 (HLL fixture-generator divergence hidden by stale gitignored
fixtures). Fixture mode here forces the regen ritual into the failure
path.

### Binary mode

```
bash integration/cross_host_parity/run_parity.sh --mode=binary
bash integration/cross_host_parity/run_parity.sh --mode=binary --include-telegraf
```

- Brings up `asap/sketchcol:dev` + `asap/sketchotap:dev` (+
  `asap/sketchtelegraf:dev` if `--include-telegraf`) plus
  `asap/query-backend:dev` and an envelope-tap container via
  `deploy/docker-compose/cross-host-parity.yml`.
- Drives the canonical input from `golden_input/inputs.json` through
  each agent.
- Captures the emitted envelope bytes per (agent, sketch) under
  `captures/<agent>/<sketch>_envelope.bin`.
- Replays `deploy/scripts/queries-e2e.json` against the per-agent
  backend and writes JSON responses under `captures/promql/`.
- Runs `parity_test.go` in `CROSS_HOST_PARITY_MODE=binary`, which
  asserts byte-equality across pairs from the captures.

Binary mode requires the four images to be pre-built. Missing
images cause a hard failure with the build commands in the message
— never a silent skip.

## Layout

- `parity_test.go` — top-level test, dispatches on
  `CROSS_HOST_PARITY_MODE`. Asserts byte-equal envelopes per
  (agent-pair, sketch); asserts byte-equal PromQL responses per
  (agent-pair, query) in binary mode.
- `golden_input/inputs.json` — canonical input fixture mirroring
  `goldenFloats()` / `goldenHllKeys()` / `goldenCsKeys()` /
  `goldenCmsKeys()` from `integration/parity/golden_test.go` and
  `asap-precompute-rs/tests/cross_language_parity.rs`.
- `run_parity.sh` — orchestrator (mode dispatch, prereq check,
  Docker compose lifecycle, test invocation).
- `configs/` — per-agent pipeline configs mounted into containers
  in binary mode (sketchcol.yaml, sketchotap.yaml,
  sketchtelegraf.toml).
- `captures/` — created at runtime by `run_parity.sh` in binary
  mode; gitignored.

## Exit criteria (per §11 row E)

Both must hold:

1. **Byte-identical envelopes** across each tested agent pair, per
   sketch type. `parity_test.go::TestCrossHostEnvelopeParity`.
2. **PromQL response equality** across each tested agent pair, per
   query in `queries-e2e.json`.
   `parity_test.go::TestCrossHostPromQLParity`.

If either fails, the test fails — *exact* parity, not approximate.
A mismatch is a real bug to surface, not a tolerance to widen.
