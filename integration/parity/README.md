# Phase 2 parity harness

End-to-end check that the new `asap-precompute-go` runtime emits
`SketchEnvelope` payload bytes identical to today's 5 OTel sketch
processors when given the same input. This is the gate before the
shim refactors in Phase 2 steps 2.5–2.9. ADR-0002 §"Behavior
preservation" promises wire-format invariance — this harness is
the test that verifies it.

## Run

```
cd integration/parity
go test -v ./...
```

The harness is fully synchronous: no goroutines, no wall-clock waits,
no flake-prone timers. Two runs of the same harness produce
byte-identical inputs and outputs.

## Subtests

- `TestParity_AllSketches` — runs all 5 sketch comparisons in one
  pass, reusing one input/output build.
- `TestParity_{DDSketch,KLL,HLL,CountSketch,CountMinSketch}` —
  isolated per-sketch tests, useful for `go test -run`.

CountSketch and CountMinSketch SKIP today — see the parity test's
`skipReason` for the structural divergence and the PR description's
follow-up list for the planned fixes.
