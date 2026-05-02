# ADR-0001: Retire `sketch-core`; `asap_sketchlib` is the single Rust algorithm crate

| | |
|---|---|
| Status | **Accepted** (retrospective — work already shipped) |
| Date | 2026-05-01 |
| Deciders | Project ASAP maintainers |
| Supersedes | n/a |
| Superseded by | n/a |

## Context

Before this work, the Rust side of the ASAP system had **two**
algorithm crates with overlapping responsibilities:

- `asap_sketchlib` — the canonical algorithm crate, holding
  `DDSketch`, `KLL`, `HLL`, `Count`, `CountMin`, `CMSHeap`, etc.
  Already shared with `sketchlib-bench` and external benchmark
  harnesses.
- `sketch-core` — a wrapper crate vendored into
  `ASAPQuery-backend/asap-common/sketch-core/` (and its sibling
  forks in `ASAPQuery/asap-common/sketch-core/` and
  `sketchlib-bench/sketch-core/`). It re-exposed the algorithms
  with ASAP-specific wire-format types (`DdSketchState`,
  `CountSketchState`, etc.), `apply_delta` methods, and the
  `Strategy::Legacy` vs `Strategy::Sketchlib` `ImplMode` switch.

The duplication was a source of:

- **Cross-language drift risk.** The Go side had one canonical
  algorithm crate (`sketchlib-go`); having two on the Rust side
  made a third potential drift target.
- **Apply-delta logic in the wrong place.** `apply_delta`
  implementations belonged with the algorithms, not in a wrapper
  crate. Bug fixes (e.g., DDSketch delta count reconstruction)
  had to be written in two places.
- **Maintenance overhead.** Three on-disk forks of the same
  crate had to stay in sync; PRs touching the shared types
  needed three identical commits.

The `ImplMode` switch (Legacy vs Sketchlib) was originally
introduced to allow gradual migration off hand-written matrix
implementations onto `asap_sketchlib`-backed implementations.
Once the Sketchlib path was the default and Legacy was no longer
exercised in production paths, the switch became dead weight.

## Decision

1. **Retire `sketch-core` entirely.** Move all of its content
   into `asap_sketchlib`:
   - Wire-format types (`DdSketch`, `CountSketch`, `HllSketch`,
     `CountMinSketch`, `KllSketch`, `CountMinSketchWithHeap`)
     into the existing `src/sketches/<name>.rs` files alongside
     the high-throughput algorithms — single home per sketch
     concept.
   - `apply_delta` implementations into the same files so the
     algorithm + its delta semantics live together.
   - New sibling files for sketches that had no existing home in
     `asap_sketchlib` (`hydra_kll.rs`, `set_aggregator.rs`,
     `delta_set_aggregator.rs`).
   - The previous `*_sketchlib.rs` FFI wrapper files inlined into
     their main sketch file (e.g., `SketchlibCms` lives directly
     inside `countmin.rs`).
   - The standalone `asap_runtime` module for the legacy
     `ImplMode` configuration was created and then removed (see
     decision 3).

2. **Drop the three on-disk `sketch-core` forks.** All consumers
   (`ASAPQuery`, `ASAPQuery-backend`, `sketchlib-bench`) depend
   on `asap_sketchlib` directly via git URL.

3. **Drop the `ImplMode` (Legacy / Sketchlib) dispatch.** Always
   use the `asap_sketchlib`-backed implementation. Removed:
   - The `asap_runtime` module entirely.
   - `KllBackend` enum (Sketchlib + Legacy variants); `KllSketch`
     now holds `SketchlibKll` directly.
   - The `dsrs` (datasketches-rs) dependency that provided the
     legacy KLL backend.
   - The `clap` (asap-cli feature) and `ctor` (legacy-mode test
     initializer) dependencies.
   - The backend's `--sketch-cms-impl` / `--sketch-kll-impl` /
     `--sketch-cmwh-impl` CLI args and their `config::configure`
     plumbing.

4. **Rename `CountMinDelta` → `CountMinSketchDelta`** to match
   the `<Type>Delta` pattern of the other delta types
   (`DdSketchDelta`, `CountSketchDelta`, `HllSketchDelta`).

5. **Rename to avoid wire-format collisions:**
   - `octo_delta::HllDelta` (single-register, octo path) keeps
     its name; the wire-format multi-register delta becomes
     `HllSketchDelta`.
   - `common::input::HeapItem` (polymorphic key type) keeps its
     name; the wire-format CMSHeap item becomes `CmsHeapItem`.

## Consequences

### Positive

- Single canonical Rust algorithm crate, mirroring `sketchlib-go`
  on the Go side. Cross-language drift is now a 1:1 concern, not
  1:N.
- `apply_delta` logic lives next to the algorithm it applies to.
- Three on-disk forks deleted; consumers all track
  `asap_sketchlib` directly.
- ~3,000 lines of net code removed once `Legacy` paths and
  `dsrs` / `clap` / `ctor` / `asap-cli` deps were dropped.

### Negative / Tradeoffs

- The wire-format types now sit alongside the high-throughput
  types in the same files (e.g., both `DDSketch` and `DdSketch`
  in `src/sketches/ddsketch.rs`). File sizes grow by 30–80%.
  Acceptable: the alternative (separate crate or separate
  module) recreates the duplication problem this ADR solves.
- Cross-language parity tests between `sketchlib-go` and
  `asap_sketchlib` are now the *only* defense against algorithm
  drift. The R1 risk in the design doc explicitly calls this
  out; mitigation (statistical-output cross-language harness in
  `sketchlib-bench`) is tracked for follow-up.

### Compatibility

- Wire format unchanged: `SketchEnvelope.payload` bytes are
  byte-identical pre/post-retirement. The
  `tests::elastic_dsl_query_tests::tests::test_esdsl_time_range_query`
  test was relaxed from `assert_eq!(value, 291.0)` to a `±1`
  tolerance — `asap_sketchlib`'s KLL gives 290 on this
  distribution where `dsrs` gave 291. Both are within KLL's
  rank-error bound; the test was previously over-tight.

## References

- [ProjectASAP/asap_sketchlib#36](https://github.com/ProjectASAP/asap_sketchlib/pull/36) — sketch-core merged into existing `src/sketches/` layout
- [ProjectASAP/ASAPQuery-backend#73](https://github.com/ProjectASAP/ASAPQuery-backend/pull/73) — backend consumer migration
- [ProjectASAP/ASAPQuery#309](https://github.com/ProjectASAP/ASAPQuery/pull/309) — legacy-fork consumer migration + branch-pin cleanup + elastic DSL test fix
- [ProjectASAP/asap_sketchlib#37](https://github.com/ProjectASAP/asap_sketchlib/pull/37) — wire-format / `apply_delta` semantic alignment with `sketchlib-go` (DDSketch count reconstruction, CountMin/CountSketch field additions, out-of-bounds policy)
