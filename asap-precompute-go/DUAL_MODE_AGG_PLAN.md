# Dual-Mode Aggregation — Implementation Spec

**Status:** design locked 2026-05-31, implementation pending (session file-read
tooling was degraded when this was written; execute edits once reads work).

## Decisions (locked with user)
1. **Two modes per aggregation:**
   - `PerSeries` (default, == today): `windowState.series map[seriesKey]→Sketch`;
     each datapoint's **value** folded into its series' sketch.
   - `WholeStream`: **collapse grouping** — one sketch instance per `AggID`
     (per shard, merged at flush) that ingests the metric **VALUE** from every
     datapoint regardless of series identity.
2. **Whole-stream ingest = the metric VALUE** (NOT the series key). So:
   - Sum → grand total across stream
   - DDSketch / KLL → value distribution pooled across all series
   - HLL → count of **distinct values**
   - CMS / CountSketch / TopK → frequency / heavy **values**
3. **Wire signal:** new explicit `Mode` enum field on `PrecomputeConfig`.
   Zero-value = `PerSeries` ⇒ no migration, existing plans behave identically.
4. **Scope:** all families in one pass.

## Mode is a routing choice, orthogonal to sketch impls
Every sketch already supports value-observe; mode only decides *how the runtime
routes observations and emits output*. No per-sketch math changes — the change is
in the precompute runtime + config + controller + flush.

---

## Change list

### A. `asap-precompute-go/config.go`
- Add enum:
  ```go
  type AggMode uint8
  const (
      ModePerSeries  AggMode = iota // 0 = default, current behavior
      ModeWholeStream
  )
  ```
- Add field to `PrecomputeConfig`: `Mode AggMode` (JSON: `"mode"`, omitempty so
  old plans stay byte-compatible).
- Doc the field next to `SketchType`/`AggregateBy`.

### B. `asap-precompute-go/window.go`
- `windowState` gains a whole-stream slot alongside `series map`:
  ```go
  wholeStream Sketch // non-nil iff cfg.Mode == ModeWholeStream
  ```
- Observe path branches on mode:
  - `ModePerSeries` → existing `admitSeriesLocked` map lookup/insert.
  - `ModeWholeStream` → lazily construct `wholeStream` once, fold value in;
    **skip** series-key construction and the map entirely (big alloc win at
    high cardinality — no per-series bookkeeping, no snapshot-cache fan-out).
- `MaxSeries` is a no-op in whole-stream (cardinality is 1); leave cap logic to
  the per-series branch.

### C. `asap-precompute-go/precompute.go`
- Wherever the Sketch is constructed from `SketchType`, reuse the SAME factory for
  both modes (value-observe contract is identical). No new constructors needed.
- Flush/encode: emit **one envelope per AggID** in whole-stream (carry empty/derived
  label set), vs one-per-series today. Reuse existing Encode/Snapshot per sketch.
- Delta/snapshot cache: whole-stream keeps a single outbound snapshot per AggID
  (not per series).

### D. `opentelemetry-collector-contrib/processor/asapedgeprocessor`
- `config.go`: surface `mode` in the per-metric family config (mapstructure
  `mode`), validate ∈ {per_series, whole_stream}, default per_series.
- `processor.go` (~150-165): pass `fam.Mode` through when building the precompute
  runtime / aggregators.
- `control_plane.go` `UpdateConfig`: mode change must rebuild the aggregator's
  windowState (can't hot-swap series-map↔single-sketch in place) — handle as a
  reset on mode transition; same-mode changes stay in-place.

### E. Controller `/mydata/ASAPController`
- Where it emits `SketchType` per aggregation, also emit `Mode` chosen from the
  query: distinct-count / global-quantile / grand-total queries → `WholeStream`;
  per-label-group queries → `PerSeries`.

### F. Tests
- `window` unit tests: same input stream, assert per-series produces N envelopes
  and whole-stream produces 1, with correct pooled value semantics per family.
- Round-trip config JSON with/without `mode` (back-comat: missing ⇒ PerSeries).
- `precompute_bench_test.go`: add a whole-stream variant to confirm the expected
  memory drop (no per-series map/snapshot fan-out).

---

## Open items to confirm against source (when reads work)
- Exact `Sketch` interface method names (Observe vs ObserveKeyed) and Encode/Reset
  signatures — confirm value-observe path is identical across families.
- Exact `windowState` rotation fn (window.go ~454-488) to add the single-sketch
  drain branch.
- Whether `asapedgeprocessor` builds one aggregator per family per shard (it does
  for sum: `newSumAggregator`, processor.go:155) — mirror for whole-stream.
- Controller plan struct name + the query→aggregation translation site.
