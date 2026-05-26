# Delta–Baseline Contract — Prerequisite for Edge Delta Transmission

**Date:** 2026-05-26
**Status:** Proposal (blocking — gates `*_ENCODING_DELTA` at the edge)
**Context:** Compatibility analysis of the edge `SnapshotCache` "always-refresh"
semantics against the backend's additive delta-apply path
(`ASAPQuery-backend/data_plane/src/drivers/ingest/otel.rs`).

---

## 0. TL;DR

Edge delta transmission (`DeltaTransmission=true`, emitting `PROTO_DELTA`
frames) is **deliberately defaulted OFF** in the edge processor
(`opentelemetry-collector-contrib-patch/processor/asapedgeprocessor/factory.go:34-35`).
Turning it on **today** would produce **wrong query answers** for every
additive sketch family because the edge and the backend disagree about
what a delta means:

- **Edge** produces `delta(N) = snapshot(N) − snapshot(N−1)` where each
  `snapshot(N)` is a **fresh, per-window** sketch (the tumbling window
  resets per-series state every window).
- **Backend** applies that delta **additively onto a running, reconstructed
  baseline** and re-caches the result as the new base:
  `state(N) = state(N−1) + delta(N)`. It assumes producer snapshots are
  **cumulative / monotone** across windows.

Two independent per-window sketches do **not** form a meaningful additive
diff. The backend ends up holding `base + (winN − winN−1)`, which is
neither the window-N value nor the running total. HLL over-counts because
its never-reset backend base keeps `max`-merging registers from unrelated
windows.

This document pins the **contract** that must hold before deltas can be
switched on, analyses each sketch family, lays out two ways to make the
contract hold, recommends one, and gives a verification/rollout plan.

---

## 1. The Mismatch

### 1.1 What the backend assumes

`ingest/otel.rs` (the OTLP ingest path) routes each data point by its
`encoding`. For a delta frame it looks up the cached per-series baseline,
applies the delta in place, and **re-caches the merged result as the new
base** (`otel.rs:1290-1357`):

```rust
let accumulator = if dp.encoding == ENCODING_PROTO_DELTA
    || dp.encoding == ENCODING_MSGPACK_DELTA
{
    // 1. look up the running base for this series
    let Some(base) = ingest_state
        .sketch_snapshots
        .get(&series_key)
        .map(|e| e.clone_boxed_core())
    else {
        // 2. no base yet → DROP the delta (agent must resend a full frame)
        decoded_failed += 1;
        debug!("OTLP delta-sketch arrived before any base snapshot … dropping");
        continue;
    };
    let mut merged = base;
    // 3. apply the delta ADDITIVELY in place
    apply_modified_otlp_delta_bytes(dp.kind, dp.encoding, &mut merged, &dp.sketch)?;
    // 4. re-cache the merged result as the NEW base for this series
    ingest_state
        .sketch_snapshots
        .insert(series_key.clone(), merged.clone_boxed_core());
    merged
} else {
    // full frame: decode standalone and REPLACE the cached base
    …
};
```

The comment block above this code states the assumption explicitly:

> "delta frames (PROTO_DELTA) look up the cached base and apply the diff in
> place … agent-side windowing guarantees one in-flight delta per (metric,
> labels) so the next full snapshot replaces the current cache entry
> cleanly."

So the backend's model is a **running accumulator**:

```
state(0) = full snapshot                 (REPLACE)
state(N) = state(N−1) ⊕ delta(N)         (ADDITIVE merge, then re-cache)
```

Critically, the backend **never subtracts** and **never resets** the base
at a window boundary. It expects each successive delta to be the marginal
*addition* relative to the previously-cached cumulative state.

### 1.2 What the edge actually produces

The edge tumbling window **resets per-series sketch state every window**.
`window.go:rotateLocked` drains the active series map and replaces it with a
brand-new empty map; each new series in the next window gets a **fresh**
sketch from `sketchFactory()`:

```go
// window.go:512-533 (rotateLocked)
closedSeries := make([]*seriesEntry, 0, len(w.series))
for _, entry := range w.series {
    closedSeries = append(closedSeries, entry)
}
…
// Reset the series map for the next window.
w.series = make(map[string]*seriesEntry)   // ← window N+1 starts EMPTY
w.advanceWindow(nowMs, cfg)
```

(The Rust runtime is identical: `asap-precompute-rs/src/window.rs:357`
does `std::mem::take(&mut self.series)`, leaving an empty map; the sliding
path `rotateSlidingLocked` likewise builds fresh throwaway sketches.)

The edge `SnapshotCache` then computes the delta with **always-refresh**
semantics (`snapshot_cache.go:74-137`, mirrored in
`asap-precompute-rs/src/snapshot_cache.rs:114-150`):

```go
// snapshot_cache.go — ComputeDelta
//
// Semantics — always-refresh: every call to ComputeDelta updates the
// cached previous snapshot to the current sketch state. Successive
// sub-threshold deltas are therefore each computed against the
// immediately preceding window …
```

So the edge produces:

```
delta(N) = snapshot(N) − snapshot(N−1)
```

where **both** `snapshot(N)` and `snapshot(N−1)` are *independent
per-window sketches*, each built from only the events that fell in their
own window. They are not nested; `snapshot(N)` is **not** `snapshot(N−1)`
plus more inserts. The "−" between them is a cell-wise subtraction of two
unrelated sketches.

### 1.3 Why the two don't compose

Put the producer and consumer together. Let `winK` denote the true sketch
of window K's events. The edge emits a full frame for window 1, then deltas:

| Window | Edge emits | Backend cached base after apply |
|--------|-----------|----------------------------------|
| 1 | full = `win1` | `win1`                          |
| 2 | `win2 − win1` | `win1 + (win2 − win1)` = `win2` |
| 3 | `win3 − win2` | `win2 + (win3 − win2)` = `win3` |
| N | `winN − winN−1` | `winN`                        |

At first glance window N looks correct — the backend base equals `winN`.
But that is **only** true cell-for-cell when the subtraction and addition
are over the *same dense representation with no information loss*, and it
is **never** the intended semantics for either system:

1. **It is not the window value the query wants, in general.** The edge's
   `ComputeDeltaAgainst` applies a **threshold** (`DeltaThreshold`): cells
   whose change is below threshold are dropped from the delta. For an
   additive sketch a *dropped decrease* is exactly the failure mode: when
   `winN[r][c] < winN−1[r][c]` (a counter that was hot last window and cold
   this window), the negative delta cell is the thing that must arrive to
   pull the base back down. Sparse/thresholded deltas systematically under-
   transmit the decreases, so the reconstructed base is
   `base + (winN − winN−1)` with the **decreases missing** — neither
   `winN` nor any running total. The error compounds every window.

2. **It is not a running total either.** A user issuing
   `count_over_time(metric[5m])` against the warm/ASAP tier expects the sum
   across the windows in the range, i.e. `win1 + win2 + … + winN`. The
   backend base holds (at best) just `winN`. The query reducer that sums
   per-window buckets will *double-subtract*: it both reads a base that was
   already differenced and then re-aggregates across buckets that were
   built from differenced state.

3. **The "clean replace" assumption is violated.** The backend comment
   promises "one in-flight delta per (metric, labels) so the next full
   snapshot replaces the current cache entry cleanly." But the edge only
   emits a full frame on the **very first** window per series
   (`snapshot_cache.go:105`, `prev == nil`); every subsequent window is a
   delta forever. There is no periodic full-frame re-baseline, so any
   single dropped/duplicated/reordered delta corrupts the base for the
   rest of the series' lifetime with no self-healing.

### 1.4 Worked example (CMS frequency, one cell)

Take one CMS counter cell `[r][c]` tracking key `k`. Suppose `k` appears
100 times in window 1, 5 times in window 2, 80 times in window 3.

- True per-window values: `win1=100`, `win2=5`, `win3=80`.
- True running total a `count_over_time` query wants: `185`.

Edge (lossless `DeltaThreshold=1`, ignore thresholding for a moment):

```
W1  full   = 100          backend base = 100
W2  delta  = 5 − 100 = −95  backend base = 100 + (−95) = 5
W3  delta  = 80 − 5 = +75   backend base = 5 + 75 = 80
```

Backend base ends at `80` = `win3`. A point-in-time "current window"
frequency query is *accidentally* right, but:

- A `count_over_time([3 windows])` reducer summing the per-window backend
  buckets sees `100, 5, 80`-derived buckets that were each produced by
  *additive merge of a difference into a prior base*, so the per-window
  bucket the reducer stores is whatever `state(N)` was at each apply —
  `100, 5, 80` if and only if every negative delta survived. The intended
  answer is the **sum** `185`; the differenced pipeline cannot produce it
  without the backend also knowing to *not* difference.

Now turn on thresholding (`DeltaThreshold` drops `|Δ| < T`). The `W2`
delta `−95` is a large decrease and survives, but the many **small**
decreases across the rest of the array (keys that were warm in W1 and cold
in W2) are below `T` and are dropped. The backend base then retains stale
W1 mass that should have been subtracted:

```
backend base after W2 ≈ win2 + (dropped W1 residue)  >  win2
```

Every window leaks a little more stale mass forward. CMS/CountSketch/
DDSketch counts come out as **`base + (winN − winN−1)` with decreases
under-applied** — systematically over-counting.

### 1.5 HLL is worse (register-MAX over a never-reset base)

HLL merges by **register-wise max**, not addition
(`hll_sketch_accumulator.rs:120-139` → inner `apply_delta` takes
`max(reg, update.value)`). The backend base is never reset, so it holds the
**element-wise max over all windows seen so far**. A delta only carries
registers that *increased* relative to the edge's previous window — but the
edge's previous window was itself reset, so "increased relative to last
window" is meaningless against a backend base that is the all-time max.

Concretely: window 1 sees cardinality 10⁶ (registers high), windows 2…N see
cardinality 10³ (registers low). The edge emits a tiny or empty delta each
later window (few registers exceed the *previous window's* low registers).
The backend base, holding the all-time max from window 1, **stays at ~10⁶
forever**. A `… by (le)` / distinct-count query for any later window
over-counts by 3 orders of magnitude. There is no subtraction in `max`
algebra, so this never self-corrects.

---

## 2. Per-Sketch Analysis

Where deltas land at the backend (`apply_modified_otlp_delta_bytes`,
`otel.rs:1875-1943`):

| Family | Backend merge algebra | Delta proto fields | Verdict |
|--------|----------------------|--------------------|---------|
| DDSketch | additive buckets + additive `d_count`/`d_sum`; min/max conditional | `buckets[]`, `d_count`, `d_sum`, `new_min`/`min_changed`, `new_max`/`max_changed` | **Easiest** — first candidate |
| CMS | additive cells | `cells[] (r,c,Δcount[,Δsum,Δsum2])` | Same caveat as DDSketch |
| CountSketch | additive cells (signed) | `cells[] (r,c,Δcount)` + full TopK | Same caveat as DDSketch |
| HLL | register-wise `max` (idempotent) | `updates[] (index,value)` | **Needs explicit care** |
| KLL | — (no delta exists) | none | **Not applicable** |

### 2.1 DDSketch — easiest to make correct

DDSketch's delta is the most self-describing of all the families. The
proto (`DdSketchDelta`) carries **explicit additive scalars**: `d_count`
(total count delta), `d_sum` (sum delta), and additive per-bucket
`d_count`s, plus **lossless** `new_min`/`new_max` guarded by `*_changed`
flags (`dd_sketch_accumulator.rs:103-128`). Because count and sum travel as
their own additive fields — not reconstructed from bucket scans — a
window-reset producer can emit a *true "this-window" delta* (the window's
own buckets/count/sum, since its base is empty), and the backend's additive
merge yields the correct running total **provided the backend does not also
subtract a prior base**. This is the **lowest-risk first candidate**: the
delta is unambiguous and the count/sum scalars give an independent
correctness check at the receiver.

> Caveat that still applies: min/max are monotone (min only decreases, max
> only increases) and are sent losslessly only *when changed*. Under a
> window-reset producer the per-window min/max are correct for that window
> but the backend's running base would carry the all-time min/max forward —
> fine for "current window" queries, wrong for "min over this window only"
> unless the base is rotated (see Option A).

### 2.2 CMS / CountSketch — additive cells, same caveat

Both are additive cell arrays (`count_min_sketch_accumulator.rs:176`,
`count_sketch_accumulator.rs:155`). A window-reset producer's per-window
delta merges additively into a per-window base correctly. The caveats:

- **Thresholding drops decreases** (§1.3 point 1). For CMS this is
  monotone-within-a-window (counters only grow), so within a single epoch
  a dropped *increase* is bounded by `T`; but **across** a reset boundary
  the producer's "decrease" relative to the previous window is real and
  must not be silently dropped, or the base over-counts.
- **CountSketch TopK is non-additive** and is retransmitted in full every
  frame; it is not part of the additive contract and does not break it, but
  its counts are upstream-local estimates (see
  `cms-cs-delta-transmission-optimizations.md` §2.4).

### 2.3 HLL — register-MAX, explicit care required

`max` is idempotent and out-of-order-safe, which is good, but it has **no
inverse**: the never-reset backend base accumulates the all-time-max across
all windows (§1.5). HLL therefore **cannot** use the running-base model for
per-window or windowed-range cardinality queries without resetting the
backend base at the window boundary (Option A). Note the existing delta
design already mandates **lossless** HLL deltas (no threshold) for a
different reason — dropping a register update underestimates permanently
(`delta-transmission-design.md` §9.3). The window-reset problem is
orthogonal and additional.

### 2.4 KLL — no delta by construction

KLL uses random compaction and is **not linearly mergeable**, so **no delta
encoding exists**. The edge wrapper makes this explicit:
`asap-precompute-go/sketches/kll.go:ComputeDeltaAgainst` always returns the
full snapshot with `isFull=true`, and the KLL processor rejects
`DeltaTransmission=true` at config validation. KLL is out of scope for this
contract — it always transmits full state.

---

## 3. Two Contract Options

The contract that MUST hold is a single sentence:

> **The meaning of `delta(N)` as produced by the edge and the meaning of
> `state(N) = f(state(N−1), delta(N))` as applied by the backend must be
> the same function over the same base.**

Today the edge produces a *difference of two independent per-window
sketches* and the backend applies an *additive merge onto a never-reset
running base*. There are exactly two clean ways to reconcile them.

### Option A — true per-window deltas + backend rotates the base each window

Make `delta(N)` mean "this window's events only" and make the backend's
`state(N)` mean "this window only" by **resetting the backend per-series
base at each window boundary**.

Producer (edge):

- Keep the per-window sketch reset (`window.go:rotateLocked` —
  **unchanged**; this is already what happens).
- Change `SnapshotCache.ComputeDelta` so that **after each window close it
  resets the outbound base to empty**, so the next window's "delta" is
  computed against an *empty* base — i.e. the delta IS that window's full
  per-window sketch encoded in delta form (true marginal). Equivalently:
  emit the window's own sketch as the delta, never diff against the prior
  window. This removes the cross-window subtraction entirely.
  (`asap-precompute-go/snapshot_cache.go`, `asap-precompute-rs/src/snapshot_cache.rs`.)
- Tag the window boundary on the wire so the backend knows when to rotate.
  The `SketchEnvelope` already carries `WindowStartMs`/`WindowEndMs`
  (`window.go:observeEnvelope` reads them); a per-window `is_window_close`
  marker (already present in the delta-design proto as
  `SketchDeltaEnvelope.is_window_close`) is the natural signal.

Consumer (backend):

- On a window-boundary marker (or a change in `(series_key, window_start)`),
  **finalize and reset** `ingest_state.sketch_snapshots[series_key]` to
  empty before applying the new window's deltas
  (`otel.rs:1290-1357`). `state(N)` is then exactly window N.
- The per-window finalized state is what feeds the existing per-bucket
  ASAP/warm-tier store, so windowed-range reducers
  (`count_over_time`, `quantile_over_time`, distinct-count over time) sum/
  merge across the correct per-window buckets.

Works for: **DDSketch, CMS, CountSketch, and HLL** — because the base is
reset per window, HLL's `max` no longer accumulates across windows, and the
additive families get a clean per-window total.

Trade-offs:

- (+) Matches the edge's existing reset behaviour — **no change to window
  rotation**, the highest-risk piece.
- (+) Fixes HLL, the only family the additive model can't express.
- (+) Self-healing: each window re-bases from empty, so a single dropped
  delta only corrupts one window, not the whole series lifetime.
- (−) Requires backend ingest changes (base rotation keyed on window
  boundary) and a reliable window-close signal on the wire.
- (−) The first delta of every window must carry the full window content
  (no inter-window compression), so the bandwidth win is the
  *within-window sub-flush* (OctoSketch-style) benefit only, not
  inter-window. (This matches `delta-transmission-design.md` §7's
  conclusion that sub-window deltas reduce burst, not total volume.)

### Option B — edge stops resetting between base and delta (cumulative epoch)

Keep the backend's existing additive-onto-running-base model **unchanged**
and make the edge produce genuinely cumulative snapshots so that
`snapshot(N)` really is `snapshot(N−1)` plus this window's events.

Producer (edge):

- **Stop resetting the per-series sketch between the base and the delta.**
  Within a longer "epoch" the running sketch accumulates across windows; a
  window boundary triggers a *delta emit* but **not** a sketch reset. The
  per-window reset in `window.go:rotateLocked` would have to be replaced (or
  bypassed for delta mode) by an accumulate-then-snapshot-then-emit-delta
  cycle that diffs the new cumulative state against the previously-emitted
  cumulative snapshot — exactly the additive model the backend wants.
  This is a **behavioural change to window rotation** and to the
  `SnapshotCache` always-refresh contract.
- A periodic full-frame re-baseline (epoch boundary) is needed to bound
  error accumulation and give the backend a clean `REPLACE` point.

Consumer (backend):

- **No change** — `otel.rs:1290-1357` already does exactly
  `state(N) = state(N−1) + delta(N)` with `REPLACE` on full frames.

Works for: **DDSketch, CMS, CountSketch** (additive families). **HLL still
needs care** — even cumulative, the never-reset base means windowed-range
cardinality queries read an all-epoch max, so per-window distinct-count is
not recoverable without an epoch reset. Cumulative-epoch HLL answers
"distinct over the whole epoch," which may be acceptable for some queries
but is not equivalent to per-window.

Trade-offs:

- (+) Zero backend change; the backend is already built for this model.
- (+) Inter-window delta compression is real (each delta is the marginal
  growth since the last emit), so bandwidth wins compound within an epoch.
- (−) Requires changing the edge's **window rotation** semantics — the
  load-bearing, well-tested hot path — and breaks the clean
  "one window = one sketch" model that the rest of the edge framework and
  the non-delta path assume.
- (−) HLL and per-window KLL-style queries become epoch-scoped, not
  window-scoped; windowed-range queries change meaning.
- (−) Cumulative state grows unbounded within an epoch (CMS/CS counters
  only grow), needing periodic full re-baseline anyway.

### 3.1 Recommendation

**Adopt Option A.** It preserves the edge's existing — and correct —
per-window reset, touches the lower-risk side (a base-rotation rule in the
backend ingest path plus a window-close marker that the proto already
defines), is the only option that makes **HLL** correct, and is
self-healing against lost frames. Option B's appeal (no backend change) is
outweighed by having to rewrite the edge's window rotation and by leaving
HLL/per-window semantics broken.

---

## 4. Recommendation & Rollout

### 4.1 Which option, which sketch first

- **Option A** (per-window deltas + backend per-window base rotation).
- **Enable DDSketch first.** It is the easiest to validate: the delta
  proto carries explicit additive `d_count`/`d_sum` scalars and lossless
  min/max, giving an independent receiver-side correctness check that the
  reconstructed per-window state matches the full-frame state. Once
  DDSketch is proven end-to-end, extend to CMS and CountSketch (same
  additive contract), then HLL (which Option A's base-rotation finally
  makes correct). KLL stays full-only forever.

### 4.2 How to gate it

The per-family `DeltaTransmission` flag is already wired through the edge:

- Fused edge processor default is **OFF**
  (`asapedgeprocessor/factory.go:34-35` — `DeltaTransmission: false`,
  "conservative: full state every window").
- Layer-3 config field `PrecomputeConfig.DeltaTransmission`
  (`asap-precompute-go/config.go:143-145`) plus `DeltaThreshold`
  (`config.go:146-153`) and `Encoding` (`config.go:154-161`) drive the
  `SnapshotCache.ComputeDelta` path (`precompute.go:495`).
- Per-family processors carry the same `delta_transmission` mapstructure
  key (e.g. `ddsketchprocessor/config.go:61-67`).

Keep the fused edge default OFF. Flip it on **per family** behind config
only after that family's Option-A path is implemented on **both** the edge
and the backend and passes the verification below. Until then, the
backend's "delta arrived before any base snapshot → drop" guard
(`otel.rs:1298-1307`) and the conservative default are the safety net.

### 4.3 Verification plan (the contract is the test)

The acceptance test is an **end-to-end equality check**: for the same input
stream and the same query set, **delta-ON must produce identical query
answers to delta-OFF (full-frame).**

1. **Edge round-trip unit test (per family):** assert that applying the
   Option-A per-window delta to an empty base reconstructs the same sketch
   bytes as the full-frame snapshot for that window
   (`ApplyDelta(ComputeDelta(empty, win)) == Snapshot(win)`), with
   `DeltaThreshold=1` (lossless).
2. **Backend ingest test:** feed a full-frame W1 then per-window deltas
   W2…WN with the window-close marker; assert the per-window finalized
   `sketch_snapshots[series_key]` equals an independently-built full sketch
   of each window, and that the negative/decrease cells are applied (not
   dropped) at `T=1`.
3. **E2E query equality:** run `deploy/scripts/queries-e2e.json`
   (`count_over_time`, `quantile_over_time(φ, …)`, distinct-count) against
   two backends — one fed delta-ON, one fed delta-OFF — and assert
   byte/numeric-equal PromQL responses.

### 4.4 Cross-runtime parity prerequisite (#243 / PR #450)

The Go and Rust edge runtimes **must emit byte-identical deltas** before
any of this is trustable in a mixed fleet — the bit-identical wire-format
promise of [#243](https://github.com/ProjectASAP/ASAPCollector/issues/243)
(closed; see `PROGRESS.md` "Cross-language byte-format parity, 5/5
sketches"). That parity was guarded by the
`integration/cross_host_parity/` golden harness (`PROGRESS.md:67-78`), which
asserted `asap-otel` (Go) ↔ `asap-otap` (Rust) byte-identical
`SketchEnvelope.Payload`s plus PromQL response equality.

**That harness was removed in PR #450** (commit `b88a837`, "chore: remove
unused integration/ test suite (#450)"). So before enabling deltas:

- **Re-establish the golden parity harness** (or an equivalent) covering the
  Option-A delta frames specifically — full-frame and per-window delta
  bytes must match across Go and Rust for the canonical golden input.
- Only then extend the parity assertion to PromQL response equality
  delta-ON vs delta-OFF (§4.3 step 3) across both runtimes.

---

## 5. Relationship to Existing Code

| Component | File | Today | Change needed for Option A |
|-----------|------|-------|-----------------------------|
| Edge window rotate | `asap-precompute-go/window.go:512-533`, `asap-precompute-rs/src/window.rs:344-357` | Resets per-series sketch each window | **No change** (this is what Option A relies on) |
| Edge snapshot cache | `asap-precompute-go/snapshot_cache.go:74-137`, `asap-precompute-rs/src/snapshot_cache.rs:114-150` | Always-refresh `delta = snap(N) − snap(N−1)` | Reset outbound base to empty at window close → delta = this-window-only |
| Wire window marker | `SketchEnvelope.WindowStartMs/EndMs`; `SketchDeltaEnvelope.is_window_close` | Present, partially used | Carry/honor a per-window close marker |
| Backend ingest delta apply | `ASAPQuery-backend/.../ingest/otel.rs:1290-1357` | Additive merge onto never-reset running base | **Reset/rotate base at window boundary** before applying new window's deltas |
| Backend delta appliers | `ingest/otel.rs:1875-1943` + `precompute_engine/operators/*_accumulator.rs` | Per-family additive `apply_proto_delta_bytes` | Unchanged (still additive; the base reset is in the caller) |
| Edge gating | `asapedgeprocessor/factory.go:34-35`; `asap-precompute-go/config.go:143-161` | `DeltaTransmission` default OFF | Flip per family only after §4.3 passes |
| Parity harness | (removed) `integration/cross_host_parity/` per PR #450 | Gone | Re-establish before enabling (#243) |
| KLL | `asap-precompute-go/sketches/kll.go` | Full-only; rejects `DeltaTransmission=true` | No change — KLL never gets deltas |

---

## 6. Open Questions

1. **Window-close signal reliability.** Option A's correctness hinges on the
   backend knowing exactly when a window closes per series. If a window-close
   frame is lost, the backend must fall back to detecting the boundary from
   `(series_key, window_start)` changing on the next frame. Which is the
   normative signal — an explicit `is_window_close` flag, or the window-start
   key transition?
2. **Fan-in / multiple senders.** If multiple edge collectors fan into one
   backend for the same `(series_key, window)`, the per-window base must be
   keyed by `(sender_id, series_key, window_start)` to avoid one sender's
   window-close resetting another's in-flight base (cf.
   `delta-transmission-design.md` §11 open question 3).
3. **Threshold and decreases across the reset.** With Option A the delta is
   this-window-only against an empty base, so all cells are increases from 0
   and thresholding is the bounded-error CMS case again — no cross-window
   decreases to drop. Confirm `DeltaThreshold` semantics with an empty base
   give the same `≤ T` per-query error bound as the full-frame path.
4. **DDSketch min/max under per-window base.** With base rotation the
   per-window min/max are correct; confirm the warm-tier reducer takes
   `min`/`max` across per-window buckets (not from a carried-forward base).
