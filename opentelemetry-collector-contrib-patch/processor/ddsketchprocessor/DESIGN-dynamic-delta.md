# Dynamic Granularity Delta (Paper §4.2 Fig.3 Mode 3)

## Problem

Currently the DDSketch processor supports two modes:
1. **Full sketch per window** — `S(t0,t1), S(t1,t2)` (strawman)
2. **Delta-of-sketches per window** — `S(t1,t2) - S(t0,t1)` (existing `delta_transmission`)

The paper claims a third mode:
3. **Dynamic granularity delta** — emit deltas at finer sub-window intervals

## Design

### New config fields

```yaml
processors:
  ddsketch:
    mode: window
    window_duration: 5m            # tumbling window size
    delta_transmission: true
    # NEW: sub-window delta interval
    sub_window_interval: 30s       # emit delta every 30s within the 5m window
```

When `sub_window_interval` is set and `delta_transmission` is true:
- The processor maintains the tumbling window sketch as before
- Every `sub_window_interval`, it computes `delta = current_sketch - last_emitted_sketch`
- Emits the delta (sparse: only changed buckets)
- At window boundary, emits the final delta and resets

### Receiver-side reconstruction

The backend merge processor accumulates deltas:
```
received_deltas = [Δ(0s-30s), Δ(30s-60s), Δ(60s-90s), ...]
full_sketch_at_time_t = sum(received_deltas[0..t])
```

### Benefits

- Bandwidth: instead of one 4KB sketch every 5m, send ~200B deltas every 30s
- Latency: 30s data freshness instead of 5m
- Flexibility: backend can reconstruct at any sub-window granularity

### Go implementation changes

**File: `processor/ddsketchprocessor/processor.go`**:
1. Add `subWindowTicker *time.Ticker` alongside the existing window ticker
2. In the window goroutine, listen on both tickers
3. On sub-window tick: compute delta, emit, update `lastEmittedSketch`
4. On window tick: emit final delta, reset both sketch and lastEmitted

**File: `processor/ddsketchprocessor/config.go`**:
1. Add `SubWindowInterval time.Duration \`mapstructure:"sub_window_interval"\``
2. Validate: `sub_window_interval` must be < `window_duration` and > 0
3. Apply same changes to CountSketch and CountMinSketch processors

### Testing

1. Unit test: verify delta output equals full sketch minus previous
2. Benchmark: compare bandwidth at 5m-window vs 30s-sub-window-delta
3. Accuracy: verify reconstruction error = 0 (lossless for additive sketches)

## Making it dynamic

The `sub_window_interval` above is a **static** cadence — a fixed timer. That
gives sub-window deltas, but it emits the same number of frames whether the
data is bursty or idle, and it cannot react to bandwidth pressure or freshness
needs. To make the granularity genuinely *dynamic*, generalize the fixed
interval into an adaptive emission policy.

### Three axes of adaptivity

1. **Data-adaptive (change-triggered).** Instead of emitting on a clock, emit
   when the *accumulated change since the last emit* crosses a threshold,
   clamped to `[min_interval, max_interval]`. Bursty windows produce many fine
   deltas; idle windows produce one. The change measure is per-family:
   - DDSketch / CMS / CountSketch (additive): number of dirty buckets/cells, or
     the L1 mass of the pending sparse delta.
   - HLL: number of registers that increased (lossless — never dropped).

   This is the *temporal* analog of the adaptive **threshold** in
   [`controller-delta-decision-design.md`](../../../../docs/controller-delta-decision-design.md)
   §5.5 (`T = max(1, p95(|ΔS|)·α)`): that knob picks *which cells* to send,
   this one picks *when* to send.
2. **Budget-adaptive.** A per-agent / per-metric bytes·s⁻¹ or msgs·s⁻¹ budget
   via a token bucket — coarsen under congestion, finen under slack. Feeds from
   the `TransmissionCostSummary` the controller already computes.
3. **Controller-adaptive.** Make the cadence a third output of the controller's
   `DeltaDecision` (today it emits `delta_transmission` on/off + `delta_threshold`;
   add a `granularity` policy), pushed and re-tuned at runtime via OpAMP →
   `Precompute.UpdateConfig` **in place** (the control-channel wiring) — no
   restart, no state rebuild. The controller's optimization
   ([`controller-optimization-problem.md`](../../../../docs/controller-optimization-problem.md))
   then trades bandwidth vs. freshness vs. accuracy globally and per-metric.
   *This* is what makes the granularity dynamic rather than a hand-tuned constant.

### Hybrid trigger

```
emit when  (pending_change ≥ change_threshold  AND  elapsed ≥ min_interval)
           OR  elapsed ≥ max_interval
```

`min_interval` debounces emit-storms under churn; `max_interval` is the
**freshness SLA** (the query staleness bound). The static `sub_window_interval`
is the degenerate case `min_interval == max_interval == sub_window_interval`
(i.e. `mode: fixed`) — so no config break.

### Config (superset of the static field)

```yaml
processors:
  ddsketch:
    window_duration: 5m
    delta_transmission: true
    delta_granularity:
      mode: change          # fixed | change | budget
      min_interval: 5s      # debounce
      max_interval: 30s     # freshness SLA (== the static sub_window_interval)
      change_threshold: 0.05   # mode=change: fraction of buckets/mass changed
      # bytes_budget: 2KiB/s   # mode=budget
```

### Composition with the delta–baseline contract

Sub-window deltas are exactly the within-window incremental emits described in
[`delta-baseline-contract.md`](../../../../docs/delta-baseline-contract.md)
(Option A): the first emit of a window carries full content (a delta against
the empty post-window-reset base), each later emit is incremental against the
previous one, and the **backend rotates its per-series base at the window
boundary** (detected by a `window_start` change). Dynamic granularity only
changes *how many* deltas arrive per window — the backend accumulates them
additively, so **no new receiver mechanism is needed** beyond Option A's base
rotation. Reconstruction stays lossless for additive sketches; HLL stays
lossless via no-drop register deltas.

### Per-family / parity notes

- **HLL** stays lossless (`delta-transmission-design.md` §9.3): the change
  trigger counts register increases but never drops one. **KLL** is full-only
  (no sub-window).
- The trigger is an **edge-local decision** — it does not change the wire bytes
  of an emitted frame, so Go↔Rust byte-parity (#243) is unaffected (only the
  emitted delta frames must be byte-identical for identical content, which they
  are).
- Change-triggered emission makes the per-series rate **data-dependent** (harder
  to predict) → `mode: budget` bounds it; the per-series "pending change"
  counter is bounded by `MaxSeries`.

### Implementation order

1. **Static mechanism** (`mode: fixed`) — the sub-window ticker described above.
2. **Hybrid trigger** (`mode: change`) — a per-series change accumulator plus the
   min/max-interval clamp.
3. **Controller-driven** — `DeltaDecision.granularity` + OpAMP push, choosing
   `mode: change|budget` per metric.

All of it sits behind the per-family `delta_transmission` flag and the Option-A
backend base rotation (delta-enablement Phase 2), so this is the natural
follow-on once that lands.
