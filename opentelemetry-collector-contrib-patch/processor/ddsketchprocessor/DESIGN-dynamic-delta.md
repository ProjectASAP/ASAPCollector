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
