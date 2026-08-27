# Controller Delta Transmission Decision — Design Document

<!-- Design metadata -->

## TL;DR

Decision policy for choosing raw, full-summary, or delta transmission.

**Status:** active

**MVP relationship:** in-scope.

This document is design-level: it defines scope, behavior, constraints, and trade-offs; implementation details are intentionally out of scope.


**Branch:** `67-controller-delta-transmission-decision`
**Date:** 2026-03-22
**Status:** Implemented

---

## 1. Problem Statement

The existing controller (see `control-plane-design.md`) selects a sketch type, aggregation
dimensions, and window strategy for each query workload, but it always emits the sketch
configuration with the same transmission mode: full sketch per flush.  Two gaps remain.

### 1.1 No Raw vs. Sketch Comparison

The cost model scores sketch types against each other but never compares them against the
baseline of simply forwarding raw OTLP samples.  For very small workloads (few series, low
sample rate) the per-sketch memory and CPU overhead can exceed the bandwidth savings.

### 1.2 Delta is Always Off

`delta-transmission-design.md` describes a mechanism that sends only cells that changed since
the last transmission, achieving 12–77× bandwidth reduction for typical workloads.  The
controller generates YAML without `delta_transmission: true`, so the feature is only enabled
by manual operator configuration.  Whether delta is actually worth enabling depends on:

- **Fill rate**: what fraction of sketch cells change per flush period (drives compression ratio).
- **Flush rate**: how often the sketch is transmitted (determines how many inserts accumulate
  per period and therefore the fill rate; also determines amortised CPU overhead per sample).
- **Memory overhead**: each delta-enabled sketch instance must maintain a snapshot of its
  previous state for computing the diff.

Without workload-aware analysis these tradeoffs are invisible to the controller.

---

## 2. Goals

1. Add a **WorkloadCharacteristics** input that the caller provides alongside a query workload
   to describe the data stream (series count, sample rate, raw sample size, key distribution,
   optional memory budget).

2. Extend the controller planner to compare three transmission strategies:

   | Strategy | Description |
   |---|---|
   | **Raw** | Forward every OTLP sample unchanged (no sketch processing). |
   | **Sketch (full)** | Aggregate into a sketch and transmit the complete payload each flush. |
   | **Sketch (delta)** | Aggregate into a sketch and transmit only the changed cells. |

3. Produce a **DeltaDecision** that records the chosen strategy, the estimated bandwidth for
   each alternative, and the CPU / memory overhead of delta encoding at the SDK or agent
   collector.

4. Surface the decision in the agent collector YAML (`delta_transmission`, `delta_threshold`)
   and in the HTTP `POST /api/v1/plan` response.

---

## 3. Key Concepts

### 3.1 Fill Rate

**Fill rate** is the fraction of sketch cells (or HLL registers) that change during one flush
period.  It is the primary driver of delta compression:

```
compression_ratio ≈ 1 / fill_rate    (rough intuition)
```

Fill rate depends on the number of **distinct keys** observed during the flush period, which
in turn depends on:

- The data distribution (Zipf → fewer distinct keys than Uniform)
- The flush period (longer period → more inserts → more distinct keys touched)
- The sketch dimensions (cols for CMS/CS, 2^precision registers for HLL)

### 3.2 Flush Rate

**Flush rate** (Hz) is how often the sketch is transmitted:

| Processor mode | Flush period |
|---|---|
| Window | `window_duration` |
| Batch | `repeat_every` from the query workload (or 1 s if unset) |

Flush rate and fill rate interact in opposite directions:

- Shorter flush period → fewer inserts per period → **lower fill rate → better delta
  compression ratio**
- Shorter flush period → more flushes per second → **higher total CPU overhead** (though
  the same CPU *per sample*, since it is amortised over fewer samples)

The latency SLA already constrains the flush period via `select_window_strategy()`, so the
delta decision evaluates the tradeoff at the chosen period rather than optimising it.

### 3.3 CPU Overhead (Amortised)

Delta encoding adds a diff-and-sparse-encode step on each flush:

```
delta_cpu_per_sample_µs = cpu_per_flush_µs / (samples_per_sec_per_series × flush_period_secs)
```

Longer flush periods have **lower per-sample CPU cost** because the fixed per-flush work is
amortised over more samples.  This is reported in `TransmissionCostSummary` so operators can
see how much extra CPU they are accepting at the agent or SDK.

### 3.4 Memory Overhead

Delta requires a snapshot of the previous transmitted sketch state in order to compute the
diff.  The snapshot is one full copy of the sketch per sketch instance:

```
delta_memory_bytes = series_count × dim_multiplier × snapshot_bytes_per_sketch
```

For CMS (5×2048, three float64 arrays): `series_count × dim_mult × 245 760 B`.  For 1 000
series with no aggregation this is ~240 MB — large enough to trigger the memory budget check
for agents with limited heap.

---

## 4. New Types

### 4.1 WorkloadCharacteristics

```rust
pub struct WorkloadCharacteristics {
    /// Number of distinct active time series.
    pub series_count: u64,
    /// Samples generated per second per series at the SDK / agent (Hz).
    pub samples_per_sec_per_series: f64,
    /// Wire size of one raw OTLP metric data point (bytes), typically 50–200.
    pub bytes_per_raw_sample: u32,
    /// Known distinct key values per flush period.  None → inferred analytically.
    pub distinct_keys_per_window: Option<u64>,
    /// Statistical distribution of keys.
    pub data_distribution: DataDistribution,  // Zipf | Uniform | Bursty
    /// Optional agent memory cap (bytes).  None → no budget constraint.
    pub memory_budget_bytes: Option<u64>,
}
```

Default: 1 000 series, 100 Hz, 100 B/sample, Zipf, no budget.  Callers may omit the field
entirely and the planner will use these conservative defaults.

### 4.2 DeltaDecision

```rust
pub enum DeltaDecision {
    UseDelta {
        threshold: f64,
        estimated_compression_ratio: f64,
        estimated_delta_bytes_per_sec: f64,
        delta_cpu_overhead_micros_per_sample: f64,
        delta_memory_overhead_bytes: f64,
    },
    UseFullSketch {
        reason: DeltaSkipReason,   // FillRateTooHigh | MemoryBudgetExceeded |
                                   // SketchTypeUnsupported | CompressionRatioBelowThreshold
        estimated_full_bytes_per_sec: f64,
    },
    UseRaw {
        reason: RawDataReason,     // WorkloadTooSmall
        estimated_raw_bytes_per_sec: f64,
    },
}
```

### 4.3 TransmissionCostSummary

Carried on every `CollectionPlan` regardless of which strategy was chosen, to give operators
visibility into the alternatives that were not selected:

```rust
pub struct TransmissionCostSummary {
    pub raw_bytes_per_sec: f64,
    pub sketch_full_bytes_per_sec: f64,
    pub sketch_delta_bytes_per_sec: f64,           // 0 if delta not viable
    pub delta_cpu_overhead_micros_per_sample: f64,
    pub delta_memory_overhead_bytes: f64,
    pub estimated_fill_rate: f64,
    pub flush_rate_hz: f64,
}
```

---

## 5. Delta Cost Model

### 5.1 Benchmark Table

Derived from `deltaaccbench` results (2026-03-15), sketch dimensions 5×2048, Zipf s=1.1,
10-second tumbling window, 2 000 inserts/window.  The three compression entries correspond to
three representative fill rates; the actual value is interpolated (§5.3).

| Sketch | Comp. @1% fill | Comp. @5% fill | Comp. @20% fill | CPU µs/flush | Snapshot bytes |
|---|---|---|---|---|---|
| CountMinSketch | 50× | 20× | 5× | 120 | 245 760 |
| CountSketch | 35× | 15× | 4× | 80 | 81 920 |
| HLL (p=14) | 12× | 6× | 2.5× | 30 | 16 384 |
| DDSketch | 8× | 3× | 1.5× | 20 | 8 192 |
| KLL | — | — | — | 0 | 0 |

KLL uses a compactor hierarchy that is not additively linear; no delta implementation exists.

### 5.2 Fill Rate Estimation

Fill rate is estimated analytically from the effective flush period and workload
characteristics.  `distinct_keys_per_flush` is either provided directly by the caller
(via `distinct_keys_per_window`) or estimated by distribution:

```
inserts_per_flush = samples_per_sec_per_series × series_count × flush_period_secs

distinct_per_flush =
  Zipf:    0.55 × inserts^0.91   (calibrated against deltaaccbench at s=1.1)
  Uniform: inserts                (every insert is a distinct key, worst case)
  Bursty:  0.30 × inserts^0.85   (traffic concentrates in a small key subset)
```

Per sketch type:

```
CMS / CS:   fill_rate = min(1, distinct_per_flush / cols)
             [each distinct key touches rows cells, one per hash function row]

HLL:         fill_rate = 1 − exp(−distinct_per_flush / registers)
             [birthday problem: fraction of 2^precision registers that get a new max]
             [conservative upper bound at steady state — registers grow monotonically
              so fewer are updated per window as the sketch matures]

DDSketch:    fill_rate = min(0.80, 0.10 × (flush_period_secs / 10.0) clamped [0.2, 8.0])
             [bucket fill depends on observed value range;
              empirical 10% baseline at 10-second window, scaled by flush period]

KLL:         0.0 (no delta)
```

### 5.3 Compression Ratio Interpolation

The three benchmark table entries at 1%, 5%, and 20% fill are linearly interpolated:

```
fill ≤ 1%:         compression_at_1pct
1% < fill ≤ 5%:    lerp(compression_at_1pct,  compression_at_5pct,  t) where t = (fill-1%)/(5%-1%)
5% < fill ≤ 20%:   lerp(compression_at_5pct,  compression_at_20pct, t) where t = (fill-5%)/(20%-5%)
fill > 20%:         lerp(compression_at_20pct, 1.0,                  t) where t = (fill-20%)/80%
```

Above 100% fill (saturation) the ratio reaches 1.0 — the delta payload is the same size as
the full payload due to index overhead.

### 5.4 Decision Algorithm

```
Input: CollectionPlan (sketch type, dims, window), QueryWorkload, WorkloadCharacteristics,
       bytes_per_series_per_sec (from benchmark table for chosen sketch type)

Compute:
  raw_bw      = series_count × samples_per_sec × bytes_per_raw_sample
  full_bw     = series_count × bytes_per_series_per_sec
  flush_secs  = window_duration  (window mode)
              = repeat_every     (batch mode, or 1 s if unset)
  flush_hz    = 1 / flush_secs

Step 1: WorkloadTooSmall?
  if series_count × samples_per_sec < 10.0 → UseRaw(WorkloadTooSmall)

Step 2: Delta supported?
  if sketch_type == KLL → UseFullSketch(SketchTypeUnsupported)

Step 3: Estimate fill rate
  fill_rate = estimate_fill_rate(wc, plan, w)   [see §5.2]

Step 4: Interpolate compression ratio
  ratio = interpolate_compression(delta_costs, fill_rate)   [see §5.3]

Step 5: Compute overhead
  snapshot_mem    = series_count × dim_multiplier × snapshot_bytes_per_sketch
  cpu_per_sample  = cpu_micros_per_flush / (samples_per_sec_per_series × flush_secs)
  delta_bw        = full_bw / ratio

Step 6: Compression ratio below threshold?
  if ratio < 2.0 → UseFullSketch(FillRateTooHigh or CompressionRatioBelowThreshold)

Step 7: Memory budget exceeded?
  if memory_budget_bytes set AND snapshot_mem > memory_budget → UseFullSketch(MemoryBudgetExceeded)

Step 8: Use delta
  → UseDelta { threshold=1.0, ratio, delta_bw, cpu_per_sample, snapshot_mem }
```

The minimum compression threshold of 2.0× ensures delta is only enabled when the bandwidth
saving clearly outweighs the snapshot memory and diff CPU cost.

### 5.5 Delta Threshold Selection

The current implementation uses `threshold = 1.0` (lossless sparse encoding: every non-zero
cell change is transmitted).  This is the conservative default from `delta-transmission-design.md`.

An adaptive threshold `T = max(1, p95(|ΔS[r][c]|) × α)` where `α ∈ [0.05, 0.2]` is left for
a future improvement; it would reduce payload further at the cost of bounded additional error.

---

## 6. Integration with the Controller

### 6.1 CostModelPlanner Changes

`CostModelPlanner::plan()` now accepts an optional `WorkloadCharacteristics`:

```rust
pub fn plan(
    &self,
    w: &QueryWorkload,
    wc: Option<&WorkloadCharacteristics>,
) -> CollectionPlan
```

After selecting the optimal sketch type (unchanged), the planner calls `apply_delta_decision()`
which:

1. Looks up `bytes_per_series_per_sec` for the chosen sketch type from the existing benchmark
   table (`benchmark_table_pub()`).
2. Calls `decide_delta(plan, w, wc, bytes_per_series_per_sec)`.
3. Writes `delta_transmission` and `delta_threshold` into `agent_config`.
4. Writes `delta_decision` and `transmission_cost_summary` into the plan.

### 6.2 YAML Generation Changes

`build_processor_block()` in `config/agent.rs` emits the delta fields only when
`delta_transmission` is true (absent from YAML when false — no change to existing processor
defaults):

```yaml
processors:
  countminsketch:
    mode: window
    window_duration: 10s
    rows: 5
    cols: 2048
    delta_transmission: true
    delta_threshold: 1.0
    aggregate_by: [service]
    enable_self_monitoring: true
    transmit_sketch: true
```

### 6.3 API Changes

`POST /api/v1/plan` request body gains an optional `workload` field:

```json
{
  "metric_name": "http_request_duration",
  "aggregations": ["frequency"],
  "time_window": "10s",
  "accuracy_sla": 0.01,
  "workload": {
    "series_count": 500,
    "samples_per_sec_per_series": 20,
    "bytes_per_raw_sample": 120,
    "data_distribution": "zipf"
  }
}
```

The response gains `delta_decision` and `transmission_costs`:

```json
{
  "metric": "http_request_duration",
  "sketch_type": "countminsketch",
  "mode": "window",
  "delta_decision": {
    "mode": "use_delta",
    "threshold": 1.0,
    "estimated_compression_ratio": 18.4,
    "estimated_delta_bytes_per_sec": 5435,
    "delta_cpu_overhead_micros_per_sample": 0.012,
    "delta_memory_overhead_bytes": 122880000
  },
  "transmission_costs": {
    "raw_bytes_per_sec": 1200000,
    "sketch_full_bytes_per_sec": 100000,
    "sketch_delta_bytes_per_sec": 5435,
    "delta_cpu_overhead_micros_per_sample": 0.012,
    "delta_memory_overhead_bytes": 122880000,
    "estimated_fill_rate": 0.054,
    "flush_rate_hz": 0.1
  }
}
```

---

## 7. Decision Interaction with Window Strategy

The window strategy (`select_window_strategy()`) and the delta decision are coupled through
the flush period:

```
latency_sla < time_window  →  Batch mode  →  flush_period = repeat_every (or 1s)
latency_sla ≥ time_window  →  Window mode →  flush_period = time_window
```

A tight latency SLA forces short flush periods (batch mode with frequent batches).  This has
two opposing effects on delta:

| Effect | Direction |
|---|---|
| Fewer inserts per flush → lower fill rate | Helps delta (better compression) |
| More flushes per second → higher CPU/s | Hurts (more diff operations per second) |

The per-sample CPU cost is invariant to flush rate (`cpu_per_flush / (rate × flush_secs)`
stays constant), so the CPU budget is not a reason to choose longer windows.  The main reason
window-mode with long periods may hurt delta is that fill rate increases with period length.

---

## 8. Worked Examples

### Example A — Frequency sketch, Zipf traffic, 10-second window

```
Input:
  series_count = 500,  samples_per_sec_per_series = 20 Hz
  data_distribution = Zipf,  bytes_per_raw_sample = 120
  sketch = CountMinSketch (rows=5, cols=2048),  window = 10s

Derivation:
  flush_period  = 10 s
  inserts       = 500 × 20 × 10 = 100 000
  distinct      = 0.55 × 100 000^0.91 ≈ 0.55 × 32 558 ≈ 17 907
  fill_rate     = 17 907 / 2048 ≈ 8.7 %
  compression   = lerp(20, 5, t=(8.7-5)/(20-5)=0.247) ≈ 16.3×
  raw_bw        = 500 × 20 × 120 = 1 200 000 B/s
  full_bw       = 500 × 200 = 100 000 B/s
  delta_bw      = 100 000 / 16.3 = 6 135 B/s
  snapshot_mem  = 500 × 1 × 245 760 = 116 MB
  cpu/sample    = 120 / (20 × 10) = 0.6 µs/sample

Decision: UseDelta { threshold=1.0, ratio=16.3×, delta_bw=6 135 B/s,
                     cpu=0.6 µs/sample, memory=116 MB }
```

Bandwidth path: raw 1.2 MB/s → sketch full 100 KB/s → sketch delta **6 KB/s**.

### Example B — Same workload, Uniform distribution, 1-hour window

```
  flush_period  = 3600 s
  inserts       = 500 × 20 × 3600 = 36 000 000
  distinct      = 36 000 000   (Uniform: all inserts distinct)
  fill_rate     = 36 000 000 / 2048 ≈ 17 578  →  capped at 1.0
  compression   = lerp(5, 1.0, t=(1.0-0.20)/0.80=1.0) = 1.0×
  ratio < 2.0   → UseFullSketch(FillRateTooHigh)
```

Uniform traffic over a long window saturates the sketch; delta encoding produces payloads the
same size as full sketches.

### Example C — HLL cardinality, Zipf traffic, 30-second window

```
  flush_period  = 30 s,  HLL precision = 14 → registers = 16 384
  inserts       = 500 × 20 × 30 = 300 000
  distinct      = 0.55 × 300 000^0.91 ≈ 0.55 × 82 700 ≈ 45 485
  fill_rate     = 1 − exp(−45 485 / 16 384) = 1 − exp(−2.78) ≈ 93.8 %
  compression   = lerp(2.5, 1.0, t=(0.938-0.20)/0.80=0.92) ≈ 1.1×
  ratio < 2.0   → UseFullSketch(CompressionRatioBelowThreshold)
```

HLL registers are monotonically increasing (max operation), so at high cardinality almost
every register reaches its final value within the first few windows; subsequent deltas are
sparse, but only after the sketch matures.  A future adaptive-threshold or "wait-for-maturity"
optimisation could handle this case.

### Example D — Memory budget constraint

Same as Example A, but the agent has a 64 MB memory budget:

```
  snapshot_mem = 116 MB > 64 MB → UseFullSketch(MemoryBudgetExceeded)
```

The operator can reduce `series_count` (by adding more selective `label_matchers`) or reduce
sketch dimensions (`cols`) to bring the snapshot within budget, at the cost of higher sketch
error.

---

## 9. File Inventory

| File | Change |
|---|---|
| `controller/src/types.rs` | Add `DataDistribution`, `WorkloadCharacteristics`, `DeltaDecision`, `DeltaSkipReason`, `RawDataReason`, `TransmissionCostSummary`; extend `AgentCollectorConfig` with `delta_transmission` + `delta_threshold`; extend `CollectionPlan` with `delta_decision` + `transmission_cost_summary` |
| `controller/src/planner/delta_cost_model.rs` | **New.** Delta benchmark table, fill-rate estimation, compression interpolation, bandwidth helpers, `decide_delta()`, full unit-test suite |
| `controller/src/planner/mod.rs` | Expose `pub mod delta_cost_model` |
| `controller/src/planner/rules.rs` | Populate new `AgentCollectorConfig` and `CollectionPlan` fields with disabled defaults |
| `controller/src/planner/cost_model.rs` | `CostModelPlanner::plan()` accepts `Option<&WorkloadCharacteristics>`; calls `apply_delta_decision()` after sketch selection |
| `controller/src/config/agent.rs` | Emit `delta_transmission` + `delta_threshold` in processor YAML when enabled |
| `controller/src/analyzer.rs` | Add `#[serde(default)] workload: WorkloadCharacteristics` to `QuerySpec` |
| `controller/src/main.rs` | Extract `wc` from spec before `analyze()`; pass to `plan()`; include `delta_decision` and `transmission_costs` in response |

---

## 10. Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Minimum compression ratio | 2.0× | Below 2× the snapshot memory and diff CPU are not justified by bandwidth savings.  Well below CMS/CS typical 15–50× at low fill rates, so the threshold is only hit when conditions are genuinely unfavourable. |
| Default delta threshold T | 1.0 (lossless sparse) | Conservative starting point.  Adaptive threshold (`p95 × α`) is a follow-on; lossless T=1 ensures the introduction of delta does not silently add approximation error on top of the sketch's existing relative error. |
| `UseRaw` condition | `total_sample_rate < 10 Hz` | All aggregation types (Quantile, Cardinality, Frequency) require sketch structures for correct approximate answers, so raw pass-through is only valid when the workload is too tiny to justify the sketch overhead. |
| Batch-mode flush period | `repeat_every` (or 1 s) | In batch mode there is no fixed window boundary; the agent flushes on each incoming metric batch.  `repeat_every` is the best available proxy for how often batches arrive. |
| Fill rate formula for HLL | Birthday-problem upper bound | HLL registers grow monotonically; at steady state fewer registers are updated per window than the formula predicts.  The upper bound makes the delta decision conservative (leans toward UseFullSketch), which is safe. |
| `WorkloadCharacteristics` optional | Default to 1 000 series / 100 Hz / Zipf | Keeps the API backward-compatible.  Callers that provide actual observations will get more accurate decisions; callers that omit the field get a reasonable conservative default. |
| Snapshot memory per instance | `series_count × dim_mult × snapshot_bytes` | Each sketch instance (unique group-by partition) needs its own snapshot.  dim_multiplier accounts for the fact that `aggregate_by` labels fan out into multiple independent sketches. |

---

## 11. Relationship to Other Design Documents

- **`delta-transmission-design.md`** describes the wire protocol, protobuf messages, and
  mathematical foundation for delta encoding.  This document describes when the controller
  decides to *enable* delta encoding and why.
- **`control-plane-design.md`** describes the overall controller architecture (sketch type
  selection, dimension aggregation, window strategy).  This document extends that architecture
  with a fourth decision: transmission mode.
