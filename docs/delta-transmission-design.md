# Delta Transmission over Time Windows — Design Document

**Branch:** `63-delta-transmission-over-time-windows`
**Date:** 2026-03-20
**Status:** Draft

---

## 1. Problem Statement

### 1.1 Static Allocation Overhead

Both CountSketch (CS) and CountMinSketch (CMS) pre-allocate a fixed two-dimensional counter array at startup, regardless of how many distinct items are actually observed during a collection interval.

| Processor | Rows | Cols | Cell type | Array size |
|-----------|------|------|-----------|-----------|
| CMS (default) | 5 | 1 024 | float64 | 40 KB per partition |
| CS (ε=0.01, δ=0.99) | ~5 | ~10 000 | float64 | ~400 KB per partition |

With Mode 3 (label selection × window), each unique `group_by` combination allocates its own full 2D array, so a collector seeing 100 distinct `{region, service}` pairs holds ~4 MB of CMS state and ~40 MB of CS state per window — most cells containing zero.

### 1.2 Full-Sketch Transmission Wastes Bandwidth

Today, every flush sends the **entire serialized sketch** (all rows × cols cells) to the downstream collector, even though most of the array did not change since the last transmission. For a 10 s window at 50 k metrics/s, the fraction of non-zero cells after one window is typically 1–5 %; the other 95–99 % is zero-padding sent over the wire for free.

### 1.3 Three Aggregation Modes Have Different Delta Characteristics

| Mode | What changes each interval | Natural delta size |
|------|---------------------------|--------------------|
| **Mode 1** — by series labels (full attribute set) | Counters of active series grow; inactive series stay at zero | Small — only active series touched |
| **Mode 2** — by window per series (tumbling window) | All counters reset at window boundary; within a window, growth is monotone | Medium — delta = full window content at each boundary; within-window deltas are small |
| **Mode 3** — matrix (selected labels × window) | Sparse across partitions; within each partition follows Mode 2 | Smallest — sparsity compounds across two dimensions |

---

## 2. Inspiration: OctoSketch (NSDI 2024)

> Zhang, Chen, Liu — *"OctoSketch: Enabling Real-Time, Continuous Network Monitoring over Multiple Cores"*, USENIX NSDI 2024.
> Source: https://github.com/Froot-NetSys/OctoSketch

OctoSketch solves an analogous problem in the multi-core setting: how to keep a shared aggregated sketch up-to-date across worker cores without transmitting full sketches on every synchronisation cycle.

### 2.1 Key Insight — Change-Based Aggregation

OctoSketch exploits the **linear mergeability** of classical sketches: for any linearly mergeable sketch S,

```
S(D₁ ∪ D₂) = S(D₁) ⊕ S(D₂)
```

where ⊕ is cell-wise addition. This means a full sketch can be reconstructed by summing any sequence of partial (delta) sketches that together cover the same data.

OctoSketch's aggregation algorithm (Algorithm 1 in the paper):

1. Each worker maintains a local counter array and a threshold **T**.
2. When a counter at position `[r, c]` exceeds **T**, the worker pushes the tuple `(flow_key, r, c, counter_value)` into a lock-free queue and **resets** that counter to 0.
3. An aggregator thread continuously dequeues tuples and applies them to the shared sketch with `aggregated[r][c] += delta_value`.

The result is that only positions whose value crossed the threshold are ever transmitted. The aggregated sketch converges to the ground-truth sketch as if all insertions had been applied directly.

### 2.2 Claimed Gains

- **2.6× smaller message footprint** than Iceberg (a competing streaming heavy-hitter system).
- **15.6× lower query error** vs. periodic full-sketch-merge at the same transmission budget.
- Near-ideal single-core accuracy at line-rate (97 Mpps on CPU).

### 2.3 Applicability to This Project

OctoSketch targets shared-memory inter-core communication; we target network transmission over OTLP/gRPC between an SDK/agent and a collector. The mathematics are identical — linearity holds for both CS and CMS — but the threshold **T** must be tuned much higher to amortise the per-message network overhead.

---

## 3. Mathematical Foundation

### 3.1 Sketch Linearity (CS and CMS are both linear)

**CountMinSketch** — counter array C[rows][cols]:
```
Insert(x): for each row r, C[r][h_r(x)] += 1
Merge:      C_merged[r][c] = C₁[r][c] + C₂[r][c]   ∀ r, c
```

**CountSketch** — signed counter array C[rows][cols]:
```
Insert(x, w): for each row r, C[r][h_r(x)] += g_r(x) · w
Merge:         C_merged[r][c] = C₁[r][c] + C₂[r][c]   ∀ r, c
```

Both are linear maps from dataset → ℝ^(rows×cols). Therefore:

```
S(t₂) − S(t₁) = S(data arriving in [t₁, t₂))   ← the "delta sketch"
```

A receiver holding `S(t₁)` can reconstruct `S(t₂)` by simply adding the delta:
```
S(t₂) = S(t₁) + ΔS(t₁ → t₂)
```

### 3.2 Sparse Representation of ΔS

Define the **delta sketch** after interval `[t_prev, t_now)` as:

```
ΔS[r][c] = S_current[r][c] − S_snapshot[r][c]
```

where `S_snapshot` is the state of the sketch at the time of the last transmission.

After applying a threshold **T**:

```
Transmitted cells = { (r, c, ΔS[r][c]) : |ΔS[r][c]| ≥ T }
```

Cells below **T** are **not transmitted**; their value is approximated as 0 at the receiver. This introduces a bounded approximation error: for any query on the aggregated sketch, the error introduced by threshold filtering is at most `T × (number of non-transmitted cells that hash to the queried key)`.

### 3.3 Error Bound Relative to Full-Sketch Transmission

Let `q*` be the exact query result on the aggregated sketch (as if full sketches were always transmitted), and `q̂` be the result after threshold filtering.

For CMS frequency queries:
```
|q̂ − q*| ≤ T · (min over rows r of |{c : dropped in row r}|)
         ≤ T · cols
```

In practice the bound is much tighter because dropped cells are zero or near-zero, and the min-over-rows operation discards most of the noise.

---

## 4. Delta Encoding Format

### 4.1 Actual Struct Shapes

Before specifying the wire format, it is important to note that CMS and CS each contain
**multiple** arrays that are all additive under `Merge`.

**CountMinSketch** (`CountMinSketch.go:14`):
```
Count [rows][cols] float64   — frequency count          (3 arrays, all linear)
Sum   [rows][cols] float64   — sum of values
Sum2  [rows][cols] float64   — sum of squares
L1    [rows]       float64   — per-row L1 norm           (2 row vectors)
L2    [rows]       float64   — per-row L2 norm
```
Full payload: `3 × rows × cols × 8 + 2 × rows × 8` bytes.
For 5×1024: `3 × 40 960 + 80 ≈ 123 KB` (not 40 KB — `Count` alone is 40 KB).

**CountSketch** (`CountSketch.go:29`):
```
Count [rows][cols] float64   — signed frequency counter  (linear, delta-able)
L2    [rows]       float64   — per-row L2 norm           (linear, delta-able)
TopK  *TopKHeap              — top-K heap                (NON-linear, cannot be subtracted)
```
`TopK` must be transmitted separately in full every flush (K entries × ~20 B, typically ≤ 2 KB).

**HLL** (`hyperloglog.go:36`):
```
Registers [16384] uint8      — leading-zero counts (max-based, not additive)
```

### 4.2 Serialization Codec

`sketchlib-go` will have full protobuf support. All sketch serialization — both full-sketch
and sparse-delta payloads — uses protobuf encoding. The existing `sketch_payload` bytes field
on CMS/CS data points and the `Sketch` bytes field on HLL data points carry a protobuf message;
the `encoding` attribute on the data point distinguishes full-sketch from sparse-delta:

```
encoding = "proto_full"   — existing full sketch (all rows × cols cells)
encoding = "proto_delta"  — sparse delta (only changed cells / registers)
```

### 4.3 Protobuf Messages

> These messages are added to the existing `proto/sketchlib.proto` in sketchlib-go
> (see §6 Phase 1). `TopKState` and `HeapEntry` already exist in that file and are reused.

```protobuf
// ─── Envelope ────────────────────────────────────────────────────────────────
// SketchDeltaEnvelope is the top-level message carried in sketch_payload bytes.
message SketchDeltaEnvelope {
  string partition_key      = 1;  // identifies which partition sketch this belongs to
  int64  window_start_ms    = 2;  // tumbling window start (unix ms); 0 = no window
  bool   is_window_close    = 3;  // true = finalize and reset accumulator at receiver

  oneof payload {
    CMSSparseSnapshot  cms = 10;
    CSSparseSnapshot   cs  = 11;
    HLLSparseSnapshot  hll = 12;
  }
}

// ─── CMS ─────────────────────────────────────────────────────────────────────
// CMSSparseSnapshot carries only the cells that changed since the last snapshot.
//
// CMS has three parallel 2D arrays (Count, Sum, Sum2) that are all additive
// under Merge. The three delta values are co-located per cell to avoid three
// separate scans and to keep the message compact.
// L1 and L2 are per-row norm vectors (rows entries each, ≤ 40 B); always
// transmitted in full because they are negligibly small.
message CMSSparseSnapshot {
  int32              rows  = 1;
  int32              cols  = 2;
  repeated CMSCell   cells = 3;  // only cells where any delta is non-zero
  repeated double    l1    = 4;  // full row vector, packed
  repeated double    l2    = 5;  // full row vector, packed
}

// CMSCell encodes deltas for all three counter arrays at a single (row, col).
// Proto varint encoding: row (≤ 8 → 1 byte), col (≤ 65535 → 2–3 bytes).
// double fields: 8 bytes each (fixed64 on wire).
// Wire size per cell: ~1 + 2 + 3×9 = ~30 bytes (field tag + varint/fixed64).
message CMSCell {
  uint32 row    = 1;
  uint32 col    = 2;
  double d_count = 3;
  double d_sum   = 4;
  double d_sum2  = 5;
}

// ─── CS ──────────────────────────────────────────────────────────────────────
// CSSparseSnapshot for CountSketch.
//
// Count[rows][cols] and L2[rows] are additive and delta-able.
// TopK is a heap (non-linear; cannot be subtracted): transmitted full every
// flush. K is typically 10–100, so TopK adds ≤ 2 KB regardless of fill rate.
message CSSparseSnapshot {
  int32            rows  = 1;
  int32            cols  = 2;
  repeated CSCell  cells = 3;  // only cells where d_count != 0
  repeated double  l2    = 4;  // full row vector, packed
  repeated TopKEntry topk = 5; // full heap, always
}

// CSCell: signed delta (CountSketch uses sign-flip counters).
// Wire size per cell: ~1 + 2 + 9 = ~12 bytes.
message CSCell {
  uint32 row     = 1;
  uint32 col     = 2;
  double d_count = 3;  // signed
}

message TopKEntry {
  string key   = 1;
  int64  count = 2;
}

// ─── HLL ─────────────────────────────────────────────────────────────────────
// HLLSparseSnapshot carries only registers that increased since the last snapshot.
// Merge operator is max (not addition) — see §9. No threshold applies:
// every increased register MUST be transmitted to avoid underestimation.
message HLLSparseSnapshot {
  repeated HLLRegisterUpdate updates = 1;
}

// HLLRegisterUpdate: one register that increased.
// index ∈ [0, 16383] → varint 2–3 bytes.
// value ∈ [0, 51]    → varint 1 byte.
// Wire size per update: ~1 + 2 + 1 + 1 = ~5 bytes (field tags + varints).
message HLLRegisterUpdate {
  uint32 index = 1;
  uint32 value = 2;
}
```

### 4.4 Corrected Size Comparison

| Sketch | Full payload (proto) | Sparse delta at 5% fill | Sparse delta at 1% fill |
|--------|---------------------|--------------------------|--------------------------|
| CMS 5×1024 | ~123 KB | 5×51 cells×30 B + 80 B ≈ **7.7 KB** | 5×10 cells×30 B + 80 B ≈ **1.6 KB** |
| CS 5×10k | ~400 KB | 5×500 cells×12 B + TopK ≈ **32 KB** | 5×100 cells×12 B + TopK ≈ **8 KB** |
| HLL 16384 regs | 16 KB | ~1000 updates×5 B ≈ **5 KB** | ~200 updates×5 B ≈ **1 KB** |

**Bandwidth reduction: 16–77× for CMS, 12–50× for CS, 3–16× for HLL at typical fill rates.**
HLL has the least relative benefit because its full payload is already the smallest (uint8 registers
vs. float64 counters) and delta transmission must be lossless (§9.3).

### 4.6 Threshold T Selection

| Threshold | Transmitted cells | Max error introduced |
|-----------|-------------------|----------------------|
| T = 0 | All non-zero (lossless) | 0 |
| T = 1 | Cells with Δ ≥ 2 | ≤ 1 per query position |
| T = 10 | Cells with Δ ≥ 10 | ≤ 10 per query position |

Recommended starting value: **T = 1** (lossless sparse encoding, no approximation error beyond the sketch's inherent ε). Higher T for bandwidth-constrained links.

### 4.7 Adaptive Threshold (OctoSketch-inspired)

Track the average delta magnitude per flush:

```
T_next = max(1, p95(|ΔS[r][c]| for non-zero cells) × α)
```

where α ∈ [0.05, 0.2] is a tunable fraction. This keeps transmitted cells roughly constant as traffic rates vary.

---

## 5. Mode-Specific Delta Design

### 5.1 Mode 1 — Aggregation by Series Labels

**State at sender:**
```
per partition_key:
  S_current   map[string]*Sketch   // live accumulator
  S_snapshot  map[string]*Sketch   // sketch state at last transmission
```

**Flush cycle (period P, e.g., every 30 s):**
1. For each partition key K:
   a. Compute `ΔS_K = S_current_K − S_snapshot_K` (cell-wise subtraction).
   b. Encode sparse cells where `|ΔS_K[r][c]| ≥ T`.
   c. Transmit `SketchDelta{partition_key=K, cells=..., is_window_close=false}`.
   d. Update `S_snapshot_K = copy(S_current_K)`.
2. Do **not** reset `S_current` — it keeps accumulating across flushes.

**At receiver:**
- Maintain `S_agg_K` per partition key.
- On receiving `SketchDelta{K, cells}`: apply `S_agg_K[r][c] += cell.delta` for each transmitted cell.
- Answer queries from `S_agg_K` at any time.

**Key property:** Within-interval query accuracy degrades only by cells below threshold T; as more deltas arrive, the aggregated sketch converges to the full cumulative sketch.

---

### 5.2 Mode 2 — Aggregation by Window per Series

Each series (full attribute set → partition key) has a **tumbling window** of duration W.

**State at sender:**
```
per partition_key:
  S_window     *Sketch    // accumulator for current window [t_start, now)
  window_start time.Time
  // No snapshot needed: within a window, delta = S_window itself (since it was
  // reset to zero at window start). Mid-window deltas are sub-sketches.
```

**Sub-window delta flush (period P < W, e.g., P = W/4):**
1. For each partition key K:
   a. Take a **snapshot of current S_window_K**.
   b. Compute `ΔS_K = S_window_K_now − S_window_K_last_delta_sent`.
   c. Transmit sparse delta.
   d. Update `S_window_K_last_delta_sent = snapshot`.

**Window close (at t = window_start + W):**
1. Flush final delta as above with `is_window_close=true`.
2. Reset `S_window_K` to zero, advance `window_start`.

**At receiver:**
- Maintain `S_agg_K` per partition key (accumulates deltas within current window).
- On `is_window_close=true`: finalize (store or forward `S_agg_K`), then reset `S_agg_K` to zero.

**Bandwidth profile:** The first sub-window flush of a window has the highest fill; subsequent flushes within the same window transmit only the marginal additions.

---

### 5.3 Mode 3 — Matrix (Selected Labels × Window)

Mode 3 is the composition of Modes 1 and 2: partition key selects a row in the matrix; the window dimension selects a column. Each cell `(partition_key, window_id)` is an independent sketch.

**State at sender:**
```
matrix: map[(partition_key, window_id)] *SketchWithDeltaState
```

**Delta flush (per partition, per window):**
- Same as Mode 2 but the `SketchDelta` message carries both `partition_key` and `window_start_unix_ms` to uniquely identify the matrix cell.

**At receiver:**
- Maintain `S_agg[partition_key][window_id]`.
- On `is_window_close=true` for a cell: finalize that cell, release memory.

**Memory at sender:** Only cells that have received at least one data point are allocated (lazy allocation). Cells not seen in the current window are never initialized → sparse matrix storage matches sparse data.

---

## 6. Implementation Plan

### Separation of Concerns

| Repo | Responsibility |
|------|---------------|
| **sketchlib-go** | Sketch data structures, insert/query/merge, `ComputeDelta`, `ApplyDelta`, protobuf serialization of full and sparse-delta payloads |
| **DataCollector** | `GroupBy` → partition key derivation, tumbling window timer, per-partition sketch lifecycle, routing data points to the right partition, wiring flush output to the OTel next consumer |

DataCollector never touches sketch internals, threshold logic, or delta channels. It calls three operations on a sketch:

```go
sketch.Add(value)                        // insert one observation
sketch.ComputeDelta(snapshot, threshold) // → proto bytes (sparse delta)
sketch.ApplyDelta(protoBytes)            // at receiver
sketch.Reset()                           // at window close
sketch.SerializeToBytes()                // full sketch, fallback / window-close path
```

The `Worker`/`Aggregator`/`AdaptiveTau` from sketchlib-go PR #41 are internal implementation
details of `Add()`. DataCollector does not instantiate them directly.

---

### Phase 1 — sketchlib-go: Add Delta Messages to `proto/sketchlib.proto`

**Scope:** The main branch of sketchlib-go already has `proto/sketchlib.proto` with a
`SketchEnvelope` and full-state messages for all sketches (`CountMinState`,
`CountSketchState`, `HyperLogLogState`, `DDSketchState`, `KLLState`). The existing gob
codec in `common/serialization.go` is for full-sketch round-trips and is left untouched.

This phase adds delta-specific messages to the **same** `proto/sketchlib.proto` file and
regenerates `proto/sketchlibpb/sketchlib.pb.go`. No existing message is modified.

**Additions to `proto/sketchlib.proto`:**

```protobuf
// ============================================================================
// Delta Transmission — Sparse Snapshots
// ============================================================================
// These messages carry only the cells / registers that changed since the last
// snapshot. They are transported inside SketchDeltaEnvelope (below) which is
// the network-level wrapper used by DataCollector processors.
//
// Full-sketch messages (CountMinState, CountSketchState, etc.) continue to be
// used for window-close flushes and fallback full transmissions.

// SketchDeltaEnvelope is the top-level wire container for delta payloads.
// It is carried in the sketch_payload bytes field on OTel data points,
// distinguished from full-sketch payloads by the encoding="proto_delta"
// attribute on the data point.
message SketchDeltaEnvelope {
  string partition_key   = 1;  // matches partition_key attribute on data point
  int64  window_start_ms = 2;  // unix ms; 0 when no window (Mode 1)
  bool   is_window_close = 3;  // receiver should finalize and reset accumulator

  oneof delta {
    CountMinDelta    count_min    = 10;
    CountSketchDelta count_sketch = 11;
    HLLDelta         hll          = 12;
    DDSketchDelta    ddsketch     = 13;
  }
}

// CountMinDelta carries only the cells that changed above threshold T.
// Receiver applies: agg[r][c] += cell.d_count / d_sum / d_sum2.
// L1 and L2 are always transmitted in full (one float64 per row, negligible).
message CountMinDelta {
  uint32               rows  = 1;
  uint32               cols  = 2;
  repeated CountMinCell cells = 3;
  repeated double      l1    = 4 [packed = true];
  repeated double      l2    = 5 [packed = true];
}

message CountMinCell {
  uint32 row     = 1;
  uint32 col     = 2;
  double d_count = 3;
  double d_sum   = 4;
  double d_sum2  = 5;
}

// CountSketchDelta — signed counters; TopK is always full (non-linear).
// Receiver applies: agg[r][c] += cell.d_count (signed).
message CountSketchDelta {
  uint32                rows  = 1;
  uint32                cols  = 2;
  repeated CountSketchCell cells = 3;
  repeated double       l2    = 4 [packed = true];
  TopKState             topk  = 5;   // reuses existing TopKState message
}

message CountSketchCell {
  uint32 row     = 1;
  uint32 col     = 2;
  double d_count = 3;  // signed
}

// HLLDelta — no threshold; every increased register is transmitted.
// Receiver applies: agg[i] = max(agg[i], update.value).
message HLLDelta {
  repeated HLLRegisterUpdate updates = 1;
}

message HLLRegisterUpdate {
  uint32 index = 1;  // 0 – 2^precision-1
  uint32 value = 2;  // new register value (only transmitted if > snapshot)
}

// DDSketchDelta — bucket count deltas (additive, threshold T applies).
// min/max are lossless (transmitted only when changed).
// Receiver applies: agg.store[b] += bucket.d_count;
//                   agg.min = min(agg.min, new_min) if min_changed;
//                   agg.max = max(agg.max, new_max) if max_changed.
message DDSketchDelta {
  repeated DDSketchBucketDelta buckets     = 1;
  int64   d_count     = 2;   // additive delta for total count
  double  d_sum       = 3;   // additive delta for sum
  double  new_min     = 4;   // lossless; only meaningful when min_changed=true
  double  new_max     = 5;   // lossless; only meaningful when max_changed=true
  bool    min_changed = 6;
  bool    max_changed = 7;
}

message DDSketchBucketDelta {
  sint32 index   = 1;   // signed bucket index (may be negative for values < 1)
  uint64 d_count = 2;   // Δcount ≥ threshold T
}
```

**Deliverables:** Regenerated `proto/sketchlibpb/sketchlib.pb.go`; no changes to existing
sketch structs or `common/serialization.go`; existing serialization tests unchanged.

---

### Phase 2 — sketchlib-go: ComputeDelta + ApplyDelta (CMS, CS, HLL, DDSketch)

**Scope:** Pure sketch library work. No DataCollector changes in this phase.

**`sketches/CountMinSketch/delta.go`:**
```go
// ComputeDelta returns cells where |current[r][c] - base[r][c]| >= threshold
// across all three arrays (Count, Sum, Sum2). L1/L2 are always included in full.
func ComputeDelta(base, current *CountMinSketch, threshold float64) *CMSSparseSnapshot

// ApplyDelta adds each cell's d_count/d_sum/d_sum2 to target.
func ApplyDelta(target *CountMinSketch, snap *CMSSparseSnapshot)
```

**`sketches/CountSketch/delta.go`:**
```go
// Same pattern; d_count is signed. TopK is always transmitted in full (non-linear).
func ComputeDelta(base, current *CountSketch, threshold float64) *CSSparseSnapshot
func ApplyDelta(target *CountSketch, snap *CSSparseSnapshot)
```

**`sketches/HLL/delta.go`:**
```go
// No threshold — every register where current[i] > base[i] must be transmitted.
func ComputeRegisterDelta(base, current *HyperLogLog) *HLLSparseSnapshot
// ApplyRegisterDelta applies max(target[i], update.value) for each update.
func ApplyRegisterDelta(target *HyperLogLog, snap *HLLSparseSnapshot)
```

**`sketches/DDSketch/delta.go`:**
```go
// Bucket deltas are additive (threshold applies). min/max are lossless (always if changed).
func ComputeDelta(base, current *DDSketch, threshold uint64) *DDSketchDelta
func ApplyDelta(target *DDSketch, delta *DDSketchDelta)
```

**`sketches/KLL/` — no delta.go.** Add comment to `kll.go` and README: KLL is not linearly
mergeable (random compaction); full sketch must always be transmitted.

**Deliverables:** Unit tests for each sketch verifying `ApplyDelta(ComputeDelta(base, current))`
reconstructs `current − base` exactly (CMS/CS/DD) or that all increased registers are covered
(HLL). Benchmark showing proto delta size vs. full proto payload at 1% and 5% fill rates.

---

### Phase 3 — DataCollector: CMS + CS Processor Delta Mode

**Scope:** Wire Phase 1+2 sketchlib-go work into the CMS and CS processors. All window/partition
logic already exists; this phase adds a snapshot map and changes the flush output format.

**Config additions (both processors, same fields):**
```go
DeltaTransmission bool          `mapstructure:"delta_transmission"` // default: false
DeltaThreshold    float64       `mapstructure:"delta_threshold"`    // default: 1.0 (lossless)
```
`DeltaTransmission` requires `TransmitSketch=true`. `Validate()` enforces this.

**Processor state additions:**
```go
snapshotMu  sync.RWMutex
snapshots   map[string]*cms.CountMinSketch  // one snapshot per partition key
                                            // (cs processor uses *cs.CountSketch)
```

**Flush path change (per partition key):**
```
existing: serialize full sketch → emit data point
new:      if DeltaTransmission:
              delta = sketch.ComputeDelta(snapshots[key], current, DeltaThreshold)
              emit data point with sketch_payload=proto(SketchDeltaEnvelope{delta, is_window_close})
              snapshots[key] = current.Clone()
              if is_window_close: current.Reset(); delete(snapshots, key)
          else:
              existing full-sketch path (no change)
```

The three aggregation modes (Mode 1 / Mode 2 / Mode 3) are already handled by the existing
`GroupBy` and `WindowSize` config fields and the partition map. Delta transmission is orthogonal
— it changes only the serialization format of each partition's flush output.

**Backward compatibility:** `encoding=proto_delta` attribute on emitted data points distinguishes
delta payloads from full-sketch payloads (`encoding=proto_full`). Receivers without delta support
skip unknown encodings or treat them as full sketches.

---

### Phase 4 — DataCollector: Receiver-Side Accumulation (CMS + CS)

**Scope:** A processor at the aggregator collector that reconstructs full sketches from received
deltas. This is the mirror of Phase 3.

**New processors:** `countminsketchmergeprocessor` / `countsketchmergeprocessor`

**Per-partition accumulator state:**
```go
accMu        sync.RWMutex
accumulators map[string]*cms.CountMinSketch  // keyed by (partition_key, window_start_ms)
```

**On each received data point:**
1. Extract `partition_key`, `window_start_ms`, `is_window_close`, `encoding` from attributes.
2. Decode `sketch_payload` as `SketchDeltaEnvelope`.
3. If `encoding=proto_delta`: call `ApplyDelta(accumulators[key], envelope.cms)`.
4. If `encoding=proto_full` or `encoding` absent: call `sketch.Merge(accumulators[key])`.
5. If `is_window_close=true`: forward `accumulators[key]` downstream, then delete it.

---

### Phase 5 — DataCollector: HLL Processor Delta Mode

Same structure as Phase 3 applied to the HLL processor
(`opentelemetry-go/sdk/metric/internal/aggregate/hllsketch.go`).

**Key difference from CMS/CS:** No `DeltaThreshold` config field — HLL delta transmission is
always lossless (§9.3). The only new config field is `DeltaTransmission bool`.

**Snapshot:** `snapshots map[string]*hll.HyperLogLog` — one per series key.

**Flush path:** Replace full register-array serialization with
`hll.ComputeRegisterDelta(snapshots[key], current)` → `proto(HLLSparseSnapshot)`. Update
snapshot. No reset on flush (HLL registers are monotone); reset only on `is_window_close`.

---

### Phase 6 — DataCollector: DDSketch Processor Delta Mode

Same structure as Phase 3 applied to the DDSketch processor.

**Config:** `DeltaTransmission bool`, `DeltaThreshold uint64` (default: 1 — lossless buckets;
min/max are always lossless regardless of threshold).

**Snapshot:** `snapshots map[string]*ddsketch.DDSketch`.

**Flush path:** `ddsketch.ComputeDelta(snapshots[key], current, DeltaThreshold)` →
`proto(DDSketchDelta)`. Lossless min/max fields are included only when changed.

---

### Phase 7 — DataCollector: KLL Guard

Add to `kllprocessor/config.go`:
```go
if cfg.DeltaTransmission {
    return fmt.Errorf("kll: delta_transmission is not supported; " +
        "KLL uses probabilistic compaction which is not linearly mergeable")
}
```
Document in the KLL processor README: for bandwidth reduction without delta, reduce the
accuracy parameter `k` or increase the flush interval.

---

### Phase 8 — DataCollector: Adaptive Threshold (CMS + CS)

Add to both processors a background goroutine that tracks fill rate across the last N flushes
and adjusts `DeltaThreshold` to target a configurable `TargetFillRate`:

```go
// Every AdjustInterval (e.g., 10 windows):
fillRate = float64(cellsTransmittedLastN) / float64(totalCells * N)
if fillRate > TargetFillRate * 1.2 {
    DeltaThreshold *= 1.5
} else if fillRate < TargetFillRate * 0.8 {
    DeltaThreshold = max(1.0, DeltaThreshold * 0.8)
}
```

Expose `countsketch_delta_fill_rate` and `countmin_delta_fill_rate` as OTel metrics.
HLL has no adaptive threshold (lossless by design). DDSketch adaptive threshold can follow
the same pattern if needed.

---

### Phase Sequencing and Dependencies

```
sketchlib-go:
  Phase 1  (proto wire format — foundation)
    └── Phase 2  (ComputeDelta + ApplyDelta: CMS, CS, HLL, DDSketch)

DataCollector (all depend on Phase 2 being merged and go.mod bumped):
  Phase 3  (CMS + CS processor delta mode)
    └── Phase 4  (receiver accumulator — CMS + CS)
    └── Phase 8  (adaptive threshold — CMS + CS)
  Phase 5  (HLL processor delta mode)           [independent of Phase 3]
  Phase 6  (DDSketch processor delta mode)      [independent of Phase 3]
  Phase 7  (KLL guard)                          [trivial, any PR]
```

**Recommended PR order:**
1. sketchlib-go: Phase 1 (proto messages + codec swap)
2. sketchlib-go: Phase 2 (delta functions for all four sketches)
3. DataCollector: bump sketchlib-go version in go.mod
4. DataCollector: Phases 3 + 4 together (CMS + CS sender + receiver)
5. DataCollector: Phase 5 (HLL)
6. DataCollector: Phase 6 (DDSketch)
7. DataCollector: Phase 7 (KLL guard — ride along with any of the above)
8. DataCollector: Phase 8 (adaptive threshold — polish)

---

## 7. Bandwidth Analysis

Assumptions: CMS 5×1024 cells, CS 5×10 000 cells, window=10 s, 1 000 unique series, transmission rate 1 Hz.

### Dense (current) vs. Sparse Delta

| Sketch | Full flush/s | Delta at 5% fill/s | Delta at 1% fill/s | Reduction |
|--------|-------------|--------------------|--------------------|-----------|
| CMS 5×1024 × 1000 series | 40 MB/s | 2.0 MB/s | 0.4 MB/s | 20–100× |
| CS 5×10k × 1000 series | 400 MB/s | 20 MB/s | 4 MB/s | 20–100× |

### Within-Window vs. Window-Close Deltas

For a 10 s window with 4 sub-flushes (P=2.5 s):

| Flush | Incremental fill | Delta size vs. full |
|-------|-----------------|---------------------|
| t+2.5 s | ~25% of final | 25% |
| t+5.0 s | ~25% marginal | 25% |
| t+7.5 s | ~25% marginal | 25% |
| t+10.0 s (close) | ~25% marginal | 25% |
| **Total** | | **≈ 100% (same as one full sketch)** |

Sub-window deltas do not reduce total data volume over a full window — they reduce **peak transmission burst** and improve **online query accuracy** at intermediate times (the OctoSketch benefit). To reduce total volume, increase T (lossy) or transmit only at window close.

---

## 8. Trade-off Summary

| Approach | Bandwidth | Online accuracy | Implementation complexity |
|----------|-----------|-----------------|--------------------------|
| Full sketch, window-close only | Baseline | Low (stale until close) | Existing |
| Full sketch, sub-window flush | P/W × baseline | High | Low |
| Sparse delta, lossless (T=1) | 1–5% of baseline | High | Medium |
| Sparse delta, lossy (T=10) | 0.1–1% of baseline | Medium (bounded error) | Medium |
| Adaptive threshold | Configurable | Configurable | Medium–High |

---

## 9. HyperLogLog — Applicability and Differences

### 9.1 Does Delta Transmission Apply to HLL?

Yes, but the underlying algebra is different in two ways that change the design constraints.

### 9.2 HLL Uses Max, Not Addition

CMS and CS are **additive (linear)** sketches: `Merge[r][c] = S₁[r][c] + S₂[r][c]`. Their deltas are
plain subtractions and receivers accumulate with `+=`.

HLL is **idempotent / max-based**. From `sketchlib-go` (`hyperloglog.go:Merge`):

```go
for i := 0; i < HLLRegisterCount; i++ {
    if otherRegs[i] > self[i] {
        self[i] = otherRegs[i]   // max, not sum
    }
}
```

An HLL "delta" therefore means: **the set of registers whose leading-zero count increased since the
last snapshot**.

```
Compute delta:  { (i, H_current[i])  :  H_current[i] > H_snapshot[i] }
Apply at receiver:  H_agg[i] = max(H_agg[i], delta[i])
```

Subtraction (`H_current - H_snapshot`) would produce meaningless register values and corrupt
the cardinality estimate.

### 9.3 No Threshold T for HLL

For CMS/CS, suppressing cells below threshold T introduces a **bounded, symmetric** error: any
missed increment is at most T, and the per-query error is bounded by `T × cols` (CMS guarantee).

For HLL, a missed register update causes permanent **underestimation** with no lower bound. If
register 42 advances from 5 → 7 and the update is dropped, the receiver believes the max
for that register is still 5. Because the final cardinality uses `max` — not `sum` — there is no
self-correcting mechanism. The underestimate persists until a future delta (from the same or
another sender) happens to cover that register.

**Consequence: HLL delta transmission must be lossless — every register that increases must be
transmitted. Threshold filtering (T > 0) is not safe for HLL.**

### 9.4 Monotonicity: A Different Bandwidth Guarantee

HLL registers are `uint8` values that can only **increase** (max 51 for precision=14). Once a
register stabilises at its true value for the current stream and the receiver has acknowledged that
value, it never needs retransmission. This means:

- **Early in a window:** Many registers transition from 0 → k. Delta fill rate is high.
- **Mid-window at stable cardinality:** Only newly discovered items trigger changes. Delta is small.
- **At window close:** If cardinality is stable, the delta may be empty.

This contrasts with CMS/CS where counters grow monotonically throughout the window; their deltas
are non-zero on every flush.

### 9.5 Out-of-Order Tolerance

Because `max` is commutative and associative, HLL deltas are safe to apply in any order and are
**idempotent** (re-applying a delta that was already applied does nothing). This makes HLL delta
transmission more resilient to network reordering and at-least-once delivery than CMS/CS (where
duplicate `+=` would inflate counts).

### 9.6 HLL Delta Format

Register indices are 14-bit (0–16383) and values are `uint8` (0–51). A sparse update list is very
compact:

```protobuf
message HLLDelta {
  string partition_key      = 1;
  int64  window_start_unix_ms = 2;
  // Only registers that increased since last snapshot.
  repeated RegisterUpdate registers = 3;
  bool   is_window_close   = 4;
}

message RegisterUpdate {
  uint32 index = 1;   // 0–16383; fits in varint
  uint32 value = 2;   // 0–51; fits in varint
}
```

**Size comparison (HLL precision=14, 16 384 registers):**

| Cardinality | Changed registers / flush (10 s) | Delta size | Full payload |
|-------------|----------------------------------|-----------|-------------|
| 1 000 | ~60 (newly set) | ~360 B | 16 KB |
| 10 000 | ~600 → ~20 steady-state | ~360 B → ~120 B | 16 KB |
| 1 000 000 | ~3 000 → ~0 steady-state | ~18 KB → ~0 B | 16 KB |

At high cardinality and steady state the delta is near-zero; the full payload is still only 16 KB
(HLL is already more compact than CMS or CS per series).

### 9.7 Comparison Table: CS / CMS / HLL

| Property | CMS | CS | HLL |
|----------|-----|----|-----|
| Merge operator | + (additive) | + (additive, signed) | max (idempotent) |
| Delta definition | `current[r][c] − snapshot[r][c]` | same | `{(i, v) : v > snapshot[i]}` |
| Receiver application | `agg[r][c] += delta` | `agg[r][c] += delta` | `agg[i] = max(agg[i], delta[i])` |
| Threshold T safe? | Yes — bounded error | Yes — bounded error | **No — underestimation risk** |
| Monotone registers? | No — within window counters always grow | No | Yes — registers only increase |
| Delta converges to zero? | No (within window) | No (within window) | Yes (at stable cardinality) |
| Out-of-order safe? | Only with careful windowing | Same | Yes — max is idempotent |
| Full payload size | 40 KB (5×1024 float64) | ~400 KB (5×10k float64) | 16 KB (16k uint8) |
| Serialisation in sketchlib-go? | Yes | No | Yes |

### 9.8 Phase 2b — HLL Processor Delta Support

The `opentelemetry-go/sdk/metric/internal/aggregate/hllsketch.go` already has `delta()` and
`cumulative()` methods that send the full register array each cycle. Adding sparse delta
transmission requires:

1. **`sketchlib-go/sketches/HLL/delta.go`** — `ComputeRegisterDelta(base, current *HyperLogLog) []RegisterUpdate`; `ApplyRegisterDelta(target *HyperLogLog, updates []RegisterUpdate)`.
2. **Snapshot per series** in `hllSketchValues` — same pattern as CMS snapshot map.
3. **`delta()` override** — instead of serializing the full register slice, compute and serialize only the changed registers; update the snapshot.
4. **No `DeltaThreshold` config field** for HLL — the threshold is always implicitly 0 (any increase is transmitted). The config should document this constraint explicitly.

---

## 10. Other Sketch Types — Delta Applicability

This section classifies the remaining sketch types in `sketchlib-go` by their merge algebra and
determines what (if any) delta transmission design applies to each.

### 10.1 Summary Table

| Sketch | Merge operator | Delta applicable? | Threshold T safe? | Notes |
|--------|---------------|-------------------|-------------------|-------|
| DDSketch | + for bucket counts; min/max for scalars | Partially | For buckets: yes; for min/max: no | Bucket array is already sparse |
| KLL | Probabilistic compaction (random coin flip) | **No** | N/A | Non-linearly mergeable; full sketch always |
| CocoSketch | Replace-minimum (approximate additive) | Approximately | Approximately (bounded extra error) | Hash-keyed entries, not a dense grid |
| ElasticSketch | Flush heavy → additive light merge | Yes (CMS-identical) | Yes | Merge output is always a pure light-layer CMS |

---

### 10.2 DDSketch

#### Struct shape (`DDSketch.go`)
```
store  Buckets     — sparse bucket array: map[int32]uint64 (log-scale bucket index → count)
count  uint64      — total observations
sum    float64     — sum of all values
min    float64     — running minimum
max    float64     — running maximum
```

#### Merge semantics
DDSketch merge is additive for `count`, `sum`, and bucket counts:
```
merged.count += other.count
merged.sum   += other.sum
merged.store[b] += other.store[b]  for each bucket b
```
However, `min` and `max` follow the same max/min pattern as HLL registers:
```
merged.min = min(merged.min, other.min)
merged.max = max(merged.max, other.max)
```

#### Delta design

**Bucket deltas (additive, threshold-safe):**
- The sparse bucket array is already only populated for buckets that contain data.
- Delta = set of `(bucket_index, Δcount)` tuples where `Δcount = current[b] − snapshot[b] ≥ T`.
- Receiver applies `agg[b] += Δcount`.
- The bucket array's natural sparsity means even "full" DDSketch payloads are small; delta adds
  a further reduction for inactive buckets between flushes.

**Scalar deltas (monotone, lossless):**
- `min` can only decrease over time; `max` can only increase. Neither is additive.
- Apply the same lossless design as HLL registers: transmit new value whenever it changes.
- Wire cost: 2 × float64 = 16 bytes — negligible; always transmit in full.

**Protobuf message:**
```protobuf
message DDSketchDelta {
  string partition_key        = 1;
  int64  window_start_unix_ms = 2;
  bool   is_window_close      = 3;
  int64  d_count              = 4;   // delta; additive
  double d_sum                = 5;   // delta; additive
  double new_min              = 6;   // current min; lossless (only if changed)
  double new_max              = 7;   // current max; lossless (only if changed)
  repeated DDBucketDelta buckets = 8;
}

message DDBucketDelta {
  int32  index   = 1;   // log-scale bucket index (signed varint)
  uint64 d_count = 2;   // Δcount ≥ T
}
```

**Error analysis:** Same as CMS/CS for bucket queries; min/max are always exact (lossless).
Queries on quantiles (DDSketch's primary use case) use only bucket counts, so threshold T applies
to them with the same bounded-error guarantee as CMS.

---

### 10.3 KLL (Karnin–Lang–Liberty)

#### Struct shape (`kll.go`)
```
levels   [][]float64   — multi-level compaction buffer (level 0 = newest samples)
capacity int           — capacity per level
```

#### Merge semantics (why delta does NOT apply)

KLL's `compact(level)` function uses a **random coin flip** (`s.co.toss()`) to decide which
half of the sorted items to keep at each level. `mergePacked` calls `sort` and `compact`
interleaved across levels.

This means:
```
Merge(S₁, S₂) ≠ S(D₁ ∪ D₂)   with probability > 0
```
The merge result depends on which items are randomly discarded during compaction. Two runs over
the same data can produce different merged sketches. **KLL is not linearly mergeable.**

**Consequence: delta transmission is not applicable to KLL.** Full sketch must be transmitted
on every flush. There is no way to represent `S(t₂) − S(t₁)` as a meaningful additive structure
that receivers can accumulate.

**Recommended approach for KLL:** Transmit the full compacted sketch. KLL's typical payload
size is `O(k log(n/k))` where k is the accuracy parameter — much smaller than the raw item
count, so full-sketch transmission is already bandwidth-efficient by design.

---

### 10.4 CocoSketch

#### Struct shape (`CocoSketch.go`)
```
table  [][]cocoBucket    — 2D hash table, each bucket: {Hash uint64, Val uint64, HasKey bool}
rows   int
cols   int
```

#### Merge semantics

CocoSketch merge calls `insertKeyValue(b.Hash, b.Val)` for each non-empty bucket of the
other sketch. `insertKeyValue` uses a **replace-minimum** eviction policy: if the target cell
is occupied by a lower-value entry, it is evicted and the incoming entry takes the slot.

This is approximately additive for stable (non-evicted) entries, but:
- Two sketches merged by the replace-minimum path are **not** exactly equivalent to a single
  sketch built from the union of both datasets.
- The eviction introduces a bounded additional error on top of the sketch's inherent accuracy
  guarantee.

#### Delta design (approximate)

CocoSketch entries are identified by `Hash` (a 64-bit item fingerprint), not a dense `(row, col)`
grid. A sparse delta can be defined as:

```
Transmitted entries = { (hash, Δval) : Δval = current[hash].Val − snapshot[hash].Val ≥ T }
```

Receiver applies each `(hash, Δval)` via `insertKeyValue(hash, Δval)`, which follows the same
replace-minimum logic as a normal insert.

**Bounded additional error:** The collision resolution during delta application may evict some
existing receiver entries. The error introduced is bounded by the same guarantee as a fresh
CocoSketch of the same dimensions, so the combined error remains O(ε) in the sketch's
conventional error model.

**Protobuf message:**
```protobuf
message CocoSketchDelta {
  string partition_key        = 1;
  int64  window_start_unix_ms = 2;
  bool   is_window_close      = 3;
  int32  rows                 = 4;
  int32  cols                 = 5;
  repeated CocoBucketDelta entries = 6;
}

message CocoBucketDelta {
  uint64 hash   = 1;   // 64-bit item fingerprint
  uint64 d_val  = 2;   // Δval ≥ T
}
```

**Threshold T:** Applies with the same semantic as CMS. Entries below T are not transmitted;
the approximation error grows by at most T per evicted query.

**Practical note:** CocoSketch is designed for heavy-hitter detection, where a small number of
flows account for most traffic. Its natural fill rate is already low; full-sketch payloads are
typically sparse. Delta transmission here adds modest marginal savings.

---

### 10.5 ElasticSketch

#### Struct shape (`ElasticSketch.go`)
```
heavy  []HeavyBucket                — explicit heavy-hitter tracking layer
                                      HeavyBucket: {Key []byte, Val float64, Flag bool}
light  *storage.Vector2D[float64]  — CMS-like light layer (additive counters)
rows   int
cols   int
```

#### Merge semantics

ElasticSketch's `MergeFrom(other)` always calls `other.flushHeavyToLight()` first, demoting the
other sketch's heavy-hitter buckets into its light (CMS) layer before merging. The result is:

1. `other.light[r][c]` is fully additive and is merged with `leftRow[c] += rightRow[c]`.
2. Heavy buckets from `other` that were not flushed are `insertKeyValue`-d into `self.heavy`,
   with overflow spilling into `self.light`.

**Key insight:** After `flushHeavyToLight()`, the other sketch's state is a pure CMS-like array.
The merge is therefore **exactly equivalent to CMS merge** for the `other.light` portion, and
replace-minimum (approximate) for any remaining `other.heavy` entries.

In practice, `flushHeavyToLight()` is always called first, so the merge result for the light layer
is **exactly additive** and threshold-filtered delta transmission is safe with the same error
semantics as CMS.

#### Delta design (CMS-identical)

1. Call `flushHeavyToLight()` on the sender's sketch before computing the delta.
   After this call, `heavy` is empty and all state is in `light`.
2. Compute sparse light-layer delta:
   ```
   Transmitted cells = { (r, c, Δlight[r][c]) : |Δlight[r][c]| ≥ T }
   ```
3. Receiver applies `agg_light[r][c] += Δlight[r][c]`.

**Protobuf message:** Reuse `CMSSparseSnapshot` (§4.3) — the light layer has the same
`Count/Sum/Sum2/L1/L2` structure as CMS.

**Heavy-hitter layer:** The heavy buckets represent very large flows. After
`flushHeavyToLight()`, they are already in the light layer. If the heavy layer must be preserved
at the receiver (for heavy-hitter queries), transmit the full heavy-bucket list as a small separate
payload (typically ≤ 100 entries × ~30 B = 3 KB).

**Error analysis:** Identical to CMS for light-layer queries. Heavy-hitter accuracy degrades only
if the heavy bucket list is sparse-transmitted (not recommended — transmit in full as it is small).

---

### 10.6 Cross-Sketch Comparison

| Sketch | Merge algebra | Delta type | Threshold T | Wire format |
|--------|--------------|------------|-------------|-------------|
| CMS | Additive (+) | Sparse `(r,c,Δ)` cells | Yes | `CMSSparseSnapshot` |
| CS | Additive (+, signed) | Sparse `(r,c,Δ)` cells | Yes | `CSSparseSnapshot` |
| HLL | Idempotent (max) | Sparse register updates | **No** | `HLLSparseSnapshot` |
| DDSketch | Additive for buckets; min/max for scalars | Sparse bucket deltas + lossless scalars | Yes (buckets) | `DDSketchDelta` |
| KLL | Non-linear (random compaction) | **Not applicable** | — | Full sketch always |
| CocoSketch | Approx. additive (replace-min) | Sparse `(hash, Δval)` entries | Yes (bounded extra error) | `CocoSketchDelta` |
| ElasticSketch | Additive (after flush-heavy-to-light) | CMS-identical sparse cells | Yes | `CMSSparseSnapshot` (reuse) |

**Tier 1 (exact delta, threshold T safe):** CMS, CS, DDSketch (buckets), ElasticSketch (light layer).
**Tier 2 (lossless required / no threshold):** HLL (max semantics), DDSketch min/max scalars.
**Tier 3 (approximate delta, bounded extra error):** CocoSketch (replace-min eviction).
**Not applicable:** KLL (probabilistic compaction, non-linearly mergeable).

> **DataCollector scope:** ElasticSketch and CocoSketch delta transmission are out of scope for
> the current implementation. The analysis above is retained for completeness; phases 7 and 8
> covering those two sketches have been removed from §6.

---

## 11. Open Questions

1. **Proto field placement:** `SketchDeltaEnvelope` is carried in the existing `sketch_payload` bytes field on CMS/CS data points and the `Sketch` bytes field on HLL data points, distinguished by the `encoding` attribute. An alternative is a first-class field in the OTLP proto extension; the bytes-field approach is faster to prototype without schema changes.

2. **CS serialization prerequisite:** `sketchlib-go` Phase 1 adds proto support for CS. Until Phase 1 is merged, `DeltaTransmission=true` on CS should be rejected by `Validate()`.

3. **Receiver identity / deduplication:** If multiple collectors fan-in to a single aggregator, the same `(partition_key, window_id)` may arrive from different senders. The accumulator must be keyed by `(sender_id, partition_key, window_id)` to avoid double-counting, or the senders must be explicitly partitioned (consistent hashing).

4. **Snapshot memory overhead:** Each sender stores a snapshot of every active sketch partition. CMS 5×1024 is ~123 KB per partition (three float64 arrays), so 1 000 partitions = ~123 MB of snapshots. CS 5×10k is ~400 KB, so 1 000 partitions = ~400 MB — likely too large. Mitigations: (a) only snapshot at window-close boundary (one snapshot per partition, not per sub-flush); (b) store only the `Count` array in the snapshot and derive full-sketch deltas from it alone if Sum/Sum2 are not queried; (c) use a compact uint32 snapshot for CS (`Count` cells truncated to integer).

5. **Threshold T and error semantics:** What SLO should the error introduced by threshold filtering satisfy? Tying T to the sketch's inherent ε (T ≤ ε × total_count) keeps the combined error within the sketch's guarantee.

---

## 12. Relationship to Existing Code

### sketchlib-go changes

| File | Current state | Change |
|------|--------------|--------|
| `proto/sketchlib.proto` | Has full-sketch state messages | Add delta messages: `SketchDeltaEnvelope`, `CountMinDelta`, `CountSketchDelta`, `HLLDelta`, `DDSketchDelta` and their cell/update sub-messages |
| `proto/sketchlibpb/sketchlib.pb.go` | Generated from above | Regenerate after proto additions |
| `common/serialization.go` | gob encode/decode | **No change** — gob remains for full-sketch round-trips |
| `sketches/CountMinSketch/delta.go` | Does not exist | New — `ComputeDelta`, `ApplyDelta` using proto types |
| `sketches/CountSketch/delta.go` | Does not exist | New — `ComputeDelta`, `ApplyDelta` (signed; TopK reuses `TopKState`) |
| `sketches/HLL/delta.go` | Does not exist | New — `ComputeRegisterDelta`, `ApplyRegisterDelta` (no threshold) |
| `sketches/DDSketch/delta.go` | Does not exist | New — `ComputeDelta`, `ApplyDelta` (buckets + lossless min/max) |
| `sketches/KLL/kll.go` | Exists | Add comment only — no delta possible |

### DataCollector changes

| File | Current state | Change |
|------|--------------|--------|
| `countminsketchprocessor/config.go` | Has `TransmitSketch`, `GroupBy`, `WindowSize` | Add `DeltaTransmission bool`, `DeltaThreshold float64` |
| `countminsketchprocessor/processor.go` | Full sketch via `SerializeToBytes`; partition map + window timer already present | Add `snapshots map[string]*cms.CountMinSketch`; change flush to call `ComputeDelta` when enabled |
| `countsketchprocessor/config.go` | Has `TransmitSketch`, `GroupBy`, `WindowSize` | Same additions as CMS |
| `countsketchprocessor/processor.go` | No serialization yet | Same flush change as CMS; gated on Phase 1 proto support |
| `opentelemetry-go/.../hllsketch.go` | Has `delta()` (full register array) | Add `snapshots map[string]*hll.HyperLogLog`; call `ComputeRegisterDelta` instead of full serialize |
| `opentelemetry-go/.../countminsketch.go` | Has `delta()` (full sketch) | Optional: wire sparse delta here for SDK-side sending |
| `opentelemetry-go/.../countsketch.go` | Has `delta()` (full sketch) | Same as CMS SDK file |
| New: `countminsketchmergeprocessor/` | Does not exist | Receiver accumulator (Phase 4) |
| New: `countsketchmergeprocessor/` | Does not exist | Receiver accumulator (Phase 4) |

---

## 13. Controller Integration

The controller is the central control plane: it accepts query specifications, selects the optimal
sketch type and parameters via cost modelling, generates OTel collector YAML configs, and
distributes them to agents via OpAMP over WebSocket. Delta transmission touches four controller
subsystems.

---

### 13.1 AgentCollectorConfig — New Fields

`AgentCollectorConfig` in `controller/src/types.rs` currently includes `transmit_sketch: bool`
and `drop_original: bool`. Delta transmission needs two additional fields:

```rust
pub struct AgentCollectorConfig {
    // ... existing fields ...
    pub transmit_sketch:    bool,
    pub drop_original:      bool,

    // NEW
    pub delta_transmission: bool,    // emit sparse delta instead of full sketch
    pub delta_threshold:    f64,     // |ΔS[r][c]| must be ≥ this to transmit; default 1.0
}
```

The `BackendCollectorConfig` (merge processor at the gateway/backend) needs a matching field so
the config generator knows to emit a delta-aware merge processor:

```rust
pub struct BackendCollectorConfig {
    // ... existing fields ...
    pub delta_transmission: bool,   // must mirror agent side
}
```

`delta_transmission` is only valid when `transmit_sketch = true` and `mode = Window`. The
planner must enforce this.

---

### 13.2 Planner — When to Enable Delta Transmission

The `CostModelPlanner` (`controller/src/planner/cost_model.rs`) scores sketches by bandwidth:

```
bandwidth = sketch.bytes_per_series_sec × (num_aggregate_dimensions + 1)
```

With delta transmission enabled, the effective bandwidth is multiplied by the expected fill rate:

```
effective_bandwidth = full_bandwidth × fill_rate(window_size, data_density)
```

`fill_rate` depends on how many sketch cells change per window. At typical telemetry densities:

| Window size | Typical fill rate | Effective bandwidth reduction |
|-------------|------------------|-------------------------------|
| 5 s | ~25–40% | ~3–4× |
| 30 s | ~5–15% | ~7–20× |
| 5 min | ~1–5% | ~20–100× |

**Decision rule:** enable delta transmission whenever `mode = Window && transmit_sketch = true`.
It always reduces bandwidth relative to full-sketch transmission and the overhead (snapshot memory)
is bounded by one sketch clone per active partition.

**Updated scoring in `cost_model.rs`:**

```rust
fn effective_bandwidth(sketch: SketchType, params: &SketchParams,
                       mode: ProcessorMode, window: Duration) -> f64 {
    let full_bw = BENCHMARK[sketch].bytes_per_series_sec;
    match mode {
        ProcessorMode::Window => {
            // Model fill rate as decaying with window size (longer window → sparser delta)
            let fill_rate = estimated_fill_rate(window);
            full_bw * fill_rate
        }
        ProcessorMode::Batch => full_bw,  // delta not applicable in batch mode
    }
}

fn estimated_fill_rate(window: Duration) -> f64 {
    // Conservative model: 30% fill at 5s window, decays as 1/sqrt(window_secs)
    let w = window.as_secs_f64();
    (0.30 / w.sqrt()).clamp(0.01, 1.0)
}
```

This changes the benchmark table the planner uses — all Window-mode candidates get a lower
effective bandwidth score, which may change sketch selection (e.g., CMS might beat DDSketch for
a frequency query at a 5-minute window once fill rate is accounted for).

**KLL exception:** delta transmission is not applicable to KLL (§10.3). The planner must never
set `delta_transmission = true` when `sketch_type = KLL`.

---

### 13.3 Config Generators — YAML Emission

**Agent config (`controller/src/config/agent.rs`):**

The processor YAML block for each sketch type needs the two new fields when delta is enabled:

```yaml
processors:
  countminsketch:
    epsilon: 0.01
    delta: 0.99
    window_size: 30s
    group_by: ["region", "service"]
    transmit_sketch: true
    drop_original: true
    delta_transmission: true   # NEW
    delta_threshold: 1.0       # NEW
```

**Backend config (`controller/src/config/backend.rs`):**

The merge processor also needs to declare it accepts delta payloads:

```yaml
processors:
  countminsketchmerge:
    delta_transmission: true   # accept proto_delta encoding
```

The config generator should only emit `delta_transmission: true` when the plan has it enabled;
absent means legacy full-sketch (backward compatible).

---

### 13.4 Monitor — Bandwidth Violation with Fill Rate Signal

The `Scraper` (`controller/src/monitor/mod.rs`) currently detects bandwidth violations by
checking `otelcol_sketch_size_bytes > 5 MB`. With delta transmission, two additional metrics
are useful:

| New metric | Source | Use |
|-----------|--------|-----|
| `otelcol_delta_fill_rate` | CMS/CS/HLL/DD processors | Fraction of cells transmitted per flush |
| `otelcol_delta_threshold` | Same | Current threshold value in use |

**New violation type — fill rate too high:**

```rust
ViolationType::DeltaFillRateTooHigh {
    agent: String,
    metric: String,
    fill_rate: f64,   // observed
    threshold: f64,   // current DeltaThreshold in plan
}
```

A fill rate persistently above ~20% means the threshold T is too low for the data density and
should be raised. The monitor should trigger a re-plan that increases `delta_threshold`, rather
than leaving it to the in-processor adaptive logic (Phase 8), so the change is durable and
versioned in the plan store.

---

### 13.5 Controller-Driven Adaptive Threshold

Phase 8 of the implementation plan adds in-processor adaptive threshold logic. The controller
provides a more powerful alternative: **centralized threshold adjustment via OpAMP**.

**Why centralized is better:**
- The controller sees fill rates across all agents simultaneously; it can detect whether high fill
  rate is correlated with specific metrics, time-of-day, or cardinality spikes.
- Threshold changes go through `PlanStore`, so they are versioned and rollback-able.
- Agents don't need adaptive logic — they just apply whatever config the controller sends.

**Flow:**

```
Monitor scrapes fill_rate from all agents
  ↓
fill_rate > TargetFillRate for N consecutive windows?
  ↓ yes
Planner.replan(metric): increase delta_threshold by Step (e.g. × 1.5, max MaxThreshold)
  ↓
PlanStore.set(metric, new_plan)   ← versioned; rollback available
  ↓
ConfigGenerator regenerates YAML with new delta_threshold
  ↓
OpampServer.push_all(RemoteConfig)
  ↓
Agents receive new config, apply updated threshold
```

**Config addition (`controller/src/types.rs`):**

```rust
pub struct AgentCollectorConfig {
    // ...
    pub delta_transmission:  bool,
    pub delta_threshold:     f64,     // current threshold; adjusted by monitor
    pub delta_target_fill:   f64,     // target fill rate; default 0.05 (5%)
    pub delta_max_threshold: f64,     // ceiling; default 100.0
}
```

With this in place, the in-processor Phase 8 adaptive logic becomes optional — useful as a
fast local fallback between controller adjustment cycles, but the durable source of truth is
always the plan.

---

### 13.6 Plan Store — Delta Fields in Versioned Plans

`CollectionPlan` is persisted in the `PlanStore` with `current` and `previous` versions.
Because `delta_threshold` can change over time (monitor-driven or manual), it must be part of
the persisted plan so that rollback restores the previous threshold, not just the previous
sketch type and parameters.

No schema change is needed — `AgentCollectorConfig` is already embedded in `CollectionPlan`;
adding the new fields there propagates automatically to storage and rollback.

---

### 13.7 Summary of Controller Changes

| Component | File | Change |
|-----------|------|--------|
| Type definitions | `src/types.rs` | Add `delta_transmission`, `delta_threshold`, `delta_target_fill`, `delta_max_threshold` to `AgentCollectorConfig` and `BackendCollectorConfig` |
| Planner | `src/planner/cost_model.rs` | Model `effective_bandwidth` with fill rate; enable delta for all Window-mode plans with `transmit_sketch=true`; never enable for KLL |
| Agent config gen | `src/config/agent.rs` | Emit `delta_transmission` and `delta_threshold` YAML fields when plan has them |
| Backend config gen | `src/config/backend.rs` | Emit `delta_transmission: true` on merge processor when plan has it |
| Monitor | `src/monitor/mod.rs` | Scrape `otelcol_delta_fill_rate`; add `DeltaFillRateTooHigh` violation; trigger re-plan |
| Proto | `proto/controller.proto` | Add delta fields to `AgentCollectorConfig` proto message |

---

## 14. SDK-to-Collector Delta Transmission

Phases 1–13 address delta transmission **within the collector pipeline** (window-mode
processors sending sparse diffs to downstream processors or the backend).  This section
covers the complementary problem: reducing the bandwidth of the **SDK → collector** hop
by having application-side aggregators send sparse deltas over OTLP instead of full
sketch payloads on every export cycle.

### 14.1 Motivation and Scope

The SDK exports sketch data via OTLP to the local collector agent every *reader interval*
(default 10 s–60 s).  For large deployments with many series and wide sketches (e.g. a
4 × 2048 CMS), each export can be tens of kilobytes per metric.  With `CumulativeTemporality`
the sketch grows monotonically; consecutive exports differ by only a sparse set of cells.
Sending only those changed cells dramatically reduces per-export payload size.

**Scope:**
- Applies to `CumulativeTemporality` exports only.  `DeltaTemporality` resets the sketch
  each export (independent windows); sparse delta between independent windows is not useful.
- Supported sketch types: **CountMinSketch**, **CountSketch**, **HLLSketch**.
- Not applicable to DDSketch or KLLSketch (different internal structures; no `ComputeDelta`
  functions provided by sketchlib-go).

### 14.2 Architecture

```
Application process                    Collector agent
┌────────────────────────────────┐     ┌───────────────────────────────┐
│  SDK Metric Reader (cumulative) │     │  OTLP Receiver                │
│                                │     │        ↓                      │
│  hllSketchValues               │     │  countsketch/countminsketch    │
│    snapshots map[key]*HLL ──── │OTLP─►  processor (existing)         │
│    delta() → HLLDelta proto    │     │        ↓                      │
│                                │     │  Exporter → Backend            │
│  countMinSketchValues          │     └───────────────────────────────┘
│    snapshots map[key]*CMS      │
│    delta() → CountMinDelta     │
│                                │
│  countSketchValues             │
│    snapshots map[key]*CS       │
│    delta() → CountSketchDelta  │
└────────────────────────────────┘
```

The SDK aggregator maintains a **snapshot** of the last-exported sketch state per
attribute series.  On each cumulative export cycle:

1. **No snapshot yet** (first export or series evicted): serialize full sketch,
   `encoding = "xxx_binary"` or `"xxx_gob"`, save snapshot clone.
2. **Snapshot exists**: call `sketchlib-go ComputeDelta(snapshot, current)`,
   `encoding = "xxx_delta"`, update snapshot clone.

The receiver reconstructs the current state by calling `ApplyDelta` on its own copy.

### 14.3 Configuration

Delta transmission is opt-in via the `AggregationXxx` struct fields populated by
`PipelineConfig.ToAggregation()`.  No new YAML keys are needed beyond what the
controller already sets.

| Aggregation type | Field | Effect |
|---|---|---|
| `AggregationCountSketch` | `DeltaTransmission bool` | Enable sparse CS delta |
| `AggregationCountSketch` | `DeltaThreshold float64` | Min cell change (default 1.0) |
| `AggregationCountMinSketch` | `DeltaTransmission bool` | Enable sparse CMS delta |
| `AggregationCountMinSketch` | `DeltaThreshold float64` | Min cell change (default 1.0) |
| `AggregationHLLSketch` | `DeltaTransmission bool` | Enable sparse HLL register delta |

HLL has no threshold because `ComputeRegisterDelta` always includes all increased
registers (HLL registers are monotone: they never decrease).

### 14.4 Encoding Wire Values

New encoding constants are added to `sdk/metric/metricdata/data.go`:

| Constant | Value | Sketch type |
|---|---|---|
| `HLLSketchEncodingDelta` | `"hll_sketch_delta"` | HyperLogLog |
| `CountMinSketchEncodingDelta` | `"count_min_sketch_delta"` | Count-Min Sketch |
| `CountSketchEncodingDelta` | `"count_sketch_delta"` | Count Sketch |

These are carried in the `Encoding` field of each data point and forwarded by the
OTLP exporter as an attribute on the OTLP metric data point so the receiver knows
how to interpret `Sketch` bytes.

### 14.5 Implementation Files

| File | Change |
|---|---|
| `sdk/metric/metricdata/data.go` | Add `HLLSketchEncodingDelta`, `CountMinSketchEncodingDelta`, `CountSketchEncodingDelta` |
| `sdk/metric/aggregation.go` | Add `DeltaTransmission bool` + `DeltaThreshold float64` to `AggregationCountSketch`, `AggregationCountMinSketch`, `AggregationHLLSketch` |
| `sdk/metric/pipeline.go` | Pass new fields to `b.CountSketch(...)`, `b.CountMinSketch(...)`, `b.HLLSketch(...)` |
| `sdk/metric/internal/aggregate/aggregate.go` | Update `Builder.CountSketch`, `CountMinSketch`, `HLLSketch` signatures; add `Builder.Noop()` |
| `sdk/metric/internal/aggregate/hllsketch.go` | Add `snapshots map`, `payloadFor` helper; delta encoding in `cumulative()` |
| `sdk/metric/internal/aggregate/countminsketch.go` | Add `snapshots map`, `payloadFor` helper; delta encoding in `cumulative()` |
| `sdk/metric/internal/aggregate/countsketch.go` | Add `snapshots map`, `payloadFor` helper; delta encoding in `cumulative()` |
| `sdk/metric/go.mod` | Add `replace github.com/ProjectASAP/sketchlib-go => /tmp/sketchlib-go` |

### 14.6 Snapshot Lifecycle

- **Creation**: on first successful export of a series, a deep clone is stored in the
  `snapshots` map (keyed by `attribute.Distinct`).
- **Update**: after each cumulative export cycle, the snapshot is replaced with a fresh
  clone of the current sketch state.
- **Eviction**: when a series is idle for `maxIdleCycles` consecutive exports it is
  evicted from `values`; its snapshot is also removed from `snapshots` at the same time
  to prevent memory leaks.
- **Thread safety**: `snapshotsMu sync.Mutex` guards the `snapshots` map independently
  of `valuesMu` so snapshot updates do not block concurrent `measure()` calls.

### 14.7 Interaction with Collector-Side Delta Transmission (Phases 1–7)

The two delta layers are orthogonal and can be enabled simultaneously:

```
SDK (cumulative, delta payload) → OTLP → Collector OTLP receiver
  → countsketchprocessor (window mode, delta_transmission=true)
  → Backend
```

The collector processor does not need to reconstruct the full sketch from deltas before
re-computing its own delta; it treats each incoming `CountSketch` data point as an
observation and counts it as `1.0` sample (existing `MetricTypeCountSketch` path in
`ingestMetric`).  The sketch payload bytes are forwarded as-is through the processor
output `sketch_payload` attribute.

---

## 15. References

- Zhang, Chen, Liu. *OctoSketch: Enabling Real-Time, Continuous Network Monitoring over Multiple Cores.* USENIX NSDI 2024. https://www.usenix.org/system/files/nsdi24-zhang-yinda.pdf
- OctoSketch source code. https://github.com/Froot-NetSys/OctoSketch
- Cormode, Muthukrishnan. *An Improved Data Stream Summary: The Count-Min Sketch and its Applications.* JALG 2005.
- Charikar, Chen, Farach-Colton. *Finding Frequent Items in Data Streams.* ICALP 2002.
- Masson, Rim, Lee. *DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees.* VLDB 2019.
- Karnin, Lang, Liberty. *Optimal Quantile Approximation in Streams.* FOCS 2016.
- Yang et al. *ElasticSketch: A Versatile and Efficient System for Big Network Data Measurement.* ACM SIGCOMM 2018.
- Tang et al. *CocoSketch: High-Performance Sketch-Based Measurement over Arbitrary Partial Key Query.* ACM SIGCOMM 2021.
