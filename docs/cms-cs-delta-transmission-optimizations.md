# CMS / CS Delta Transmission — Payload Optimisation Design
**Date:** 2026-03-28
**Status:** Proposal
**Context:** Benchmark findings from `docs/benchmark-delta-vs-raw-baseline-2026-03-28.md`

---

## 1. Problem

Running `deltaaccbench` (20 windows × 5 000 inserts, Zipf s=1.10) showed that CMS and CS full-sketch
payloads are **larger than the raw sample stream**, and even the delta payloads remain 2–4× larger:

| Sketch | Raw B/win | Full B/win | Delta B/win | Delta / Raw |
|---|---|---|---|---|
| CMS (5×2048) | 40 000 | 246 003 | 150 014 | 3.75× larger |
| CS (ε=0.01) | 40 000 | 655 554 | 87 507 | 2.19× larger |
| HLL (16k regs) | 40 000 | 16 532 | 8 543 | 4.68× **smaller** |

Three independent root causes explain the CMS/CS bloat. Each can be addressed separately; together
they bring both sketches well below the raw-sample baseline.

---

## 2. Root Cause Analysis

### 2.1 CMS: Three Parallel Float64 Arrays (3× bloat)

`SerializePortable` always emits three co-located matrices:

```
counts_float  [rows × cols × 8 B]   ← frequency counts
sum_counts    [rows × cols × 8 B]   ← weighted sums (Go-only extension)
sum2_counts   [rows × cols × 8 B]   ← squared weighted sums (Go-only extension)
```

For CMS 5×2048 that is 3 × 10 240 × 8 = **245 760 B** of array data before any proto overhead.

`sum_counts` and `sum2_counts` are Go-producer-only fields absent from the Rust producer. The
downstream processors (`countminsketchprocessor`, `countminsketchmergeprocessor`) never call
`QuerySum` or `QuerySum2`; they only call `QueryFrequency` and `.Merge()`. The sum arrays are
carried through but never consumed.

**For unweighted insertion (weight = 1, the common telemetry case):**

```
Sum[r][c]  = Count[r][c]     // sum of 1s = count
Sum2[r][c] = Count[r][c]     // sum of 1²s = count
```

The downstream can reconstruct both arrays from Count alone at zero cost.

### 2.2 Float64 Counters for Integer Counts (4–8× wasted bytes)

All counts are integers (each insert adds 1 or an integer weight) but the proto always populates
`counts_float` (`repeated double`, 8 B fixed per cell). Proto3 already has a `counts_int` field
typed `repeated sint64` with packed zigzag-varint encoding:

| Counter value | `double` (current) | `sint64` varint |
|---|---|---|
| 1 | 8 B | 1 B |
| 10 | 8 B | 1 B |
| 100 | 8 B | 2 B |
| 10 000 | 8 B | 3 B |

For a Zipf(s=1.10) stream most cells carry small counts (median ≈ 1–3). Average varint size is
**1–2 B per cell** → 4–8× reduction on the count array.

This applies identically to CS (`counts_float` in `CountSketchState`), and to the delta payloads
(`d_count` / `d_sum` / `d_sum2` fields in `CountMinCell`, `CountSketchCell`).

### 2.3 CS: Column Count Over-Provisioned for the Workload

The benchmark constructs CS via `computeCSCols(epsilon=0.01)`:

```go
ceil(1 / 0.01²) = 10 000  →  next power-of-2 = 16 384 columns
```

For monitoring workloads a 1–2% relative error bound is typically acceptable:

| ε | Cols (padded) | Full float64 | Full int64 est. | vs 40 KB raw |
|---|---|---|---|---|
| 0.01 (current) | 16 384 | 655 KB | ~16–25 KB | still larger |
| 0.02 | 4 096 | 163 KB | ~4–6 KB | **7–10× smaller** |
| 0.05 | 512 | 20 KB | ~0.5–1 KB | **40–80× smaller** |

Choosing ε = 0.02 as the default halves error relative to raw (still well within monitoring SLOs)
and reduces the array by 4×.

### 2.4 CS TopK Heap: Stale Counts and Unnecessary Upstream Cost

`CountSketchDelta` always retransmits the full TopK heap on every flush (it is non-additive and
cannot be delta-encoded). The heap stores `(key_string, estimated_count)` pairs where the counts
are the upstream's **local** estimates. After merging at the downstream the CS count matrix has
contributions from all upstream nodes, making the forwarded counts stale.

Additionally the upstream pays O(log k) per insert to maintain the min-heap via `UpdateCS`.

---

## 3. Optimisations

### Opt-1 — Omit `sum_counts` / `sum2_counts` from CMS Payloads

**Scope:** `sketchlib-go` portable serialisation + downstream deserialisation
**Savings:** 3× reduction on CMS full and delta payloads (246 KB → ~82 KB at float64)

Add a serialisation mode flag:

```go
// SerializePortable already exists; add FrequencyOnly option.
type SerializeOptions struct {
    FrequencyOnly bool  // omit Sum / Sum2 arrays; downstream reconstructs from Count
}
```

In `DeserializeCountMinSketchFromProtoBytes`, when `SumCounts` / `Sum2Counts` are absent, copy
`Count` into both:

```go
if len(sumFlat) == 0 {
    sumFlat = flat    // Sum = Count for unweighted streams
}
if len(sum2Flat) == 0 {
    sum2Flat = flat   // Sum2 = Count for unweighted streams
}
```

For weighted streams (sum ≠ count) the sender must either include the sum arrays or signal to the
downstream that reconstruction is not valid. A `CounterType` enum value
`COUNTER_TYPE_INT64_FREQ_ONLY` can carry this signal without a schema change.

**Delta impact:** `CountMinCell` also carries `d_sum` and `d_sum2`. Omitting them when
`FrequencyOnly=true` saves 2 of the 3 doubles per changed cell.

---

### Opt-2 — Use `sint64` Packed Varint Instead of `double`

**Scope:** `sketchlib-go` Go producer + consumer for CMS and CS
**Savings:** 4–8× on all count arrays; applies to full payloads, delta cell lists, and norm vectors

Change `SerializePortable` to populate `counts_int` (sint64) instead of `counts_float`:

```go
// Before
state := &cmpb.CountMinState{
    CounterType: commonpb.CounterType_COUNTER_TYPE_FLOAT64,
    CountsFloat: countsFloat,
    ...
}

// After
countsInt := make([]int64, 0, s.Rows*s.Cols)
for r := 0; r < s.Rows; r++ {
    for _, v := range s.Count[r] {
        countsInt = append(countsInt, int64(v))
    }
}
state := &cmpb.CountMinState{
    CounterType: commonpb.CounterType_COUNTER_TYPE_INT64,
    CountsInt:   countsInt,
    ...
}
```

The proto schema already has `repeated sint64 counts_int = 4 [packed = true]` — no proto changes
needed. The same applies to `CountSketchState.counts_int`.

For delta cells, change `d_count` / `d_sum` / `d_sum2` from `double` to `sint64` in the proto.
Delta values (differences between integer counts) are also small integers and compress equally well.

**Combined effect of Opt-1 + Opt-2 on CMS 5×2048:**

```
Current:   3 × 10 240 cells × 8 B/cell  = 245 760 B  (≈ 246 KB)
Opt-1:     1 × 10 240 cells × 8 B/cell  =  81 920 B  (≈ 82 KB)
Opt-1+2:   1 × 10 240 cells × ~1.5 B/cell = ~15 360 B (≈ 15 KB, below 40 KB raw)
```

---

### Opt-3 — Reduce CS Column Count to Match Monitoring SLOs

**Scope:** processor YAML configuration
**Savings:** proportional to `ε_new² / ε_old²`; no code change required

Change the default CS processor config from `epsilon: 0.01` to `epsilon: 0.02`:

```yaml
# Before
processors:
  countsketch:
    epsilon: 0.01       # cols = 16 384, full payload ~655 KB

# After
processors:
  countsketch:
    epsilon: 0.02       # cols = 4 096, full payload ~163 KB (float64)
                        #                             ~4–6 KB (int64, after Opt-2)
```

For heavy-hitter monitoring where the query is "is flow X a top-1% talker?", a 2% frequency error
is indistinguishable from a 1% error at the dashboard level.

---

### Opt-4 — Replace CS TopK Heap with Space Saving Key Candidates

**Scope:** `sketchlib-go` CountSketch insert path + delta wire format
**Savings:** eliminates O(log k) heap sift per insert; downstream TopK is more accurate

#### 4.1 Why Space Saving, Not Misra-Gries

Standard Misra-Gries is O(1) amortised for unit-weight inserts but degrades for weighted streams:
inserting an item with weight w requires subtracting 1 from all k entries w times → O(k × w) in
the worst case.

**Weighted Space Saving** (Metwally et al., 2005) handles arbitrary weights in O(log k) per insert
regardless of weight magnitude:

```
insert(x, w):
  if x tracked:
    table[x].count += w
  else:
    min_entry = entry with minimum counter    // O(log k), min-heap
    table[x] = { count: min_entry.count + w,
                 max_error: min_entry.count } // error bounds overcounting
    evict min_entry
```

The table always holds **exactly k entries**. Any item whose total weight exceeds `W / k`
(W = total weight inserted) is guaranteed to appear. For unweighted streams this reduces to
standard Misra-Gries.

#### 4.2 Upstream: Track Key Candidates Only, No Counts

The upstream's `UpdateString` method currently:
1. Updates the CS count matrix (needed)
2. Queries the count matrix for the estimated frequency of this key
3. Inserts `(key, estimated_count)` into the TopK min-heap (O(log k))

With the new design:
1. Update the CS count matrix (unchanged)
2. Insert `key` into a **Space Saving tracker** with weight w (no CS query needed)
   — the tracker maintains its own internal counter, not tied to the CS matrix

The Space Saving tracker only needs to know whether key x is a heavy-hitter candidate; its internal
counter is used for the eviction decision, not forwarded downstream.

#### 4.3 Wire Format Change

Replace `TopKState topk` (key + count pairs) with a plain key list in the delta message:

```proto
// countsketch.proto — BEFORE
message CountSketchDelta {
  uint32                   rows  = 1;
  uint32                   cols  = 2;
  repeated CountSketchCell cells = 3;
  repeated double          l2    = 4 [packed = true];
  TopKState                topk  = 5;   // repeated HeapEntry { string key; double count }
}

// AFTER
message CountSketchDelta {
  uint32                   rows       = 1;
  uint32                   cols       = 2;
  repeated CountSketchCell cells      = 3;
  repeated double          l2         = 4 [packed = true];
  repeated string          hh_keys    = 5;  // heavy-hitter candidate keys, no counts
}
```

Bandwidth comparison for k = 100, average key length = 10 bytes:

| Format | Bytes / flush |
|---|---|
| Current: `repeated HeapEntry { string key; double count }` | ~2 100 B |
| New: `repeated string hh_keys` | ~1 200 B |

The bandwidth saving is modest (~900 B). The more significant wins are:

| | Current heap | Space Saving candidates |
|---|---|---|
| CPU per insert | O(log k) — CS query + heap sift | O(log k) — Space Saving update only, no CS query |
| Counts forwarded | Stale upstream-local estimates | None — downstream queries merged matrix |
| TopK accuracy | Upstream-local view | Downstream's globally-merged CS estimates |

#### 4.4 Downstream Reconstruction

On receiving a `CountSketchDelta` the downstream:

1. Applies the sparse cell delta to its accumulator (unchanged)
2. For each key string in `hh_keys`, queries the **updated** accumulator via
   `QueryWithHash(QueryFrequency, hash(key))` to get a globally-merged frequency estimate
3. Updates its own TopK heap with `(key, merged_estimate)` via `TopK.Update`

The downstream TopK is built from globally-merged counts, which is strictly more accurate than
forwarding upstream-local counts.

#### 4.5 Weighted Insert Guarantee

For weighted streams (e.g. byte counts, span durations):

- Weighted Space Saving guarantees all keys with **total weight > W/k** appear in the candidate set
- W = total weight inserted in the window, k = Space Saving capacity
- For safety set k = 2 × desired_top_n (e.g. k = 200 to reliably find top 100)
- The error bound `max_error` stored in the Space Saving table can be forwarded as metadata
  if the downstream needs to bound its TopK estimation error

---

## 4. Combined Impact

With all four optimisations applied (Opt-1 + Opt-2 + Opt-3 + Opt-4):

| Sketch | Current delta B/win | Optimised delta B/win | vs 40 KB raw |
|---|---|---|---|
| CMS 5×2048 | 150 014 | **~6–10 KB** | **4–6× smaller** |
| CS ε=0.02 | — (was ε=0.01: 87 507) | **~2–4 KB** | **10–20× smaller** |

Breakdown of savings for CMS window-mode delta at 5 000 inserts/window, ~5% cell fill rate:

| Optimisation | Payload before | Payload after | Saving |
|---|---|---|---|
| Baseline | 150 014 B | — | — |
| Opt-1 (drop Sum/Sum2 from delta cells) | 150 014 B | ~70 000 B | ~2× |
| Opt-2 (sint64 varint for d_count) | ~70 000 B | ~10 000 B | ~7× |
| Total | 150 014 B | **~10 000 B** | **~15×** |

---

## 5. Implementation Order

| Priority | Optimisation | Effort | Code location |
|---|---|---|---|
| 1 | **Opt-2** sint64 varint counts | Medium — Go producer + consumer | `sketchlib-go/sketches/CountMinSketch/portable.go`, `CountSketch/portable.go` |
| 2 | **Opt-1** FrequencyOnly CMS serialisation | Small — serialise option + deserialise fallback | `sketchlib-go/sketches/CountMinSketch/portable.go` |
| 3 | **Opt-3** CS epsilon config change | Trivial — YAML default | processor configs |
| 4 | **Opt-4** Space Saving key candidates | Medium — new tracker + proto field | `sketchlib-go/common/`, `proto/countsketch/`, `sketches/CountSketch/` |

Opt-2 is the highest-priority because it benefits both full-sketch and delta payloads for CMS, CS,
and all their delta cell types, with no schema changes and no downstream behaviour change.

---

## 6. Non-Goals

- **HLL, DD, KLL**: these sketches are already at or below the raw-sample baseline; no changes needed.
- **Wire compression (gzip/zstd)**: orthogonal; can be applied on top at the transport layer and
  would further reduce payloads by 2–4× but requires no changes to the sketch serialisation layer.
- **Weighted Sum/Sum2 reconstruction**: Opt-1 is only valid for unweighted (count=1) insertion.
  Weighted CMS must either continue sending Sum/Sum2 or signal `FREQ_ONLY` to the downstream.
