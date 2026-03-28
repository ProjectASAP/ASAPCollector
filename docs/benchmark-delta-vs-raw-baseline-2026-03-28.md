# Benchmark: Delta Transmission vs Raw Baseline
**Date:** 2026-03-28
**Tool:** `opentelemetry-app/cmd/deltaaccbench`
**Config:** 20 windows · 5 000 inserts/window · Zipf(s=1.10, v=1.00, max=5 000) · delta-threshold=1.0

---

## How to reproduce

```bash
cd opentelemetry-app
go run ./cmd/deltaaccbench \
  --windows=20 --inserts=5000 \
  --delta-threshold=1.0 \
  --output-dir=/tmp/acc_bench
```

---

## Raw-data baseline

Each window inserts 5 000 elements.
A raw payload transmits every insert as a uint64 hash (8 bytes):

| Description | Bytes / window |
|---|---|
| Raw key stream (5 000 × 8 B) | **40 000 B** (~39 KB) |

> For real telemetry spans/metrics (name + labels + timestamp + value ≈ 200 B each)
> the raw payload is ≈ **1 MB / window** — making sketches far more compact than raw data.

---

## Bandwidth: Full Sketch vs Delta

| Sketch | Mode | Full B/win | Delta B/win | Compression | Notes |
|---|---|---|---|---|---|
| CMS (5×2048) | batch | 246 003 | 170 480 | **1.44×** | |
| CMS (5×2048) | window | 246 003 | 150 014 | **1.64×** | |
| CS (ε=0.01) | batch | 655 554 | 116 987 | **5.60×** | |
| CS (ε=0.01) | window | 655 554 | 87 507 | **7.49×** | |
| HLL (16 k regs) | batch | 16 532 | — | — | delta N/A in batch |
| HLL (16 k regs) | window | 16 532 | 8 543 | **1.94×** | |
| DD | batch | 725 | — | — | no delta needed |
| DD | window | 882 | 0 | — | delta produces empty payload¹ |
| KLL | batch | 2 797 | — | — | no delta support |
| KLL | window | 3 121 | — | — | no delta support |

¹ DDSketch delta returned 0 bytes in window mode — investigate separately.

**vs raw key stream (40 KB baseline):**

| Sketch | Delta B/win | vs raw stream |
|---|---|---|
| CMS delta (window) | 150 014 | 3.75× **larger** than raw key stream |
| CS delta (window) | 87 507 | 2.19× **larger** than raw key stream |
| HLL delta (window) | 8 543 | **4.68× smaller** than raw key stream |
| CMS delta (window) | 150 014 | **6.67× smaller** vs 1 MB real-telemetry baseline |
| CS delta (window) | 87 507 | **11.4× smaller** vs 1 MB real-telemetry baseline |

> CMS and CS full-sketch payloads are large (246–656 KB) because of their 2D count arrays.
> Delta encoding shrinks these significantly, but vs a raw uint64 key stream the crossover
> depends heavily on sketch dimensions. Against realistic telemetry payloads (with labels, timestamps)
> all sketch variants are substantially smaller.

---

## CPU & Memory Overhead

| Sketch | Mode | Delta | Wall ms | CPU user ms | CPU sys ms | Heap MB |
|---|---|---|---|---|---|---|
| CMS | batch | off (baseline) | 111 | 168 | 26 | 3.0 |
| CMS | batch | **on** | 250 | 400 | 52 | 3.8 |
| CMS | window | off (baseline) | 191 | 324 | 22 | 3.6 |
| CMS | window | **on** | 292 | 424 | 70 | 4.3 |
| CS | batch | off (baseline) | 326 | 462 | 105 | 7.2 |
| CS | batch | **on** | 454 | 597 | 159 | 5.3 |
| CS | window | off (baseline) | 354 | 497 | 107 | 8.6 |
| CS | window | **on** | 485 | 681 | 128 | 7.3 |
| HLL | batch | off (baseline) | 44 | 52 | 5 | 6.0 |
| HLL | window | off (baseline) | 32 | 38 | 1 | 5.9 |
| HLL | window | **on** | 55 | 70 | 1 | 5.6 |
| DD | batch | off | 35 | 39 | 0 | 2.9 |
| DD | window | off | 234 | 231 | 34 | 4.2 |
| KLL | batch | off | 65 | 77 | 1 | 2.2 |
| KLL | window | off | 268 | 265 | 39 | 2.7 |

### Delta overhead (delta=on vs delta=off, same sketch+mode)

| Sketch | Mode | Wall overhead | CPU user overhead | Heap overhead |
|---|---|---|---|---|
| CMS | batch | +125% | +137% | +25% |
| CMS | window | +53% | +31% | +20% |
| CS | batch | +39% | +29% | −27% |
| CS | window | +37% | +37% | −15% |
| HLL | window | +72% | +85% | −5% |

> Delta computation requires cloning the processor-side snapshot and computing a diff at each window
> boundary, which adds wall-clock and CPU time. Memory overhead is moderate (snapshot copy per partition).

---

## Accuracy: Delta vs Full (no accuracy loss)

All reconstructed sketches exactly match the full-sketch reference (`correct_recon=true` on every window).
Mean relative query error is **identical** between delta=on and delta=off for the same sketch+mode,
confirming lossless round-trip at threshold=1.0.

| Sketch | Mode | Mean rel err (both) | Max rel err (both) |
|---|---|---|---|
| CMS | batch | 0.1563% | 18.75% |
| CMS | window | 4.6953% | 150.0% |
| CS | batch | 0.0056% | 7.14% |
| CS | window | 0.0911% | 40.0% |
| HLL | window | 0.7297% | 1.34% |

---

## Summary

| Metric | Result |
|---|---|
| Best bandwidth reduction (CS window) | **7.49× vs full sketch** |
| vs real-telemetry raw baseline (~1 MB/win) | **up to 11× smaller** |
| CPU overhead of delta (worst case) | **+137%** user CPU (CMS batch) |
| CPU overhead of delta (typical window mode) | **+31–85%** user CPU |
| Memory overhead | **−27% to +25%** heap (snapshot copy cost) |
| Accuracy impact | **Zero** — lossless at threshold=1.0 |
