# Benchmark: Raw Samples vs Full Sketch vs Delta Sketch

<!-- Design metadata -->

## TL;DR

Benchmark of raw, full-summary, and delta-summary transmission costs.

**Status:** draft

**MVP relationship:** supporting.

This document is design-level: it defines scope, behavior, constraints, and trade-offs; implementation details are intentionally out of scope.

**Date:** 2026-03-28
**Tool:** `otel-app/cmd/deltaaccbench`
**Config:** 20 windows · 5 000 inserts/window · Zipf(s=1.10, v=1.00, max=5 000) · delta-threshold=1.0

Raw bytes = proto-packed fixed64 encoding: **8 bytes per sample** (5 000 × 8 = 40 000 B / window).
This is the minimum on-wire cost for transmitting every raw data point.

---

## How to reproduce

```bash
cd otel-app
go run ./cmd/deltaaccbench \
  --windows=20 --inserts=5000 \
  --delta-threshold=1.0 \
  --output-dir=/tmp/acc_bench
```

---

## Bandwidth: Raw Samples vs Full Sketch vs Delta Sketch

| Sketch | Mode | Delta | Raw B/win | Full B/win | Delta B/win | Full/Delta | Raw/Full | Raw/Delta |
|---|---|---|---|---|---|---|---|---|
| CMS (5×2048) | batch | off | 40 000 | 246 003 | — | — | 0.16× | — |
| CMS (5×2048) | batch | **on** | 40 000 | 246 003 | 170 480 | 1.44× | 0.16× | 0.23× |
| CMS (5×2048) | window | off | 40 000 | 246 003 | — | — | 0.16× | — |
| CMS (5×2048) | window | **on** | 40 000 | 246 003 | 150 014 | 1.64× | 0.16× | 0.27× |
| CS (ε=0.01) | batch | off | 40 000 | 655 554 | — | — | 0.06× | — |
| CS (ε=0.01) | batch | **on** | 40 000 | 655 554 | 116 987 | 5.60× | 0.06× | 0.34× |
| CS (ε=0.01) | window | off | 40 000 | 655 554 | — | — | 0.06× | — |
| CS (ε=0.01) | window | **on** | 40 000 | 655 554 | 87 507 | 7.49× | 0.06× | 0.46× |
| HLL (16 k regs) | batch | off | 40 000 | 16 532 | — | — | **2.42×** | — |
| HLL (16 k regs) | window | off | 40 000 | 16 532 | — | — | **2.42×** | — |
| HLL (16 k regs) | window | **on** | 40 000 | 16 532 | 8 543 | 1.94× | **2.42×** | **4.68×** |
| DD (α=0.01) | batch | off | 40 000 | 725 | — | — | **55×** | — |
| DD (α=0.01) | window | off | 40 000 | 882 | — | — | **45×** | — |
| DD (α=0.01) | window | on¹ | 40 000 | 882 | 0 | — | **45×** | — |
| KLL (k=256) | batch | off | 40 000 | 3 074 | — | — | **13×** | **13×** |
| KLL (k=256) | window | off | 40 000 | 2 992 | — | — | **13×** | **13×** |

¹ DD delta returned 0 bytes in window mode (falls back to full when delta ≥ full) — investigate separately.

**Key takeaways:**

- **CMS and CS full sketches are larger than raw samples** (6–16× bigger). Delta helps a lot (5–7×
  compression vs full) but the delta payload is still 2–4× larger than raw samples at 5 000 inserts/window.
  This is inherent to their 2D count array wire format; with smaller sketch dimensions or larger windows
  the crossover shifts in sketches' favour.
- **HLL full sketch is already smaller than raw** (raw is 2.42× larger than full). Delta pushes that to
  4.68× — the best raw/delta ratio of the frequency/cardinality sketches.
- **DD and KLL are dramatically smaller than raw** (13–55× compression vs raw samples). Quantile
  sketches are highly compact because they only store bucket boundaries, not counts per cell.

---

## CPU & Memory Overhead

| Sketch | Mode | Delta | Wall ms | CPU user ms | CPU sys ms | Heap MB |
|---|---|---|---|---|---|---|
| CMS | batch | off | 111 | 168 | 26 | 3.0 |
| CMS | batch | **on** | 250 | 400 | 52 | 3.8 |
| CMS | window | off | 191 | 324 | 22 | 3.6 |
| CMS | window | **on** | 292 | 424 | 70 | 4.3 |
| CS | batch | off | 326 | 462 | 105 | 7.2 |
| CS | batch | **on** | 454 | 597 | 159 | 5.3 |
| CS | window | off | 354 | 497 | 107 | 8.6 |
| CS | window | **on** | 485 | 681 | 128 | 7.3 |
| HLL | batch | off | 44 | 52 | 5 | 6.0 |
| HLL | window | off | 32 | 38 | 1 | 5.9 |
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

Delta computation requires cloning the processor-side snapshot and diffing at each window boundary.

---

## Accuracy: Delta vs Full (no loss)

All reconstructed sketches exactly match the full-sketch reference (`correct_recon=true` every window).
Mean relative error is **identical** between delta=on and delta=off at threshold=1.0 — lossless round-trip.

| Sketch | Mode | Mean rel err | Max rel err |
|---|---|---|---|
| CMS | batch | 0.1563% | 18.75% |
| CMS | window | 4.6953% | 150.0% |
| CS | batch | 0.0056% | 7.14% |
| CS | window | 0.0911% | 40.0% |
| HLL | window | 0.7297% | 1.34% |
| DD | batch/window | 0.29–0.37% | <1% |
| KLL | batch/window | 0.30–0.46% | <2% |

---

## Summary

| Metric | Result |
|---|---|
| CMS/CS vs raw (delta, window) | Delta sketch is **2–4× larger** than raw sample stream |
| HLL vs raw (delta, window) | Delta sketch is **4.68× smaller** than raw sample stream |
| DD/KLL vs raw (full) | Full sketch is **13–55× smaller** than raw sample stream |
| Best sketch/delta compression vs full | CS window: **7.49×** |
| CPU overhead of delta (worst case) | **+137%** user CPU (CMS batch) |
| CPU overhead of delta (typical) | **+31–85%** user CPU (window mode) |
| Memory overhead | **−27% to +25%** heap |
| Accuracy impact | **Zero** — lossless at threshold=1.0 |
