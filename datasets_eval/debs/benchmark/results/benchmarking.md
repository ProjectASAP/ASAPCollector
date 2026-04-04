# Benchmark Results

## Setup

| Parameter | Value |
|---|---|
| Day | `08-11-21` |
| Dataset | `data_filtered` (Q1, Q4, Q5, Q7; starts at **09:00 CET**); `data/` (Q3, Q6 — full feed) |
| Statistical queries (Q1, Q3–Q7) | `sketch-finance`, `--accuracy-minutes 60`, `--batch-size 50`, paced (speed 1×), `accuracy_sla=0.01` |
| Q8 | `sketch-finance`, `--accuracy-minutes 60`, `--batch-size 50`, paced (speed 1×), `accuracy_sla=0.001` |
| Non-statistical queries (Q2, Q9, Q10) | `throughput`, full day, `--batch-size 5000`, max speed |
| Evaluation method | OTel file exporter JSONL — one line per window flush; all non-warmup windows compared |
| Window size | Q1/Q3–Q7: 5-min tumbling; Q8: 15-min tumbling |

---

## Summary — 60-minute accuracy (statistical queries)

| Query | Purpose | Sketch | Metric | Windows | Pass rate | Verdict |
|---|---|---|---|---|---|---|
| Q1 | EMA indicators | DDSketch | `frac_lt_1pct` ≥ 0.95 | 12 | **11/12** | ⚠️ One early market-open window fails |
| Q3 | Top-K frequency movers | CountSketch | `q3_score` ≥ 1.0 | — | — | ❌ Not run — `group_by` fix pending (Issue 2) |
| Q4 | Price range hi/lo | DDSketch | `frac_hilo_lt_2pct` ≥ 0.90 | 12 | **12/12** | ✅ |
| Q5 | Realized volatility | DDSketch | `frac_sigma_lt_10pct` ≥ 0.90 | 12 | **3/12** | ⚠️ Fails early market-open; passes from ~09:45 |
| Q6 | Distinct cardinality | HLL | `hll_max_rel_err` ≤ 0.02 | 12 | **12/12** | ✅ |
| Q7 | TWAP mean proxy | DDSketch | `frac_mean_lt_2pct` ≥ 0.90 | 12 | **12/12** | ✅ |
| Q8 | Price anomaly IQR | DDSketch | precision ≥ 0.70 / recall ≥ 0.80 / F1 ≥ 0.75 | 4 | **0/4** | ❌ Fails — best F1=0.51 at mult=1.0; needs `accuracy_sla=0.0001` (Issue 8) |

---

## Q1 — EMA indicators (DDSketch p50 vs EMA38)

**Sketch:** `ddsketchprocessor`, `relative_accuracy: 0.01`, `quantiles: [0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0]`, `window: 5m`  
**Metric:** `|ema38 − sketch_p50| / |ema38|` per (symbol, window) pair; passes if fraction with error < 1% is ≥ 95%.

| Window (CET) | `frac_lt_1pct` | Threshold | Pass |
|---|---|---|---|
| 09:00–09:05 | 0.9628 | ≥ 0.95 | ✓ |
| 09:05–09:10 | 0.9525 | ≥ 0.95 | ✓ |
| 09:10–09:15 | **0.9432** | ≥ 0.95 | **✗** |
| 09:15–09:20 | 0.9516 | ≥ 0.95 | ✓ |
| 09:20–09:25 | 0.9557 | ≥ 0.95 | ✓ |
| 09:25–09:30 | 0.9529 | ≥ 0.95 | ✓ |
| 09:30–09:35 | 0.9672 | ≥ 0.95 | ✓ |
| 09:35–09:40 | 0.9630 | ≥ 0.95 | ✓ |
| 09:40–09:45 | 0.9640 | ≥ 0.95 | ✓ |
| 09:45–09:50 | 0.9635 | ≥ 0.95 | ✓ |
| 09:50–09:55 | 0.9614 | ≥ 0.95 | ✓ |
| 09:55–10:00 | 0.9632 | ≥ 0.95 | ✓ |

> **Note:** The 09:10–09:15 window (0.943) is the only failure. EMA38 is still settling on the initial large price moves at market open. All subsequent windows pass comfortably.

---

## Q3 — Top-K frequency movers (CountSketch)

❌ **Not run.** Two blocking issues must be fixed first:
- **Issue 2** (`KNOWN_ISSUES.md`): `run.py` Q3 `group_by` is `()` (empty). Fix: change to `("symbol",)`.
- **Issue 3** (`KNOWN_ISSUES.md`): `run.py` Q3 uses `data_filtered` — spec requires full feed `data/`.

---

## Q4 — Price range hi/lo (DDSketch p0/p100)

**Sketch:** `ddsketchprocessor`, `relative_accuracy: 0.01`, `quantiles: [0.0, 1.0]`, `window: 5m`  
**Metric:** fraction of symbols where both sketch p0 ≤ 2% error of exact low AND sketch p100 ≤ 2% error of exact high.

| Window (CET) | `frac_hilo_lt_2pct` | Threshold | Pass |
|---|---|---|---|
| 09:00–09:05 | 0.9997 | ≥ 0.90 | ✓ |
| 09:05–09:10 | 1.0000 | ≥ 0.90 | ✓ |
| 09:10–09:15 | 1.0000 | ≥ 0.90 | ✓ |
| 09:15–09:20 | 1.0000 | ≥ 0.90 | ✓ |
| 09:20–09:25 | 1.0000 | ≥ 0.90 | ✓ |
| 09:25–09:30 | 1.0000 | ≥ 0.90 | ✓ |
| 09:30–09:35 | 1.0000 | ≥ 0.90 | ✓ |
| 09:35–09:40 | 1.0000 | ≥ 0.90 | ✓ |
| 09:40–09:45 | 1.0000 | ≥ 0.90 | ✓ |
| 09:45–09:50 | 1.0000 | ≥ 0.90 | ✓ |
| 09:50–09:55 | 1.0000 | ≥ 0.90 | ✓ |
| 09:55–10:00 | 1.0000 | ≥ 0.90 | ✓ |

---

## Q5 — Realized volatility σ proxy (DDSketch IQR/1.349)

**Sketch:** `ddsketchprocessor`, `relative_accuracy: 0.01`, `quantiles: [0.25, 0.75]`, `window: 5m`  
**Metric:** fraction of symbols where `|sketch_σ − exact_σ| / max(|sketch_σ|, |exact_σ|, ref_floor) < 10%`.

| Window (CET) | `frac_sigma_lt_10pct` | Threshold | Pass |
|---|---|---|---|
| 09:00–09:05 | 0.7945 | ≥ 0.90 | **✗** |
| 09:05–09:10 | 0.8148 | ≥ 0.90 | **✗** |
| 09:10–09:15 | 0.7896 | ≥ 0.90 | **✗** |
| 09:15–09:20 | 0.8130 | ≥ 0.90 | **✗** |
| 09:20–09:25 | 0.8642 | ≥ 0.90 | **✗** |
| 09:25–09:30 | 0.8397 | ≥ 0.90 | **✗** |
| 09:30–09:35 | 0.8786 | ≥ 0.90 | **✗** |
| 09:35–09:40 | 0.8860 | ≥ 0.90 | **✗** |
| 09:40–09:45 | 0.8962 | ≥ 0.90 | **✗** |
| 09:45–09:50 | 0.9090 | ≥ 0.90 | ✓ |
| 09:50–09:55 | 0.9158 | ≥ 0.90 | ✓ |
| 09:55–10:00 | 0.9104 | ≥ 0.90 | ✓ |

---

## Q6 — Distinct symbol cardinality (HLL)

**Sketch:** `hllprocessor`, `precision: 14`, `window: 5m`  
**Metric:** `|hll_estimate − exact_count| / exact_count`; passes if max relative error < 2%.

| Window (CET) | `hll_max_rel_err` | Threshold | Pass |
|---|---|---|---|
| 09:00–09:05 | 0.0000 | ≤ 0.02 | ✓ |
| 09:05–09:10 | 0.0000 | ≤ 0.02 | ✓ |
| 09:10–09:15 | 0.0006 | ≤ 0.02 | ✓ |
| 09:15–09:20 | 0.0003 | ≤ 0.02 | ✓ |
| 09:20–09:25 | 0.0012 | ≤ 0.02 | ✓ |
| 09:25–09:30 | 0.0009 | ≤ 0.02 | ✓ |
| 09:30–09:35 | 0.0013 | ≤ 0.02 | ✓ |
| 09:35–09:40 | 0.0006 | ≤ 0.02 | ✓ |
| 09:40–09:45 | 0.0000 | ≤ 0.02 | ✓ |
| 09:45–09:50 | 0.0003 | ≤ 0.02 | ✓ |
| 09:50–09:55 | 0.0000 | ≤ 0.02 | ✓ |
| 09:55–10:00 | 0.0000 | ≤ 0.02 | ✓ |

---

## Q7 — TWAP mean proxy (DDSketch p50)

**Sketch:** `ddsketchprocessor`, `relative_accuracy: 0.01`, `quantiles: [0.5]`, `window: 5m`  
**Metric:** fraction of symbols where `|sketch_p50 − exact_mean| / |exact_mean| < 2%`.

| Window (CET) | `frac_mean_lt_2pct` | Threshold | Pass |
|---|---|---|---|
| 09:00–09:05 | 0.9992 | ≥ 0.90 | ✓ |
| 09:05–09:10 | 0.9988 | ≥ 0.90 | ✓ |
| 09:10–09:15 | 1.0000 | ≥ 0.90 | ✓ |
| 09:15–09:20 | 0.9997 | ≥ 0.90 | ✓ |
| 09:20–09:25 | 0.9997 | ≥ 0.90 | ✓ |
| 09:25–09:30 | 1.0000 | ≥ 0.90 | ✓ |
| 09:30–09:35 | 0.9997 | ≥ 0.90 | ✓ |
| 09:35–09:40 | 0.9997 | ≥ 0.90 | ✓ |
| 09:40–09:45 | 0.9997 | ≥ 0.90 | ✓ |
| 09:45–09:50 | 1.0000 | ≥ 0.90 | ✓ |
| 09:50–09:55 | 0.9997 | ≥ 0.90 | ✓ |
| 09:55–10:00 | 0.9997 | ≥ 0.90 | ✓ |

---

## Q8 — Price anomaly IQR fence (DDSketch)

**Sketch:** `ddsketchprocessor`, `relative_accuracy: 0.001`, `quantiles: [0.25, 0.75]`, `window: 15m`  
**Metric:** anomaly flag precision, recall, F1 — per symbol, per 15-min window.

### Per-window results (`accuracy_sla=0.001`, `k=1.5`)

| Window (CET) | Symbols w/ valid IQR | Precision | Recall | F1 |
|---|---|---|---|---|
| 09:00–09:15 | 37.7% (1 434 / 3 803) | 0.776 | 0.450 | 0.570 |
| 09:15–09:30 | 37.3% (1 327 / 3 558) | 0.731 | 0.362 | 0.484 |
| 09:30–09:45 | 34.2% (1 150 / 3 364) | 0.653 | 0.278 | 0.390 |
| 09:45–10:00 | 30.3% (1 016 / 3 354) | 0.547 | 0.257 | 0.349 |
| **Aggregate** | — | **0.719** | **0.368** | **0.487** |

Thresholds: precision ≥ 0.70, recall ≥ 0.80, F1 ≥ 0.75. All windows fail recall.

> **Next step (Issue 8):** Set `accuracy_sla: 0.0001` for Q8 (already applied in `run.py`) and re-run the 60-minute Q8 benchmark.

---

## Non-statistical queries

Non-statistical queries (Q2, Q9, Q10) use a NOP collector — no sketch, no accuracy evaluation.

### Q2 — EMA crossover (NOP)

Day `08-11-21`, `throughput`, full day, `--batch-size 5000`, max speed.

| Total events | Wall time (s) | Events / sec | Exports | Batch size |
|---:|---:|---:|---:|---:|
| 11,794,520 | 163.3 | 72,246 | 2,359 | 5,000 |

### Q9 — Trade price index (NOP)

❌ **Not run yet.**

### Q10 — Market impact (NOP)

❌ **Not run yet.**
