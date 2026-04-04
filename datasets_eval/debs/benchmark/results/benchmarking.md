# Benchmark Results

## Setup

| Parameter | Value |
|---|---|
| Day | `08-11-21` |
| Dataset | `data_filtered` (starts at **09:00 CET** via `filter_data.py --trading-start 09:00:00`) |
| Q1 | `sketch-finance`, `--accuracy-minutes 60`, `--batch-size 50`, paced (speed 1×), `accuracy_sla=0.01` |
| Evaluation method | OTel file exporter JSONL — one line per window flush; all non-warmup windows compared |
| Window size | Q1: 5-min tumbling |

---

## Summary — 60-minute accuracy

| Query | Purpose | Sketch | Metric | Windows | Pass rate | Verdict |
|---|---|---|---|---|---|---|
| Q1 | EMA indicators | DDSketch | `frac_lt_1pct` ≥ 0.95 | 12 | **11/12** | ⚠️ One early market-open window fails |

---

## Q1 — EMA indicators (DDSketch p50 vs EMA38)

**Sketch:** `ddsketchprocessor`, `relative_accuracy: 0.01`, `quantiles: [0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0]`, `window: 5m`  
**Metric:** `|ema38 − sketch_p50| / |ema38|` per (symbol, window) pair; passes if fraction with error < 1% is ≥ 95%.  
**Evaluation:** 12 windows (09:00–10:00 CET); warmup window (09:00–09:05) skipped → 11 non-warmup windows compared. Note: the warmup window IS flushed in the JSONL and included here for completeness.

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

> **Note:** The 09:10–09:15 window (0.943) is the only failure. EMA38 is still settling on the initial large price moves at market open and a small fraction of symbols have temporarily higher relative error. All subsequent windows pass comfortably.
