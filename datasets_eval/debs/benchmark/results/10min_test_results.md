# 10-Minute Benchmark Test Results

## Setup

- Days tested: `08-11-21`
- Mode: `sketch-finance`, `--accuracy-minutes 10`
- Evaluation: Q1 compares all non-warmup 5-min windows; Q3–Q8 compare last completed window only.

---

## Accuracy results — statistical queries

| Query | Metric | Description | Avg | Min | Max | Threshold | Pass |
|---|---|---|---|---|---|---|---|
| Q1 | `symbols_within_1pct_ema` | Fraction of (symbol, window) pairs where sketch EMA38 is within 1% relative error of exact EMA38. `1.0` = 100% correct. All non-warmup windows evaluated. | 1.0000 | 1.0000 | 1.0000 | ≥ 0.95 | ✓ |
| Q3 | `topk_accuracy_score` | `min(overlap/0.8, Spearman_ρ/0.7)`. Overlap = fraction of exact top-10 symbols also in sketch top-10. `1.0` means both overlap ≥ 80% and ρ ≥ 0.7. Last window only. | 1.0000 | 1.0000 | 1.0000 | ≥ 1.00 | ✓ |
| Q4 | `symbols_within_2pct_price_range` | Fraction of symbols where sketch p0 (min) and p100 (max) are each within 2% relative error of the exact min/max. `1.0` = 100%. Last window only. | 1.0000 | 1.0000 | 1.0000 | ≥ 0.90 | ✓ |
| Q5 | `symbols_within_10pct_volatility` | Fraction of symbols where the sketch-derived volatility (IQR/1.349) is within 10% relative error of exact volatility. `1.0` = 100%. Last window only. | 1.0000 | 1.0000 | 1.0000 | ≥ 0.90 | ✓ |

---

## Throughput — NOP queries (full day, max speed, day `08-11-21`)

| Query | Events / sec | Total events | Elapsed (s) |
|---|---|---|---|
| Q2 | 58,397 | 11,794,520 | 201 |
