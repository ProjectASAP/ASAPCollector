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

---

## Throughput — NOP queries (full day, max speed, day `08-11-21`)

| Query | Events / sec | Total events | Elapsed (s) |
|---|---|---|---|
| Q2 | 58,397 | 11,794,520 | 201 |
