# DEBS 2022 — Benchmark Queries (Q1–Q13)

## Q1 - DEBS EMA indicators (per symbol)

**Purpose:** Trend from short vs long EMA; tests per-symbol state, ordering, windowed aggregation under ~5504 series.

**Formula:** $\mathrm{EMA}_t = \alpha p_t + (1-\alpha)\mathrm{EMA}_{t-1}$, $\alpha = 2/(n+1)$; $n \in \{38,100\}$.

where: p_t = price (last field) at tick t; EMA_{t-1} = EMA value at the previous tick; α = smoothing factor = 2/(n+1); n = EMA period (38 or 100).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]` (symbol), `SecType` (sectype), `Last` (last), `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price`; value = `last`; attributes `symbol`, `exchange` (suffix of ID, e.g. `.NL` → `NL`), `sectype`; timestamp from `Date` + `Trading time` (CEST, `Europe/Berlin`)
- Windows: 5-minute tumbling (300 s), clock-aligned to Berlin local wall clock

**Approach:**
- Use `ddsketchprocessor` or `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.5]`
- Set `drop_original: false` during validation to retain raw gauges alongside sketch output
- Set `transmit_sketch: false` if downstream requires gauge-compatible output
- Controller: `aggregations: ["quantile"]`

**Validation:**  
- **Ground truth:** CSV → exact EMA per symbol per window.  
- **Sketch path:** replay OTLP → scrape quantiles/counts; compare to EMA from raw if retained.  
- **Metrics:** abs/rel error on EMA (or proxy agreement).  
- **Success:** rel error < 1% for ≥95% of (symbol, window); no missing windows; ordering sane.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — independent per-symbol sketches |
| Test types | **Sketch-finance** (ddsketch/kll quantiles as EMA proxy) + **Latency** |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH ordered AS (
  SELECT symbol, last, ts,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
ema AS (
  SELECT symbol, last, ts, rn,
         last AS ema38,
         last AS ema100
  FROM   ordered WHERE rn = 1

  UNION ALL

  SELECT o.symbol, o.last, o.ts, o.rn,
         (2.0/39)  * o.last + (1 - 2.0/39)  * e.ema38,
         (2.0/101) * o.last + (1 - 2.0/101) * e.ema100
  FROM   ordered o
  JOIN   ema e ON e.symbol = o.symbol AND e.rn = o.rn - 1
)
SELECT symbol, ts, ema38, ema100
FROM   ema
ORDER  BY symbol, ts
```

**Controller response — HTTP 422**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
Recursive CTE with no `GROUP BY` aggregate at the top level — not supported by the unmodified controller.

**References:**

- [DEBS 2022 call for solutions — **Query 1** (exponential moving average trend indicators, 5-minute windows)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 1** specification](https://arxiv.org/abs/2206.13237)  
- [Wikipedia — *Moving average* § Exponential moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average)

---

## Q2 - DEBS EMA crossover (buy/sell advice)

**Purpose:** Bullish when $\mathrm{EMA}_{38}-\mathrm{EMA}_{100}$ crosses from ≤0 to >0; bearish when ≥0 to <0. Tests chaining Q1→Q2 and no duplicate signals.

**Formula:** $\mathrm{diff}_t = \mathrm{EMA}_{38,t}-\mathrm{EMA}_{100,t}$; bullish: $\mathrm{diff}_{t-1}\le 0 \land \mathrm{diff}_t>0$; bearish: $\mathrm{diff}_{t-1}\ge 0 \land \mathrm{diff}_t<0$.

where: diff_t = EMA(38) minus EMA(100) at window t; EMA_38 / EMA_100 = exponential moving averages from Q1; a bullish crossover occurs when diff_t crosses from ≤ 0 to > 0; bearish when it crosses from ≥ 0 to < 0.

**Data requirements:**
- Inputs: per symbol, ordered by event time — EMA(38) and EMA(100) at each 5-minute window boundary (from pipeline/Prometheus scrape or recomputed from ticks)
- If recomputing from ticks — DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`); 5-minute tumbling windows for per-window EMA outputs

**Approach:**
- Downstream script on Prometheus export or CSV — not a built-in processor
- Per symbol: compare sign of `EMA_38 − EMA_100` across consecutive windows
- Emit bullish signal when diff crosses from ≤0 to >0; emit bearish signal when ≥0 to <0
- Deduplicate: suppress repeated signal if same direction is already active
- Optional: future custom processor for in-collector crossover detection

**Validation:**  
- **Ground truth:** crossovers from ground-truth EMAs.  
- **Sketch path:** crossovers from sketch/raw-derived EMAs.  
- **Metrics:** precision, recall, F1; duplicate count; optional time tolerance (e.g. ±5 min).  
- **Success:** precision > 90%, recall > 90%, zero duplicate alerts.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (derived from Q1 EMA outputs) |
| Avg samples / window (global, all symbols) | ~61K (inherited from Q1) |
| Avg samples / window (per symbol) | ~12 (inherited from Q1) |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol sign-flip detection |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — exact sign-flip detection required) |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH q1 AS (
  SELECT symbol, last, ts,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn,
         last AS ema38, last AS ema100
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
diffs AS (
  SELECT symbol, ts, ema38 - ema100 AS diff,
         LAG(ema38 - ema100) OVER (PARTITION BY symbol ORDER BY ts) AS prev_diff
  FROM   q1
)
SELECT symbol, ts,
       CASE
         WHEN prev_diff <= 0 AND diff > 0 THEN 'bullish'
         WHEN prev_diff >= 0 AND diff < 0 THEN 'bearish'
       END AS signal
FROM   diffs
WHERE  (prev_diff <= 0 AND diff > 0)
    OR (prev_diff >= 0 AND diff < 0)
```

**Controller response — HTTP 422**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
`CASE`/`LAG` window function with no recognised aggregate — not supported by the unmodified controller.

**References:**

- [DEBS 2022 call for solutions — **Query 2** (buy/sell advice from EMA crossover)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 2** specification](https://arxiv.org/abs/2206.13237)

---

## Q3 - Nexmark Q8 adapted (windowed top-K movers)

**Purpose:** Top-$K$ symbols per window by activity or price move; tests heavy-hitters + ranking under cardinality.

**Formula:** e.g. $\mathrm{score}(s,w)=\max_{t\in w} p_t - \min_{t\in w} p_t$, or $|p_{\mathrm{end}}-p_{\mathrm{start}}|$, or $\max p$; take top $K$ (e.g. $K=10$). Frequency-only variant: rank by event count.

where: s = symbol; w = 5-min tumbling window; p_t = price (last) at tick t within the window; p_end / p_start = last / first price in the window; K = number of top symbols to return (e.g. 10).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`); event time from `Date` + `Time` for the full feed (all row types) or `Date` + `Trading time` for the last-trade variant
- Windows: 5-minute tumbling; any row type counts for the frequency-only variant; only `Last`-populated rows count for the price-move variant

**Approach:**
- Use `countsketchprocessor`, `mode: window`, `window_size: 300s`, `aggregate_by: [symbol]`; tune `epsilon`/`delta` per README
- Controller: `aggregations: ["frequency"]`
- Downstream: sort CountSketch frequency estimates → take top-K (e.g. K=10) symbols per window
- For price-move score variant: retain raw `last` via `drop_original: false`; compute max − min per symbol in each window downstream

**Validation:**  
- **Ground truth:** per (window, symbol) counts and/or scores → top-$K$.  
- **Sketch path:** rank CountSketch frequency (or combined pipeline).  
- **Metrics:** top-$K$ set overlap, Spearman $\rho$, freq error on top-$K$.  
- **Success:** overlap ≥ 80%, $\rho$ > 0.7, freq rel error < 5% on top-$K$.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | **All 5178 series → top-K (K=10) per window** via CountSketch sort |
| Test types | **Sketch-finance** (CountSketch frequency ranking) + **Throughput** |

**Queries sent to `/api/v1/plan` (`query_string` API):**

```promql
count_over_time(financial_last_trade_price[5m])
```

```sql
SELECT symbol, COUNT(*) AS freq
FROM   financial_last_trade_price
GROUP  BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)
```

**Controller response — HTTP 200 (both variants)**

| Field | PromQL | SQL |
|---|---|---|
| sketch_type | CountSketch | CountSketch |
| mode | window | window |
| aggregate_by | `[]` | `["symbol"]` |
| bandwidth sent | 69 040 B/s (delta) | 69 040 B/s (delta) |
| delta_mode | use_delta (×15) | use_delta (×15) |
| agent memory | 0 B | 80 000 B |

**References:**

- [Apache Beam — Nexmark suite: **Query 5** (*Hot items*), **Query 7** (*Highest bid*), **Query 8** (*Monitor new users*)](https://beam.apache.org/documentation/sdks/java/testing/nexmark/)  
- [GitHub — `nexmark/nexmark` (query definitions in repo)](https://github.com/nexmark/nexmark)

---

## Q4 - Per-symbol price stats (high / low / last / range)

**Purpose:** Window high, low, last, range; tests min/max style outputs vs sketches.

**Formula:** $\mathrm{high}=\max p_t$, $\mathrm{low}=\min p_t$, $\mathrm{last}=$ last tick in window, $\mathrm{range}=\mathrm{high}-\mathrm{low}$.

where: p_t = price (last field) at tick t within the 5-min window; high / low = maximum / minimum price in the window; last = final price of the window; range = high minus low.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: 5-minute tumbling per symbol for high/low/last/range aggregation

**Approach:**
- For approximate min/max: use `ddsketchprocessor` or `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.0, 1.0]` (or `[0.01, 0.99]` for robustness against extreme outliers)
- For exact min/max: use **NOP** processor and compute exact aggregates downstream from retained raw gauges
- Compute `range = high − low` downstream after scraping high/low from Prometheus
- Controller: `aggregations: ["quantile"]`

**Validation:**  
- **Ground truth:** exact min, max, last, range per (symbol, window).  
- **Sketch path:** compare to p00/p100 (or retained last).  
- **Metrics:** abs error on min/max; rel error on range.  
- **Success:** range rel error < 5% for ≥90% of cases; no absurd tails.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — independent per-symbol sketches |
| Test types | **Sketch-finance** (ddsketch p0/p100 for approx min/max, exact NOP path) + **Throughput** |

**Queries sent to `/api/v1/plan` (`query_string` API):**

```promql
max_over_time(financial_last_trade_price{sectype="E"}[5m])
min_over_time(financial_last_trade_price{sectype="E"}[5m])
last_over_time(financial_last_trade_price{sectype="E"}[5m])
```

```sql
SELECT symbol,
       MAX(last) AS high,
       MIN(last) AS low,
       LAST_VALUE(last) OVER (
         PARTITION BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)
         ORDER BY ts
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_price,
       MAX(last) - MIN(last) AS range
FROM   financial_last_trade_price
WHERE  sectype = 'E'
GROUP  BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)
```

**Controller response — HTTP 200**

| Variant | Sketch | Mode | Bandwidth | Delta mode |
|---|---|---|---|---|
| `max_over_time` | KLL | window | 414 240 B/s (full) | use_full_sketch (sketch_type_unsupported) |
| `min_over_time` | KLL | window | 414 240 B/s (full) | use_full_sketch (sketch_type_unsupported) |
| `last_over_time` | ddsketch | batch | 92 053 B/s (delta) | use_delta (×6.75) |
| SQL MAX/MIN | KLL | window | 414 240 B/s (full) | use_full_sketch (sketch_type_unsupported) |

**References:**

- [Tiger Data tutorial — *Analyze financial tick data* (OHLC / OHLCV aggregation)](https://docs.timescale.com/tutorials/latest/financial-tick-data/)

---

## Q5 - Realized volatility (per symbol, windowed)

**Purpose:** Dispersion of log returns; tests second-moment behavior and sparse windows.

**Formula:** $r_i=\ln(p_i/p_{i-1})$; $\sigma_w = \sqrt{\frac{1}{n-1}\sum(r_i-\bar r)^2}$.

where: p_i = price (last) at step i; p_{i-1} = previous price; r_i = log return at step i = ln(p_i / p_{i-1}); r̄ = mean log return over all n returns in window w; n = number of returns in the window; σ_w = realized volatility (sample std dev of log returns) for window w.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: 5-minute tumbling per symbol; volatility is computed from consecutive `last` prices ordered by event time within each window

**Approach:**
- For exact σ: use **NOP** processor; compute log returns $r_i = \ln(p_i/p_{i-1})$ for consecutive prices per symbol per window; compute sample std dev downstream
- For IQR proxy: use `ddsketchprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.25, 0.75]`; estimate $\sigma \approx \mathrm{IQR}/1.349$ (normal approximation)
- Controller: `aggregations: ["quantile"]` for the sketch path

**Validation:**  
- **Ground truth:** $\sigma_w$ from ordered prices per window.  
- **Sketch path:** IQR-based estimate vs truth.  
- **Metrics:** abs/rel error on $\sigma$; optional regime bucket match.  
- **Success:** rel error < 10% for ≥90% of (symbol, window); regime accuracy > 85% if used.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol log-return sequences |
| Test types | **Sketch-finance** (ddsketch IQR proxy: σ ≈ IQR/1.349) + **Latency** |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH ticks AS (
  SELECT symbol, ts, last,
         LN(last / LAG(last) OVER (PARTITION BY symbol ORDER BY ts)) AS log_return
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
windowed AS (
  SELECT symbol, log_return,
         TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start
  FROM   ticks
  WHERE  log_return IS NOT NULL
)
SELECT symbol, window_start,
       STDDEV_SAMP(log_return) AS realized_vol
FROM   windowed
GROUP  BY symbol, window_start
```

**Controller response — HTTP 422**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
`STDDEV_SAMP` match arm is commented out in the unmodified `sql.rs`.

**References:**

- [Wikipedia — *Volatility (finance)* (realized / historical volatility)](https://en.wikipedia.org/wiki/Volatility_(finance))

---

## Q6 - Distinct symbol cardinality (active per window)

**Purpose:** Count distinct symbols with ≥1 event in window; tests HLL.

**Formula:** $|\{s : \exists t\in w,\ \mathrm{event}(s,t)\}|$.

where: s = symbol (distinct label value); t = event timestamp; w = 5-min tumbling window; event(s, t) = any tick from symbol s arriving at time t; |{...}| = count of distinct symbols with at least one tick in the window.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `Date`, `Time` (only symbol identity and event timestamp are needed; `Last` optional)
- OTLP metric: Gauge `financial.last_trade_price` with labels `symbol`, `exchange`, `sectype` on each point; value can be 1.0 or actual `last` if present; event time from `Date` + `Time` (full feed — all row types)
- Windows: 5-minute tumbling; aggregation counts **distinct `symbol` values per window** (global cardinality across all series, not per-symbol)

**Approach:**
- Use `hllprocessor`, `mode: window`, `window_duration: 300s`, precision = 14 (gives ~0.8% standard error)
- The HLL aggregates all arriving `symbol` labels across all 5502 series into a single distinct count per window
- Controller: `aggregations: ["cardinality"]`
- Downstream: compare HLL estimate to exact distinct-symbol count computed from raw data

**Validation:**  
- **Ground truth:** exact distinct count per window.  
- **Sketch path:** HLL estimate per window.  
- **Metrics:** abs/rel error.  
- **Success:** rel error < 2% per window; stable adjacent windows.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | N/A — global distinct count, not per-symbol |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | **All 5178 series → 1 distinct count per window** via HLL |
| Test types | **Sketch-finance** (HLL cardinality estimation) + **Throughput** |

**Queries sent to `/api/v1/plan` (`query_string` API):**

```promql
count(count_over_time(financial_last_trade_price[5m]))
```

```sql
SELECT TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,
       COUNT(DISTINCT symbol) AS active_symbols
FROM   financial_last_trade_price
GROUP  BY TUMBLE_START(ts, INTERVAL '5' MINUTE)
```

**Controller response — HTTP 200 (both variants)**

| Field | PromQL | SQL |
|---|---|---|
| sketch_type | HLL | HLL |
| mode | window | window |
| bandwidth sent | 207 120 B/s (full) | 207 120 B/s (full) |
| delta_mode | use_full_sketch (fill_rate_too_high: 96.5%) | use_full_sketch (fill_rate_too_high: 96.5%) |
| agent memory | 32 768 B | 16 384 B |

Delta rejected because HLL fill rate is 96.5% — the sketch is almost always full, so delta compression would not save bandwidth.

**References:**

- [Wikipedia — *HyperLogLog* (cardinality estimation)](https://en.wikipedia.org/wiki/HyperLogLog)  
- [Flajolet et al. — *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm* (PDF)](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

---

## Q7 - TWAP (time-weighted average price, no volume)

**Purpose:** Simple average of ticks in window (DEBS has no volume for true VWAP).

**Formula:** $\mathrm{TWAP}_w = \frac{1}{n}\sum_{i=1}^n p_i$ (optional time-weighted variant with $\Delta t_i$ if you model durations).

where: n = number of ticks in window w; p_i = price (last) of the i-th tick; TWAP_w = arithmetic mean price for window w; Δt_i = time the i-th price was held (used only in the time-weighted variant).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: 5-minute tumbling per symbol for mean/median price aggregation

**Approach:**
- Use `ddsketchprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, `quantiles: [0.5]` — p50 (median) serves as proxy to arithmetic mean for approximately symmetric price distributions
- Controller: `aggregations: ["quantile"]`
- Downstream: compare p50 from sketch to exact arithmetic mean per (symbol, window)
- Optional: retain raw gauges via `drop_original: false` for an exact mean benchmark alongside the sketch path

**Validation:**  
- **Ground truth:** arithmetic mean per (symbol, window).  
- **Sketch path:** p50 vs mean.  
- **Metrics:** abs/rel error; correlation mean vs median.  
- **Success:** rel error < 2% for ≥90% of windows; correlation > 0.95.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~61K |
| Avg samples / window (per symbol) | ~12 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — independent per-symbol sketches |
| Test types | **Sketch-finance** (ddsketch p50 as mean proxy) + **Throughput** |

**Queries sent to `/api/v1/plan` (`query_string` API):**

```promql
avg_over_time(financial_last_trade_price{sectype="E"}[5m])
```

```sql
SELECT symbol,
       TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,
       AVG(last) AS twap
FROM   financial_last_trade_price
WHERE  sectype = 'E'
GROUP  BY symbol, TUMBLE_START(ts, INTERVAL '5' MINUTE)
```

**Controller response — HTTP 200 (both variants)**

| Field | PromQL | SQL |
|---|---|---|
| sketch_type | KLL | KLL |
| mode | window | window |
| aggregate_by | `["sectype"]` | `["sectype", "symbol"]` |
| bandwidth sent | 414 240 B/s (full) | 414 240 B/s (full) |
| delta_mode | use_full_sketch (sketch_type_unsupported) | use_full_sketch (sketch_type_unsupported) |
| sketch quantiles | `[0.5]` | `[0.5]` |
| agent memory | 4 096 B | 4 096 B |

`AVG` → KLL with p50 quantile only (median proxy for arithmetic mean).

**References:**

- [Wikipedia — *Time-weighted average price*](https://en.wikipedia.org/wiki/Time-weighted_average_price)

---

## Q8 - Price anomaly (z-score or IQR)

**Purpose:** Flag unusual prices vs window distribution.

**Formula:** $z_i=(p_i-\mu_w)/\sigma_w$; flag if $|z|>2.5$ (tune as needed). **IQR:** outlier if outside $[Q_1-1.5\,\mathrm{IQR},\, Q_3+1.5\,\mathrm{IQR}]$.

where: p_i = price at tick i; μ_w = mean price in window w; σ_w = sample std dev of prices in window w; z_i = z-score of p_i; Q1 / Q3 = first / third quartile of prices in window; IQR = Q3 − Q1.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: **15-minute tumbling** (900 s) — chosen over 5-min because per-symbol sample count is ~33 vs ~12, giving statistically meaningful IQR/z-score estimates

**Approach:**
- For exact z-score: use **NOP** processor; compute μ and σ per (symbol, window) downstream; flag $|z| > 2.5$ where $z_i = (p_i - \mu_w)/\sigma_w$
- For sketch-based IQR anomaly detection: use `ddsketchprocessor`, `mode: window`, `window_duration: 900s`, `aggregate_by: [symbol]`, `quantiles: [0.25, 0.5, 0.75]`; compute $\mathrm{IQR} = Q_3 - Q_1$; flag prices outside $[Q_1 - 1.5 \cdot \mathrm{IQR},\ Q_3 + 1.5 \cdot \mathrm{IQR}]$
- Controller: `aggregations: ["quantile"]`

**Validation:**  
- **Ground truth:** z-score flags from raw.  
- **Sketch path:** IQR flags vs ground truth.  
- **Metrics:** precision, recall, F1.  
- **Success:** precision > 70%, recall > 80%, F1 > 0.75 (tunable).

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | **15-min** (900 s) — wider window for statistically meaningful per-symbol samples |
| Avg samples / window (global, all symbols) | ~172K |
| Avg samples / window (per symbol) | ~33 |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol IQR/z-score |
| Test types | **Sketch-finance** (ddsketch quantiles [0.25, 0.5, 0.75] for IQR anomaly flags) + **Latency** |

**Queries sent to `/api/v1/plan` (`query_string` API):**

```sql
-- z-score variant
WITH stats AS (
  SELECT symbol, ts, last,
         TUMBLE_START(ts, INTERVAL '15' MINUTE) AS window_start,
         AVG(last) OVER w AS mu,
         STDDEV_SAMP(last) OVER w AS sigma
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  WINDOW w AS (PARTITION BY symbol, TUMBLE(ts, INTERVAL '15' MINUTE))
)
SELECT symbol, ts, last,
       (last - mu) / NULLIF(sigma, 0) AS z_score,
       CASE WHEN ABS((last - mu) / NULLIF(sigma, 0)) > 2.5
            THEN true ELSE false END AS is_anomaly
FROM   stats
```

```sql
-- IQR variant
WITH quartiles AS (
  SELECT symbol,
         TUMBLE_START(ts, INTERVAL '15' MINUTE) AS window_start,
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY last) AS q1,
         PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY last) AS q3
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  GROUP  BY symbol, TUMBLE_START(ts, INTERVAL '15' MINUTE)
)
SELECT f.symbol, f.ts, f.last,
       CASE WHEN f.last < q.q1 - 1.5*(q.q3 - q.q1)
              OR f.last > q.q3 + 1.5*(q.q3 - q.q1)
            THEN true ELSE false
       END AS is_anomaly
FROM   financial_last_trade_price f
JOIN   quartiles q ON f.symbol = q.symbol
       AND TUMBLE_START(f.ts, INTERVAL '15' MINUTE) = q.window_start
```

**Controller response — HTTP 422 (both variants)**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
z-score: `STDDEV_SAMP` not supported; outer SELECT projects arithmetic expressions without a top-level aggregate.  
IQR: `PERCENTILE_CONT` not supported in the unmodified `sql.rs`.

**References:**

- [Google Cloud Dataflow docs — *Anomaly detection with z-score* (Apache Beam notebook)](https://cloud.google.com/dataflow/docs/notebooks/anomaly_detection_zscore)  
- [Wikipedia — *Interquartile range* (Tukey fences / outlier rule)](https://en.wikipedia.org/wiki/Interquartile_range)

---

## Q9 - Bollinger bands (breakout)

**Purpose:** Band around SMA ± $k\sigma$; breakout beyond band.

**Formula:** $\mathrm{Middle}=\mathrm{SMA}_n$, $\mathrm{Upper}=\mathrm{SMA}_n+k\sigma_n$, $\mathrm{Lower}=\mathrm{SMA}_n-k\sigma_n$ (often $k=2$).

where: SMA_n = simple moving average of prices across the N most recent 5-min bars (N = 3); σ_n = sample std dev of prices over the same N bars; k = band-width multiplier (default 2); Middle = centre band = SMA_n.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Windows: rolling span of N=3 consecutive 5-minute bars per symbol (15-min effective lookback); SMA and σ are recomputed at each new bar

**Approach:**
- Use **NOP** processor to retain raw `last` gauges per symbol — no single-window sketch can replace sequential SMA/σ across multiple bars
- Downstream per symbol: collect `last` prices in each 5-min tumbling window; maintain a rolling buffer of N=3 windows
- Compute $\mathrm{SMA}_N$ = mean of all prices across the N most recent windows; compute $\sigma_N$ = sample std dev of the same price set
- Derive Bollinger bands: $\mathrm{Upper} = \mathrm{SMA}_N + k \cdot \sigma_N$, $\mathrm{Lower} = \mathrm{SMA}_N - k \cdot \sigma_N$ (use $k=2$)
- Detect breakout events: flag any `last` price in the current window that falls outside $[\mathrm{Lower},\ \mathrm{Upper}]$

**Validation:**
- **Ground truth:** compute exact SMA_N and σ_N per (symbol, window) from ordered raw prices; derive exact band values and breakout flags.
- **Metrics:** compare band midpoint, width, and breakout event set to ground truth; define thresholds explicitly.
- **Success:** band midpoint rel error < 1%; breakout event precision > 80%, recall > 80%.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | 5-min (300 s); SMA/σ span = N=3 consecutive bars (15-min lookback) |
| Avg samples / window (global, all symbols) | ~61K per 5-min bar |
| Avg samples / window (per symbol) | ~12 per 5-min bar |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol rolling band computation |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — rolling sequential SMA/σ required) |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH bars AS (
  SELECT symbol,
         TUMBLE_START(ts, INTERVAL '5' MINUTE) AS bar_start,
         AVG(last) AS bar_avg
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  GROUP  BY symbol, TUMBLE_START(ts, INTERVAL '5' MINUTE)
),
bands AS (
  SELECT symbol, bar_start,
         AVG(bar_avg)         OVER w AS sma,
         STDDEV_SAMP(bar_avg) OVER w AS sigma
  FROM   bars
  WINDOW w AS (PARTITION BY symbol ORDER BY bar_start ROWS 2 PRECEDING)
)
SELECT symbol, bar_start,
       sma,
       sma + 2 * sigma AS upper_band,
       sma - 2 * sigma AS lower_band
FROM   bands
WHERE  sigma IS NOT NULL
```

**Controller response — HTTP 422**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
The outer SELECT contains arithmetic expressions without a top-level aggregate. `STDDEV_SAMP` in the CTE window clause is also not supported.

**References:**

- [Wikipedia — *Bollinger Bands*](https://en.wikipedia.org/wiki/Bollinger_Bands)

---

## Q10 - RSI

**Purpose:** Momentum 0–100 from avg gain/loss (typically 14 periods).

**Formula:** $\mathrm{RSI}=100-100/(1+\mathrm{RS})$, $\mathrm{RS}=\mathrm{avg\_gain}/\mathrm{avg\_loss}$ (EMA-smoothed).

where: RS = relative strength = avg_gain / avg_loss; avg_gain = Wilder's smoothed EMA of price up-moves (α = 1/14); avg_loss = Wilder's smoothed EMA of price down-moves; lookback = 14 consecutive price changes.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Processing: per-symbol stream ordered by event time; RSI requires a lookback of n=14 consecutive price changes — state accumulates across the full stream, not isolated per window

**Approach:**
- Use **NOP** processor or downstream computation from raw CSV — not suited to single-window sketch aggregation
- Per symbol: compute price changes $\Delta p_i = p_i - p_{i-1}$ for each consecutive pair of `last` prices
- Separate gains ($\Delta p_i > 0$) and losses ($\Delta p_i < 0$)
- Apply Wilder's EMA-smoothed average: $\overline{g}_t = \alpha \cdot g_t + (1-\alpha) \cdot \overline{g}_{t-1}$, $\alpha = 1/14$; same for losses
- $\mathrm{RS} = \overline{g}/\overline{l}$; $\mathrm{RSI} = 100 - 100/(1 + \mathrm{RS})$
- Emit overbought signal when RSI > 70, oversold when RSI < 30

**Validation:** Ground truth from CSV vs pipeline output; metrics: value error and/or overbought/oversold event match.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | N/A — per-symbol full stream state (n=14 price-change lookback) |
| Avg samples / window (global, all symbols) | N/A |
| Avg samples / window (per symbol) | N/A |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol momentum state |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — sequential lookback state required) |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH changes AS (
  SELECT symbol, ts, last,
         last - LAG(last) OVER (PARTITION BY symbol ORDER BY ts) AS delta
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
gains_losses AS (
  SELECT symbol, ts,
         GREATEST(delta, 0)  AS gain,
         GREATEST(-delta, 0) AS loss,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn
  FROM   changes
  WHERE  delta IS NOT NULL
),
seed AS (
  SELECT symbol,
         AVG(gain) AS avg_gain,
         AVG(loss) AS avg_loss,
         MAX(ts)   AS ts,
         14        AS rn
  FROM   gains_losses
  WHERE  rn <= 14
  GROUP  BY symbol
),
rsi_calc AS (
  SELECT symbol, ts, avg_gain, avg_loss, rn FROM seed
  UNION ALL
  SELECT g.symbol, g.ts,
         (r.avg_gain * 13 + g.gain) / 14.0,
         (r.avg_loss * 13 + g.loss) / 14.0,
         g.rn
  FROM   gains_losses g
  JOIN   rsi_calc r ON r.symbol = g.symbol AND g.rn = r.rn + 1
)
SELECT symbol, ts,
       100 - 100 / (1 + avg_gain / NULLIF(avg_loss, 0)) AS rsi
FROM   rsi_calc
WHERE  rn > 14
ORDER  BY symbol, ts
```

**Controller response — HTTP 422**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
Final SELECT projects arithmetic expression with no top-level aggregate. Recursive CTE also not supported.

**References:**

- [Wikipedia — *Relative strength index*](https://en.wikipedia.org/wiki/Relative_strength_index)

---

## Q11 - MACD

**Purpose:** $\mathrm{MACD}=\mathrm{EMA}_{12}-\mathrm{EMA}_{26}$, $\mathrm{Signal}=\mathrm{EMA}_9(\mathrm{MACD})$.

where: EMA_12 = 12-period exponential moving average of price (α = 2/13); EMA_26 = 26-period EMA of price (α = 2/27); MACD line = EMA_12 − EMA_26; Signal line = 9-period EMA of the MACD line (α = 2/10); Histogram = MACD line − Signal line.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Processing: per-symbol ordered ticks; MACD is a chain of EMAs on prices and on the MACD line — state carries across the full stream

**Approach:**
- Use **NOP** processor or downstream computation from raw CSV — single-window sketches are insufficient for chained EMA state
- Per symbol, maintain three running EMA states: EMA_12 ($\alpha = 2/13$), EMA_26 ($\alpha = 2/27$), and EMA_9 on the MACD line ($\alpha = 2/10$)
- On each new `last` price: update EMA_12 and EMA_26; MACD line = EMA_12 − EMA_26
- Update Signal line = EMA_9 applied to MACD line values; Histogram = MACD − Signal
- Emit MACD, Signal, and Histogram per symbol at each event tick or at 5-min window boundaries

**Validation:** Compare MACD/signal/histogram to ground truth from CSV; define acceptable error.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | N/A — per-symbol full stream state (EMA chain across full stream) |
| Avg samples / window (global, all symbols) | N/A |
| Avg samples / window (per symbol) | N/A |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol EMA chain |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — chained EMA state required) |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH ordered AS (
  SELECT symbol, last, ts,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
macd AS (
  SELECT symbol, ts, rn, last,
         last AS ema12,
         last AS ema26,
         0.0  AS macd_line,
         0.0  AS signal_line
  FROM   ordered WHERE rn = 1

  UNION ALL

  SELECT o.symbol, o.ts, o.rn, o.last,
         (2.0/13) * o.last + (1 - 2.0/13) * m.ema12,
         (2.0/27) * o.last + (1 - 2.0/27) * m.ema26,
         ((2.0/13) * o.last + (1 - 2.0/13) * m.ema12)
           - ((2.0/27) * o.last + (1 - 2.0/27) * m.ema26),
         (2.0/10) * (
           ((2.0/13) * o.last + (1 - 2.0/13) * m.ema12)
           - ((2.0/27) * o.last + (1 - 2.0/27) * m.ema26)
         ) + (1 - 2.0/10) * m.signal_line
  FROM   ordered o
  JOIN   macd m ON m.symbol = o.symbol AND m.rn = o.rn - 1
)
SELECT symbol, ts, ema12, ema26,
       macd_line,
       signal_line,
       macd_line - signal_line AS histogram
FROM   macd
ORDER  BY symbol, ts
```

**Controller response — HTTP 422**
```
could not infer aggregation type from query_string; provide explicit aggregations
```
Recursive CTE with no `GROUP BY` aggregate — not supported. Same limitation as Q1.

**References:**

- [Wikipedia — *MACD*](https://en.wikipedia.org/wiki/MACD)

---

## Q12 - Stochastic oscillator (%K / %D)

**Purpose:** Position of price within recent high–low range.

**Formula:** $\%K=100\cdot(C-L_n)/(H_n-L_n)$; $\%D=\mathrm{SMA}_3(\%K)$.

where: C = current price (last field); H_n = highest price over the n-period lookback (n = 14 ticks); L_n = lowest price over the same lookback; %K = position of C within the H/L range (0–100); %D = 3-period simple moving average of %K.

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from `Date` + `Trading time`)
- Processing: per symbol, rolling window of n=14 ticks (or n=14 bars) to find the highest high $H_n$ and lowest low $L_n$ in that span

**Approach:**
- Use **NOP** processor or downstream computation from raw CSV — quantile sketches only approximate extrema, producing incorrect %K/%D
- Per symbol: maintain a sliding deque of the n=14 most recent `last` prices; $H_n = \max(\mathrm{deque})$, $L_n = \min(\mathrm{deque})$
- $\%K = 100 \cdot (C - L_n) / (H_n - L_n)$, where $C$ = current `last` price
- Maintain a rolling buffer of 3 %K values; $\%D = \mathrm{SMA}_3(\%K)$ (3-period simple moving average of %K)
- Emit %K and %D per symbol at each tick or at window boundaries

**Validation:** Ground truth from CSV vs pipeline; error on %K/%D or zone crossings.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` |
| Window size | N/A — per-symbol rolling n=14 ticks (or n bars) for H/L lookback |
| Avg samples / window (global, all symbols) | N/A |
| Avg samples / window (per symbol) | N/A |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | None — per-symbol H/L range tracking |
| Test types | **Throughput + Latency** (Sketch-finance: not applicable — exact min/max over rolling window required) |

**Query sent to `/api/v1/plan` (`query_string` API):**

```sql
WITH rolling AS (
  SELECT symbol, ts, last,
         MIN(last) OVER w AS low_14,
         MAX(last) OVER w AS high_14
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  WINDOW w AS (PARTITION BY symbol ORDER BY ts ROWS 13 PRECEDING)
),
pct_k AS (
  SELECT symbol, ts, last,
         100.0 * (last - low_14) / NULLIF(high_14 - low_14, 0) AS k
  FROM   rolling
)
SELECT symbol, ts, k,
       AVG(k) OVER (PARTITION BY symbol ORDER BY ts ROWS 2 PRECEDING) AS d
FROM   pct_k
ORDER  BY symbol, ts
```

**Controller response — HTTP 200**

| Field | Value |
|---|---|
| sketch_type | KLL |
| mode | window |
| metric resolved as | `pct_k` (last CTE name — metric name mangling ineffective) |
| bandwidth sent | 414 240 B/s (full) |
| delta_mode | use_full_sketch (sketch_type_unsupported) |
| sketch quantiles | `[0.5]` |

The planner finds `AVG(k)` in the final `OVER (...)` window clause and picks KLL — a false positive. The actual stochastic oscillator is not sketch-approximable. The metric resolves as the last CTE name (`pct_k`) rather than the source table.

**References:**

- [Investopedia — *Stochastic oscillator* (definition and %K / %D)](https://www.investopedia.com/terms/s/stochasticoscillator.asp)  
- [Wikipedia — *Stochastic oscillator* § Calculation](https://en.wikipedia.org/wiki/Stochastic_oscillator)

---

## Q13 - Top-K symbols by median price (topk + avg_over_time)

**Purpose:** Top-10 symbols per window ranked by estimated mean price; tests price-based heavy-hitter ranking under CountSketch. Contrasts with Q3 (frequency / event-count ranking): both follow the same `topk(...)` planner path but Q13 ranks by price level rather than tick frequency. The `topk()` wrapper always routes to CountSketch regardless of the wrapped aggregate function.

**Formula:** Rank all equity symbols by 5-min mean price and emit the 10 highest:
$$\mathrm{score}(s,w) = \overline{p}_{s,w} = \frac{1}{|w|}\sum_{t\in w} p_t, \quad \text{take top-}K=10$$

where: s = symbol; w = 5-min tumbling window; p_t = price (last) at tick t within the window; |w| = number of ticks for symbol s in window w; score(s, w) = arithmetic mean price for symbol s in window w; K = 10 (number of top symbols to return).

**Data requirements:**
- DEBS CSV columns: `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Date`
- OTLP metric: Gauge `financial.last_trade_price` (value = `last`, label `sectype`); filter `sectype="E"` (equities only)
- Windows: 5-minute tumbling; only `Last`-populated rows (`data_filtered`)

**Approach:**
- Use `countsketchprocessor`, `mode: window`, `window_size: 300s`, `aggregate_by: [sectype]`
- Controller: `aggregations: ["topk"]`, `k: 10`
- Downstream: CountSketch top-K extraction → top-10 symbols per window sorted by estimated mean price

**Validation:**
- **Ground truth:** per (window, symbol) arithmetic mean price → exact top-10 ranking.
- **Sketch path:** CountSketch top-K extraction per window.
- **Metrics:** top-K set overlap, Spearman ρ.
- **Success:** overlap ≥ 80%, ρ > 0.7.

**Evaluation configuration:**

| Parameter | Value |
|-----------|-------|
| Dataset | `data_filtered` (equities only, `sectype="E"`) |
| Window size | 5-min (300 s) |
| Unique series (active per day) | 5178 |
| Cross-series aggregation | **5178 → top-10 per window** via CountSketch |
| Test types | **Sketch-finance** (CountSketch top-K price ranking) + **Throughput** |

**Query sent to `/api/v1/plan` (`query_string` API):**

```promql
topk(10, avg_over_time(financial_last_trade_price{sectype="E"}[5m]))
```

**Controller response — HTTP 200**

| Field | Value |
|---|---|
| sketch_type | CountSketch |
| mode | window |
| aggregate_by | `["sectype"]` |
| bandwidth sent | 69 040 B/s (delta) |
| delta_mode | use_delta (×15) |
| K | 10 — 640 B precompute memory |

**References:**

- [PromQL — `topk` aggregation operator](https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators)
- [PromQL — `avg_over_time` function](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time)
