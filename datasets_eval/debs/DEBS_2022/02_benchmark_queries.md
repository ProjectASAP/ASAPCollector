# DEBS 2022 — Benchmark Queries (Q1–Q12)

## Q1 - DEBS EMA indicators (per symbol)

**Purpose:** Trend from short vs long EMA; tests per-symbol state, ordering, windowed aggregation under ~5504 series.

**Formula:** $\mathrm{EMA}_t = \alpha p_t + (1-\alpha)\mathrm{EMA}_{t-1}$, $\alpha = 2/(n+1)$; $n \in \{38,100\}$.

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

**References:**

- [DEBS 2022 call for solutions — **Query 1** (exponential moving average trend indicators, 5-minute windows)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 1** specification](https://arxiv.org/abs/2206.13237)  
- [Wikipedia — *Moving average* § Exponential moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average)

---

## Q2 - DEBS EMA crossover (buy/sell advice)

**Purpose:** Bullish when $\mathrm{EMA}_{38}-\mathrm{EMA}_{100}$ crosses from ≤0 to >0; bearish when ≥0 to <0. Tests chaining Q1→Q2 and no duplicate signals.

**Formula:** $\mathrm{diff}_t = \mathrm{EMA}_{38,t}-\mathrm{EMA}_{100,t}$; bullish: $\mathrm{diff}_{t-1}\le 0 \land \mathrm{diff}_t>0$; bearish: $\mathrm{diff}_{t-1}\ge 0 \land \mathrm{diff}_t<0$.

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

**References:**

- [DEBS 2022 call for solutions — **Query 2** (buy/sell advice from EMA crossover)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 2** specification](https://arxiv.org/abs/2206.13237)

---

## Q3 - Nexmark Q8 adapted (windowed top-K movers)

**Purpose:** Top-$K$ symbols per window by activity or price move; tests heavy-hitters + ranking under cardinality.

**Formula:** e.g. $\mathrm{score}(s,w)=\max_{t\in w} p_t - \min_{t\in w} p_t$, or $|p_{\mathrm{end}}-p_{\mathrm{start}}|$, or $\max p$; take top $K$ (e.g. $K=10$). Frequency-only variant: rank by event count.

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
| Dataset | `data` (full feed) |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~216K |
| Avg samples / window (per symbol) | ~39 |
| Unique series (active per day) | 5497 |
| Cross-series aggregation | **All 5502 series → top-K (K=10) per window** via CountSketch sort |
| Test types | **Sketch-finance** (CountSketch frequency ranking) + **Throughput** |

**References:**

- [Apache Beam — Nexmark suite: **Query 5** (*Hot items*), **Query 7** (*Highest bid*), **Query 8** (*Monitor new users*)](https://beam.apache.org/documentation/sdks/java/testing/nexmark/)  
- [GitHub — `nexmark/nexmark` (query definitions in repo)](https://github.com/nexmark/nexmark)

---

## Q4 - Per-symbol price stats (high / low / last / range)

**Purpose:** Window high, low, last, range; tests min/max style outputs vs sketches.

**Formula:** $\mathrm{high}=\max p_t$, $\mathrm{low}=\min p_t$, $\mathrm{last}=$ last tick in window, $\mathrm{range}=\mathrm{high}-\mathrm{low}$.

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

**References:**

- [Tiger Data tutorial — *Analyze financial tick data* (OHLC / OHLCV aggregation)](https://docs.timescale.com/tutorials/latest/financial-tick-data/)

---

## Q5 - Realized volatility (per symbol, windowed)

**Purpose:** Dispersion of log returns; tests second-moment behavior and sparse windows.

**Formula:** $r_i=\ln(p_i/p_{i-1})$; $\sigma_w = \sqrt{\frac{1}{n-1}\sum(r_i-\bar r)^2}$.

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

**References:**

- [Wikipedia — *Volatility (finance)* (realized / historical volatility)](https://en.wikipedia.org/wiki/Volatility_(finance))

---

## Q6 - Distinct symbol cardinality (active per window)

**Purpose:** Count distinct symbols with ≥1 event in window; tests HLL.

**Formula:** $|\{s : \exists t\in w,\ \mathrm{event}(s,t)\}|$.

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
| Dataset | `data` (full feed) |
| Window size | 5-min (300 s) |
| Avg samples / window (global, all symbols) | ~216K |
| Avg samples / window (per symbol) | N/A — global distinct count, not per-symbol |
| Unique series (active per day) | 5497 |
| Cross-series aggregation | **All 5502 series → 1 distinct count per window** via HLL |
| Test types | **Sketch-finance** (HLL cardinality estimation) + **Throughput** |

**References:**

- [Wikipedia — *HyperLogLog* (cardinality estimation)](https://en.wikipedia.org/wiki/HyperLogLog)  
- [Flajolet et al. — *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm* (PDF)](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

---

## Q7 - TWAP (time-weighted average price, no volume)

**Purpose:** Simple average of ticks in window (DEBS has no volume for true VWAP).

**Formula:** $\mathrm{TWAP}_w = \frac{1}{n}\sum_{i=1}^n p_i$ (optional time-weighted variant with $\Delta t_i$ if you model durations).

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

**References:**

- [Wikipedia — *Time-weighted average price*](https://en.wikipedia.org/wiki/Time-weighted_average_price)

---

## Q8 - Price anomaly (z-score or IQR)

**Purpose:** Flag unusual prices vs window distribution.

**Formula:** $z_i=(p_i-\mu_w)/\sigma_w$; flag if $|z|>2.5$ (tune as needed). **IQR:** outlier if outside $[Q_1-1.5\,\mathrm{IQR},\, Q_3+1.5\,\mathrm{IQR}]$.

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

**References:**

- [Google Cloud Dataflow docs — *Anomaly detection with z-score* (Apache Beam notebook)](https://cloud.google.com/dataflow/docs/notebooks/anomaly_detection_zscore)  
- [Wikipedia — *Interquartile range* (Tukey fences / outlier rule)](https://en.wikipedia.org/wiki/Interquartile_range)

---

## Q9 - Bollinger bands (breakout)

**Purpose:** Band around SMA ± $k\sigma$; breakout beyond band.

**Formula:** $\mathrm{Middle}=\mathrm{SMA}_n$, $\mathrm{Upper}=\mathrm{SMA}_n+k\sigma_n$, $\mathrm{Lower}=\mathrm{SMA}_n-k\sigma_n$ (often $k=2$).

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

**References:**

- [Wikipedia — *Bollinger Bands*](https://en.wikipedia.org/wiki/Bollinger_Bands)

---

## Q10 - RSI

**Purpose:** Momentum 0–100 from avg gain/loss (typically 14 periods).

**Formula:** $\mathrm{RSI}=100-100/(1+\mathrm{RS})$, $\mathrm{RS}=\mathrm{avg\_gain}/\mathrm{avg\_loss}$ (EMA-smoothed).

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

**References:**

- [Wikipedia — *Relative strength index*](https://en.wikipedia.org/wiki/Relative_strength_index)

---

## Q11 - MACD

**Purpose:** $\mathrm{MACD}=\mathrm{EMA}_{12}-\mathrm{EMA}_{26}$, $\mathrm{Signal}=\mathrm{EMA}_9(\mathrm{MACD})$.

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

**References:**

- [Wikipedia — *MACD*](https://en.wikipedia.org/wiki/MACD)

---

## Q12 - Stochastic oscillator (%K / %D)

**Purpose:** Position of price within recent high–low range.

**Formula:** $\%K=100\cdot(C-L_n)/(H_n-L_n)$; $\%D=\mathrm{SMA}_3(\%K)$.

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

**References:**

- [Investopedia — *Stochastic oscillator* (definition and %K / %D)](https://www.investopedia.com/terms/s/stochasticoscillator.asp)  
- [Wikipedia — *Stochastic oscillator* § Calculation](https://en.wikipedia.org/wiki/Stochastic_oscillator)
