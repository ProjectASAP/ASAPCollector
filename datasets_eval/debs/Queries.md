## DEBS 2022 - dataset fields

| Field | Meaning |
|--------|---------|
| `symbol` | Instrument ID with exchange suffix (e.g. `RDSA.NL`) |
| `sectype` | `E` equity, `I` index |
| `last` | Last trade price |
| `trading_time` | `HH:MM:SS.ssss` (CEST) |
| `trading_date` | `DD-MM-YYYY` |

**Corpus**

- [Zenodo record — DEBS 2022 Grand Challenge: Trading Data](https://doi.org/10.5281/zenodo.6382482)  
- [arXiv:2206.13237 — *The DEBS 2022 Grand Challenge* (dataset and queries)](https://arxiv.org/abs/2206.13237)

---

## Time windows (DEBS)

- **Type:** non-overlapping tumbling windows  
- **Size:** 5 minutes (300 s)  
- **Alignment:** clock-aligned (e.g. 09:00–09:05, 09:05–09:10)

---

## OTLP mapping

- **Metric:** `financial.last_trade_price` (Gauge)  
- **Value:** `last`  
- **Labels:** `symbol`, `exchange` (from suffix), `sectype`  
- **Timestamp:** `trading_date` + `trading_time`  
- **Window:** 5-minute tumbling unless noted  

**Controller:** `POST /api/v1/plan` with `metric_name`, `aggregations` (`quantile` / `frequency` / `cardinality`), `time_window: "5m"`, workload hints; `GET /api/v1/config/{metric_name}` for YAML.

---

## Q1 - DEBS EMA indicators (per symbol)

**Purpose:** Trend from short vs long EMA; tests per-symbol state, ordering, windowed aggregation under ~5504 series.

**Formula:** $\mathrm{EMA}_t = \alpha p_t + (1-\alpha)\mathrm{EMA}_{t-1}$, $\alpha = 2/(n+1)$; $n \in \{38,100\}$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]` (symbol), `SecType` (`sectype`), `Last` (`last`), `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` with value = `last`; attributes `symbol`, `exchange` (suffix of id, e.g. `.NL` → `NL`), `sectype`; timestamp from `trading_date` + `trading_time` (CEST). **Windows:** 5-minute tumbling (300 s), clock-aligned.

**Approach:** `ddsketchprocessor` or `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [symbol]`, quantiles e.g. `[0.5]`; `drop_original: false` while validating; `transmit_sketch: false` if emitting gauges. Controller: `aggregations: ["quantile"]`.

**Validation:**  
- **Ground truth:** CSV → exact EMA per symbol per window.  
- **Sketch path:** replay OTLP → scrape quantiles/counts; compare to EMA from raw if retained.  
- **Metrics:** abs/rel error on EMA (or proxy agreement).  
- **Success:** rel error < 1% for ≥95% of (symbol, window); no missing windows; ordering sane.

**References:**

- [DEBS 2022 call for solutions — **Query 1** (exponential moving average trend indicators, 5-minute windows)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 1** specification](https://arxiv.org/abs/2206.13237)  
- [Wikipedia — *Moving average* § Exponential moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average)

---

## Q2 - DEBS EMA crossover (buy/sell advice)

**Purpose:** Bullish when $\mathrm{EMA}_{38}-\mathrm{EMA}_{100}$ crosses from ≤0 to >0; bearish when ≥0 to <0. Tests chaining Q1→Q2 and no duplicate signals.

**Formula:** $\mathrm{diff}_t = \mathrm{EMA}_{38,t}-\mathrm{EMA}_{100,t}$; bullish: $\mathrm{diff}_{t-1}\le 0 \land \mathrm{diff}_t>0$; bearish: $\mathrm{diff}_{t-1}\ge 0 \land \mathrm{diff}_t<0$.

**Data requirements:** **Inputs:** Per symbol, ordered in event time: EMA(38) and EMA(100) at each 5-minute window boundary (from pipeline/Prometheus or recomputed). **If recomputing from ticks:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time); 5-minute tumbling windows for per-window EMA outputs.

**Approach:** **Downstream** (script on Prometheus export or CSV) - not a built-in processor. Optional future custom processor.

**Validation:**  
- **Ground truth:** crossovers from ground-truth EMAs.  
- **Sketch path:** crossovers from sketch/raw-derived EMAs.  
- **Metrics:** precision, recall, F1; duplicate count; optional time tolerance (e.g. ±5 min).  
- **Success:** precision > 90%, recall > 90%, zero duplicate alerts.

**References:**

- [DEBS 2022 call for solutions — **Query 2** (buy/sell advice from EMA crossover)](https://2022.debs.org/call-for-grand-challenge-solutions/)  
- [arXiv:2206.13237 — Grand Challenge **Query 2** specification](https://arxiv.org/abs/2206.13237)

---

## Q3 - Nexmark Q8 adapted (windowed top-K movers)

**Purpose:** Top-$K$ symbols per window by activity or price move; tests heavy-hitters + ranking under cardinality.

**Formula:** e.g. $\mathrm{score}(s,w)=\max_{t\in w} p_t - \min_{t\in w} p_t$, or $|p_{\mathrm{end}}-p_{\mathrm{start}}|$, or $\max p$; take top $K$ (e.g. $K=10$). Frequency-only variant: rank by event count.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` with value = `last`, labels `symbol`, `exchange`, `sectype`, event time from date+time; **windows:** 5-minute tumbling for per-window counts/scores.

**Approach:** `countsketchprocessor`, `mode: window`, `window_size: 300s`, `aggregate_by: [symbol]`, `epsilon`/`delta` per README; `aggregations: ["frequency"]` in controller. Downstream: sort estimates → top-$K$. Move-score variant needs raw or extra aggregates.

**Validation:**  
- **Ground truth:** per (window, symbol) counts and/or scores → top-$K$.  
- **Sketch path:** rank CountSketch frequency (or combined pipeline).  
- **Metrics:** top-$K$ set overlap, Spearman $\rho$, freq error on top-$K$.  
- **Success:** overlap ≥ 80%, $\rho$ > 0.7, freq rel error < 5% on top-$K$.

**References:**

- [Apache Beam — Nexmark suite: **Query 5** (*Hot items*), **Query 7** (*Highest bid*), **Query 8** (*Monitor new users*)](https://beam.apache.org/documentation/sdks/java/testing/nexmark/)  
- [GitHub — `nexmark/nexmark` (query definitions in repo)](https://github.com/nexmark/nexmark)

---

## Q4 - Per-symbol price stats (high / low / last / range)

**Purpose:** Window high, low, last, range; tests min/max style outputs vs sketches.

**Formula:** $\mathrm{high}=\max p_t$, $\mathrm{low}=\min p_t$, $\mathrm{last}=$ last tick in window, $\mathrm{range}=\mathrm{high}-\mathrm{low}$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Windows:** 5-minute tumbling per symbol for high/low/last/range.

**Approach:** `ddsketchprocessor` / `kllprocessor`, `window` 300s, quantiles e.g. `[0, 1]` or `[0.01, 0.99]` for approx min/max; `aggregate_by: [symbol]`. Exact min/max: **NOP** + raw.

**Validation:**  
- **Ground truth:** exact min, max, last, range per (symbol, window).  
- **Sketch path:** compare to p00/p100 (or retained last).  
- **Metrics:** abs error on min/max; rel error on range.  
- **Success:** range rel error < 5% for ≥90% of cases; no absurd tails.

**References:**

- [Tiger Data tutorial — *Analyze financial tick data* (OHLC / OHLCV aggregation)](https://docs.timescale.com/tutorials/latest/financial-tick-data/)

---

## Q5 - Realized volatility (per symbol, windowed)

**Purpose:** Dispersion of log returns; tests second-moment behavior and sparse windows.

**Formula:** $r_i=\ln(p_i/p_{i-1})$; $\sigma_w = \sqrt{\frac{1}{n-1}\sum(r_i-\bar r)^2}$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Windows:** 5-minute tumbling; volatility uses consecutive `last` prices within each symbol ordered by event time.

**Approach:** **NOP** + downstream for exact $\sigma$; optional `ddsketchprocessor` with quantiles → IQR proxy $\sigma \approx \mathrm{IQR}/1.349$ (normal approx).

**Validation:**  
- **Ground truth:** $\sigma_w$ from ordered prices per window.  
- **Sketch path:** IQR-based estimate vs truth.  
- **Metrics:** abs/rel error on $\sigma$; optional regime bucket match.  
- **Success:** rel error < 10% for ≥90% of (symbol, window); regime accuracy > 85% if used.

**References:**

- [Wikipedia — *Volatility (finance)* (realized / historical volatility)](https://en.wikipedia.org/wiki/Volatility_(finance))

---

## Q6 - Distinct symbol cardinality (active per window)

**Purpose:** Count distinct symbols with ≥1 event in window; tests HLL.

**Formula:** $|\{s : \exists t\in w,\ \mathrm{event}(s,t)\}|$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `Trading time`, `Trading date` (only these are needed to know *which* symbols fired; `Last` optional if you still emit a gauge). **OTLP:** Same metric name `financial.last_trade_price` with `symbol` / `exchange` / `sectype` on each point (value can be constant or `last` if present). **Windows:** 5-minute tumbling; aggregation is **distinct symbols per window** (global cardinality, not per-symbol series).

**Approach:** `hllprocessor`, `mode: window`, `window_duration: 300s`, precision e.g. 14; controller `aggregations: ["cardinality"]`.

**Validation:**  
- **Ground truth:** exact distinct count per window.  
- **Sketch path:** HLL estimate per window.  
- **Metrics:** abs/rel error.  
- **Success:** rel error < 2% per window; stable adjacent windows.

**References:**

- [Wikipedia — *HyperLogLog* (cardinality estimation)](https://en.wikipedia.org/wiki/HyperLogLog)  
- [Flajolet et al. — *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm* (PDF)](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

---

## Q7 - TWAP (time-weighted average price, no volume)

**Purpose:** Simple average of ticks in window (DEBS has no volume for true VWAP).

**Formula:** $\mathrm{TWAP}_w = \frac{1}{n}\sum_{i=1}^n p_i$ (optional time-weighted variant with $\Delta t_i$ if you model durations).

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Windows:** 5-minute tumbling per symbol for mean/median of prices in the window.

**Approach:** `ddsketchprocessor`, `window` 300s, `quantiles: [0.5]`, `aggregate_by: [symbol]`-median as proxy to mean.

**Validation:**  
- **Ground truth:** arithmetic mean per (symbol, window).  
- **Sketch path:** p50 vs mean.  
- **Metrics:** abs/rel error; correlation mean vs median.  
- **Success:** rel error < 2% for ≥90% of windows; correlation > 0.95.

**References:**

- [Wikipedia — *Time-weighted average price*](https://en.wikipedia.org/wiki/Time-weighted_average_price)

---

## Q8 - Price anomaly (z-score or IQR)

**Purpose:** Flag unusual prices vs window distribution.

**Formula:** $z_i=(p_i-\mu_w)/\sigma_w$; flag if $|z|>2.5$ (tune as needed). **IQR:** outlier if outside $[Q_1-1.5\,\mathrm{IQR},\, Q_3+1.5\,\mathrm{IQR}]$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Windows:** 5-minute tumbling by default; for stabler $\mu$/$\sigma$ or IQR you may use a longer tumbling window in the processor (e.g. 900 s)—document the choice when comparing to ground truth.

**Approach:** **NOP** + exact $\mu,\sigma$ downstream; or `ddsketchprocessor` with `quantiles: [0.25, 0.5, 0.75]` for IQR rules.

**Validation:**  
- **Ground truth:** z-score flags from raw.  
- **Sketch path:** IQR flags vs ground truth.  
- **Metrics:** precision, recall, F1.  
- **Success:** precision > 70%, recall > 80%, F1 > 0.75 (tunable).

**References:**

- [Google Cloud Dataflow docs — *Anomaly detection with z-score* (Apache Beam notebook)](https://cloud.google.com/dataflow/docs/notebooks/anomaly_detection_zscore)  
- [Wikipedia — *Interquartile range* (Tukey fences / outlier rule)](https://en.wikipedia.org/wiki/Interquartile_range)

---

## Q9 - Bollinger bands (breakout)

**Purpose:** Band around SMA ± $k\sigma$; breakout beyond band.

**Formula:** $\mathrm{Middle}=\mathrm{SMA}_n$, $\mathrm{Upper}=\mathrm{SMA}_n+k\sigma_n$, $\mathrm{Lower}=\mathrm{SMA}_n-k\sigma_n$ (often $k=2$).

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Windows:** SMA/$\sigma$ need a rolling span of $n$ bars (e.g. $n$ consecutive 5-minute windows per symbol)—define $n$ and alignment in the evaluator to match ground truth.

**Approach:** Same as Q5/Q8-raw for exact SMA/$\sigma$, or quantile proxies.

**Validation:** Same pattern as Q5/Q8 (compare breakout events or band values to ground truth); define thresholds explicitly.

**References:**

- [Wikipedia — *Bollinger Bands*](https://en.wikipedia.org/wiki/Bollinger_Bands)

---

## Q10 - RSI

**Purpose:** Momentum 0–100 from avg gain/loss (typically 14 periods).

**Formula:** $\mathrm{RSI}=100-100/(1+\mathrm{RS})$, $\mathrm{RS}=\mathrm{avg\_gain}/\mathrm{avg\_loss}$ (EMA-smoothed).

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Processing:** Per-symbol stream ordered by event time; RSI needs a lookback of $n$ steps (commonly $n=14$ price changes)—not a single 5-minute sketch in isolation.

**Approach:** **Downstream from raw** or custom processor — not a standard sketch processor.

**Validation:** Ground truth from CSV vs pipeline output; metrics: value error and/or overbought/oversold event match.

**References:**

- [Wikipedia — *Relative strength index*](https://en.wikipedia.org/wiki/Relative_strength_index)

---

## Q11 - MACD

**Purpose:** $\mathrm{MACD}=\mathrm{EMA}_{12}-\mathrm{EMA}_{26}$, $\mathrm{Signal}=\mathrm{EMA}_9(\mathrm{MACD})$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Processing:** Per-symbol ordered ticks; MACD is a chain of EMAs on prices (and on MACD series)—state carries across the full stream.

**Approach:** Downstream EMA chain from raw; sketches at each step are weak for exact MACD.

**Validation:** Compare MACD/signal/histogram to ground truth from CSV; define acceptable error.

**References:**

- [Wikipedia — *MACD*](https://en.wikipedia.org/wiki/MACD)

---

## Q12 - Stochastic oscillator (%K / %D)

**Purpose:** Position of price within recent high–low range.

**Formula:** $\%K=100\cdot(C-L_n)/(H_n-L_n)$; $\%D=\mathrm{SMA}_3(\%K)$.

**Data requirements:** **DEBS CSV:** `ID.[Exchange]`, `SecType`, `Last`, `Trading time`, `Trading date`. **OTLP:** Gauge `financial.last_trade_price` (value = `last`, labels `symbol`, `exchange`, `sectype`, timestamp from trading date+time). **Processing:** Per symbol, rolling window of $n$ ticks or $n$ bars (e.g. $n=14$) for highest/lowest `last` in that span.

**Approach:** Exact $H_n,L_n$ from **raw** or custom min/max; quantile sketches only approximate extrema.

**Validation:** Ground truth from CSV vs pipeline; error on %K/%D or zone crossings.

**References:**

- [Investopedia — *Stochastic oscillator* (definition and %K / %D)](https://www.investopedia.com/terms/s/stochasticoscillator.asp)  
- [Wikipedia — *Stochastic oscillator* § Calculation](https://en.wikipedia.org/wiki/Stochastic_oscillator)
