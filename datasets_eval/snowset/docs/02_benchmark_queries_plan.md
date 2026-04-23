# Snowset — Benchmark Queries (Q1–Q5)

---

## Q1 — Warehouse request frequency (heavy-hitter detection via CMS)

**Scenario:** LinkedIn Brooklin Mirror Maker (BMM) — hot partition identification

**Purpose:** Identify which `warehouseId` values are submitting S3 read requests at anomalously high frequency within a sliding window; tests Count-Min Sketch heavy-hitter detection over a high-cardinality categorical key stream under Zipfian distribution. Analogous to BMM's documented failure to quickly identify topic partitions whose throughput rate exceeds task capacity, because partition-level frequency metrics are not readily available and count-based assignment masks throughput skew.

**Urgency:** A warehouse submitting S3 read requests at 10× its peers saturates the shared request quota for all co-located warehouses. The monitoring failure is structural: total fleet request count remains flat and holds the per-warehouse mean near a stable baseline while a single warehouse ID accounts for the majority of requests in any window. A mean-based or total-rate alert never fires. Only frequency estimation over the `warehouseId` key stream — preserving the per-key dimension — can surface the heavy hitter before downstream Brooklin-equivalent replication lag accumulates to an SLO breach.

**Formula:**

$$\hat{f}(w) = \min_{1 \le j \le d} \; C[j,\, h_j(w)]$$

where: $w$ = `warehouseId` (stream key); $d$ = number of hash rows in the CMS array; $h_j$ = $j$-th independent hash function; $C[j, h_j(w)]$ = counter at position $h_j(w)$ in row $j$; $\hat{f}(w)$ = estimated frequency of warehouse $w$ in the current window (always $\ge$ true frequency; overestimates by at most $\varepsilon N$).

CMS dimensions at $\varepsilon = 0.001$, $\delta = 0.01$:

$$w = \left\lceil \frac{e}{\varepsilon} \right\rceil = 2719, \quad d = \left\lceil \ln \frac{1}{\delta} \right\rceil = 5$$

Memory footprint: $w \times d \times 4\,\text{B} = 2719 \times 5 \times 4 \approx 54\,\text{KB}$ regardless of fleet cardinality.

Heavy-hitter threshold: warehouse $w$ is a heavy hitter if $\hat{f}(w) \ge \phi \cdot N$, with $\phi = 0.10$ (accounts for $\ge 10\%$ of all requests in the window).

**Data requirements:**
- Snowset main dataset columns: `queryId`, `warehouseId`, `warehouseSize`, `persistentReadRequestsS3`, `createdTime`
- Stream key: `warehouseId` (int64, anonymised; cardinality unknown — hundreds to low thousands)
- Window: 5-minute tumbling, aligned to Unix minute boundaries
- Filter: `warehouseSize = 4` to isolate one tier and exclude structural differences in per-query S3 request counts across tiers

**Approach:**
- Feed `warehouseId` as the CMS stream key; increment sketch by `persistentReadRequestsS3` (weighted update) rather than by 1, so the sketch tracks total request volume per warehouse rather than query submission count
- Use `countsketchprocessor`, `mode: window`, `window_size: 300s`, `aggregate_by: [warehouseId]`; tune `epsilon: 0.001`, `delta: 0.01`
- Controller: `aggregations: ["frequency"]`
- Downstream: query CMS for each `warehouseId` seen in the window; compare estimated frequency to fleet median; emit heavy-hitter alert when ratio > 10×

**Validation:**
- **Ground truth:** `GROUP BY warehouseId, TIME_BUCKET(INTERVAL '5 minutes', createdTime)` over main dataset → exact per-warehouse request count per window.
- **Sketch path:** CMS estimated frequency per `warehouseId` per window.
- **Metrics:** relative frequency error $(\hat{f}(w) - f(w)) / N$ for each warehouse; false-positive heavy-hitter rate (warehouses flagged with true frequency $< \phi N$); false-negative rate (true heavy hitters missed).
- **Success:** relative error $\le \varepsilon = 0.001$ for all warehouses with probability $\ge 1 - \delta = 0.99$; no false negatives on true heavy hitters (CMS never underestimates); false-positive rate $< 1\%$ of non-heavy-hitter warehouses.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | Snowset main (`warehouseSize = 4`) |
| Window size | 5-min (300 s) |
| Stream key | `warehouseId` (int64, anonymised) |
| Sketch dimensions | $w = 2719$, $d = 5$ ($\varepsilon = 0.001$, $\delta = 0.01$) |
| Sketch memory | ~54 KB per window (fixed, independent of fleet cardinality) |
| Heavy-hitter threshold $\phi$ | 0.10 (≥ 10 % of total window request volume) |
| Unique series (warehouses per window) | Unknown — empirically determined from data |
| Cross-series aggregation | All `warehouseId` values → heavy-hitter set per window |
| Test types | **Sketch-snowset** (CMS frequency estimation) + **Throughput** |

**Ground-truth SQL (DuckDB):**

```sql
-- Step 1: exact per-warehouse request frequency per 5-min window
-- This is the ground truth the CMS estimate is validated against.
SELECT
    TIME_BUCKET(INTERVAL '5 minutes', createdTime)  AS window_start,
    warehouseId,
    SUM(persistentReadRequestsS3)                   AS total_requests,
    SUM(SUM(persistentReadRequestsS3)) OVER (
        PARTITION BY TIME_BUCKET(INTERVAL '5 minutes', createdTime)
    )                                               AS fleet_total_requests,
    ROUND(
        SUM(persistentReadRequestsS3) * 100.0 /
        SUM(SUM(persistentReadRequestsS3)) OVER (
            PARTITION BY TIME_BUCKET(INTERVAL '5 minutes', createdTime)
        ), 4
    )                                               AS pct_of_fleet
FROM read_parquet('snowset-main.parquet')
WHERE warehouseSize = 4
  AND persistentReadRequestsS3 > 0
GROUP BY window_start, warehouseId
ORDER BY window_start, total_requests DESC;

-- Step 2: identify true heavy hitters (phi = 0.10)
WITH freq AS (
    SELECT
        TIME_BUCKET(INTERVAL '5 minutes', createdTime)  AS window_start,
        warehouseId,
        SUM(persistentReadRequestsS3)                   AS wh_requests,
        SUM(SUM(persistentReadRequestsS3)) OVER (
            PARTITION BY TIME_BUCKET(INTERVAL '5 minutes', createdTime)
        )                                               AS N
    FROM read_parquet('snowset-main.parquet')
    WHERE warehouseSize = 4
      AND persistentReadRequestsS3 > 0
    GROUP BY window_start, warehouseId
)
SELECT
    window_start,
    warehouseId,
    wh_requests,
    N,
    ROUND(wh_requests * 1.0 / N, 6)                AS true_frequency,
    wh_requests >= 0.10 * N                         AS is_heavy_hitter
FROM freq
WHERE wh_requests >= 0.10 * N
ORDER BY window_start, wh_requests DESC;
```

**PromQL (after exporting warehouse request counts as a Prometheus counter):**

```promql
-- Top-5 warehouses by S3 request rate in the current 5-min window
topk(5,
  rate(snowflake_persistent_read_requests_s3_total{
    warehouse_size="4"
  }[5m])
) by (warehouse_id)

-- Heavy-hitter alert: fire when any warehouse exceeds 10x the fleet median
(
  rate(snowflake_persistent_read_requests_s3_total{
    warehouse_size="4"
  }[5m])
)
> 10 *
quantile(0.50,
  rate(snowflake_persistent_read_requests_s3_total{
    warehouse_size="4"
  }[5m])
)
```

**References:**

- [LinkedIn Engineering — *Load-balanced Brooklin Mirror Maker: Replicating large-scale Kafka clusters at LinkedIn*, April 2022](https://engineering.linkedin.com/blog/2022/load-balanced-brooklin-mirror-maker--replicating-large-scale-kaf)
- [Apache Kafka Community — *KIP-977: Partition-Level Throughput Metrics*, 2023](https://cwiki.apache.org/confluence/display/KAFKA/KIP-977:+Partition-Level+Throughput+Metrics)
- [G. Cormode and S. Muthukrishnan — *An Improved Data Stream Summary: The Count-Min Sketch and its Applications*, Journal of Algorithms, 2005](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)
- [M. Vuppalapati et al. — *Building An Elastic Query Engine on Disaggregated Storage*, USENIX NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati)

---

## Q2 — Query latency tail detection (quantile via KLL)

**Scenario:** Uber Presto fleet — spill-to-disk silent SLO breach

**Purpose:** Estimate p95 and p99 of `durationTotal` per warehouse within a 5-minute tumbling window; tests KLL sketch quantile estimation over a heavily right-skewed, continuous metric stream. Analogous to Uber's documented multi-month latency regression where mean-based monitoring held green while tail-latency degradation accumulated silently, because the per-query `durationTotal` distribution is dominated by a long-running minority that the mean cannot expose.

**Urgency:** With ~70 million queries over 14 days, the per-query duration stream is violently right-skewed: the majority of queries complete in under one second, while a small cohort of spill-to-S3 queries runs for minutes. A mean-based alert set at any reasonable millisecond threshold holds flat while the p99 triples, because the heavy tail contributes negligibly to the mean. The monitoring failure is structural and identical to Uber's GC pause case: stop-the-world events (spill-to-S3 queries) are episodic, accumulate in the tail, and leave the mean dashboard green until the dependent pipeline has already missed its SLO.

**Formula:**

KLL sketch rank error guarantee:

$$\Pr\left[\left|\hat{r}(q) - r(q)\right| \le \varepsilon \cdot N\right] \ge 1 - \delta$$

where: $q$ = query value (e.g. `durationTotal` in ms); $\hat{r}(q)$ = estimated rank returned by the sketch; $r(q)$ = true rank of $q$ in the stream; $\varepsilon$ = relative rank error (< 0.5% at $k = 400$); $\delta$ = failure probability (< 0.01 at $k = 400$); $N$ = total elements ingested into the sketch in the window.

Spike detection rule (3× rolling baseline):

$$\text{alert if } \hat{Q}_{0.99}^{[5m]} > 3 \times \hat{Q}_{0.99}^{[1h]}$$

where $\hat{Q}_{0.99}^{[5m]}$ and $\hat{Q}_{0.99}^{[1h]}$ are the KLL-estimated 99th-percentile durations over the current 5-minute and 1-hour rolling windows respectively.

**Data requirements:**
- Snowset main dataset columns: `queryId`, `warehouseId`, `warehouseSize`, `durationTotal`, `createdTime`
- Metric stream: `durationTotal` (int64, milliseconds) keyed by `warehouseId`
- Window: 5-minute tumbling for current estimate; 1-hour rolling for baseline
- No auxiliary dataset required — `durationTotal` is a per-query completion summary

**Approach:**
- Use `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [warehouseId]`, `quantiles: [0.50, 0.95, 0.99]`, `k: 400`
- Controller: `aggregations: ["quantile"]`
- Downstream: compare 5-min p99 to 1-hour rolling p99 baseline per `warehouseId`; fire alert when ratio > 3×
- Set `drop_original: false` during validation to retain raw `durationTotal` values alongside sketch output for error measurement

**Validation:**
- **Ground truth:** `APPROX_QUANTILE(durationTotal, 0.99)` (exact, not sketch) per `warehouseId` per 5-minute window from DuckDB over the full Parquet file.
- **Sketch path:** KLL-estimated p99 per window.
- **Metrics:** absolute rank error $|\hat{r} - r|$ normalised by $N$; relative quantile value error $|(\hat{Q}_{0.99} - Q_{0.99})| / Q_{0.99}$; spike detection precision and recall against ground-truth spike windows.
- **Success:** rank error $< \varepsilon N = 0.005N$ for $\ge 99\%$ of windows; quantile value rel error $< 1\%$; spike detection recall $> 95\%$; no false-positive alerts on baseline fluctuation $< 2\times$.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | Snowset main |
| Window size | 5-min (300 s) current; 1-hour rolling baseline |
| Metric | `durationTotal` (ms, int64) |
| KLL parameter $k$ | 400 |
| Rank error bound $\varepsilon$ | < 0.5% |
| Failure probability $\delta$ | < 0.01 |
| Sketch memory per warehouse | < 3 KB |
| Spike detection multiplier | 3× baseline p99 |
| Unique series (warehouses) | Unknown — empirically determined |
| Cross-series aggregation | Independent per-`warehouseId` sketches |
| Test types | **Sketch-snowset** (KLL p95/p99) + **Latency** |

**Ground-truth SQL (DuckDB):**

```sql
-- Exact p50 / p95 / p99 of durationTotal per warehouse per 5-min window
-- Used as ground truth to validate KLL sketch estimates
SELECT
    TIME_BUCKET(INTERVAL '5 minutes', createdTime)   AS window_start,
    warehouseId,
    COUNT(*)                                          AS query_count,
    APPROX_QUANTILE(durationTotal, 0.50)              AS p50_ms,
    APPROX_QUANTILE(durationTotal, 0.95)              AS p95_ms,
    APPROX_QUANTILE(durationTotal, 0.99)              AS p99_ms
FROM read_parquet('snowset-main.parquet')
WHERE durationTotal > 0
GROUP BY window_start, warehouseId
ORDER BY window_start, warehouseId;

-- Spike detection: 5-min p99 vs 1-hour rolling p99 baseline
-- Identifies windows where tail latency has tripled vs recent history
WITH windowed AS (
    SELECT
        TIME_BUCKET(INTERVAL '5 minutes', createdTime)  AS window_start,
        warehouseId,
        APPROX_QUANTILE(durationTotal, 0.99)             AS p99_5min
    FROM read_parquet('snowset-main.parquet')
    WHERE durationTotal > 0
    GROUP BY window_start, warehouseId
)
SELECT
    window_start,
    warehouseId,
    p99_5min,
    AVG(p99_5min) OVER (
        PARTITION BY warehouseId
        ORDER BY window_start
        ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING   -- 1-hour rolling (12 × 5min)
    )                                                AS p99_1h_baseline,
    p99_5min / NULLIF(
        AVG(p99_5min) OVER (
            PARTITION BY warehouseId
            ORDER BY window_start
            ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
        ), 0
    )                                                AS spike_ratio
FROM windowed
WHERE p99_5min / NULLIF(
    AVG(p99_5min) OVER (
        PARTITION BY warehouseId
        ORDER BY window_start
        ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
    ), 0
) > 3.0
ORDER BY window_start, spike_ratio DESC;
```

**PromQL (after exporting `durationTotal` as a Prometheus histogram):**

```promql
-- Current 5-min p99 per warehouse
histogram_quantile(
  0.99,
  sum by (le, warehouse_id) (
    rate(snowflake_query_duration_ms_bucket{
      warehouse_size="4"
    }[5m])
  )
)

-- Spike alert: 5-min p99 > 3× the 1-hour rolling p99 baseline
histogram_quantile(
  0.99,
  sum by (le, warehouse_id) (
    rate(snowflake_query_duration_ms_bucket{warehouse_size="4"}[5m])
  )
)
> 3 *
histogram_quantile(
  0.99,
  sum by (le, warehouse_id) (
    rate(snowflake_query_duration_ms_bucket{warehouse_size="4"}[1h])
  )
)
```

**References:**

- [Uber Engineering — *Presto Express: Speeding up Query Processing with Minimal Resources*](https://www.uber.com/in/en/blog/presto-express/)
- [Uber Engineering & Alluxio — *Speed Up Presto at Uber with Alluxio Local Cache*, 2022](https://www.uber.com/blog/speed-up-presto-with-alluxio-local-cache/)
- [Apache DataSketches — *KLL Sketch: Quantiles Sketch Overview*, 2023](https://datasketches.apache.org/docs/Quantiles/KLLSketch.html)
- [Prometheus Authors — *Histograms and Summaries*, 2024](https://prometheus.io/docs/practices/histograms)

---

## Q3 — S3 spill-to-remote tail detection (quantile via DDSketch)

**Scenario:** Uber Presto fleet — remote-spill latency penalty identification

**Purpose:** Estimate p95 and p99 of `persistentReadBytesS3` per warehouse within a 5-minute tumbling window; tests DDSketch relative-error quantile estimation over a zero-inflated, heavy-tailed byte-count stream. The majority of queries read zero bytes from S3 (cache hits), while a small cohort of cache-miss queries reads at the terabyte scale, producing a distribution where the mean is near zero and the tail is orders of magnitude larger.

**Urgency:** Queries that exhaust the local SSD cache fall through to synchronous S3 reads, serialising the entire scan phase against object storage round-trip time. A dashboard showing total fleet S3 read bytes per window remains flat and green while a single warehouse accounts for a large fraction of all bytes in that window, because aggregating across all warehouses collapses the per-key frequency information. DDSketch's relative-error guarantee is specifically suited to this distribution: unlike rank-error sketches, DDSketch guarantees that the estimate for any value $q$ falls within $\pm \alpha q$, so the error on large byte values (the tail) is proportionally bounded, not absolutely bounded.

**Formula:**

DDSketch relative error guarantee:

$$\left|\frac{\hat{Q}_\phi - Q_\phi}{Q_\phi}\right| \le \alpha$$

where: $\hat{Q}_\phi$ = estimated $\phi$-quantile returned by DDSketch; $Q_\phi$ = true $\phi$-quantile of the stream; $\alpha$ = relative accuracy parameter (e.g. $\alpha = 0.01$ → 1% relative error at every quantile); this guarantee holds for all $\phi$ simultaneously, in a single pass, with no prior knowledge of the distribution range.

Bucket index mapping:

$$i(v) = \left\lceil \frac{\log v}{\log \gamma} \right\rceil, \quad \gamma = \frac{1 + \alpha}{1 - \alpha}$$

**Data requirements:**
- Snowset main dataset columns: `queryId`, `warehouseId`, `warehouseSize`, `persistentReadBytesS3`, `createdTime`
- Metric stream: `persistentReadBytesS3` (int64, bytes); zero-inflated — filter `persistentReadBytesS3 > 0` for sketch input, count zero-byte queries separately
- Window: 5-minute tumbling
- No auxiliary dataset required

**Approach:**
- Use `ddsketchprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [warehouseId]`, `quantiles: [0.95, 0.99]`, `relative_accuracy: 0.01`
- Pre-filter zero-read queries upstream to avoid collapsing the sketch on the zero-mass; track zero-read fraction as a separate gauge
- Controller: `aggregations: ["quantile"]`
- Spike detection: 5-min p99 bytes > 3× 1-hour rolling p99 baseline per `warehouseId`

**Validation:**
- **Ground truth:** exact p95/p99 of `persistentReadBytesS3` (excluding zeros) per `warehouseId` per window from DuckDB.
- **Sketch path:** DDSketch-estimated p95/p99.
- **Metrics:** relative quantile error $|\hat{Q}_\phi - Q_\phi| / Q_\phi$; false-positive spike rate; false-negative spike rate.
- **Success:** relative error $\le \alpha = 0.01$ (1%) for all quantiles across all warehouses; spike detection recall $> 95\%$.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | Snowset main (`persistentReadBytesS3 > 0`) |
| Window size | 5-min (300 s) current; 1-hour rolling baseline |
| Metric | `persistentReadBytesS3` (bytes, int64) |
| DDSketch relative accuracy $\alpha$ | 0.01 (1%) |
| Sketch memory per warehouse | < 4 KB (relative accuracy = 0.01) |
| Spike detection multiplier | 3× baseline p99 |
| Zero-read fraction | Tracked separately as gauge |
| Cross-series aggregation | Independent per-`warehouseId` sketches |
| Test types | **Sketch-snowset** (DDSketch p95/p99) + **Latency** |

**Ground-truth SQL (DuckDB):**

```sql
-- Exact p95 / p99 of S3 read bytes (non-zero reads only) per warehouse
SELECT
    TIME_BUCKET(INTERVAL '5 minutes', createdTime)      AS window_start,
    warehouseId,
    COUNT(*)                                             AS cache_miss_queries,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
        PARTITION BY TIME_BUCKET(INTERVAL '5 minutes', createdTime),
                     warehouseId
    )                                                    AS cache_miss_pct,
    APPROX_QUANTILE(persistentReadBytesS3, 0.95)         AS p95_bytes,
    APPROX_QUANTILE(persistentReadBytesS3, 0.99)         AS p99_bytes
FROM read_parquet('snowset-main.parquet')
WHERE persistentReadBytesS3 > 0
GROUP BY window_start, warehouseId
ORDER BY window_start, p99_bytes DESC;

-- S3 read storm detection: flag warehouses whose p99 triples vs baseline
WITH windowed AS (
    SELECT
        TIME_BUCKET(INTERVAL '5 minutes', createdTime)   AS window_start,
        warehouseId,
        APPROX_QUANTILE(persistentReadBytesS3, 0.99)      AS p99_bytes
    FROM read_parquet('snowset-main.parquet')
    WHERE persistentReadBytesS3 > 0
    GROUP BY window_start, warehouseId
)
SELECT
    window_start,
    warehouseId,
    p99_bytes,
    AVG(p99_bytes) OVER (
        PARTITION BY warehouseId
        ORDER BY window_start
        ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
    )                                                    AS p99_baseline,
    p99_bytes / NULLIF(
        AVG(p99_bytes) OVER (
            PARTITION BY warehouseId
            ORDER BY window_start
            ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
        ), 0
    )                                                    AS spike_ratio
FROM windowed
WHERE p99_bytes / NULLIF(
    AVG(p99_bytes) OVER (
        PARTITION BY warehouseId
        ORDER BY window_start
        ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING
    ), 0
) > 3.0
ORDER BY spike_ratio DESC;
```

**PromQL (after exporting S3 read bytes as a Prometheus histogram):**

```promql
-- Current 5-min p99 S3 read bytes per warehouse
histogram_quantile(
  0.99,
  sum by (le, warehouse_id) (
    rate(snowflake_persistent_read_bytes_s3_bucket{
      warehouse_size="4"
    }[5m])
  )
)

-- S3 read storm alert: p99 > 3× 1-hour rolling baseline
histogram_quantile(
  0.99,
  sum by (le, warehouse_id) (
    rate(snowflake_persistent_read_bytes_s3_bucket{warehouse_size="4"}[5m])
  )
)
> 3 *
histogram_quantile(
  0.99,
  sum by (le, warehouse_id) (
    rate(snowflake_persistent_read_bytes_s3_bucket{warehouse_size="4"}[1h])
  )
)
```

**References:**

- [C. Masson, J. E. Rim, H. K. Lee — *DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees*, PVLDB Vol. 12, 2019](https://arxiv.org/abs/1908.10693)
- [Uber Engineering — *Presto Express: Speeding up Query Processing with Minimal Resources*](https://www.uber.com/in/en/blog/presto-express/)
- [M. Vuppalapati et al. — *Building An Elastic Query Engine on Disaggregated Storage*, USENIX NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati)

---

## Q4 — Query archetype frequency (operator profile CMS)

**Scenario:** Indicium Tech / dbt Labs — operator bottleneck identification and optimizer prioritization

**Purpose:** Estimate the frequency of each query execution archetype (derived from the dominant operator profiling column) within a 5-minute tumbling window; tests Count-Min Sketch frequency estimation over a low-cardinality derived categorical key. Directly analogous to the dbt Labs internal initiative where 26 bottleneck models were identified by examining query metadata — a process that took months because no streaming frequency estimate was maintained over operator-profile columns.

**Urgency:** Query optimizer teams that tune based on synthetic benchmarks (TPC-DS) invest effort in operators that are rare in production. Without a streaming frequency estimate over actual operator execution profiles, the dominant archetype (join-heavy, sort-heavy, scan-heavy) is invisible until a retrospective analysis is run. The CMS over `query_archetype` provides this estimate continuously, in constant memory, and merges across warehouses without requiring a GROUP BY materialization over the full 70M-row dataset.

**Formula:**

Archetype derivation (applied per query before CMS update):

$$\text{archetype}(q) = \arg\max_{o \in \mathcal{O}} \; \text{prof}_o(q)$$

$$\mathcal{O} = \{\text{profHjRso},\; \text{profSortRso},\; \text{profAggRso},\; \text{profScanRso},\; \text{profFilterRso}\}$$

where: $q$ = query (one row of main dataset); $\text{prof}_o(q)$ = milliseconds spent in operator $o$ for query $q$; $\text{archetype}(q)$ = label of the operator consuming the most time; the derived label becomes the CMS stream key.

CMS frequency estimate (same parameters as Q1):

$$\hat{f}(\text{arch}) = \min_{1 \le j \le d} \; C[j,\, h_j(\text{arch})]$$

Archetype share of total window volume:

$$\hat{s}(\text{arch}, w) = \frac{\hat{f}(\text{arch}, w)}{N_w}$$

where $N_w$ = total queries in window $w$.

**Data requirements:**
- Snowset main dataset columns: `queryId`, `warehouseId`, `warehouseSize`, `createdTime`, `profHjRso`, `profSortRso`, `profAggRso`, `profScanRso`, `profFilterRso`
- Derived stream key: `query_archetype` ∈ {`join_heavy`, `sort_heavy`, `agg_heavy`, `scan_heavy`, `filter_heavy`, `other`}
- Window: 5-minute tumbling
- Filter: `GREATEST(profHjRso, profSortRso, profAggRso, profScanRso, profFilterRso) > 0` (exclude queries with no measurable operator time)

**Approach:**
- Derive `query_archetype` at ingestion time using CASE/GREATEST logic before feeding the key to the CMS
- Use `countsketchprocessor`, `mode: window`, `window_size: 300s`, `aggregate_by: [query_archetype, warehouseSize]`; `epsilon: 0.001`, `delta: 0.01`
- Controller: `aggregations: ["frequency"]`
- Downstream: compute archetype share $\hat{s}$ per window; alert when `join_heavy` share exceeds 40% of any warehouse's window volume (signals potential rightsizing or dedicated warehouse need)
- Low cardinality (6 archetypes) means the CMS is effectively exact at these dimensions; the value of the sketch is in the streaming and merging properties, not compression

**Validation:**
- **Ground truth:** `GROUP BY query_archetype, TIME_BUCKET(...)` over main dataset with CASE/GREATEST derivation → exact archetype count and share per window.
- **Sketch path:** CMS estimated frequency per archetype per window.
- **Metrics:** relative frequency error per archetype; archetype share error $|\hat{s} - s|$; alert precision/recall on join-heavy threshold crossings.
- **Success:** frequency rel error $< 0.1\%$ (low cardinality means CMS operates in near-exact regime at $\varepsilon = 0.001$); archetype share error $< 0.5\%$; alert precision $> 95\%$.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | Snowset main (`GREATEST(prof*) > 0`) |
| Window size | 5-min (300 s) |
| Stream key | `query_archetype` (6 values; low cardinality) |
| CMS dimensions | $w = 2719$, $d = 5$ ($\varepsilon = 0.001$, $\delta = 0.01$) |
| Sketch memory | ~54 KB per window (overkill for 6 keys — validates mergeability) |
| Alert threshold | `join_heavy` share $> 40\%$ of warehouse window |
| Cross-series aggregation | Per `(query_archetype, warehouseSize)` pair |
| Test types | **Sketch-snowset** (CMS archetype frequency) + **Throughput** |

**Ground-truth SQL (DuckDB):**

```sql
-- Derive query archetype and compute exact frequency per window
WITH archetypes AS (
    SELECT
        queryId,
        warehouseId,
        warehouseSize,
        createdTime,
        CASE GREATEST(profHjRso, profSortRso, profAggRso, profScanRso, profFilterRso)
            WHEN profHjRso     THEN 'join_heavy'
            WHEN profSortRso   THEN 'sort_heavy'
            WHEN profAggRso    THEN 'agg_heavy'
            WHEN profScanRso   THEN 'scan_heavy'
            WHEN profFilterRso THEN 'filter_heavy'
            ELSE                    'other'
        END AS query_archetype
    FROM read_parquet('snowset-main.parquet')
    WHERE GREATEST(profHjRso, profSortRso, profAggRso,
                   profScanRso, profFilterRso) > 0
)
SELECT
    TIME_BUCKET(INTERVAL '5 minutes', createdTime)   AS window_start,
    warehouseSize,
    query_archetype,
    COUNT(*)                                          AS frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
        PARTITION BY
            TIME_BUCKET(INTERVAL '5 minutes', createdTime),
            warehouseSize
    ), 4)                                             AS pct_of_warehouse_window
FROM archetypes
GROUP BY window_start, warehouseSize, query_archetype
ORDER BY window_start, warehouseSize, frequency DESC;

-- Alert: warehouses where join_heavy exceeds 40% of window volume
WITH arch_share AS (
    SELECT
        TIME_BUCKET(INTERVAL '5 minutes', createdTime)  AS window_start,
        warehouseId,
        warehouseSize,
        CASE GREATEST(profHjRso, profSortRso, profAggRso, profScanRso, profFilterRso)
            WHEN profHjRso THEN 'join_heavy' ELSE 'other'
        END                                              AS is_join,
        COUNT(*)                                         AS cnt
    FROM read_parquet('snowset-main.parquet')
    WHERE GREATEST(profHjRso, profSortRso, profAggRso, profScanRso, profFilterRso) > 0
    GROUP BY window_start, warehouseId, warehouseSize, is_join
)
SELECT
    window_start,
    warehouseId,
    warehouseSize,
    SUM(CASE WHEN is_join = 'join_heavy' THEN cnt ELSE 0 END) * 1.0
    / SUM(cnt)                                           AS join_heavy_share
FROM arch_share
GROUP BY window_start, warehouseId, warehouseSize
HAVING join_heavy_share > 0.40
ORDER BY join_heavy_share DESC;
```

**PromQL (after exporting archetype counts as Prometheus counters):**

```promql
-- Archetype rate share per warehouse size
sum(rate(snowflake_queries_total[5m])) by (query_archetype, warehouse_size)
  /
sum(rate(snowflake_queries_total[5m])) by (warehouse_size)

-- Alert: join_heavy exceeds 40% of any warehouse size's window volume
(
  sum(rate(snowflake_queries_total{query_archetype="join_heavy"}[5m]))
    by (warehouse_size)
  /
  sum(rate(snowflake_queries_total[5m])) by (warehouse_size)
) > 0.40
```

**References:**

- [Indicium Tech — *How we reduced a 6-hour runtime in Alteryx to 9 minutes with dbt and Snowflake*, dbt Developer Blog, 2022](https://docs.getdbt.com/blog/framework-refactor-alteryx-dbt)
- [dataexpert.io — *Case Study: Optimizing Analytics with dbt and Snowflake* (dbt Labs internal + Siemens), 2024](https://www.dataexpert.io/blog/case-study-optimizing-analytics-dbt-snowflake)
- [S. Bress et al. — *Workload Insights from the Snowflake Data Cloud*, PVLDB Vol. 18, 2024](https://dl.acm.org/doi/10.14778/3750601.3750632)
- [G. Cormode — *Count-Min Sketch*, Encyclopedia of Database Systems, Springer, 2009](http://dimacs.rutgers.edu/~graham/pubs/papers/encalgs-cm.pdf)

---

## Q5 — Active warehouse distinct count (HyperLogLog)

**Scenario:** Atheon Analytics — warehouse rightsizing and autoscale threshold calibration

**Purpose:** Estimate the number of distinct `warehouseId` values with at least one active query in each 5-minute window; tests HyperLogLog (HLL) distinct-count estimation over a streaming cardinality query. Complements Q1 (which `warehouseId` is the heavy hitter?) with a fleet-level question (how many distinct warehouses are concurrently active?), which is the prerequisite metric for autoscale threshold calibration.

**Urgency:** Atheon Analytics documented a 60% warehouse spend reduction after identifying that their transformation warehouses processed a wide range of query types simultaneously, with XL and 2XL warehouses executing many short queries that did not justify the tier. Calibrating the autoscale trigger requires knowing how many warehouses are active in each window — a distinct-count query. Exact COUNT DISTINCT over 70M queries is expensive to maintain continuously; HLL computes the same estimate in under 2 KB of memory per window, merges across shards in constant time, and achieves a standard error of ~0.8% at precision $p = 14$.

**Formula:**

HyperLogLog estimator:

$$\hat{n} = \alpha_m \cdot m^2 \cdot \left(\sum_{j=1}^{m} 2^{-M[j]}\right)^{-1}$$

where: $m = 2^p$ = number of registers ($p$ = precision parameter); $M[j]$ = maximum leading-zero count seen in register $j$; $\alpha_m$ = bias-correction constant ($\alpha_{16384} \approx 0.7213 / (1 + 1.079/m)$); $\hat{n}$ = estimated distinct count of `warehouseId` values in the window.

Standard error:

$$\sigma(\hat{n}) \approx \frac{1.04}{\sqrt{m}} = \frac{1.04}{\sqrt{16384}} \approx 0.81\%$$

at $p = 14$ ($m = 16384$ registers, memory $= 16\,\text{KB}$).

**Data requirements:**
- Snowset main dataset columns: `queryId`, `warehouseId`, `createdTime`
- Stream element: one `warehouseId` per arriving query (int64); HLL hashes each and updates the appropriate register
- Window: 5-minute tumbling for distinct count; auxiliary dataset optional — `createdTime` from main suffices for assigning queries to windows
- No auxiliary dataset required for this query

**Approach:**
- Use `hllprocessor`, `mode: window`, `window_duration: 300s`, `precision: 14` (standard error ~0.81%)
- The HLL ingests all `warehouseId` values across all arriving queries and emits one distinct-count estimate per window
- Controller: `aggregations: ["cardinality"]`
- Downstream: plot distinct active warehouse count over time; correlate with p99 latency from Q2 to find the concurrency level at which latency begins degrading (the autoscale trigger point)

**Validation:**
- **Ground truth:** `COUNT(DISTINCT warehouseId)` per 5-minute window from DuckDB over main dataset.
- **Sketch path:** HLL-estimated distinct count per window.
- **Metrics:** absolute error $|\hat{n} - n|$; relative error $|\hat{n} - n| / n$; stability across adjacent windows (no large variance from identical true cardinality).
- **Success:** relative error $< 2\%$ per window; zero systematic bias across the 14-day period; stable estimates for windows with identical true cardinality.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | Snowset main |
| Window size | 5-min (300 s) |
| Stream element | `warehouseId` (int64, anonymised) |
| HLL precision $p$ | 14 |
| Number of registers $m$ | 16 384 ($2^{14}$) |
| Standard error | ~0.81% |
| Sketch memory | 16 KB per window |
| Cross-series aggregation | All `warehouseId` values → single distinct-count estimate per window |
| Test types | **Sketch-snowset** (HLL cardinality) + **Throughput** |

**Ground-truth SQL (DuckDB):**

```sql
-- Exact distinct active warehouse count per 5-min window
-- Ground truth for HLL estimate validation
SELECT
    TIME_BUCKET(INTERVAL '5 minutes', createdTime)   AS window_start,
    COUNT(DISTINCT warehouseId)                       AS exact_distinct_warehouses,
    COUNT(*)                                          AS total_queries
FROM read_parquet('snowset-main.parquet')
GROUP BY window_start
ORDER BY window_start;

-- Correlate distinct warehouse count with p99 latency
-- Identifies the concurrency threshold where latency begins degrading
WITH cardinality AS (
    SELECT
        TIME_BUCKET(INTERVAL '5 minutes', createdTime)   AS window_start,
        COUNT(DISTINCT warehouseId)                       AS distinct_warehouses
    FROM read_parquet('snowset-main.parquet')
    GROUP BY window_start
),
latency AS (
    SELECT
        TIME_BUCKET(INTERVAL '5 minutes', createdTime)   AS window_start,
        APPROX_QUANTILE(durationTotal, 0.99)              AS p99_ms
    FROM read_parquet('snowset-main.parquet')
    WHERE durationTotal > 0
    GROUP BY window_start
)
SELECT
    c.window_start,
    c.distinct_warehouses,
    l.p99_ms,
    -- Bin cardinality to find the degradation threshold
    (c.distinct_warehouses / 50) * 50                    AS cardinality_band
FROM cardinality c
JOIN latency l ON c.window_start = l.window_start
ORDER BY c.window_start;
```

**PromQL (after exporting warehouse activity as a Prometheus metric):**

```promql
-- Estimated distinct active warehouses per window (HLL approximation in Prometheus)
-- Prometheus does not natively implement HLL; use count of active time series as proxy
count(
  count by (warehouse_id) (
    rate(snowflake_queries_total[5m])
  )
)

-- Autoscale alert: distinct active warehouses exceeds empirically derived threshold
count(
  count by (warehouse_id) (
    rate(snowflake_queries_total[5m])
  )
) > 200
```

**References:**

- [Atheon Analytics — *Optimising Snowflake spend by balancing warehouse workload — introducing Warehouse Optimiser*, Medium, February 2025](https://medium.com/@AtheonAnalytics/optimising-snowflake-spend-by-balancing-warehouse-workload-introducing-warehouse-optimiser-7012b73bdcad)
- [P. Flajolet, É. Fusy, O. Gandouet, F. Meunier — *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm*, DMTCS, 2007](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
- [M. Vuppalapati et al. — *Building An Elastic Query Engine on Disaggregated Storage*, USENIX NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati)
- [Prometheus Authors — *Histograms and Summaries*, 2024](https://prometheus.io/docs/practices/histograms)

---

## Q6 — Concurrency-vs-latency degradation threshold (KLL over joined dataset)

**Scenario:** Atheon Analytics — warehouse rightsizing and autoscale threshold calibration

**Purpose:** Estimate p95 query latency as a function of concurrent active query count per warehouse size, using a KLL sketch over the joined main + auxiliary dataset; tests quantile estimation over a temporally-structured stream where the sketch input requires knowing which queries were simultaneously in-flight at each timestamp tick — information that exists only after joining the auxiliary dataset to main. This is the canonical query that **cannot** be answered from the main dataset alone.

**Urgency:** Atheon Analytics documented a 60% warehouse spend reduction after discovering that their XL and 2XL warehouses were over-provisioned relative to actual concurrent load, while simultaneously missing autoscale triggers during genuine surge windows. The root cause was the absence of a continuous concurrency-vs-latency curve: without knowing at what concurrent query count p95 latency begins to degrade, the autoscale trigger threshold is set by intuition rather than by data. The main dataset records `createdTime` and `endTime` per query, but computing concurrent occupancy at an arbitrary timestamp T — "how many queries had `createdTime ≤ T ≤ endTime`?" — requires the auxiliary dataset's pre-materialised per-tick active-query explosion. An interval join on main alone produces the same answer but at prohibitive compute cost when run continuously; the auxiliary dataset is the pre-computed optimisation that makes this query viable as a streaming operation.

**Why the join is mandatory:** The auxiliary dataset encodes one row per `(timestamp_tick, queryId)` pair, representing that query being active at that tick. Joining it to main propagates per-query metrics (`durationTotal`, `memoryUsed`, `warehouseSize`) onto every tick where the query was alive. Grouping by `timestamp_tick` then gives the set of all concurrently active queries at each tick, from which concurrent count and per-tick latency quantiles are derived. This traversal direction — from timestamp to active query set — is structurally impossible from the main dataset alone without a full interval join against a timestamp spine, which is O(spine\_size × query\_count) and orders of magnitude more expensive than the auxiliary join.

**Formula:**

Concurrent query count at tick $t$:

$$C(t) = \left|\{q \in \text{aux} : \text{sec}(q) = t\}\right|$$

where: $t$ = timestamp tick (seconds, from `aux.sec`); $C(t)$ = number of queries active at tick $t$ (the `COUNT(*)` after grouping the joined result by `sec`).

KLL-estimated p95 latency within concurrency band $b$:

$$\hat{Q}_{0.95}(b) = \text{KLL.query}\!\left(0.95,\;\{durationTotal(q) : C(\text{sec}(q)) \in b\}\right)$$

where: $b$ = concurrency band (e.g. $[150, 175)$, width 25); $durationTotal(q)$ = total duration of query $q$ from the main dataset, propagated to every tick where $q$ was active; the KLL sketch ingests one `durationTotal` value per `(tick, query)` pair within band $b$ and returns the p95 estimate at query time.

Autoscale trigger threshold $b^*$:

$$b^* = \min\left\{b : \hat{Q}_{0.95}(b) > \tau\right\}$$

where $\tau$ = p95 latency SLO threshold (e.g. $\tau = 10{,}000\,\text{ms}$); $b^*$ is the lowest concurrency band at which p95 first exceeds the SLO — the empirically derived autoscale trigger point.

**Data requirements:**
- **Auxiliary dataset columns:** `sec` (timestamp tick, Unix seconds), `queryId` (int64, join key)
- **Main dataset columns:** `queryId` (int64, join key), `warehouseSize`, `durationTotal`, `memoryUsed`, `persistentReadBytesS3`
- **Join key:** `queryId` (int64, hash join, no casting overhead)
- **Join semantics:** LEFT JOIN aux → main; one output row per `(sec, queryId)` pair; main metrics repeat on every tick where the query was active
- **Window:** per-tick aggregation (`GROUP BY sec`) then re-aggregated into concurrency bands of width 25
- **Filter:** `warehouseSize = 4` to isolate one tier; removes structural confounding from size differences

**Approach:**
- Run the join using the pre-aggregation pattern from `join_dataset.py`: collapse main to required columns only before the hash join to keep the hash table under 500 MB; see `join_dataset.py` docstring for the full rationale
- Use `kllprocessor`, `mode: window`, `window_duration: 300s`, `aggregate_by: [warehouseSize, concurrency_band]`, `quantiles: [0.50, 0.95, 0.99]`, `k: 400`
- Derive `concurrency_band` as `(C(t) / 25) * 25` after the per-tick GROUP BY; attach as a label before sketch ingestion
- Controller: `aggregations: ["quantile"]`
- Downstream: plot $\hat{Q}_{0.95}(b)$ vs $b$ per `warehouseSize`; find $b^*$ where p95 first crosses $\tau$; set autoscale trigger at $b^* - 1$ band with 30-second cooldown

**Validation:**
- **Ground truth:** exact `APPROX_QUANTILE(durationTotal, 0.95)` per `(warehouseSize, concurrency_band)` from the full joined DuckDB query (Steps 1–2 below).
- **Sketch path:** KLL-estimated p95 per `(warehouseSize, concurrency_band)`.
- **Metrics:** relative quantile error $|\hat{Q}_{0.95} - Q_{0.95}| / Q_{0.95}$ per band; correctness of $b^*$ identification — does the sketch identify the same degradation threshold as the ground truth?
- **Success:** relative error $< 1\%$ per band; $b^*$ identified within $\pm 1$ concurrency band of the ground-truth threshold; no false autoscale triggers on bands below ground-truth $b^*$.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | **Main + auxiliary joined** (`warehouseSize = 4`) |
| Join type | LEFT JOIN aux → main on `queryId` (hash join, int64) |
| Join implementation | Pre-aggregation pattern — main collapsed to 5 columns before hash table build |
| Estimated join output size | Many times larger than either source — auxiliary is the dominant side |
| Window | Per-tick (`GROUP BY sec`) then re-aggregated into concurrency bands |
| Concurrency band width | 25 concurrent queries per band |
| Metric | `durationTotal` (ms) propagated from main to each active tick |
| KLL parameter $k$ | 400 |
| Rank error bound $\varepsilon$ | < 0.5% |
| SLO threshold $\tau$ | 10 000 ms (p95) — adjust empirically per warehouse size |
| Autoscale trigger $b^*$ | Empirically derived from degradation curve |
| Cross-series aggregation | Per `(warehouseSize, concurrency_band)` pair |
| Test types | **Sketch-snowset** (KLL p95 over joined stream) + **Latency** + **Join overhead** |

**Ground-truth SQL (DuckDB — requires both Parquet files):**

```sql
-- Step 1: per-tick concurrent query count and p95 latency
-- Core joined aggregation; main metrics repeat per active tick.
-- Pre-aggregation pattern keeps hash table footprint manageable.
WITH pre_agg AS (
    SELECT
        queryId,
        warehouseSize,
        durationTotal,
        memoryUsed,
        persistentReadBytesS3
    FROM read_parquet('snowset-main.parquet')
    WHERE warehouseSize = 4
),
per_tick AS (
    SELECT
        a.sec                                          AS timestamp_sec,
        COUNT(*)                                       AS concurrent_queries,
        APPROX_QUANTILE(p.durationTotal, 0.50)         AS p50_ms,
        APPROX_QUANTILE(p.durationTotal, 0.95)         AS p95_ms,
        APPROX_QUANTILE(p.durationTotal, 0.99)         AS p99_ms,
        SUM(p.memoryUsed)                              AS total_memory_bytes
    FROM read_parquet('ts-explosion.parquet') a
    LEFT JOIN pre_agg p ON a.queryId = p.queryId
    GROUP BY a.sec
)
SELECT * FROM per_tick ORDER BY timestamp_sec;

-- Step 2: re-aggregate into concurrency bands to find degradation threshold
WITH pre_agg AS (
    SELECT queryId, warehouseSize, durationTotal, memoryUsed
    FROM read_parquet('snowset-main.parquet')
    WHERE warehouseSize = 4
),
per_tick AS (
    SELECT
        a.sec,
        COUNT(*)                               AS concurrent_queries,
        APPROX_QUANTILE(p.durationTotal, 0.95) AS p95_ms
    FROM read_parquet('ts-explosion.parquet') a
    LEFT JOIN pre_agg p ON a.queryId = p.queryId
    GROUP BY a.sec
)
SELECT
    (concurrent_queries / 25) * 25             AS concurrency_band,
    COUNT(*)                                   AS tick_count,
    AVG(p95_ms)                                AS avg_p95_ms,
    APPROX_QUANTILE(p95_ms, 0.95)              AS p95_of_p95_ms,
    MIN(concurrent_queries)                    AS band_min,
    MAX(concurrent_queries)                    AS band_max
FROM per_tick
GROUP BY concurrency_band
ORDER BY concurrency_band;

-- Step 3: identify autoscale threshold b* —
-- lowest concurrency band where avg p95 first exceeds tau = 10,000 ms
WITH pre_agg AS (
    SELECT queryId, warehouseSize, durationTotal
    FROM read_parquet('snowset-main.parquet')
    WHERE warehouseSize = 4
),
per_tick AS (
    SELECT
        a.sec,
        COUNT(*)                               AS concurrent_queries,
        APPROX_QUANTILE(p.durationTotal, 0.95) AS p95_ms
    FROM read_parquet('ts-explosion.parquet') a
    LEFT JOIN pre_agg p ON a.queryId = p.queryId
    GROUP BY a.sec
),
banded AS (
    SELECT
        (concurrent_queries / 25) * 25         AS concurrency_band,
        AVG(p95_ms)                            AS avg_p95_ms
    FROM per_tick
    GROUP BY concurrency_band
)
SELECT
    concurrency_band                           AS autoscale_trigger_b_star,
    avg_p95_ms
FROM banded
WHERE avg_p95_ms > 10000
ORDER BY concurrency_band ASC
LIMIT 1;
```

**PromQL (after exporting per-tick concurrency and latency as Prometheus metrics):**

```promql
-- P95 latency by warehouse size at current concurrency level
histogram_quantile(
  0.95,
  sum by (le, warehouse_size) (
    rate(snowflake_query_duration_ms_bucket{
      warehouse_size="4"
    }[5m])
  )
)

-- Autoscale trigger: p95 latency exceeds SLO threshold tau
histogram_quantile(
  0.95,
  sum by (le, warehouse_size) (
    rate(snowflake_query_duration_ms_bucket{warehouse_size="4"}[5m])
  )
) > 10000

-- Corroborate with active query count crossing empirical b*
-- Replace 150 with the b* value derived from Step 3 SQL above
sum(snowflake_active_queries{warehouse_size="4"}) > 150
```

**References:**

- [Atheon Analytics — *Optimising Snowflake spend by balancing warehouse workload — introducing Warehouse Optimiser*, Medium, February 2025](https://medium.com/@AtheonAnalytics/optimising-snowflake-spend-by-balancing-warehouse-workload-introducing-warehouse-optimiser-7012b73bdcad)
- [M. Vuppalapati et al. — *Building An Elastic Query Engine on Disaggregated Storage*, USENIX NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati)
- [Apache DataSketches — *KLL Sketch: Quantiles Sketch Overview*, 2023](https://datasketches.apache.org/docs/Quantiles/KLLSketch.html)
- [Prometheus Authors — *Histograms and Summaries*, 2024](https://prometheus.io/docs/practices/histograms)