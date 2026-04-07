## Dataset-driven baseline used by all queries

From `analysis/results/summaries/`:

- Files: **93**
- Global cardinality:
  - `time_series`: **3939**
  - `entity`: **10**
  - `metric_base`: **3629**
- Per-file cardinality:
  - `time_series_cardinality`: **2283**
- Sampling characteristics:
  - mean inter-arrival: ~**1.003 s**
  - 5-min window density (per file): ~**298.5 rows/window** (median)

Interpretation:
- For per-metric analysis in one file, each active metric gets roughly one sample per second, so ~300 points/5-min window.

Controller-input note:
- `Formula input path` snippets are parser-compatible PromQL or SQL (SeQuAL) templates for `DataCollector/controller`.
- Replace placeholder metric/table names (for example `exathlon_metric`, `exathlon_metrics`) with your deployed schema names.

---

## Q1 - Windowed distribution profiling (p50/p95/p99) per metric group

**Purpose:** Validate quantile sketch fidelity for system telemetry distributions.

**Use case domain:** Distributed systems performance monitoring — SRE teams track p50/p95/p99 latency and resource utilization per service/entity to define SLOs and detect degradation without storing raw samples.

**Formula:** $Q_{0.50}, Q_{0.95}, Q_{0.99}$ per $(\mathrm{entity}, \mathrm{metric\_base}, w)$.

**Formula input path (controller, PromQL):**
```promql
quantile_over_time(0.50, exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
quantile_over_time(0.95, exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
quantile_over_time(0.99, exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
```

**Approach:** `ddsketch` / `kll` in tumbling window mode, grouped by entity.

**Validation:** exact quantiles from raw vs sketch quantiles.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | `exathlon/data/raw` |
| Window | 5-min (primary), plus 1/15/30/60-min sensitivity |
| Avg rows/window (per file) | ~298.5 |
| Cardinality pressure | 2283 series/file, 3939 global |
| Test types | **Sketch-telemetry** (KLL/DDSketch) + **Latency** |

**References:**

- [Jacob et al. — *Exathlon: A Benchmark for Explainable Anomaly Detection over Time Series*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — source dataset; Spark + HPC telemetry at ~1 Hz justifies 5-min windowed quantile analysis
- [Masson, Rim, Lee — *DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees*, PVLDB 2019](https://arxiv.org/abs/1908.10693)
- [Karnin, Lang, Liberty — *Optimal Quantile Approximation in Streams*, FOCS 2016](https://arxiv.org/abs/1603.05346) — KLL sketch theoretical basis
- [Beyer, Jones, Petoff, Murphy — *Site Reliability Engineering*, O'Reilly 2016, ch. 4 "Service Level Objectives"](https://sre.google/sre-book/service-level-objectives/) — industry motivation for percentile-based SLOs

---

## Q2 - Tail amplification ratio

**Purpose:** Track instability/spikiness using robust tail ratio.

**Use case domain:** SRE reliability monitoring — the p99/p50 ratio exposes bursty, non-steady-state behavior in cluster jobs (e.g., GC pauses, shuffle spills) that mean alone misses.

**Formula:** $\mathrm{tail\_ratio}(m,w)=Q_{0.99}(m,w)/Q_{0.50}(m,w)$ (or $Q_{0.95}/Q_{0.50}$).

**Formula input path (controller, PromQL):**
```promql
quantile_over_time(0.99, exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
/
quantile_over_time(0.50, exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
```

**Approach:** derive from sketch quantiles (Q1 outputs).

**Validation:** compare ratio from sketch vs exact.

**Evaluation configuration:** same as Q1.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — Spark app telemetry exhibits strong tail amplification under injected faults (resource contention, process failures)
- [Masson, Rim, Lee — *DDSketch*, PVLDB 2019](https://arxiv.org/abs/1908.10693) — relative-error guarantees make DDSketch accurate for tail quantiles
- [Schwartz, Wilkes — *Practical Monitoring*, O'Reilly 2018, ch. 6](https://www.oreilly.com/library/view/practical-monitoring/9781491957349/) — tail ratio as a spikiness/instability signal in operational monitoring

---

## Q3 - Top-K heavy metrics by threshold exceedance count

**Purpose:** Identify most problematic metrics (frequent breaches) per window.

**Use case domain:** Incident triage in cluster observability — ranking which metrics breach thresholds most often per window guides operator attention and automated remediation in large-scale Spark / HPC deployments.

**Formula:** $\mathrm{score}(m,w)=\sum_{t\in w}\mathbf{1}[x_t>\tau]$; return $\mathrm{TopK}_m(\mathrm{score})$.

**Formula input path (controller, PromQL):**
```promql
topk(10, count_over_time(exathlon_metric{entity!="",metric_base!=""}[5m])) by (entity, metric_base)
```

**Approach:** `count-min + spacesaving` over exceedance events.

**Validation:** top-K overlap and rank correlation against exact counts.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | `exathlon/data/raw` |
| Window | 5-min |
| K | 10 |
| Cross-series aggregation | all active metrics in file |
| Test types | **Sketch-telemetry** (CMS+SpaceSaving) + **Throughput** |

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — 3,629 distinct metric bases per corpus create a realistic heavy-hitter workload
- [Cormode, Muthukrishnan — *An Improved Data Stream Summary: The Count-Min Sketch and its Applications*, J. Algorithms 2005](https://doi.org/10.1016/j.jalgor.2003.12.001)
- [Metwally, Agrawal, El Abbadi — *Efficient Computation of Frequent and Top-k Elements in Data Streams* (SpaceSaving), ICDT 2005](https://doi.org/10.1007/978-3-540-30570-5_27)

---

## Q4 - Windowed min/max/range for utilization metrics

**Purpose:** Characterize operational envelope (e.g., CPU busy, memory usage).

**Use case domain:** Capacity planning and resource utilization profiling — min/max/range per window reveals resource saturation ceilings and floor behavior for Spark executors and HPC nodes across job phases.

**Formula:** $\min(m,w),\ \max(m,w),\ \mathrm{range}(m,w)=\max(m,w)-\min(m,w)$.

**Formula input path (controller, PromQL):**
```promql
min_over_time(exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
max_over_time(exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
max_over_time(exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
-
min_over_time(exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
```

**Approach:** approximate via quantiles (`q0`, `q1`) or exact NOP baseline.

**Validation:** absolute/relative error for min/max/range.

**Evaluation configuration:** 1/5/15/30/60-min windows.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — CPU/memory/network metrics in the corpus are direct capacity indicators for Spark executor nodes
- [Gregg — *Systems Performance: Enterprise and the Cloud*, 2nd ed., Pearson 2020, ch. 2 "Methodologies"](https://www.brendangregg.com/systems-performance-2nd-edition-book.html) — min/max/range as first-pass utilization profiling tools

---

## Q5 - IQR-based anomaly flags

**Purpose:** Detect window-local outliers robustly.

**Use case domain:** Automated anomaly detection in telemetry streams — Tukey-fence IQR flagging provides threshold-free, distribution-agnostic outlier detection that tolerates the skewed, heavy-tailed metric distributions typical of Spark cluster telemetry.

**Formula:** $\mathrm{IQR}=Q_3-Q_1$; outlier if $x<Q_1-1.5\,\mathrm{IQR}$ or $x>Q_3+1.5\,\mathrm{IQR}$.

**Formula input path (controller, PromQL):**
```promql
quantile_over_time(0.25, exathlon_metric{entity!="",metric_base!=""}[15m]) by (entity, metric_base)
quantile_over_time(0.50, exathlon_metric{entity!="",metric_base!=""}[15m]) by (entity, metric_base)
quantile_over_time(0.75, exathlon_metric{entity!="",metric_base!=""}[15m]) by (entity, metric_base)
```

**Approach:** `ddsketch` quantiles `[0.25, 0.5, 0.75]`.

**Validation:** precision/recall/F1 against exact IQR flags.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | `exathlon/data/raw` |
| Window | 15-min primary (more stable), compare 5-min |
| Test types | **Sketch-telemetry** + **Latency** |

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — ground-truth anomaly labels enable precision/recall validation of sketch-derived IQR flags
- [Tukey — *Exploratory Data Analysis*, Addison-Wesley 1977](https://www.worldcat.org/title/exploratory-data-analysis/oclc/3058187) — original Tukey fences definition
- [Chandola, Banerjee, Kumar — *Anomaly Detection: A Survey*, ACM CSUR 2009](https://doi.org/10.1145/1541880.1541882) — statistical outlier detection methods applicable to telemetry streams

---

## Q6 - Distinct active metrics per window

**Purpose:** Estimate active-series footprint over time.

**Use case domain:** Observability cardinality management — monitoring teams track distinct active series per window to detect cardinality explosions (e.g., label churn from restarted executors) without enumerating the full series space.

**Formula:** $\left|\left\{\mathrm{metric\_id}\mid \mathrm{appears\ in}\ w\right\}\right|$.

**Formula input path (controller, SQL/SeQuAL):**
```sql
SELECT
  entity,
  COUNT(DISTINCT metric_base) AS active_metric_count
FROM exathlon_metrics
GROUP BY entity, TUMBLE(ts, INTERVAL '5' MINUTE)
```

**Approach:** `HyperLogLog` per window.

**Validation:** exact distinct count vs HLL estimate (relative error).

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | `exathlon/data/raw` |
| Window | 5-min |
| Global series space | 3939 |
| Test types | **Sketch-telemetry** (HLL) + **Throughput** |

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — 3,939 global unique time series over 93 files provides a bounded but realistic cardinality estimation target
- [Flajolet, Fusy, Gandouet, Meunier — *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm*, DMTCS 2007](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

---

## Q7 - Top-K entities by anomaly volume

**Purpose:** Rank node/executor entities by anomaly activity.

**Use case domain:** Multi-node cluster health ranking — in a 10-entity Spark/HPC deployment, identifying which executor or node generates the most anomaly events per window directs on-call response to the highest-impact component.

**Formula:** $\mathrm{score}(e,w)=\sum_{t\in w}\mathbf{1}[\mathrm{anomaly}_t(e)]$; return $\mathrm{TopK}_e(\mathrm{score})$.

**Formula input path (controller, PromQL):**
```promql
topk(5, count_over_time(anomaly_events{entity!=""}[5m])) by (entity)
```

**Approach:** produce anomaly events (Q5), aggregate with CMS+SpaceSaving.

**Validation:** top-K entity overlap/ranking vs exact.

**Evaluation configuration:** 5-min and 15-min windows.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — 10 distinct app/node entities with labelled fault injection periods; entity-level anomaly ranking is directly testable
- [Cormode, Muthukrishnan — *Count-Min Sketch*, J. Algorithms 2005](https://doi.org/10.1016/j.jalgor.2003.12.001)
- [Metwally et al. — *SpaceSaving*, ICDT 2005](https://doi.org/10.1007/978-3-540-30570-5_27)

---

## Q8 - Quantile drift between adjacent windows

**Purpose:** Detect distribution shift over time.

**Use case domain:** Streaming concept drift detection in cluster telemetry — sudden shifts in p95 between consecutive windows signal phase transitions in Spark jobs (e.g., map → shuffle → reduce) or onset of resource pressure, enabling early warning before thresholds are breached.

**Formula:** $\mathrm{drift}_{0.95}(t)=\left|Q_{0.95}(t)-Q_{0.95}(t-1)\right|$ (also for $Q_{0.50}$).

**Formula input path (controller, PromQL):**
```promql
quantile_over_time(0.95, exathlon_metric{entity!="",metric_base!=""}[5m]) by (entity, metric_base)
-
quantile_over_time(0.95, exathlon_metric{entity!="",metric_base!=""}[5m] offset 5m) by (entity, metric_base)
```

**Approach:** compute from sketch quantile outputs per consecutive windows.

**Validation:** drift error vs exact quantile drift.

**Evaluation configuration:** 5-min primary, 1-min stress.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — fault injection experiments (resource contention, process failures) produce measurable distribution shifts across consecutive windows
- [Karnin, Lang, Liberty — *KLL sketch*, FOCS 2016](https://arxiv.org/abs/1603.05346) — mergeable sketch structure enables efficient consecutive-window comparison
- [Gama et al. — *A Survey on Concept Drift Adaptation*, ACM CSUR 2014](https://doi.org/10.1145/2523813) — drift detection in streaming data; motivates window-to-window quantile comparison

---

## Q9 - Saturation ratio per entity

**Purpose:** Measure fraction of metrics crossing saturation threshold.

**Use case domain:** Resource saturation alerting under the USE method — the saturation ratio (`exceeded_metrics / total_metrics` per entity) directly implements the "S" of the USE (Utilization, Saturation, Errors) framework for Spark executor and HPC node health.

**Formula:** $\mathrm{sat\_ratio}(e,w)=\frac{\mathrm{exceeded\_metrics}(e,w)}{\mathrm{total\_metrics}(e,w)}$.

**Formula input path (controller, SQL/SeQuAL):**
```sql
SELECT
  entity,
  COUNT(*) AS exceeded_metrics
FROM metric_exceeded_events
GROUP BY entity, TUMBLE(ts, INTERVAL '5' MINUTE)
```

**Approach:** threshold events + distinct/denominator counts (HLL + exact metadata).

**Validation:** absolute error on saturation ratio.

**Evaluation configuration:** 5-min, 15-min windows.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — threshold-crossing events in Spark executor metrics (CPU, memory, GC) are directly observable in the corpus
- [Gregg — *The USE Method*](https://www.brendangregg.com/usemethod.html) — saturation as a first-class resource health signal; motivates per-entity saturation ratio
- [Flajolet et al. — *HyperLogLog*, DMTCS 2007](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) — HLL used for distinct metric counting in the numerator

---

## Q10 - EWMA change-point on selected KPI stream

**Purpose:** Sequential change detection on critical metrics.

**Use case domain:** Real-time KPI monitoring and alerting — EWMA control charts are a standard SRE tool for detecting sustained shifts in a single critical metric stream (e.g., executor CPU, heap utilization) with low latency and configurable sensitivity.

**Formula:** $\mathrm{EMA}_t=\alpha x_t+(1-\alpha)\mathrm{EMA}_{t-1}$, $\alpha=\frac{2}{n+1}$; trigger on residual/control-limit breach.

**Formula input path (controller, PromQL):**
```promql
avg_over_time(exathlon_kpi{entity!="",metric_base="cpu_busy"}[1m]) by (entity)
```

**Approach:** exact stateful stream computation (NOP/raw path), not a pure sketch query.

**Validation:** alert agreement and detection delay vs exact baseline.

**Evaluation configuration:**

| Parameter | Value |
|---|---|
| Dataset | `exathlon/data/raw` |
| Window | N/A (stateful stream) |
| Test types | **Throughput + Latency** (no sketch substitution) |

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — labelled change points in fault-injection experiments provide ground truth for detection delay measurement
- [Lucas, Saccucci — *Exponentially Weighted Moving Average Control Schemes: Properties and Enhancements*, Technometrics 1990](https://doi.org/10.1080/00401706.1990.10484583)
- [Beyer et al. — *SRE Book*, ch. 6 "Monitoring Distributed Systems"](https://sre.google/sre-book/monitoring-distributed-systems/) — EWMA as a smoothing baseline for alert suppression and change detection

---

## Q11 - Rolling cross-metric correlation per entity

**Purpose:** Capture coupling shifts (e.g., CPU vs memory vs IO).

**Use case domain:** Causal telemetry analysis and dependency detection — correlation changes between CPU, memory, and I/O metrics within a Spark executor reveal job phase transitions and resource coupling breakdowns (e.g., GC pressure decoupling CPU from network throughput).

**Formula:** $\rho_w(x,y)=\mathrm{corr}\!\left(x_{t-w:t},y_{t-w:t}\right)$ (Pearson or Spearman).

**Formula input path (controller, PromQL):**
```promql
avg_over_time(cpu_busy{entity!=""}[15m]) by (entity)
avg_over_time(mem_used{entity!=""}[15m]) by (entity)
```

**Approach:** exact rolling-state pipeline, not sketch-native in this setup.

**Validation:** correlation error and regime-change agreement.

**Evaluation configuration:** 15-min and 30-min rolling windows.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — multivariate metric streams per file enable cross-metric correlation analysis across fault and baseline regimes
- [Münz, Li, Carle — *Traffic Anomaly Detection Using K-Means Clustering*, GI/ITG Workshop MMBnet 2007](https://www.net.in.tum.de/fileadmin/TUM/NET/NET-2007-06-1/NET-2007-06-1_03.pdf) — correlation-based anomaly detection in telemetry streams

---

## Q12 - Multi-window health score

**Purpose:** Build composite health score from quantile, anomaly, and saturation signals.

**Use case domain:** Composite cluster health scoring — SRE dashboards combine tail ratio, anomaly rate, and saturation signals into a single entity-level health index that is stable across window sizes and actionable without requiring threshold tuning per metric.

**Formula:** $\mathrm{health\_score}=w_1\cdot \mathrm{tail\_ratio}+w_2\cdot \mathrm{anomaly\_rate}+w_3\cdot \mathrm{sat\_ratio}$, with $w_1+w_2+w_3=1$.

**Formula input path (controller, SQL/SeQuAL):**
```sql
SELECT
  entity,
  AVG(tail_ratio) + AVG(anomaly_rate) + AVG(sat_ratio) AS health_score
FROM exathlon_health_features
GROUP BY entity, TUMBLE(ts, INTERVAL '15' MINUTE)
```

**Approach:** combine outputs of Q2 + Q5 + Q9.

**Validation:** consistency and ranking stability across window sizes.

**Evaluation configuration:** compare 1/5/15/30/60-min.

**References:**

- [Jacob et al. — *Exathlon*, VLDB 2021](https://doi.org/10.14778/3476249.3476307) — multi-regime fault labels allow validation of health score ranking against known fault severity ordering
- [Beyer et al. — *SRE Book*, ch. 4 "Service Level Objectives"](https://sre.google/sre-book/service-level-objectives/) — composite health scoring as a multi-signal SLO aggregation pattern

---

## Mapping of Qn to sketch support

| Query | Use case domain | Sketch-support status | Main sketch/tool |
|---|---|---|---|
| Q1 | Distributed systems performance monitoring | Direct sketch | KLL / DDSketch |
| Q2 | SRE reliability monitoring | Derived from sketch outputs | KLL / DDSketch |
| Q3 | Incident triage / cluster observability | Direct sketch | Count-Min + SpaceSaving |
| Q4 | Capacity planning / utilization profiling | Sketch + exact baseline | KLL / DDSketch (+ NOP exact) |
| Q5 | Automated anomaly detection | Direct sketch | DDSketch / KLL |
| Q6 | Observability cardinality management | Direct sketch | HyperLogLog |
| Q7 | Multi-node cluster health ranking | Direct sketch (on anomaly events) | Count-Min + SpaceSaving |
| Q8 | Streaming concept drift detection | Derived from sketch outputs | KLL / DDSketch |
| Q9 | Resource saturation alerting (USE method) | Hybrid | HLL + exact denominator metadata |
| Q10 | Real-time KPI monitoring | Not sketch-native | Exact stateful stream |
| Q11 | Causal telemetry analysis | Not sketch-native | Exact rolling computation |
| Q12 | Composite cluster health scoring | Composite | Combines Q2/Q5/Q9 outputs |
