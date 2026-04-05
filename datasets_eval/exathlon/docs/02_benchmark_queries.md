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

---

## Q1 - Windowed distribution profiling (p50/p95/p99) per metric group

**Purpose:** Validate quantile sketch fidelity for system telemetry distributions.

**Use case domain:** Distributed systems performance monitoring — SRE teams track p50/p95/p99 latency and resource utilization per service/entity to define SLOs and detect degradation without storing raw samples.

**Formula:** compute quantiles `Q0.50`, `Q0.95`, `Q0.99` per `(entity, metric_base, window)`.

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

**Formula:** `tail_ratio = p99 / p50` (or `p95/p50`) per metric per window.

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

**Formula:** `score(metric, w) = count(value > threshold)`; take top-K.

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

**Formula:** `min`, `max`, `range = max - min` per metric/window.

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

**Formula:** outlier if `x < Q1 - 1.5*IQR` or `x > Q3 + 1.5*IQR`, `IQR = Q3-Q1`.

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

**Formula:** `|{metric_id : appears in window}|`.

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

**Formula:** `score(entity,w)=count(anomaly_events)`; top-K entities/window.

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

**Formula:** `drift = |p95_t - p95_{t-1}|` (also p50 drift).

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

**Formula:** `sat_ratio = exceeded_metrics / total_metrics` per entity/window.

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

**Formula:** EWMA / residual-based trigger.

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

**Formula:** rolling Pearson/Spearman correlation.

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

**Formula:** weighted score from `tail_ratio`, `anomaly_rate`, `sat_ratio`.

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
