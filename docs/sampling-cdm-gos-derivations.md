# Sampling + Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) derivations for windowed sketch telemetry

> Paper-facing derivation note. This document ties together
> [distributed-nitrosketch-coordinated-sampling.md](distributed-nitrosketch-coordinated-sampling.md),
> [continuous-monitoring-aggregation-taxonomy.md](continuous-monitoring-aggregation-taxonomy.md),
> [continuous-monitoring-tumbling-cost-analysis.md](continuous-monitoring-tumbling-cost-analysis.md),
> and [design-gos-unified-edge-telemetry.md](design-gos-unified-edge-telemetry.md).
>
> Scope: fixed tumbling-window epochs; open-window freshness inside one epoch;
> additive linear sketch states as the fully proved case; family-specific
> extensions for quantiles and cardinality sketches.
>
> Acronyms: **CDM** = **Continuous Distributed Monitoring**; **GOS** =
> **Geometric-OctoSketch**.

## Paper positioning: bottleneck -> solution map

The system bottleneck addressed by these derivations is
**resolution-coupled central ingestion**. In conventional observability
pipelines, higher temporal resolution, higher label cardinality, and lower
freshness latency all require more raw samples to traverse the central path:

```text
collector/exporter -> remote write / queue / WAL -> backend ingest/index/storage
-> query scan
```

ASAPCollector changes the unit of work from raw-sample ingestion to
query-bounded sketch-state synchronization. Edge collectors still absorb the
high-resolution stream, but the backend receives bounded sketch summaries,
error-triggered deltas, and optional cold raw fallback instead of every raw
sample on the warm path.

The problem-solution pairs are:

| Existing-system bottleneck | Why it matters | ASAPCollector mechanism | Error/control consequence |
| --- | --- | --- | --- |
| Central ingest bottleneck | Cost scales with $\mathrm{series\_cardinality} \times \mathrm{sample\_frequency}$; raising resolution pushes more samples through write queues, indexing, storage, and query scan. | Maintain mergeable sketches at edge collectors and transmit sketch state/deltas. | Backend warm-path load scales with summary size and threshold crossings, not directly with raw sample rate. |
| Freshness vs cost bottleneck | Shorter scrape/export intervals improve open-window freshness but increase CPU, network, and backend ingest pressure; longer intervals reduce cost but make queries and alerts stale. | Use CDM/GOS residual thresholds $T_j$ for error-triggered synchronization. | Freshness becomes a bounded staleness term $\mathrm{Err}_q^{cdm}$ rather than an implicit consequence of a fixed reporting interval. |
| Query scan / post-ingest downsampling bottleneck | TSDB compression and downsampling help after raw data has already been ingested, and long-range queries still depend on stored sample layout. | Build query-ready sketches before/during ingestion. | Query cost is paid against sketch summaries, while the cold raw path remains available for unsupported or forensic queries. |
| Fixed-statistics / early-binding bottleneck | Histograms, summaries, and pre-aggregations commit early to bucket layouts, quantiles, windows, or rollups; changing the query later may be impossible or inaccurate. | Use a sketch-family adapter per query class: Sum/CMS/CountSketch for additive readouts, DDSketch/KLL for rank queries, HLL for cardinality. | The controller applies the correct sampling and staleness model for each sketch family instead of using one proof for all summaries. |
| Control-plane bottleneck | Existing cost controls such as dropping labels, filtering metrics, increasing intervals, or coarse downsampling often do not expose a query-level error budget. | Jointly tune sketch size, sampling probabilities $p_i$, and GOS thresholds $T_j$. | The system can allocate a query error budget across $\mathrm{Err}_q^{sk} + \mathrm{Err}_q^{sa} + \mathrm{Err}_q^{cdm}$. |
| Theory-to-system bottleneck | Prior sketching, approximate query processing, and CDM work each solve part of the problem, but not the end-to-end telemetry ingestion control loop. | Combine update sampling, mergeable sketches, and CDM/GOS synchronization inside the collector/backend architecture. | The paper claim is a system-level accuracy envelope, not only a faster sketch update or a lower communication protocol in isolation. |

Compared with existing work, the paper's positioning is:

| Existing work / system family | Solves | Leaves open | ASAPCollector angle |
| --- | --- | --- | --- |
| Prometheus / OpenTelemetry / remote-write pipelines | Standard metric collection, export, and backend ingestion. | Higher resolution and cardinality still increase central ingest work. | Move high-resolution absorption to edge sketches and synchronize bounded state. |
| Native histograms / exponential histograms / DDSketch-style distribution metrics | Compact, mergeable distribution summaries. | Mainly distribution-specific; does not give a general control plane for sampling plus staleness across sketch families. | Treat DDSketch as one adapter in a broader sketch taxonomy. |
| TSDB compression / Thanos-style downsampling | Long-range query acceleration and storage-layout optimization. | Mostly after-ingest optimization; raw samples still enter the central path first. | Summarize before or during ingestion, then query the warm sketch path. |
| BlinkDB / VerdictDB-style AQP | Approximate analytics over stored data with statistical error. | Data is already in the warehouse; not a continuous telemetry freshness and ingestion-control problem. | Provide telemetry-native edge ingestion, open-window freshness, and sketch-state synchronization. |
| NitroSketch | Sampling sketch updates to reduce sketch CPU. | Focuses on update work for sketches, not multi-sketch observability queries, backend freshness, or GOS/CDM synchronization. | Reuse the sampling idea but compose it with query-level variance budgets and delta suppression. |
| Continuous Distributed Monitoring / distributed functional monitoring | Communication-efficient tracking of distributed functions. | Mostly a theoretical monitoring model, not an observability pipeline with sketch-family adapters and cold fallback. | Use CDM as the proof model for bounded sketch residual synchronization. |
| Learned reconstruction systems such as Zoom2Net | Infer fine-grained telemetry from coarse measurements. | Error semantics depend on reconstruction/model behavior rather than preserving query-sufficient sketch state. | Maintain sketch statistics with explicit sketch, sampling, and staleness error terms. |

The intended claim boundary is narrow:

- ASAPCollector does not replace raw telemetry for every task; it provides a
  warm approximate path plus cold raw fallback.
- ASAPCollector does not support arbitrary PromQL under one theorem; it supports
  sketchable/decomposable query classes with family-specific adapters.
- The fully proved theorem target is fixed-query, fixed-time, additive-state
  telemetry with independent Horvitz-Thompson update sampling and an
  update-synchronous or explicitly overshoot-bounded residual protocol.
- The core claim is that the collector can reduce update work and network
  traffic while keeping a query-level accuracy envelope.

## 1. Scope and aggregation modes

Continuous monitoring in ASAP is not one protocol. The aggregation axis decides
which proof model applies.

| Mode | Query shape | Protocol | Coordinator? | Main guarantee |
| --- | --- | --- | --- | --- |
| A | per-series $\times$ window | local $\epsilon$-gated delta emission | no | open-window freshness for one local series/group |
| B | series $\times$ timestamp | mergeable sketch fan-in | no | sealed/instant merge; no monitoring suppression |
| C | series $\times$ window, fused spatial+temporal | Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) over edge collectors | yes | global open-window tracking/alerting across $k$ sites |

This note proves the composition used by modes A and C for the additive-state
case:

1. sketch approximation,
2. update sampling,
3. stale backend state due to suppressed deltas.

For sealed tumbling windows, only the first two sources matter. The merge of
mergeable summaries does not compound the base sketch error.

## 2. Notation

One fixed tumbling window is the universe of discourse.

| Symbol | Meaning |
| --- | --- |
| $k$ | number of stable edge collectors/sites |
| $i$ | site index, $i \in \{1,\ldots,k\}$ |
| $u$ | update/sample index |
| $x_u$ | raw item/value |
| $f_i$ | local frequency vector at site $i$ |
| $f=\sum_i f_i$ | global frequency vector |
| $S_i$ | exact local sketch state for site $i$ |
| $S=\sum_i S_i$ | exact global additive sketch state |
| $\widehat S_i$ | sampled local sketch state |
| $\widehat S=\sum_i \widehat S_i$ | sampled global sketch state |
| $\widetilde S$ | backend's stale copy of $\widehat S$ |
| $j$ | sketch cell/bucket index |
| $a_j$ | linear readout coefficient for query $q(S)=\langle a,S\rangle$ |
| $p_i$ | sampling probability at site $i$ |
| $T_j$ | cell/bucket delta threshold |
| $V_j$ | cell/bucket activity rate or expected change mass |
| $B$ | staleness budget for a query/function |
| $\epsilon_{sk}$ | base sketch error budget |
| $\epsilon_{sa}$ | sampling error budget |
| $\epsilon_{cdm}$ | Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) staleness budget |
| $\delta$ | failure probability |

For additive sketches, an update $u$ touches one or more sketch cells. Let
$j(u)$ be a touched cell in a one-cell-per-update sketch such as a DDSketch
bucket, or let $j=(r,c)$ be a row/cell pair for CMS/CountSketch.

## 3. Generic error decomposition

For a fixed query $q$ at a fixed time inside a window:

```text
true answer        q(f)
exact sketch       q(S)
sampled sketch     q(\widehat S)
backend answer     q(\widetilde S)
```

The triangle inequality gives

```math
|q(\widetilde S)-q(f)|
\le
|q(S)-q(f)|
+ |q(\widehat S)-q(S)|
+ |q(\widetilde S)-q(\widehat S)|.
```

We name the three terms:

```math
\mathrm{Err}_{q}
\le
\mathrm{Err}^{sk}_{q}
+ \mathrm{Err}^{sa}_{q}
+ \mathrm{Err}^{cdm}_{q}.
```

For a sealed tumbling window, $\widetilde S=\widehat S$ after boundary emission,
so

```math
\mathrm{Err}_{q,sealed}
\le
\mathrm{Err}^{sk}_{q}
+ \mathrm{Err}^{sa}_{q}.
```

For an open window, Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch
(GOS) controls the third term.

This statement is pointwise: one fixed query and one fixed time. To claim a
finite workload $Q$ and $M$ possible query times, replace $\delta$ by
$\delta/(|Q|M)$ in each concentration bound and union bound.

## 4. Additive-state sampling model

### 4.1 Horvitz-Thompson update sampling

For an update $u$, site $i(u)$ admits it with probability $p_{i(u)}$:

```math
Z_u \sim \mathrm{Bernoulli}(p_{i(u)}).
```

Here $Z_u$ is the admission indicator:

```math
Z_u =
\begin{cases}
1, & \text{if update }u\text{ is admitted and applied to the sketch},\\
0, & \text{if update }u\text{ is skipped}.
\end{cases}
```

The inverse-probability weighted contribution of update $u$ is

```math
Y_u = Z_u/p_{i(u)}.
```

Equivalently,

```math
Y_u =
\begin{cases}
1/p_{i(u)}, & \text{if update }u\text{ is admitted},\\
0, & \text{if update }u\text{ is skipped}.
\end{cases}
```

Thus $Y_u$ is the effective update weight written into an additive sketch
counter/cell. Without sampling, the update would contribute weight $1$; with
sampling, it contributes weight $1/p_{i(u)}$ only on admitted updates.

Then

```math
\mathbb{E}[Y_u]=1,
\qquad
\mathrm{Var}(Y_u)=\frac{1-p_{i(u)}}{p_{i(u)}}.
```

For a linear readout $q(S)=\langle a,S\rangle$, define $g_u$ as the contribution of update
$u$ to this readout if it were not sampled. For a one-bucket counter,
$g_u=a_{j(u)}$. For signed sketches, $g_u$ also includes the row sign.

The sampled readout error is

```math
X_q
=q(\widehat S)-q(S)
=\sum_u (Y_u-1)g_u.
```

It is unbiased:

```math
\mathbb{E}[X_q]=0.
```

The variance follows from the variance of one update contribution. First,

```math
\mathbb{E}[Y_u^2]
=
p_{i(u)}\cdot \frac{1}{p_{i(u)}^2}
=
\frac{1}{p_{i(u)}}.
```

Since $\mathbb{E}[Y_u]=1$,

```math
\mathrm{Var}(Y_u)
=
\mathbb{E}[Y_u^2]-\mathbb{E}[Y_u]^2
=
\frac{1}{p_{i(u)}}-1
=
\frac{1-p_{i(u)}}{p_{i(u)}}.
```

Subtracting a constant does not change variance, so

```math
\mathrm{Var}(Y_u-1)=\mathrm{Var}(Y_u).
```

Multiplying by the deterministic readout contribution $g_u$ scales variance by
$g_u^2$:

```math
\mathrm{Var}((Y_u-1)g_u)
=
g_u^2\mathrm{Var}(Y_u)
=
g_u^2\frac{1-p_{i(u)}}{p_{i(u)}}.
```

If the sampling randomness is independent across updates, i.e. the admission
indicators $Z_u$ are independent,

```math
\mathrm{Var}(X_q)
=
\sum_u g_u^2 \frac{1-p_{i(u)}}{p_{i(u)}}.
```

This assumption is only about the sampling coin flips, not about the input data
streams. The metric values or workloads at different sites may be correlated.
The variance identity needs the random admission decisions to be independent, or
at least uncorrelated. The high-probability Bernstein bound in Section 4.2 uses
the stronger independent-sampling assumption.

This equation is the sampling-error budget for query $q$. The random variable
$X_q=q(\widehat S)-q(S)$ is the difference between answering $q$ from the sampled
sketch and answering $q$ from the same sketch without update sampling. Each term
in the sum says how much update $u$ contributes to that sampling error:

- $g_u^2$ is the squared sensitivity of query $q$ to update $u$;
- $(1-p_{i(u)})/p_{i(u)}$ is the noise introduced by sampling at the site that
  produced update $u$.

Thus lowering $p_i$ saves update work at site $i$, but increases the variance of
queries whose relevant updates come from that site. This is the quantity the
controller constrains when it chooses sampling probabilities.

The independence assumption is what removes covariance terms:

```math
\mathrm{Var}\left(\sum_u A_u\right)
=
\sum_u \mathrm{Var}(A_u)
+2\sum_{u<v}\mathrm{Cov}(A_u,A_v),
\qquad
A_u=(Y_u-1)g_u.
```

If the admission indicators are independent, then the $A_u$ terms are
independent, so the covariance terms are zero:

```math
\mathrm{Cov}(A_u,A_v)=0
\qquad
(u\ne v).
```

Therefore,

```math
\mathrm{Var}\left(\sum_u (Y_u-1)g_u\right)
=
\sum_u \mathrm{Var}((Y_u-1)g_u)
```

when the admission indicators are independent. Intuitively, independent
sampling decisions mean the per-update sampling errors do not move together, so
their variances add. If multiple sites reuse the same sampler seed and their
admissions become correlated, the covariance terms may be nonzero and this
simplification no longer holds.

Grouped by site:

```math
\mathrm{Var}(X_q)
=
\sum_i \frac{1-p_i}{p_i}
\sum_{u\in i} g_u^2.
```

This is only algebraic regrouping: for all updates from site $i$, the sampling
probability is the same $p_i$, so $(1-p_i)/p_i$ is constant and can be pulled
outside the inner sum.

This is the quantity the controller must budget. A per-site floor on $p_i$ is
not sufficient by itself; the variance sum must be bounded.

### 4.2 Concentration

Section 4.1 gives a variance:

```math
\mathrm{Var}(X_q)=\sigma_q^2.
```

Variance describes the average scale of the sampling fluctuation, but a theorem
or alert-safety claim needs a high-probability statement:

```text
with probability at least 1-delta, the sampling error is at most ...
```

This section converts the variance budget into such a bound. The random
variable $X_q=q(\widehat S)-q(S)$ is the sampling error of query $q$. The
quantity $\sigma_q^2$ is the variance derived above. The constant $G$ bounds the
largest possible contribution of one raw update to the query readout:

```math
|g_u|\le G.
```

The value

```math
p_{\min}=\min_i p_i
```

is needed because an admitted update from site $i$ is weighted by $1/p_i$; a very
small sampling probability can make one admitted update large.

For unit updates, bounded $|g_u| \le G$, and independent admission indicators,
Bernstein's inequality gives, with probability at least $1-\delta$ (derived in
Appendix A),

```math
|X_q|
\le
\sqrt{2\sigma_q^2\log(2/\delta)}
+ \frac{2G}{3p_{\min}}\log(2/\delta),
```

where

```math
\sigma_q^2
=
\sum_u g_u^2 \frac{1-p_{i(u)}}{p_{i(u)}},
\qquad
p_{\min}=\min_i p_i.
```

The first term,

```math
\sqrt{2\sigma_q^2\log(2/\delta)},
```

is the variance-driven part of the bound. The second term,

```math
\frac{2G}{3p_{\min}}\log(2/\delta),
```

is the worst-case single-update correction: inverse-probability weighting can
amplify an admitted update by as much as $1/p_{\min}$.

For a uniform sampling probability $p$ and a point-frequency count $f(x)=N(x)$,
the usual relative sampling term is

```math
\epsilon_{sa}(x,\delta)
=
O\left(
\sqrt{\frac{(1-p)\log(1/\delta)}{p\,N(x)}}
+\frac{\log(1/\delta)}{p\,N(x)}
\right).
```

The one-standard-deviation shorthand is

```math
\epsilon_{sa}(x)
=
\sqrt{\frac{1-p}{p\,N(x)}}.
```

Use the high-probability form in theorem statements and alert safety claims.

## 5. Edge admission vs NitroSketch row sampling

NitroSketch does not advocate naive packet-level sampling as the main design.
Its key observation is that packet-level sampling can give poor sketch accuracy:
when a packet is dropped before the sketch, all rows/counter arrays miss the same
packet, so the sampling noise is correlated across rows and median/min
amplification cannot remove that shared error. NitroSketch instead samples
sketch counter-array updates and applies inverse-probability weights to the
arrays that are updated. This preserves the expected counter value while keeping
row-level sampling noise closer to the sketch's native amplification model.

Therefore, the allocation below should not be cited as a NitroSketch theorem. It
is an ASAPCollector control-plane extension inspired by NitroSketch's
inverse-probability sketch-update sampling. There are two possible deployment
levels:

- **Nitro-style row/counter-array sampling:** site $i$, row $r$ uses probability
  $p_{i,r}$. This is closest to the NitroSketch paper and is the preferred
  target for CountSketch/CMS point queries because row errors can still be
  amplified across rows.
- **Edge admission sampling:** site $i$ admits an entire raw update with
  probability $p_i$. This is simpler and works cleanly for scalar sums/counts and
  additive bucket counts, but for multi-row sketches it can inherit the
  packet-level sampling weakness NitroSketch warns about.

The derivation in this section is for the second case: edge-level
Horvitz-Thompson admission. It is useful as a baseline controller and for
single-readout additive summaries, but it should not be presented as the
NitroSketch algorithm.

For a known point query/key $x$, the coordinator may choose different $p_i$
across sites. Let $f_i=f_i(x)$ be the key mass or protected query sensitivity at
site $i$, and let $r_i$ be the site's raw update rate or sketch-update cost
weight. Under edge admission sampling, the expected update work at site $i$ is
proportional to $r_i p_i$.

The query-specific CPU minimization problem is

```math
\begin{aligned}
\min_{0<p_i\le 1}\quad & \sum_i r_i p_i \\
\text{s.t.}\quad & \sum_i f_i\frac{1-p_i}{p_i} \le V .
\end{aligned}
```

Ignoring clamps, the Lagrangian is

```math
\mathcal{L}
=
\sum_i r_i p_i
+\lambda\left(\sum_i f_i\frac{1-p_i}{p_i}-V\right).
```

Since $(1-p_i)/p_i = 1/p_i - 1$,

```math
\frac{\partial \mathcal{L}}{\partial p_i}
=
r_i-\lambda f_i/p_i^2.
```

The interior optimum satisfies

```math
p_i
=
\sqrt{\lambda}\sqrt{\frac{f_i}{r_i}}.
```

The scalar $\sqrt{\lambda}$ is selected so that the variance constraint binds,
and the result is clamped to $(0,1]$.

For Nitro-style row sampling, the analogous controller would optimize
probabilities $p_{i,r}$ over site/row pairs, with a cost term such as
$\sum_{i,r} r_i p_{i,r}$ and row-level variance constraints. That is the more
faithful extension for CountSketch/CMS. The row-level version is not derived in
this note.

For whole-sketch or unknown-key deployments, replace the query-specific
$f_i(x)$ by the protected mass for the query class. A conservative whole-sketch
floor commonly used in deployments is derived from

```math
\frac{1-p_i}{p_i R_i}\le \epsilon_{sa}^2,
```

where $R_i$ is an observable per-site rate/mass. This gives

```math
p_i \ge \frac{1}{1+\epsilon_{sa}^2 R_i}.
```

This floor protects aggregate sketch quality without pretending to optimize for
one known key.

## 6. Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) staleness model

### 6.1 Residual invariant

Let $A_i[j]$ be the last state for site $i$, cell $j$, that the backend has
acknowledged. The unshipped residual is

```math
\rho_i[j](t)=\widehat S_i[j](t)-A_i[j](t).
```

If the edge protocol guarantees

```math
|\rho_i[j](t)|\le T_j
```

for every site and cell, then

```math
|\widehat S[j](t)-\widetilde S[j](t)|
=
\left|\sum_i \rho_i[j](t)\right|
\le
\sum_i |\rho_i[j](t)|
\le
kT_j.
```

For a linear readout $q(S)=\langle a,S\rangle$:

```math
\mathrm{Err}^{cdm}_q
=
|q(\widetilde S)-q(\widehat S)|
\le
k\sum_j |a_j|T_j.
```

If the implementation checks thresholds only every $\Delta_{check}$ seconds, not
after every update, include an overshoot term. If site $i$, cell $j$ can change
at rate at most $U_{ij}$ between checks, then

```math
|\rho_i[j](t)|\le T_j+U_{ij}\Delta_{check},
```

and

```math
\mathrm{Err}^{cdm}_q
\le
\sum_j |a_j|
\sum_i (T_j+U_{ij}\Delta_{check}).
```

Use the no-overshoot form only for an update-synchronous threshold check.

### 6.2 Sampling / Continuous Distributed Monitoring (CDM) coupling

Sampling perturbs the value tested by the CDM threshold. If the sampling noise
is larger than the CDM band, the value can jitter across the threshold and cause
spurious emits or delayed alerts. A practical coupling rule is

```math
\epsilon_{sa}(\delta) \le \epsilon_{cdm}.
```

Equivalently, for per-cell thresholds, avoid transmitting finer than the
sampling noise scale. If $N_j(\Delta)$ is the expected true cell mass over the
control horizon $\Delta$, a heuristic floor is

```math
T_j^{floor}
\asymp
\sqrt{N_j(\Delta)(1-p_j)/p_j}.
```

This is not a theorem-level threshold by itself. The exact constant and the
choice of $N_j(\Delta)$ depend on the concentration bound, the failure
probability allocation, and whether the controller protects one query, a query
class, or a whole sketch.

## 7. Geometric-OctoSketch (GOS) threshold water-filling

For additive sketch cells, use the threshold-crossing cost model in which the
communication/upload rate of cell $j$ is proportional to $V_j/T_j$: higher
activity or smaller thresholds cause more emits. This is a modeling assumption,
not a protocol invariant; bursty cells, signed oscillations, batching, ACK delay,
and retries can change the constant or require an overshoot term.

Given this model and a staleness budget $B$, solve

```math
\begin{aligned}
\min_{T_j>0}\quad & \sum_j \frac{V_j}{T_j} \\
\text{s.t.}\quad & \sum_j c_jT_j \le B ,
\end{aligned}
```

where

```math
c_j = k|a_j|
```

for a linear query. For a differentiable monitored functional, use

```math
c_j = k|g_j|,
\qquad
g_j=\partial F/\partial S[j],
```

plus a separate curvature budget if the functional is nonlinear.

The Lagrangian is

```math
\mathcal{L}
=
\sum_j \frac{V_j}{T_j}
+\lambda\left(\sum_j c_jT_j-B\right).
```

The first-order condition is

```math
\frac{\partial \mathcal{L}}{\partial T_j}
=
-\frac{V_j}{T_j^2}+\lambda c_j=0.
```

Thus

```math
T_j=\sqrt{\frac{V_j}{\lambda c_j}}.
```

Solving for $\lambda$ from the active budget constraint:

```math
\sum_j c_j\sqrt{\frac{V_j}{\lambda c_j}}
=B,
```

so

```math
\frac{1}{\sqrt{\lambda}}
=
\frac{B}{\sum_\ell \sqrt{c_\ell V_\ell}}.
```

The closed form is

```math
\boxed{
T_j
=
\frac{B\sqrt{V_j/c_j}}
{\sum_\ell \sqrt{c_\ell V_\ell}}
}.
```

With floors and caps:

```math
T_j
=
\mathrm{clamp}\left(
\frac{B\sqrt{V_j/c_j}}
{\sum_\ell \sqrt{c_\ell V_\ell}},
\;
T_j^{floor},
\;
\min(T_j^{query}, V_j\Delta^*)
\right).
```

Clamped cells consume or release budget; recompute the water-filling expression
over the remaining free cells until no cell changes clamp status.

## 8. Sketch-family instantiations

### 8.1 Sum / Count

State:

```math
S_i=\sum_{u\in i} x_u
```

or $S_i=N_i$ for count.

Base sketch error:

```math
\mathrm{Err}^{sk}=0
```

up to floating-point arithmetic.

Sampling error for unit counts under uniform $p$:

```math
|\widehat N-N|
\le
O\left(
\sqrt{\frac{N\log(1/\delta)}{p}}
+\frac{\log(1/\delta)}{p}
\right).
```

CDM staleness with scalar threshold $T$:

```math
\mathrm{Err}^{cdm}\le kT.
```

For a threshold alert $N>\tau$, deterministic no-missed-crossing holds only in
the unsampled monotone setting. With sampling, use a high-probability margin.

### 8.2 Count-Min Sketch point query

State: $d \times w$ nonnegative counter matrix.

Point query for key $x$:

```math
\widehat f_{CMS}(x)
=
\min_r S[r,h_r(x)].
```

Base sketch error:

```math
f(x)
\le
\widehat f_{CMS}(x)
\le
f(x)+\epsilon_{sk}N
```

with probability at least $1-\delta_{sk}$, for the standard choice of width/depth.

Sampling error applies to every row counter. For row $r$, define

```math
X_r(x)
=
\widehat S[r,h_r(x)]-S[r,h_r(x)].
```

Then $X_r(x)$ is unbiased with variance controlled by the updates landing in
that cell:

```math
\mathrm{Var}(X_r(x))
=
\sum_{u:h_r(x_u)=h_r(x)}
\frac{1-p_{i(u)}}{p_{i(u)}}.
```

A conservative high-probability point statement can be made by union-bounding
the row-level sampling events with the usual CMS collision event:

```math
\widetilde f_{CMS}(x)
\le
f(x)
+\epsilon_{sk}N
+m_{sa}(x,\delta)
+k\max_r T_{r,h_r(x)}.
```

Because CMS uses a $\min$, the usual no-underestimate property is not preserved
by two-sided sampling noise. This makes CMS a caveated adapter rather than the
clean first theorem target. For alerting, fire with a confidence margin:

```math
\widetilde f_{CMS}(x)+m_{sa}(x,\delta)+m_{cdm}(x)
\ge
(1-\epsilon)\tau.
```

CMS is therefore useful for upper-bound-oriented point queries, but sampled CMS
alerts should be presented as probabilistic and margin-based. CountSketch, Sum,
and DDSketch range counts are cleaner first theorem targets for sampled GOS.

### 8.3 CountSketch point query

State: $d \times w$ signed counter matrix.

Row estimate:

```math
E_r(x)=s_r(x)S[r,h_r(x)].
```

Point estimate:

```math
\widehat f_{CS}(x)=\mathrm{median}_{r=1}^d E_r(x).
```

Base sketch error:

```math
|\widehat f_{CS}(x)-f(x)|
\le
\epsilon_{sk}\|f\|_2
```

with probability at least $1-\delta_{sk}$.

Sampling error is two-sided and unbiased at each row. Let

```math
X_r(x)
=
s_r(x)(\widehat S[r,h_r(x)]-S[r,h_r(x)]).
```

Then

```math
\mathbb{E}[X_r(x)]=0,
```

and its variance is the sum of inverse-probability variances of the sampled
updates colliding into the queried cell. A median-of-rows bound follows by
combining row-level concentration with the usual CountSketch row amplification.
This row amplification statement assumes Nitro-style row/counter-array sampling
or otherwise independent row-level sampling noise. If the implementation uses
edge admission sampling, the true-key sampling noise is shared across rows; then
the sampling term must be bounded before the median step and should not be
credited with CountSketch row amplification.

CDM staleness for row $r$ is

```math
|s_r(x)(\widetilde S-\widehat S)[r,h_r(x)]|
\le
kT_{r,h_r(x)}.
```

Since the median is 1-Lipschitz in the $L_\infty$ perturbation across rows:

```math
|\mathrm{median}_r(a_r+e_r)-\mathrm{median}_r(a_r)|
\le
\max_r |e_r|,
```

the point-query staleness satisfies

```math
\mathrm{Err}^{cdm}_{CS}(x)
\le
k\max_r T_{r,h_r(x)}.
```

Thus a paper-safe fixed-time statement is

```math
|\widetilde f_{CS}(x)-f(x)|
\le
\epsilon_{sk}\|f\|_2
+m_{sa}(x,\delta)
+k\max_r T_{r,h_r(x)}.
```

CountSketch is the cleanest first target for sampled GOS: additive state,
two-sided estimator, and no CMS one-sided caveat.

### 8.4 DDSketch value-range counts

DDSketch maps a positive value $x$ to a logarithmic bucket $b(x)$. The bucket
mapping is chosen so values in one bucket have relative value error at most
$\alpha$.

For a value range $[L,U]$, define a linear bucket readout

```math
q_{[L,U]}(S)
=
\sum_{b: v_b\in[L,U]} S[b],
```

where $v_b$ is the representative value of bucket $b$.

This is an additive nonnegative aggregate. Sampling and CDM compose exactly as
in the generic additive-state theorem:

```math
|q_{[L,U]}(\widetilde S)-q_{[L,U]}(S)|
\le
m_{sa}([L,U],\delta)
+k\sum_{b:v_b\in[L,U]}T_b.
```

This is the DDSketch query class that behaves like a scalar/bucket-count monitor.

### 8.5 DDSketch quantiles

DDSketch quantile queries are not linear readouts. They depend on prefix counts.
Sampling and CDM should therefore be expressed as rank error.

Let the true bucket count be

```math
n_b=\sum_{u=1}^N 1\{b(x_u)=b\}.
```

Let

```math
N_{\le b}=\sum_{j\le b}n_j.
```

#### Implemented thinning view

The current DDSketch implementation performs value-independent uniform admission
before the bucket update: if the item is not admitted, the entire update is
skipped. This subsection assumes a single uniform probability $p$ for all items.
Let

```math
Z_u\sim\mathrm{Bernoulli}(p).
```

The sampled bucket count is

```math
n_b^{sample}
=
\sum_{u:b(x_u)=b}Z_u.
```

Quantile lookup over the sampled DDSketch uses the sampled total

```math
M=\sum_u Z_u.
```

Uniform thinning preserves the distribution in expectation. With high
probability, the empirical CDF of the sampled stream is close to the true CDF.
For a finite set of $B$ nonempty buckets, a union bound over bucket prefixes
gives

```math
\sup_b
\left|
\frac{N^{sample}_{\le b}}{M}
-
\frac{N_{\le b}}{N}
\right|
\le
\epsilon_{sa}
```

with

```math
\epsilon_{sa}
=
O\left(
\sqrt{\frac{\log(B/\delta)}{pN}}
+\frac{\log(B/\delta)}{pN}
\right),
```

assuming $pN$ is not too small.

#### Horvitz-Thompson analysis view

Equivalently, for analysis one may assign every admitted item weight $1/p$:

```math
Y_u=Z_u/p.
```

Then

```math
\widehat n_b=\sum_{u:b(x_u)=b}Y_u
```

is an unbiased estimator of $n_b$, and

```math
\mathrm{Var}(\widehat N_{\le b})
=
N_{\le b}\frac{1-p}{p}.
```

Bernstein plus a union bound over $B$ prefixes yields

```math
\sup_b
|\widehat N_{\le b}-N_{\le b}|
\le
O\left(
\sqrt{\frac{N\log(B/\delta)}{p}}
+\frac{\log(B/\delta)}{p}
\right).
```

Dividing by $N$ gives the same rank scale:

```math
\epsilon_{sa}
=
O\left(
\sqrt{\frac{\log(B/\delta)}{pN}}
+\frac{\log(B/\delta)}{pN}
\right).
```

This weighted view is useful for proof. The implemented unweighted sampled
quantile returns the same bucket as the uniformly weighted view, because every
admitted item has the same weight. Counts, however, need `sample_p` rescaling if
they are queried as counts.

If the controller uses nonuniform per-site probabilities $p_i$, the unweighted
sampled DDSketch quantile is biased toward high-$p_i$ sites. A nonuniform
deployment needs weighted quantile semantics, a resampling correction, or a
separate proof for the chosen estimator.

#### CDM rank staleness

If bucket residuals satisfy $|\rho_i[b]|\le T_b$, then for any prefix:

```math
|\widetilde N_{\le b}-\widehat N_{\le b}|
\le
k\sum_{j\le b}T_j.
```

Hence the CDM-induced rank error is

```math
\epsilon_{cdm}
=
\frac{k}{N}
\sup_b\sum_{j\le b}T_j.
```

If the backend's total count is also stale, allocate a small additional budget
for denominator error, or normalize by a lower bound on $N$.

#### DDSketch quantile corollary

Let $\widetilde x_q$ be the sampled and stale DDSketch estimate of the
$q$-quantile. With probability at least $1-\delta$,

```math
\boxed{
(1-\alpha)x_{q-\epsilon_{sa}-\epsilon_{cdm}}
\le
\widetilde x_q
\le
(1+\alpha)x_{q+\epsilon_{sa}+\epsilon_{cdm}}
}.
```

Thus DDSketch keeps its multiplicative value error $\alpha$, while sampling and
CDM/GOS add rank error.

### 8.6 KLL quantiles

KLL is mergeable, but it is not a subtractive additive counter array. It should
not be forced into the per-cell linear residual theorem.

For a sealed window, mergeability preserves the KLL rank guarantee:

```math
|\mathrm{rank}(\widehat x_q)-qN|
\le
\epsilon_{KLL}N
```

with the configured KLL failure probability.

If updates are uniformly sampled before KLL insertion, the same thinning rank
term appears:

```math
\epsilon_{sa}
=
O\left(
\sqrt{\frac{\log(1/\delta)}{pN}}
+\frac{\log(1/\delta)}{pN}
\right).
```

For open-window freshness, use a segment model: unshipped KLL segments contain
$R$ samples. Then the stale-rank contribution is bounded by

```math
\epsilon_{cdm}
\le
R/N.
```

The combined quantile rank error is

```math
\epsilon_{rank}
\le
\epsilon_{KLL}
+\epsilon_{sa}
+R/N.
```

KLL is therefore a mergeable-summary adapter, not a GOS per-cell water-filling
instance.

### 8.7 HyperLogLog distinct count

HLL state is a vector of max registers, with merge defined by register-wise max.
It is mergeable but non-additive.

Base error:

```math
\epsilon_{HLL}\approx 1.04/\sqrt{m}
```

for $m$ registers.

Nitro-style inverse-probability update sampling is not appropriate for HLL
register updates: the update is a max operation, not an additive counter
increment. Hash-threshold distinct sampling can be analyzed separately, but it
does not fit the additive sampling theorem above.

For CDM/freshness, use a family-specific register-change adapter. The linear
per-cell bound $k\sum_j |a_j|T_j$ does not apply directly.

## 9. Nonlinear monitored functionals

For differentiable functionals $F(S)$, write the stale perturbation as

```math
e=\widetilde S-\widehat S.
```

At reference state $S_0$,

```math
F(S_0+e)-F(S_0)
=
\langle \nabla F(S_0),e\rangle
+R_2(e).
```

If the Hessian spectral norm is bounded by $\lambda$, then

```math
|R_2(e)|\le \frac{1}{2}\lambda\|e\|_2^2.
```

The first-order term can be controlled by GOS water-filling with

```math
c_j=k|\nabla_jF(S_0)|.
```

The curvature term needs a separate budget. Do not spend the whole error budget
on the first-order term unless $\lambda=0$.

### F2 example

For

```math
F(S)=\|S\|_2^2,
```

we have

```math
F(S+e)-F(S)
=
2\langle S,e\rangle+\|e\|_2^2.
```

Therefore

```math
|F(S+e)-F(S)|
\le
2\|S\|_2\|e\|_2+\|e\|_2^2.
```

To guarantee relative error $\epsilon$:

```math
2\|S\|_2\|e\|_2+\|e\|_2^2
\le
\epsilon\|S\|_2^2.
```

Let $\alpha_e=\|e\|_2/\|S\|_2$. Then

```math
2\alpha_e+\alpha_e^2\le \epsilon,
```

so it suffices to enforce

```math
\alpha_e
\le
\sqrt{1+\epsilon}-1.
```

If thresholds are isotropic, $T_j=T$ for $n$ sketch cells and each site residual
is bounded by $T$, then

```math
\|e\|_2
\le
k\sqrt{n}T.
```

Thus a curvature-safe isotropic threshold is

```math
T
\le
\frac{(\sqrt{1+\epsilon}-1)\|S\|_2}
{k\sqrt{n}}.
```

For small $\epsilon$, this is approximately

```math
T\lesssim \frac{\epsilon\|S\|_2}{2k\sqrt{n}},
```

but the exact expression should be used in formal claims.

## 10. Alert mode

Threshold/alert monitoring is different from value-estimation. In the
unsampled monotone case, slack-countdown can provide deterministic no-missed
crossing. With two-sided sampling noise, alert safety is probabilistic.

For a global monitored value $G$, sampled estimate $\widehat G$, and sampling
margin $m_{sa}(\delta)$, a safe fire rule is

```math
\widehat G + m_{sa}(\delta) + m_{cdm}
\ge
(1-\epsilon)\tau.
```

Here $m_{cdm}$ is the worst-case stale backend margin. For a linear additive
readout:

```math
m_{cdm}=k\sum_j |a_j|T_j.
```

For quantiles, convert the alert predicate to a rank/count predicate first. For
example, a DDSketch predicate $q_\phi > v^*$ is equivalent to a bucket-prefix
count predicate up to the DDSketch value bucket error. The sampling and CDM
terms then enter as rank/count margins.

## 11. Summary table

| Family/query | Sampling term | Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) term | Safe paper claim |
| --- | --- | --- | --- |
| Sum/count | scalar Bernoulli count variance | $kT$ | exact base sketch plus sampling plus scalar staleness |
| CMS point | row-counter sampling variance | $k\max_r T_{r,h_r(x)}$ | caveated adapter: row union plus collision event; probabilistic alert margin |
| CountSketch point | row-counter sampling variance | $k\max_r T_{r,h_r(x)}$ | clean additive/two-sided composition; best first theorem target |
| DDSketch range count | bucket-count sampling variance | $k\sum_{b\in \mathrm{range}}T_b$ | additive bucket-count composition |
| DDSketch quantile | rank error $\epsilon_{sa}$ | prefix rank error $\epsilon_{cdm}$ | $(1\pm \alpha)$ value error around $q\pm \epsilon$ rank |
| KLL quantile | thinning rank error | unshipped segment rank $R/N$ | mergeable adapter, not per-cell GOS |
| HLL distinct | exclude Nitro-style additive sampling | register-change adapter | mergeable but non-additive; family-specific proof |

## 12. Paper theorem templates

### Theorem 1: additive-state composition

For a fixed linear query $q(S)=\langle a,S\rangle$ at a fixed time in a tumbling window,
assume:

1. sampling randomness is independent across updates, i.e. the admission
   indicators $Z_u$ are independent;
2. sampled updates use inverse-probability weights for additive counters;
3. each site/cell residual is bounded by $R_{ij}$; in the update-synchronous
   ideal protocol $R_{ij}=T_j$, while periodic checks can use
   $R_{ij}=T_j+U_{ij}\Delta_{check}$;
4. the base sketch error is at most $E_{sk}(q)$ with probability
   $1-\delta_{sk}$.

Then with probability at least $1-\delta_{sk}-\delta_{sa}$,

```math
|q(\widetilde S)-q(f)|
\le
E_{sk}(q)
+m_{sa}(q,\delta_{sa})
+\sum_j |a_j|\sum_i R_{ij}.
```

If only a variance statement is needed, pairwise uncorrelated admission
indicators are sufficient for the variance identity, but not for the Bernstein
margin above. If all sites use the update-synchronous bound $R_{ij}=T_j$, the
staleness term reduces to $k\sum_j |a_j|T_j$.

### Theorem 2: Geometric-OctoSketch (GOS) threshold allocation

Under the threshold-crossing cost model, given additive-state staleness budget
$B$ and cell activity $V_j$, the threshold vector minimizing $\sum_j V_j/T_j$
subject to $\sum_j c_jT_j\le B$ is

```math
T_j
=
\frac{B\sqrt{V_j/c_j}}
{\sum_\ell\sqrt{V_\ell c_\ell}},
```

before floors and caps. The clamped solution is obtained by iterative
water-filling over unclamped cells.

### Corollary: DDSketch sampled open-window quantile

For DDSketch relative value parameter $\alpha$, uniform value-independent
admission probability $p$, $B$ nonempty buckets, and bucket residual thresholds
$T_b$, with probability at least $1-\delta$:

```math
(1-\alpha)x_{q-\epsilon_{sa}-\epsilon_{cdm}}
\le
\widetilde x_q
\le
(1+\alpha)x_{q+\epsilon_{sa}+\epsilon_{cdm}},
```

where

```math
\epsilon_{sa}
=
O\left(
\sqrt{\frac{\log(B/\delta)}{pN}}
+\frac{\log(B/\delta)}{pN}
\right),
```

and

```math
\epsilon_{cdm}
=
\frac{k}{N}\sup_b\sum_{j\le b}T_j.
```

## 13. Implementation notes

- The Go DDSketch wrapper performs value-independent admission before the
  bucket update. This is equivalent to uniform thinning for quantile rank
  analysis. Counts need `sample_p` rescaling if they are queried as counts.
- CountSketch sparse deltas currently require integral cell deltas. If sampling
  creates fractional weighted cells, the implementation can fall back to full
  frames. That preserves accuracy but weakens communication claims for the
  sampled+delta combination unless a fractional delta wire is added.
- The strongest continuous $|rho_i[j]|<=T_j$ proof assumes an update-synchronous
  threshold check. Current sub-window/tick-based emit paths should be stated
  with an overshoot term in formal claims.
- For SDK-source or multi-unit sampling, the sampling randomness must make the
  admission indicators independent per `(edge_id, agg_id, window)` or hash-based
  per item. Shared sampler seeds can correlate admissions and invalidate
  variance-addition by introducing covariance terms.

## 14. Evaluation checklist

The system claim is only credible if the evaluation shows that ASAPCollector
moves work off the central raw-sample path while preserving the advertised error
envelope for supported queries. At minimum, measure:

- **Edge update work:** sketch updates/sec, CPU, and memory as $p_i$ changes.
- **Network load:** bytes/sec and messages/sec for full raw export, periodic
  sketch export, and GOS thresholded deltas.
- **Backend ingest load:** accepted samples/sec or summary frames/sec, WAL/queue
  pressure, index/storage growth, and write amplification if applicable.
- **Freshness:** open-window staleness measured as both wall-clock lag and
  query-space error $\mathrm{Err}_q^{cdm}$.
- **Accuracy envelope:** empirical query error decomposed into sketch error,
  sampling error, and staleness error; report coverage of the claimed
  high-probability bound.
- **Controller behavior:** selected $p_i$ and $T_j$ under hot/cold sites,
  query-sensitive/query-insensitive cells, and changing workloads.
- **Fallback boundary:** unsupported query classes and cold raw fallback cost,
  so the paper does not imply arbitrary PromQL support.
- **Failure modes:** tick-based overshoot, delayed ACKs, retries, collector
  restart, and correlated sampler seeds.

Use CountSketch, Sum/count, and DDSketch range counts as the first theorem-backed
accuracy experiments. Treat CMS, DDSketch quantiles, KLL, and HLL as
family-specific adapter experiments with their own stated caveats.

## Appendix A. Bernstein inequality step

This appendix derives the concentration bound used in Section 4.2:

```math
|X_q|
\le
\sqrt{2\sigma_q^2\log(2/\delta)}
+\frac{2G}{3p_{\min}}\log(2/\delta)
```

with probability at least $1-\delta$.

Recall the sampled query error:

```math
X_q
=
\sum_u (Y_u-1)g_u.
```

Define the centered per-update random variable

```math
A_u=(Y_u-1)g_u.
```

Then

```math
X_q=\sum_u A_u.
```

### A.1 Zero mean

Because $E[Y_u]=1$,

```math
\mathbb{E}[A_u]
=
\mathbb{E}[(Y_u-1)g_u]
=
g_u(\mathbb{E}[Y_u]-1)
=0.
```

Thus $X_q$ is a sum of independent centered random variables, assuming the
sampling admission indicators are independent.

### A.2 Variance parameter

From Section 4.1:

```math
\mathrm{Var}(A_u)
=
g_u^2\frac{1-p_{i(u)}}{p_{i(u)}}.
```

Therefore the total variance parameter is

```math
\sigma_q^2
=
\sum_u \mathrm{Var}(A_u)
=
\sum_u g_u^2\frac{1-p_{i(u)}}{p_{i(u)}}.
```

### A.3 Uniform bound on one summand

Bernstein's inequality also needs an almost-sure bound on $|A_u|$.

Since

```math
Y_u =
\begin{cases}
1/p_{i(u)}, & \text{if update }u\text{ is admitted},\\
0, & \text{if update }u\text{ is skipped},
\end{cases}
```

we have

```math
Y_u-1 =
\begin{cases}
(1-p_{i(u)})/p_{i(u)}, & \text{if update }u\text{ is admitted},\\
-1, & \text{if update }u\text{ is skipped}.
\end{cases}
```

Thus

```math
|Y_u-1|
\le
\frac{1}{p_{i(u)}}
\le
\frac{1}{p_{\min}}.
```

If $|g_u| \le G$, then

```math
|A_u|
=
|(Y_u-1)g_u|
\le
\frac{G}{p_{\min}}.
```

Let

```math
M=\frac{G}{p_{\min}}.
```

### A.4 Apply Bernstein

One standard two-sided Bernstein bound for independent centered random variables
$A_u$ with $|A_u| \le M$ and variance sum $\sigma_q^2$ is

```math
\Pr\left[
\left|\sum_u A_u\right|
\ge
\sqrt{2\sigma_q^2 t}
+\frac{2M}{3}t
\right]
\le
2e^{-t}.
```

Set

```math
t=\log(2/\delta).
```

Then $2e^{-t}=\delta$, so with probability at least $1-\delta$,

```math
\left|\sum_u A_u\right|
\le
\sqrt{2\sigma_q^2\log(2/\delta)}
+\frac{2M}{3}\log(2/\delta).
```

Substituting $M=G/p_{\min}$ and $X_q=sum_u A_u$ gives

```math
|X_q|
\le
\sqrt{2\sigma_q^2\log(2/\delta)}
+\frac{2G}{3p_{\min}}\log(2/\delta).
```

This is the Section 4.2 bound.

## 15. References to cite

- Prometheus remote write and storage documentation: central ingest, queue/WAL,
  CPU, memory, and network costs for exporting high-resolution metrics.
- OpenTelemetry metrics data model: metric events, aggregation temporality,
  histograms/exponential histograms, and the motivation for aggregation before
  export.
- Prometheus histograms and summaries documentation: early binding of buckets,
  quantiles, and aggregation constraints.
- Thanos compactor/downsampling documentation and TSDB compression papers:
  post-ingest query/storage optimization rather than pre-ingest load reduction.
- BlinkDB and VerdictDB: approximate query processing over already-ingested
  data.
- NitroSketch: update sampling for sketch update work.
- DDSketch: mergeable relative-error quantile sketch.
- Count-Min Sketch and CountSketch: base frequency guarantees.
- KLL: mergeable rank-error quantile sketch.
- HyperLogLog: base cardinality guarantee.
- Cormode et al. distributed functional monitoring / Continuous Distributed
  Monitoring (CDM).
- Mergeable summaries: no error compounding under merge.
- Zoom2Net and related telemetry reconstruction work: learned fine-grained
  reconstruction from coarse measurements, distinct from preserving
  query-sufficient sketch state.
