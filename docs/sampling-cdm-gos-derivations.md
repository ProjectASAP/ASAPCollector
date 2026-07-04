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

## 1. Scope and aggregation modes

Continuous monitoring in ASAP is not one protocol. The aggregation axis decides
which proof model applies.

| Mode | Query shape | Protocol | Coordinator? | Main guarantee |
| --- | --- | --- | --- | --- |
| A | per-series x window | local epsilon-gated delta emission | no | open-window freshness for one local series/group |
| B | series x timestamp | mergeable sketch fan-in | no | sealed/instant merge; no monitoring suppression |
| C | series x window, fused spatial+temporal | Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) over edge collectors | yes | global open-window tracking/alerting across `k` sites |

This note proves the composition used by modes A and C:

1. sketch approximation,
2. update sampling,
3. stale backend state due to suppressed deltas.

For sealed tumbling windows, only the first two sources matter. The merge of
mergeable summaries does not compound the base sketch error.

## 2. Notation

One fixed tumbling window is the universe of discourse.

| Symbol | Meaning |
| --- | --- |
| `k` | number of stable edge collectors/sites |
| `i` | site index, `i in {1,...,k}` |
| `u` | update/sample index |
| `x_u` | raw item/value |
| `f_i` | local frequency vector at site `i` |
| `f = sum_i f_i` | global frequency vector |
| `S_i` | exact local sketch state for site `i` |
| `S = sum_i S_i` | exact global additive sketch state |
| `\widehat S_i` | sampled local sketch state |
| `\widehat S = sum_i \widehat S_i` | sampled global sketch state |
| `\widetilde S` | backend's stale copy of `\widehat S` |
| `j` | sketch cell/bucket index |
| `a_j` | linear readout coefficient for query `q(S)=<a,S>` |
| `p_i` | sampling probability at site `i` |
| `T_j` | cell/bucket delta threshold |
| `V_j` | cell/bucket activity rate or expected change mass |
| `B` | staleness budget for a query/function |
| `epsilon_sk` | base sketch error budget |
| `epsilon_sa` | sampling error budget |
| `epsilon_cdm` | Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) staleness budget |
| `delta` | failure probability |

For additive sketches, an update `u` touches one or more sketch cells. Let
`j(u)` be a touched cell in a one-cell-per-update sketch such as a DDSketch
bucket, or let `j=(r,c)` be a row/cell pair for CMS/CountSketch.

## 3. Generic error decomposition

For a fixed query `q` at a fixed time inside a window:

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

For a sealed tumbling window, `\widetilde S=\widehat S` after boundary emission,
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
finite workload `Q` and `M` possible query times, replace `delta` by
`delta/(|Q|M)` in each concentration bound and union bound.

## 4. Additive-state sampling model

### 4.1 Horvitz-Thompson update sampling

For an update `u`, site `i(u)` admits it with probability `p_{i(u)}`:

```math
Z_u \sim \mathrm{Bernoulli}(p_{i(u)}).
```

Here `Z_u` is the admission indicator:

```math
Z_u =
\begin{cases}
1, & \text{if update }u\text{ is admitted and applied to the sketch},\\
0, & \text{if update }u\text{ is skipped}.
\end{cases}
```

The inverse-probability weighted contribution of update `u` is

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

Thus `Y_u` is the effective update weight written into an additive sketch
counter/cell. Without sampling, the update would contribute weight `1`; with
sampling, it contributes weight `1/p_{i(u)}` only on admitted updates.

Then

```math
\mathbb{E}[Y_u]=1,
\qquad
\mathrm{Var}(Y_u)=\frac{1-p_{i(u)}}{p_{i(u)}}.
```

For a linear readout `q(S)=<a,S>`, define `g_u` as the contribution of update
`u` to this readout if it were not sampled. For a one-bucket counter,
`g_u=a_{j(u)}`. For signed sketches, `g_u` also includes the row sign.

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

Since `\mathbb{E}[Y_u]=1`,

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

Multiplying by the deterministic readout contribution `g_u` scales variance by
`g_u^2`:

```math
\mathrm{Var}((Y_u-1)g_u)
=
g_u^2\mathrm{Var}(Y_u)
=
g_u^2\frac{1-p_{i(u)}}{p_{i(u)}}.
```

If the sampling randomness is independent across updates, i.e. the admission
indicators `Z_u` are independent,

```math
\mathrm{Var}(X_q)
=
\sum_u g_u^2 \frac{1-p_{i(u)}}{p_{i(u)}}.
```

This assumption is only about the sampling coin flips, not about the input data
streams. The metric values or workloads at different sites may be correlated;
the proof needs the random admission decisions to be independent, or at least
uncorrelated.

This equation is the sampling-error budget for query `q`. The random variable
`X_q=q(\widehat S)-q(S)` is the difference between answering `q` from the sampled
sketch and answering `q` from the same sketch without update sampling. Each term
in the sum says how much update `u` contributes to that sampling error:

- `g_u^2` is the squared sensitivity of query `q` to update `u`;
- `(1-p_{i(u)})/p_{i(u)}` is the noise introduced by sampling at the site that
  produced update `u`.

Thus lowering `p_i` saves update work at site `i`, but increases the variance of
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

If the admission indicators are independent, then the `A_u` terms are
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

This is only algebraic regrouping: for all updates from site `i`, the sampling
probability is the same `p_i`, so `(1-p_i)/p_i` is constant and can be pulled
outside the inner sum.

This is the quantity the controller must budget. A per-site floor on `p_i` is
not sufficient by itself; the variance sum must be bounded.

### 4.2 Concentration

For unit updates and bounded `|g_u| <= G`, Bernstein's inequality gives, with
probability at least `1-delta`,

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

For a uniform sampling probability `p` and a point-frequency count `f(x)=N(x)`,
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

## 5. Coordinated NitroSketch allocation

For a known point query/key `x`, the coordinator may choose different `p_i`
across sites. Let `f_i=f_i(x)` be the key mass at site `i`, and `r_i` be the
site's total update rate/cost weight for the sketch-update path.

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

Since `(1-p_i)/p_i = 1/p_i - 1`,

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

The scalar `sqrt(lambda)` is selected so that the variance constraint binds,
and the result is clamped to `(0,1]`.

For whole-sketch or unknown-key deployments, replace the query-specific
`f_i(x)` by the protected mass for the query class. A conservative whole-sketch
floor commonly used in deployments is derived from

```math
\frac{1-p_i}{p_i R_i}\le \epsilon_{sa}^2,
```

where `R_i` is an observable per-site rate/mass. This gives

```math
p_i \ge \frac{1}{1+\epsilon_{sa}^2 R_i}.
```

This floor protects aggregate sketch quality without pretending to optimize for
one known key.

## 6. Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) staleness model

### 6.1 Residual invariant

Let `A_i[j]` be the last state for site `i`, cell `j`, that the backend has
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

For a linear readout `q(S)=<a,S>`:

```math
\mathrm{Err}^{cdm}_q
=
|q(\widetilde S)-q(\widehat S)|
\le
k\sum_j |a_j|T_j.
```

If the implementation checks thresholds only every `Delta_check` seconds, not
after every update, include an overshoot term. If site `i`, cell `j` can change
at rate at most `U_{ij}` between checks, then

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
sampling noise scale:

```math
T_j^{floor}
\asymp
\sqrt{V_j(1-p_j)/p_j}.
```

The exact constant depends on the chosen concentration bound and the failure
probability allocation.

## 7. Geometric-OctoSketch (GOS) threshold water-filling

For additive sketch cells, the communication/upload rate of cell `j` is
proportional to `V_j/T_j`: higher activity or smaller thresholds cause more
emits. Given a staleness budget `B`, solve

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

Solving for `lambda` from the active budget constraint:

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

or `S_i=N_i` for count.

Base sketch error:

```math
\mathrm{Err}^{sk}=0
```

up to floating-point arithmetic.

Sampling error for unit counts under uniform `p`:

```math
|\widehat N-N|
\le
O\left(
\sqrt{\frac{N\log(1/\delta)}{p}}
+\frac{\log(1/\delta)}{p}
\right).
```

CDM staleness with scalar threshold `T`:

```math
\mathrm{Err}^{cdm}\le kT.
```

For a threshold alert `N>\tau`, deterministic no-missed-crossing holds only in
the unsampled monotone setting. With sampling, use a high-probability margin.

### 8.2 Count-Min Sketch point query

State: `d x w` nonnegative counter matrix.

Point query for key `x`:

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

with probability at least `1-delta_sk`, for the standard choice of width/depth.

Sampling error applies to every row counter. For row `r`, define

```math
X_r(x)
=
\widehat S[r,h_r(x)]-S[r,h_r(x)].
```

Then `X_r(x)` is unbiased with variance controlled by the updates landing in
that cell:

```math
\mathrm{Var}(X_r(x))
=
\sum_{u:h_r(x_u)=h_r(x)}
\frac{1-p_{i(u)}}{p_{i(u)}}.
```

A conservative high-probability point bound is

```math
\widetilde f_{CMS}(x)
\le
f(x)
+\epsilon_{sk}N
+m_{sa}(x,\delta)
+k\max_r T_{r,h_r(x)}.
```

Because CMS uses a `min`, the usual no-underestimate property is not preserved
by two-sided sampling noise. For alerting, fire with a confidence margin:

```math
\widetilde f_{CMS}(x)+m_{sa}(x,\delta)+m_{cdm}(x)
\ge
(1-\epsilon)\tau.
```

CMS is therefore useful but subtler than CountSketch for sampled alerts.

### 8.3 CountSketch point query

State: `d x w` signed counter matrix.

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

with probability at least `1-delta_sk`.

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

CDM staleness for row `r` is

```math
|s_r(x)(\widetilde S-\widehat S)[r,h_r(x)]|
\le
kT_{r,h_r(x)}.
```

Since the median is 1-Lipschitz in the `L_\infty` perturbation across rows:

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

DDSketch maps a positive value `x` to a logarithmic bucket `b(x)`. The bucket
mapping is chosen so values in one bucket have relative value error at most
`alpha`.

For a value range `[L,U]`, define a linear bucket readout

```math
q_{[L,U]}(S)
=
\sum_{b: v_b\in[L,U]} S[b],
```

where `v_b` is the representative value of bucket `b`.

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

The current DDSketch implementation performs value-independent admission before
the bucket update: if the item is not admitted, the entire update is skipped.
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
For a finite set of `B` nonempty buckets, a union bound over bucket prefixes
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

assuming `pN` is not too small.

#### Horvitz-Thompson analysis view

Equivalently, for analysis one may assign every admitted item weight `1/p`:

```math
Y_u=Z_u/p.
```

Then

```math
\widehat n_b=\sum_{u:b(x_u)=b}Y_u
```

is an unbiased estimator of `n_b`, and

```math
\mathrm{Var}(\widehat N_{\le b})
=
N_{\le b}\frac{1-p}{p}.
```

Bernstein plus a union bound over `B` prefixes yields

```math
\sup_b
|\widehat N_{\le b}-N_{\le b}|
\le
O\left(
\sqrt{\frac{N\log(B/\delta)}{p}}
+\frac{\log(B/\delta)}{p}
\right).
```

Dividing by `N` gives the same rank scale:

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

#### CDM rank staleness

If bucket residuals satisfy `|\rho_i[b]|\le T_b`, then for any prefix:

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
for denominator error, or normalize by a lower bound on `N`.

#### DDSketch quantile corollary

Let `\widetilde x_q` be the sampled and stale DDSketch estimate of the
`q`-quantile. With probability at least `1-delta`,

```math
\boxed{
(1-\alpha)x_{q-\epsilon_{sa}-\epsilon_{cdm}}
\le
\widetilde x_q
\le
(1+\alpha)x_{q+\epsilon_{sa}+\epsilon_{cdm}}
}.
```

Thus DDSketch keeps its multiplicative value error `alpha`, while sampling and
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
`R` samples. Then the stale-rank contribution is bounded by

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

for `m` registers.

Nitro-style inverse-probability update sampling is not appropriate for HLL
register updates: the update is a max operation, not an additive counter
increment. Hash-threshold distinct sampling can be analyzed separately, but it
does not fit the additive sampling theorem above.

For CDM/freshness, use a family-specific register-change adapter. The linear
per-cell bound `k sum_j |a_j|T_j` does not apply directly.

## 9. Nonlinear monitored functionals

For differentiable functionals `F(S)`, write the stale perturbation as

```math
e=\widetilde S-\widehat S.
```

At reference state `S_0`,

```math
F(S_0+e)-F(S_0)
=
\langle \nabla F(S_0),e\rangle
+R_2(e).
```

If the Hessian spectral norm is bounded by `lambda`, then

```math
|R_2(e)|\le \frac{1}{2}\lambda\|e\|_2^2.
```

The first-order term can be controlled by GOS water-filling with

```math
c_j=k|\nabla_jF(S_0)|.
```

The curvature term needs a separate budget. Do not spend the whole error budget
on the first-order term unless `lambda=0`.

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

To guarantee relative error `epsilon`:

```math
2\|S\|_2\|e\|_2+\|e\|_2^2
\le
\epsilon\|S\|_2^2.
```

Let `alpha_e=\|e\|_2/\|S\|_2`. Then

```math
2\alpha_e+\alpha_e^2\le \epsilon,
```

so it suffices to enforce

```math
\alpha_e
\le
\sqrt{1+\epsilon}-1.
```

If thresholds are isotropic, `T_j=T` for `n` sketch cells and each site residual
is bounded by `T`, then

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

For small `epsilon`, this is approximately

```math
T\lesssim \frac{\epsilon\|S\|_2}{2k\sqrt{n}},
```

but the exact expression should be used in formal claims.

## 10. Alert mode

Threshold/alert monitoring is different from value-estimation. In the
unsampled monotone case, slack-countdown can provide deterministic no-missed
crossing. With two-sided sampling noise, alert safety is probabilistic.

For a global monitored value `G`, sampled estimate `\widehat G`, and sampling
margin `m_{sa}(\delta)`, a safe fire rule is

```math
\widehat G + m_{sa}(\delta) + m_{cdm}
\ge
(1-\epsilon)\tau.
```

Here `m_{cdm}` is the worst-case stale backend margin. For a linear additive
readout:

```math
m_{cdm}=k\sum_j |a_j|T_j.
```

For quantiles, convert the alert predicate to a rank/count predicate first. For
example, a DDSketch predicate `q_phi > v^*` is equivalent to a bucket-prefix
count predicate up to the DDSketch value bucket error. The sampling and CDM
terms then enter as rank/count margins.

## 11. Summary table

| Family/query | Sampling term | Continuous Distributed Monitoring (CDM) / Geometric-OctoSketch (GOS) term | Safe paper claim |
| --- | --- | --- | --- |
| Sum/count | scalar Bernoulli count variance | `kT` | exact base sketch plus sampling plus scalar staleness |
| CMS point | row-counter sampling variance | `k max_r T_{r,h_r(x)}` | upper bound with collision plus probabilistic alert margin |
| CountSketch point | row-counter sampling variance | `k max_r T_{r,h_r(x)}` | clean additive/two-sided composition; best first theorem target |
| DDSketch range count | bucket-count sampling variance | `k sum_{b in range}T_b` | additive bucket-count composition |
| DDSketch quantile | rank error `epsilon_sa` | prefix rank error `epsilon_cdm` | `(1\pm alpha)` value error around `q\pm epsilon` rank |
| KLL quantile | thinning rank error | unshipped segment rank `R/N` | mergeable adapter, not per-cell GOS |
| HLL distinct | exclude Nitro-style additive sampling | register-change adapter | mergeable but non-additive; family-specific proof |

## 12. Paper theorem templates

### Theorem 1: additive-state composition

For a fixed linear query `q(S)=<a,S>` at a fixed time in a tumbling window,
assume:

1. sampling randomness is independent across updates, i.e. the admission
   indicators `Z_u` are independent or at least uncorrelated;
2. sampled updates use inverse-probability weights for additive counters;
3. each site/cell residual satisfies `|\rho_i[j]|\le T_j`;
4. the base sketch error is at most `E_sk(q)` with probability
   `1-delta_sk`.

Then with probability at least `1-delta_sk-delta_sa`,

```math
|q(\widetilde S)-q(f)|
\le
E_{sk}(q)
+m_{sa}(q,\delta_{sa})
+k\sum_j |a_j|T_j.
```

If threshold checks are periodic, replace `T_j` by
`T_j+U_{ij}\Delta_{check}` inside the site sum.

### Theorem 2: Geometric-OctoSketch (GOS) threshold allocation

Given additive-state staleness budget `B` and cell activity `V_j`, the threshold
vector minimizing `sum_j V_j/T_j` subject to `sum_j c_jT_j<=B` is

```math
T_j
=
\frac{B\sqrt{V_j/c_j}}
{\sum_\ell\sqrt{V_\ell c_\ell}},
```

before floors and caps. The clamped solution is obtained by iterative
water-filling over unclamped cells.

### Corollary: DDSketch sampled open-window quantile

For DDSketch relative value parameter `alpha`, uniform admission probability
`p`, `B` nonempty buckets, and bucket residual thresholds `T_b`, with
probability at least `1-delta`:

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
- The strongest continuous `|rho_i[j]|<=T_j` proof assumes an update-synchronous
  threshold check. Current sub-window/tick-based emit paths should be stated
  with an overshoot term in formal claims.
- For SDK-source or multi-unit sampling, the sampling randomness must make the
  admission indicators independent per `(edge_id, agg_id, window)` or hash-based
  per item. Shared sampler seeds can correlate admissions and invalidate
  variance-addition by introducing covariance terms.

## 14. References to cite

- NitroSketch: update sampling for sketch update work.
- DDSketch: mergeable relative-error quantile sketch.
- Count-Min Sketch and CountSketch: base frequency guarantees.
- KLL: mergeable rank-error quantile sketch.
- HyperLogLog: base cardinality guarantee.
- Cormode et al. distributed functional monitoring / Continuous Distributed
  Monitoring (CDM).
- Mergeable summaries: no error compounding under merge.
