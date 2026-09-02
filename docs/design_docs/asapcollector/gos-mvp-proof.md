# Isotropic GOS MVP Proof Contract

## Scope

This note states the correctness contract for the first GOS MVP. It covers:

- isotropic thresholds;
- Sum, Count-Min Sketch (CMS), Count Sketch, and DDSketch;
- source-SDK admission sampling followed by Sum or sketch construction in the
  OTel processor;
- threshold-triggered sparse deltas;
- a periodic freshness fallback;
- exact plan, producer, window, and sequence evidence; and
- measured accuracy, freshness, bytes, CPU, and memory.

The sampling claim covers Sum and DDSketch as one-row families and CMS and
Count Sketch as row-replicated families. Anisotropic water-filling, arbitrary
nonlinear queries, HLL, KLL, and a claim of matching the Woodruff–Zhang lower
bound are out of scope.

## Model and protocol assumptions

There are $k$ Collector sites. For one materialization and window, site $i$
maintains additive state

$$
x_i(t) \in \mathbb{R}^n.
$$

The model applies to one scalar for Sum, $d w$ counters for CMS or Count
Sketch, and one count per active bucket for DDSketch. The true global state is

$$
x(t) = \sum_{i=1}^{k} x_i(t).
$$

Let $r_i(t)$ be the state from site $i$ that the backend has acknowledged and
applied. The backend reconstruction and the site's unacknowledged drift are

$$
\widehat{x}(t) = \sum_{i=1}^{k} r_i(t),
\qquad
D_i(t) = x_i(t) - r_i(t).
$$

`D_i` includes both queued or in-flight deltas and updates accumulated after a
frame was sent. Enqueuing a frame therefore does not remove its contribution
from `D_i`; only an acknowledgement for an atomically applied frame advances
`r_i`.

Every frame is identified by at least
`(plan_id, producer_id, materialization_id, window_id, sequence)`. The backend
must apply duplicates idempotently, reject incompatible plans or windows,
detect sequence gaps, and request a full checkpoint after an unrecoverable
gap. Window close sends all residual state.

## Source sampling and processor sketch construction

Sampling happens at `Record()` in the data-source SDK, before OTLP transport.
Sum and DDSketch have one admission row; CMS and Count Sketch have one
admission decision per sketch row. For occurrence $q$ and row $r$, let
$I_{q,r}$ be an independently admitted row indicator with

$$
\Pr[I_{q,r}=1]=p,
\qquad 0<p\le 1.
$$

The SDK sends an admitted occurrence together with its row bitmask, $p$, and
the policy identity. For Sum, CMS, and Count Sketch, the OTel processor applies
exactly those admitted rows and scales an admitted update $u_q$ by $1/p$:

$$
\widehat{u}_{q,r}=\frac{I_{q,r}}{p}u_q.
$$

For DDSketch, the processor inserts each admitted value exactly once without
inverse-weighting its bucket. It stamps $p$ on the envelope: quantiles use the
sampled empirical distribution directly, while count-like consumers apply
$1/p$. This distinction is necessary because duplicating a value $1/p$ times
would require integral weights and would not improve the sampled quantile.

The processor must not draw a second sampling decision. It must also remove the
reserved sampling metadata before constructing the series identity. A missing
or incompatible policy, row count, $p$, hash seed, sketch dimensions, or
materialization identity is a protocol error and the observation must not be
silently inserted through the ordinary unsampled path.

For Sum, CMS, and Count Sketch this is a Horvitz--Thompson estimator. For each
update and row,

$$
\mathbb{E}[\widehat{u}_{q,r}]=u_q,
$$

so the processor-built Sum or counter is unbiased relative to the state that
would have been built from all source occurrences. If cell $j$ receives update
set $Q_j$, its sampling error $E_j$ has

$$
\mathbb{E}[E_j]=0,
\qquad
\mathrm{Var}(E_j)
=
\frac{1-p}{p}\sum_{q\in Q_j}u_q^2.
$$

Unbiasedness is not a deterministic accuracy bound. The MVP must obtain or
measure a simultaneous high-probability bound

$$
\Pr\!\left[\lVert E\rVert_\infty
\le \epsilon_{\mathrm{samp}}\right]
\ge 1-\delta_{\mathrm{samp}}
$$

for the declared workload, sampling policy, and query window. This bound may
come from a concentration bound under the declared independence and bounded
update assumptions, or from an empirical confidence procedure stated in the
evaluation. The evidence must record both source input counts and admitted
row counts; $p$ alone is not evidence that the bound held.

Let $x^{(p)}$ denote the sampled state built by the processors, including
inverse-probability weighting where defined above. GOS operates on $x^{(p)}$,
not on the unavailable full-input state $x$. Its deterministic invariant is

$$
\left\lVert x^{(p)}(t)-\widehat{x}^{(p)}(t)\right\rVert_\infty < kT.
$$

For Sum, CMS, and Count Sketch, on the sampling-success event the triangle
inequality composes the two stages:

$$
\left\lVert x(t)-\widehat{x}^{(p)}(t)\right\rVert_\infty
< \epsilon_{\mathrm{samp}}+kT.
$$

## Isotropic drift invariant

For a scalar threshold $T > 0$, each site maintains

$$
\lVert D_i(t) \rVert_\infty < T.
$$

When a cell reaches $|D_{i,j}(t)| \ge T$, the Collector marks it for export and
wakes the flush loop. If the active cell is reset when a frame is formed, its
value moves to explicit in-flight state, so the total unacknowledged drift is
still used by the invariant.

The difference between the true and reconstructed global state is

$$
x(t) - \widehat{x}(t) = \sum_{i=1}^{k} D_i(t).
$$

For every cell $j$, the triangle inequality gives

$$
\left|x_j(t) - \widehat{x}_j(t)\right|
\le \sum_{i=1}^{k} \left|D_{i,j}(t)\right|
< kT.
$$

Therefore the central deterministic GOS bound is

$$
\boxed{
\lVert x(t) - \widehat{x}(t) \rVert_\infty < kT
}.
$$

This is an unacknowledged-drift bound, not merely a bound on the active mutable
buffer. It requires bounded in-flight state or backpressure; otherwise multiple
outstanding threshold crossings could violate it before acknowledgements
arrive.

## Sum

Sum has one cell. Let $\widehat{S}_p(t)$ be the SDK-sampled,
inverse-probability-weighted Sum and let $\widehat{S}_{\mathrm{backend}}(t)$ be
the backend Sum. GOS guarantees

$$
\boxed{
\left|\widehat{S}_p(t)-\widehat{S}_{\mathrm{backend}}(t)\right| < kT
}.
$$

On an event where source sampling satisfies
$|S(t)-\widehat{S}_p(t)|\le\epsilon_{\mathrm{sum,samp}}$, composition gives

$$
\boxed{
\left|S(t)-\widehat{S}_{\mathrm{backend}}(t)\right|
< \epsilon_{\mathrm{sum,samp}}+kT
}.
$$

For a relative staleness budget $\epsilon_{\mathrm{st}}$ and a declared,
positive scale lower bound $S_{\min}$, choose

$$
T \le \frac{\epsilon_{\mathrm{st}} S_{\min}}{k}.
$$

Then, whenever $|S(t)| \ge S_{\min}$,

$$
\frac{|\widehat{S}_p(t)-\widehat{S}_{\mathrm{backend}}(t)|}
     {|S(t)|}
< \epsilon_{\mathrm{st}},
$$

and the sampling error remains a separate term in the end-to-end budget.

Near zero, or when positive and negative updates cancel, the MVP uses a mixed
absolute-relative contract instead:

$$
|S - \widehat{S}|
\le \max\!\left(\epsilon_{\mathrm{abs}},
                 \epsilon_{\mathrm{rel}} |S|\right).
$$

## Count-Min Sketch point queries

For key $y$, the ideal CMS row estimates and point estimate are

$$
z_r(y) = C[r,h_r(y)],
\qquad
\widetilde{f}(y) = \min_r z_r(y).
$$

The minimum is 1-Lipschitz in the infinity norm:

$$
\left|\min_r a_r - \min_r b_r\right|
\le \max_r |a_r-b_r|.
$$

Because the backend differs from the processor-built sampled sketch by less
than $kT$ in every cell, GOS adds less than $kT$ point-query error:

$$
\boxed{
\left|
\widetilde{f}_{\mathrm{ideal}}(y) -
\widetilde{f}_{\mathrm{backend}}(y)
\right| < kT
}.
$$

If the underlying full-input CMS guarantee is

$$
0 \le \widetilde{f}_{\mathrm{ideal}}(y)-f(y)
\le \epsilon_{\mathrm{sk}} \lVert f \rVert_1,
$$

then the conservative composed bound is

$$
\boxed{
\left|
\widetilde{f}_{\mathrm{backend}}(y)-f(y)
\right|
< \epsilon_{\mathrm{sk}} \lVert f \rVert_1
+ \epsilon_{\mathrm{samp}} + kT
}.
$$

This bound holds with the joint success probability of the CMS and source
sampling guarantees. Independence between their failure events is not needed
for the union-bound probability
$1-\delta_{\mathrm{sk}}-\delta_{\mathrm{samp}}$.

Choosing

$$
T \le \frac{\epsilon_{\mathrm{st}}\lVert f\rVert_1}{k}
$$

gives GOS staleness below
$\epsilon_{\mathrm{st}}\lVert f\rVert_1$; the sampling term remains separate
and must be included in the total error budget.
This statement concerns the backend reconstruction. A local CMS minimum is not
valid after individual local cells have been reset for GOS transmission.

## Count Sketch point queries

For key $y$, Count Sketch uses

$$
z_r(y) = s_r(y) C[r,h_r(y)],
\qquad
\widetilde{f}(y) = \mathrm{median}_r\, z_r(y).
$$

Multiplication by $s_r(y) \in \{-1,+1\}$ preserves absolute error, and the
median is 1-Lipschitz in the infinity norm. Hence

$$
\boxed{
\left|
\widetilde{f}_{\mathrm{ideal}}(y) -
\widetilde{f}_{\mathrm{backend}}(y)
\right| < kT
}.
$$

If the ordinary Count Sketch guarantee is

$$
\Pr\!\left[
\left|\widetilde{f}_{\mathrm{ideal}}(y)-f(y)\right|
\le \epsilon_{\mathrm{sk}}\lVert f\rVert_2
\right] \ge 1-\delta_{\mathrm{sk}},
$$

then

$$
\boxed{
\Pr\!\left[
\left|\widetilde{f}_{\mathrm{backend}}(y)-f(y)\right|
< \epsilon_{\mathrm{sk}}\lVert f\rVert_2
+\epsilon_{\mathrm{samp}}+kT
\right]
\ge 1-\delta_{\mathrm{sk}}-\delta_{\mathrm{samp}}
}.
$$

Choosing

$$
T \le \frac{\epsilon_{\mathrm{st}}\lVert f\rVert_2}{k}
$$

gives GOS staleness below
$\epsilon_{\mathrm{st}}\lVert f\rVert_2$; the sampling term remains separate
in the total error budget.

## DDSketch sampling and rank staleness

Let $M$ be the number of source-SDK-admitted DDSketch values in the window.
Because Bernoulli admission is independent of value, the admitted values form
an unbiased sample of the source distribution. Under the declared independent
and identically distributed input assumption, the Dvoretzky--Kiefer--Wolfowitz
bound gives

$$
\Pr\!\left[
\sup_z\left|F_M(z)-F(z)\right|
\le
\sqrt{\frac{\ln(2/\delta_{\mathrm{dd,samp}})}{2M}}
\right]
\ge 1-\delta_{\mathrm{dd,samp}}.
$$

The MVP must report $M$; a useful bound cannot be claimed for an empty sample.
This distributional guarantee is separate from DDSketch's intrinsic relative
value accuracy $\alpha$.

DDSketch bucket counts are additive. If there are $B$ active sampled buckets,
the admitted count mass missing from the backend is bounded by

$$
M_{\mathrm{stale}} < BkT.
$$

For $M>0$, the additional GOS CDF or rank error is therefore

$$
\boxed{
\epsilon_{\mathrm{rank,st}}
< \frac{BkT}{M}
}.
$$

To keep this contribution below $\epsilon_{\mathrm{rank}}$, choose

$$
\boxed{
T \le \frac{\epsilon_{\mathrm{rank}}M}{kB}
}.
$$

On the DKW success event, the total additional rank error beyond the intrinsic
DDSketch value guarantee is bounded by

$$
\boxed{
\epsilon_{\mathrm{rank,total}}
<
\sqrt{\frac{\ln(2/\delta_{\mathrm{dd,samp}})}{2M}}
+\frac{BkT}{M}
}.
$$

This does not, without an assumption on probability mass near the requested
quantile, turn rank error into another relative value-error bound. The MVP
reports $\alpha$, sampling rank error, and GOS rank staleness separately.

## Sparse-delta telescoping

For an additive cell $j$, let the acknowledged sparse deltas be
$\Delta_j^{(1)},\ldots,\Delta_j^{(m)}$ and let $\rho_j$ be the final residual.
Then

$$
x_j
=
\sum_{\ell=1}^{m}\Delta_j^{(\ell)} + \rho_j.
$$

Consequently, cells may cross thresholds and reset at different times without
changing the eventual reconstructed state. This argument depends on
idempotent sequence handling and on sending the residual or a full checkpoint
at window close.

## Periodic freshness fallback

Let a nonzero pending change first appear at time $t_0$. The MVP runs a
periodic residual flush with period $P$ and requires

$$
P \le \Delta_{\mathrm{edge}}.
$$

The change is therefore exported no later than
$t_0+\Delta_{\mathrm{edge}}$. If accepted frames become atomically queryable
within $\Delta_{\mathrm{delivery}}$, the end-to-end bound is

$$
\boxed{
\Delta_* = \Delta_{\mathrm{edge}} + \Delta_{\mathrm{delivery}}
}.
$$

A heartbeat containing no pending residual is not sufficient evidence for
this bound. The MVP evidence must include the producer timestamp, application
timestamp, plan and sequence identity, and the observed end-to-end delay.

## What the proof does and does not establish

Under the stated assumptions, the MVP may claim:

$$
\lVert x^{(p)}-\widehat{x}^{(p)}\rVert_\infty < kT,
$$

with the following query-level consequences:

| Family | SDK sampling in this MVP | Additional pipeline error |
| --- | --- | --- |
| Sum | One-row admission | $<\epsilon_{\mathrm{sum,samp}}+kT$ absolute error |
| CMS point query | Row sampling | $<\epsilon_{\mathrm{samp}}+kT$ beyond intrinsic sketch error |
| Count Sketch point query | Row sampling | $<\epsilon_{\mathrm{samp}}+kT$ beyond intrinsic sketch error |
| DDSketch | One-row admission | $<\sqrt{\ln(2/\delta_{\mathrm{dd,samp}})/(2M)}+BkT/M$ additional rank error |

The proof does not establish that GOS always reduces cost. Cost reduction is an
experimental hypothesis and is accepted only if the measured run satisfies

$$
\mathrm{accuracy}_{\mathrm{pass}}
\land \mathrm{freshness}_{\mathrm{pass}}
\land \mathrm{bytes}_{\mathrm{GOS}} < \mathrm{bytes}_{\mathrm{full}}
\land \mathrm{CPU}_{\mathrm{GOS}} \le \mathrm{CPU}_{\mathrm{budget}}.
$$

The report must also include peak memory and must compare identical workloads,
seeds, windows, query anchors, and deployment topology. It must report source
SDK to Collector bytes separately from Collector to backend bytes: SDK sampling
can reduce the first link and processor insert CPU, while GOS sparse deltas can
reduce the second link. A useful end-to-end communication metric is

$$
\mathrm{saving}_{\mathrm{bytes}}
=
1-
\frac{\mathrm{bytes}_{\mathrm{GOS}}}
     {\mathrm{bytes}_{\mathrm{periodic\ full}}}.
$$

No asymptotic lower-bound claim follows from this measured ratio.
