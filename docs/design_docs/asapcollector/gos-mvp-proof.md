# Isotropic GOS MVP Proof Contract

## Scope

This note states the correctness contract for the first GOS MVP. It covers:

- isotropic thresholds;
- Sum, Count-Min Sketch (CMS), Count Sketch, and DDSketch;
- threshold-triggered sparse deltas;
- a periodic freshness fallback;
- exact plan, producer, window, and sequence evidence; and
- measured accuracy, freshness, bytes, CPU, and memory.

Sampling is disabled in this proof ($p = 1$). Sampling can still be evaluated
experimentally, but it is not part of the MVP correctness claim. Anisotropic
water-filling, arbitrary nonlinear queries, HLL, KLL, and a claim of matching
the Woodruff–Zhang lower bound are also out of scope.

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

Sum has one cell. If $S(t)$ is the true sum and $\widehat{S}(t)$ is the backend
sum, then

$$
\boxed{
\left|S(t) - \widehat{S}(t)\right| < kT
}.
$$

For a relative staleness budget $\epsilon_{\mathrm{st}}$ and a declared,
positive scale lower bound $S_{\min}$, choose

$$
T \le \frac{\epsilon_{\mathrm{st}} S_{\min}}{k}.
$$

Then, whenever $|S(t)| \ge S_{\min}$,

$$
\frac{|S(t) - \widehat{S}(t)|}{|S(t)|}
< \epsilon_{\mathrm{st}}.
$$

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

Because the backend differs from the ideal sketch by less than $kT$ in every
cell, GOS adds less than $kT$ point-query error:

$$
\boxed{
\left|
\widetilde{f}_{\mathrm{ideal}}(y)
-
\widetilde{f}_{\mathrm{backend}}(y)
\right| < kT
}.
$$

If the underlying CMS guarantee is

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
\le \epsilon_{\mathrm{sk}} \lVert f \rVert_1 + kT
}.
$$

Choosing

$$
T \le \frac{\epsilon_{\mathrm{st}}\lVert f\rVert_1}{k}
$$

gives total error at most
$(\epsilon_{\mathrm{sk}}+\epsilon_{\mathrm{st}})\lVert f\rVert_1$.
This statement concerns the backend reconstruction. A local CMS minimum is not
valid after individual local cells have been reset for GOS transmission.

## Count Sketch point queries

For key $y$, Count Sketch uses

$$
z_r(y) = s_r(y) C[r,h_r(y)],
\qquad
\widetilde{f}(y) = \operatorname{median}_r z_r(y).
$$

Multiplication by $s_r(y) \in \{-1,+1\}$ preserves absolute error, and the
median is 1-Lipschitz in the infinity norm. Hence

$$
\boxed{
\left|
\widetilde{f}_{\mathrm{ideal}}(y)
-
\widetilde{f}_{\mathrm{backend}}(y)
\right| < kT
}.
$$

If the ordinary Count Sketch guarantee is

$$
\Pr\!\left[
\left|\widetilde{f}_{\mathrm{ideal}}(y)-f(y)\right|
\le \epsilon_{\mathrm{sk}}\lVert f\rVert_2
\right] \ge 1-\delta,
$$

then

$$
\boxed{
\Pr\!\left[
\left|\widetilde{f}_{\mathrm{backend}}(y)-f(y)\right|
\le \epsilon_{\mathrm{sk}}\lVert f\rVert_2+kT
\right] \ge 1-\delta
}.
$$

Choosing

$$
T \le \frac{\epsilon_{\mathrm{st}}\lVert f\rVert_2}{k}
$$

gives total error at most
$(\epsilon_{\mathrm{sk}}+\epsilon_{\mathrm{st}})\lVert f\rVert_2$ with
probability at least $1-\delta$.

## DDSketch rank staleness

DDSketch bucket counts are additive. If there are $B$ active buckets, the
total count mass missing from the backend is bounded by

$$
M_{\mathrm{stale}} < BkT.
$$

For total count $N>0$, the additional CDF or rank error is therefore

$$
\boxed{
\epsilon_{\mathrm{rank,st}}
< \frac{BkT}{N}
}.
$$

To keep this contribution below $\epsilon_{\mathrm{rank}}$, choose

$$
\boxed{
T \le \frac{\epsilon_{\mathrm{rank}}N}{kB}
}.
$$

This proves a rank-mass staleness bound. It does not, without a distributional
assumption near the requested quantile, turn that rank error into an additional
relative value-error bound. The MVP reports DDSketch's intrinsic relative
value parameter $\alpha$ separately from
$\epsilon_{\mathrm{rank,st}}$.

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
\lVert x-\widehat{x}\rVert_\infty < kT,
$$

with the following query-level consequences:

| Family | Additional GOS error |
| --- | --- |
| Sum | $<kT$ absolute error |
| CMS point query | $<kT$ additive point-query error |
| Count Sketch point query | $<kT$ additive point-query error |
| DDSketch | $<BkT/N$ additional rank error |

The proof does not establish that GOS always reduces cost. Cost reduction is an
experimental hypothesis and is accepted only if the measured run satisfies

$$
\text{accuracy pass}
\land \text{freshness pass}
\land \text{bytes}_{\mathrm{GOS}} < \text{bytes}_{\mathrm{full}}
\land \text{CPU}_{\mathrm{GOS}} \le \text{declared CPU budget}.
$$

The report must also include peak memory and must compare identical workloads,
seeds, windows, query anchors, and deployment topology. A useful communication
metric is

$$
\operatorname{saving}_{\mathrm{bytes}}
=
1-
\frac{\text{bytes}_{\mathrm{GOS}}}
     {\text{bytes}_{\mathrm{periodic\ full}}}.
$$

No asymptotic lower-bound claim follows from this measured ratio.
