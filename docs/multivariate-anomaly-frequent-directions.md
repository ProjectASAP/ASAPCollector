# Multivariate Correlation Anomaly via Covariance Sketching (Frequent Directions)

> **Design-doc / paper section** — a new sketch family for the ASAP backend:
> fleet-wide, multi-metric *correlation* anomaly detection. The edge ships a
> **linear covariance summary**; the backend does all spectral work
> (eigendecomposition / Frequent Directions). Citation markers like
> `[huang2007anomaly]` are placeholders; see the [table at the end](#references).
>
> Companion to [continuous-windows-related-work.md](continuous-windows-related-work.md)
> (windowing substrate) — this doc adds the *function* and its edge/backend split;
> CDM-style emission/alerting integration is a separate discussion.

---

## TL;DR

- **What it adds.** A query family none of our current sketches answer:
  **cross-metric correlation drift** — "did the *joint* behavior of
  (cpu, mem, latency, qps, error_rate, …) shift," catching anomalies that
  per-metric thresholds miss (each metric individually normal, their
  *relationship* broken).
- **Core trick.** Frequent Directions (FD) [liberty2013fd] only ever
  approximates the **covariance / Gram matrix** `G = AᵀA = Σ_t a_t a_tᵀ`.
  That matrix is a **sum of rank-1 outer products → linear, additive,
  mergeable, *subtractable*.** FD's lossy SVD-truncation is needed *only* when
  the dimension `d` is too large to carry `G` exactly.
- **Edge/backend split.** The edge does cheap **rank-1 accumulation, no SVD**,
  and ships the covariance (exact, or randomly projected). The backend does
  **all** eigendecomposition / FD / residual-energy scoring.
- **Free win on `[a,b]`.** Because the edge ships the *covariance* (linear), not
  the FD *sketch* (non-linear), this family lands in the **linear /
  prefix-subtractable** column — so it answers tumbling, sliding, and arbitrary
  `[a,b]` windows by **prefix-difference**, unlike a shipped FD sketch which
  would be stuck on the tree-merge path.
- **Cost headline** (derived in [§6](#6-theory--cost-analysis)). For a *registered
  threshold on a linear functional* of `G` (e.g. total variance), adaptive
  monitoring costs **`O(k log 1/ε)` scalars — independent of `n` and `d`** —
  versus the periodic baseline's **`Θ(k d² · W/τ)`** words. Quadratic/spectral
  thresholds are data-dependent: `Θ(k d² · L/slack)` where `L` is the drift path
  length. The covariance-shipping regime is backed by **communication-optimal
  distributed-covariance / distributed-PCA bounds** ([§7](#7-post-2013-theory--updated-related-work)).

---

## 1. Motivation & Problem

Our existing families answer **per-dimension** questions: quantiles
(DDSketch/KLL), frequency / heavy hitters (CMS / Count-Sketch), distinct counts
(HLL), sums. None captures **how multiple metrics move together.**

Many real incidents are *correlation-structure* failures, not threshold
crossings:

- CPU and latency are each within their individual SLOs, but their usual
  coupling has inverted (degraded dependency / noisy neighbor).
- A set of services that normally vary together has decoupled (partial outage).
- Network-wide traffic shifts its principal modes — the canonical *network-wide
  anomaly* setting [huang2007anomaly, lakhina2005].

The quantity that exposes these is the **covariance / Gram matrix** of the
metric vector and its **principal subspace**, tracked online with **Frequent
Directions** [liberty2013fd, ghashami2016fd].

> **Problem.** Add fleet-wide multivariate-correlation anomaly detection to the
> backend **without** putting heavy linear algebra (SVD) on the
> resource-constrained edge, and **without** forfeiting arbitrary-window queries.

---

## 2. Key Insight: ship the covariance, not the FD sketch

FD's entire guarantee is about approximating

```
G = AᵀA = Σ_t a_t a_tᵀ        (a_t ∈ ℝ^d : the metric vector at time t)
```

`G` is a **sum of outer products**, hence:

| Property | Holds for `G`? | Consequence |
| --- | --- | --- |
| Additive (`G_{A∪B} = G_A + G_B`) | ✓ | mergeable across edges by summation |
| **Subtractable** (`G_{[a,b]} = G_{≤b} − G_{≤a}`) | ✓ | **arbitrary `[a,b]` via prefix-difference, `O(1)`** |
| Convex-combinable (`G = Σ λ_i G_i`) | ✓ | geometric monitoring applies ([§8](#8-alert-plane-hook-geometric-monitoring)) |

The FD *sketch* `B` is **none** of these (subtracting two FD sketches can yield
an indefinite/invalid sketch — why a shipped FD sketch is stuck on the
non-linear tree-merge path). **By shipping `G` and deferring SVD/FD to query
time, we move this family from the non-linear column into the linear column.**

FD's lossy truncation only earns its keep when `d` is so large that carrying `G`
(size `d²/2`) is infeasible — a *large-`d`* concern handled in [§4.2](#42-large-d-random-projection--backend-fd).

---

## 3. Scope & data model

- **Aggregation scope: whole-stream.** Correlation anomaly is a property of the
  metric *vector* across the fleet/service, so we keep **one `d×d` covariance
  per (service, window-slice)** — cost `O(d²)`, **independent of series
  cardinality.** Per-series correlation is a non-goal for v1.
- A **row** `a_t ∈ ℝ^d` is the vector of the `d` monitored metrics at time `t`,
  aligned to a common tick and standardized ([§12](#12-open-questions--for-the-monitoring-integration-discussion)).
- The edge maintains running **un-centered moments** per `τ`-slice:
  `n` (count), `s = Σ a_t` (`d`-vector), `Q = Σ a_t a_tᵀ` (`d×d`, upper triangle).
  Covariance is recovered at the backend as `C = Q/n − (s/n)(s/n)ᵀ`. We transmit
  the **raw** moments because **only the un-centered moments are additive /
  subtractable** — centering must happen *after* assembling the window.

---

## 4. Edge / backend split

### 4.1 Small `d` (≈ tens of metrics) — exact, no FD

| Side | Work | Cost |
| --- | --- | --- |
| **Edge — hot path** | rank-1 update `Q += a aᵀ`, `s += a`, `n++` | `Θ(d²)` per sample, allocation-free, **no SVD** |
| **Edge — flush** | delta-encode `(n, s, Q)` per `τ`-slice (one per service, whole-stream) | `Θ(d²)` per emission |
| **Backend** | sum slices for the window; center → `C`; eigendecompose; score | SVD `Θ(d³)`, **at query time, off the edge** |

For `d` in the tens, `Q` is hundreds–few-thousand floats: tiny, **exact** (no FD
error), fully linear. **FD is not used at all here.**

### 4.2 Large `d` (hundreds–thousands) — random projection + backend FD

When `d²` is too large for the hot path or the wire, apply a fixed JL projection
`R ∈ ℝ^{m×d}` (`m ≪ d`, shared seed [achlioptas2003jl]) and accumulate the
projected Gram `Q̃ = Σ (Ra)(Ra)ᵀ = R Q Rᵀ ∈ ℝ^{m×m}`:

| Side | Work | Cost |
| --- | --- | --- |
| **Edge** | `Ra` then rank-1 in `m`-space; **no SVD** | `Θ(md + m²)` per sample |
| **Backend** | sum `Q̃` slices; SVD / FD in projected space; recover approx subspace & residual | `Θ(m³)` |

`Q̃` is still additive/subtractable → `[a,b]` still works; FD lossiness lives at
the backend. (Edge-side local FD with per-edge SVD, merged per [ghashami2016fd],
is **rejected** for v1: SVD on the edge + non-subtractable.) The communication of
this regime has a known optimum — see the distributed-covariance / distributed-PCA
bounds in [§7.3](#73-distributed-pca--covariance-sketch-communication-the-backbone-for-this-doc).

> **Decision.** Default to **§4.1 exact** unless configured `d > D_max`; above
> it switch to **§4.2 projection**. Control plane selects `d`, `D_max`, `m`,
> seed.

---

## 5. Backend scoring (the anomaly function)

From the assembled centered covariance `C`, eigenvalues `λ_1 ≥ … ≥ λ_d`:

- **Residual energy after top-`r` subspace:** `r_r(C) = trace(C) − Σ_{i≤r} λ_i =
  Σ_{i>r} λ_i`. Anomaly when the live `r_r` departs from baseline `r_r^ref`
  (the network-wide-anomaly score of [huang2007anomaly]).
- **Subspace drift:** principal-angle / projection distance between live and
  baseline top-`r` subspaces.
- **Mahalanobis point score** (optional): flag rows with large `aᵀ C⁻¹ a`.

Baseline `C^ref` is slow-moving (trailing window / EWMA / learned normal),
maintained at the backend, broadcast to edges only when the alert plane needs it.

---

## 6. Theory & Cost Analysis

We analyze this family in the **continuous distributed monitoring (CDM)** cost
model [cormode2013survey]: `k` observers (edges) feed one coordinator (backend),
and we bound, against an accuracy guarantee, the three resources the model cares
about — **communication**, **edge computation**, and **edge memory**.

### 6.1 Model & notation

| Symbol | Meaning |
| --- | --- |
| `k` | number of edges (observers) |
| `d` | monitored metric dimension (row size) |
| `n`, `n_i` | total / per-edge samples over the horizon (`n = Σ_i n_i`) |
| `W`, `τ` | monitoring horizon and emission granularity (`W/τ` slices) |
| `r` | target principal-subspace rank (`r ≪ d`) |
| `m`, `ℓ` | projection dim (large-`d`); FD sketch rows |
| `ε`, `δ` | relative accuracy; failure probability |
| `M` | covariance message size `= d(d+1)/2 + d + 1 = Θ(d²)` words (`Θ(m²)` projected) |
| `L_f` | **drift path length** of monitored quantity `f`: `L_f = ∫_0^W |df/dt| dt` (total variation) |

`A_i ∈ ℝ^{n_i×d}` stacks edge `i`'s rows; `G_i = A_iᵀA_i`; `G = Σ_i G_i = AᵀA`,
symmetric **PSD**. Edge `i` holds last-synced `G_i^0` and current drift
`ΔG_i = G_i − G_i^0`; the global drift is `ΔG = Σ_i ΔG_i` (additivity is what
makes the drift itself a clean object).

### 6.2 The fundamental functions

We monitor functions of `G` (or centered `C`), grouped by analytic structure —
this grouping *is* what determines achievable cost:

```
F1  Linear:    f(G) = ⟨W, G⟩       (trace(G) = total energy; a fixed-direction
                                     variance wᵀGw = ⟨wwᵀ,G⟩; one entry G_ab)
F2  Quadratic: f(G) = ‖G − G_ref‖_F²  (Frobenius correlation drift)
F3  Spectral:  f(G) = r_r(C) = Σ_{i>r} λ_i(C)   (residual energy; also λ_1,
                                     subspace angle) — non-linear, non-smooth
```

### 6.3 Accuracy guarantees

**(i) Exact regime (small `d`), forward accumulation.** The edge transmits `G_i`
in float64; the backend forms `Ĝ = Σ_i G_i`. Because every term `a_t a_tᵀ` is
**PSD, the summation has no cancellation**, so the forward error is benign:

```
‖Ĝ − G‖_F ≤ (n·u)/(1 − n·u) · ‖G‖_F ≈ n·u·‖G‖_F    (naive float64, u ≈ 2⁻⁵³)
          ≤ c·u·‖G‖_F                               (with Kahan summation)
```

i.e. **machine-precision exact**. Eigenvalues, residual, subspace are then exact
up to this `ε_fp`.

**(ii) Exact regime, `[a,b]` by subtraction.** `Ĝ_{[a,b]} = G_{≤b} − G_{≤a}`
**does** cancel, so the absolute error is bounded by the *operands*, not the
result:

```
‖Ĝ_{[a,b]} − G_{[a,b]}‖_F ≤ u·(‖G_{≤b}‖_F + ‖G_{≤a}‖_F) ≈ 2u·‖G_{≤b}‖_F
   ⇒  relative error ≤ 2u · ‖G_{≤b}‖_F / ‖G_{[a,b]}‖_F
```

So a **short window over a long history is the dangerous case** (the ratio
blows up). Mitigation: multi-resolution prefix checkpoints (re-baseline) so the
nearest prefix point keeps `‖G_{≤b}‖` bounded — ties directly to the
[retention/roll-up tier](continuous-windows-related-work.md#23-cross-cutting-requirements).

**(iii) Projected regime (large `d`).** With `R` an (`m×d`) JL map and
`Ĝ = R⁺(RGRᵀ)R⁺ᵀ` recovered via randomized low-rank [halko2011randomized], for
target rank `r` and `m = O(r/ε + log(1/δ))` columns:

```
‖G − Ĝ_r‖₂ ≤ (1 + ε)·‖G − G_r‖₂      w.p. ≥ 1 − δ
```

(`G_r` = best rank-`r`). Equivalently, if **FD** is run at the backend with
`ℓ = r + 1/ε` rows, the deterministic guarantee is

```
0 ⪯ G − BᵀB,    ‖G − BᵀB‖₂ ≤ ‖A − A_r‖_F² / (ℓ − r) ≤ ε·‖A − A_r‖_F²
```

**(iv) From matrix error to function error.** Given `‖Ĝ − G‖₂ ≤ η` (either
`ε_fp` or the projection bound):

```
Eigenvalues (Weyl):        |λ_i(Ĝ) − λ_i(G)| ≤ η   ∀i
Residual energy:           |r̂_r − r_r| ≤ r·η        (trace exact + top-r within η each)
Subspace (Davis–Kahan):    sin Θ(Û_r, U_r) ≤ η / (λ_r − λ_{r+1})
```

The subspace bound needs a **spectral gap** `λ_r − λ_{r+1}`; residual energy
`r_r` does **not** (gap-robust) — a reason to alert on residual rather than on
individual eigenvectors ([§8](#8-alert-plane-hook-geometric-monitoring)).

### 6.4 Communication cost

**(a) Periodic baseline (analytics plane; supports `[a,b]`).** One message per
slice per edge:

```
Comm_periodic = k · (W/τ) · M = Θ(k d² · W/τ)     words
```

Data-independent. Delta-encoding shrinks the *bytes per message* but not the
*message count*, so the `Θ(k·W/τ)` factor stands.

**(b) Adaptive value-monitoring of `G` within `‖Ĝ−G‖_F ≤ φ` (no threshold —
answers the "bounded-accuracy, no alert" case).** Split slack `φ` across edges
via the filter/slack scheme [olston2003filters]: edge `i` resyncs when
`‖ΔG_i‖_F ≥ φ/k` (triangle bound `‖ΔG‖_F ≤ Σ‖ΔG_i‖_F ≤ φ`). Number of resyncs
for edge `i` over the horizon is `L_{G_i}/(φ/k)`, each costing `M`:

```
Comm_value = M · Σ_i (k·L_{G_i}/φ) = Θ( (k d² / φ) · Σ_i L_{G_i} )
           = Θ( k² d² · L̄_G / φ )          (L̄_G = mean per-edge drift length)
```

Under *incoherent* (independent) drift, `‖Σ ΔG_i‖_F ≈ √(Σ‖ΔG_i‖_F²)` lets the
per-edge slack be `φ/√k`, improving the leading factor `k² → k√k`.

**(c) Adaptive threshold, F1 linear `⟨W,G⟩ ≷ τ`.** A linear functional collapses
to a **single scalar** `y = ⟨W,G⟩ = Σ_i ⟨W,G_i⟩ = Σ_i y_i` — exactly the
**distributed countdown / functional-monitoring** problem [cormode2008functional]:

```
Comm_F1 = O(k · log(1/ε))   scalar messages    (approximate, ε-relaxed threshold)
Lower bound (deterministic, exact): Ω(k · log(τ/k))
```

**Independent of `n` and of `d`** after the scalar reduction. This is the
order-of-magnitude win: `O(k log 1/ε)` scalars vs. `Θ(k d² W/τ)` words. *Caveat:*
`y` is **non-monotone** (covariance drifts both ways), so the clean countdown
bound assumes monotonicity; for non-monotone streams use the random-walk
guarantee [liu2012nonmonotonic] or a value-monitoring ladder.

**(d) Adaptive threshold, F2 quadratic `‖G−G_ref‖_F² ≷ τ` (sphere) and F3
spectral `r_r(C) ≷ τ`.** Geometric monitoring [sharfman2006geometric]. There is
**no clean closed-form worst-case bound for general non-linear `f`** — the survey
notes the method is evaluated empirically. We give the honest **data-dependent
(path-length) bound**. Each edge is silent while its drift ball
`B(e + ½ΔG_i, ½‖ΔG_i‖)` stays monochromatic w.r.t. the threshold surface; a
local safe radius `ρ_safe` follows from the distance to that surface:

```
F2 (sphere of radius √τ, ε-relaxed):  ρ_safe ≈ ε·√τ
F3 (Weyl):  residual moves ≤ r·‖ball‖₂  ⇒  ρ_safe ≈ (τ − r̂_r)/r
```

A global resync happens once the accumulated drift exhausts `ρ_safe`, so the
number of resyncs is bounded by the drift path length over the safe radius:

```
N_sync ≤ L_f / ρ_safe ,    Comm_F2/F3 = Θ(k d² · L_f / ρ_safe)
```

So the method wins precisely when the monitored quantity is **stable relative to
its slack** (`L_f/ρ_safe ≪ W/τ`). F3 is strictly more expensive than F2 (the
`1/r` factor) and **degrades near eigenvalue crossings** (the safe radius shrinks
with the gap) — mitigate by monitoring gap-robust residual energy, not
individual eigenvectors, and by floor-clamping `ρ_safe`.

> **Use the modern local test, not covering spheres.** The `½‖ΔG_i‖` *ball*
> above is Sharfman's original covering-spheres construction. It is **superseded**
> by **safe zones** and **convex decompositions** [lazerson2015convex,
> keren2014safezones], which are *provably never worse* than covering spheres and
> often several × tighter, plus *lightweight* variants with cheaper local tests
> [lazerson2016lightweight] — directly lowering both the `N_sync` and the per-edge
> CPU of F2/F3. See [§7.2](#72-geometric-monitoring-after-2013-safe-zones--convex-decompositions).

> **Lower-bound context.** Tracking quadratic/second-moment quantities under
> insert+delete has a `Ω(k/ε²)` communication lower bound [woodruff2012tight];
> our PSD, insert-only covariance is easier, but this sets the ceiling. For the
> covariance/PCA value-monitoring regime specifically, the communication-optimal
> targets are now known [boutsidis2016optimalpca, huang2021covsketch] — see
> [§7.3](#73-distributed-pca--covariance-sketch-communication-the-backbone-for-this-doc).

### 6.5 Edge computation cost

| Path | Exact (small `d`) | Projected (large `d`) |
| --- | --- | --- |
| **Hot path / sample** | `Θ(d²)` rank-1 update, alloc-free, **no SVD** | `Θ(md + m²)` (`Θ(nnz·m)` with sparse `R`) |
| **Flush / emission** | `Θ(d²)` serialize+delta | `Θ(m²)` |
| **Backend / query** | `Θ(d³)` eigendecomp | `Θ(m³)` |

The hot-path `Θ(d²)` is **not `O(1)`** — in tension with the edge framework's
`O(1)` hot-path requirement. Three knobs: (1) keep `d` small; (2) project to
`m`; (3) **subsample** — update the covariance every `s`-th sample (covariance
is robust to uniform subsampling), giving amortized `Θ(d²/s)` at the cost of a
`√s`-style variance increase. State the chosen `s` so the accuracy budget stays
honest.

### 6.6 Edge memory cost

```
Exact:      Q (upper triangle) + s + scalars = d(d+1)/2 + d + 1 = Θ(d²) floats
            d=50 → ~1.3k floats ≈ 10 KB;  d=100 → ~5k floats ≈ 40 KB
Projected:  Θ(m²)
```

**Independent of `n` and of series cardinality** (whole-stream scope) — it does
*not* contribute to the cardinality-proportional memory floor that worries the
per-series families. Caveat: an **exact bounded-space sliding `last-T`** via
Exponential Histograms would need an EH *per covariance entry* —
`Θ(d² · (1/ε_w) log N)`, expensive. For the alert plane prefer **geometric
monitoring** (keeps only current `Q` + last-synced `G^0`, i.e. `Θ(d²)`) over EH;
reserve EH for when bounded-space exact sliding is explicitly required. The
multi-resolution prefix store lives at the **backend**, not the edge.

### 6.7 Summary

| | Accuracy | Communication | Edge compute (hot) | Edge memory |
| --- | --- | --- | --- | --- |
| **Periodic / `[a,b]` substrate** | exact to `ε_fp` (subtraction-conditioned) | `Θ(k d² · W/τ)` | `Θ(d²)` | `Θ(d²)` |
| **Value-monitor `G` within `φ`** | `‖Ĝ−G‖_F ≤ φ` | `Θ(k²d²·L̄_G/φ)` (`k√k` if incoherent) | `Θ(d²)` | `Θ(d²)` |
| **F1 linear threshold** | exact / ε-relaxed | **`O(k log 1/ε)` scalars** (`Ω(k log τ/k)`) | `Θ(d²)` | `Θ(d²)` |
| **F2 drift threshold** | JL `(1±ε)` (large `d`) | `Θ(k d² · L_f/(ε√τ))` (data-dep.) | `Θ(d²)`/`Θ(md+m²)` | `Θ(d²)`/`Θ(m²)` |
| **F3 residual threshold** | Weyl `r·η`; gap for subspace | `Θ(k d² · r·L_f/slack)` (data-dep.) | as above | as above |

**Reading of the table.** The substrate (periodic) cost is the price of
arbitrary `[a,b]` flexibility and is data-independent. Registering a *linear*
SLO collapses to `n,d`-independent scalar tracking — take it wherever the alert
is expressible as `⟨W,G⟩ ≷ τ`. Quadratic/spectral alerts buy generality at a
data-dependent cost that is only attractive when the covariance is slowly
drifting; otherwise fall back to periodic + backend scoring.

---

## 7. Post-2013 Theory & Updated Related Work

The 2013 CDM survey [cormode2013survey] predates four lines of work that
directly tighten or modernize the analysis above. This section records them and
how each one revises this design.

### 7.1 Functional-monitoring lower bounds are now closed

The survey listed Woodruff–Zhang as "recent"; the line has since matured into
matching bounds we treat as ceilings:

- **Woodruff–Zhang (STOC 2012)** [woodruff2012tight] — first bounds depending on
  the **product `k·(1/ε²)`**: `F₀` randomized `Θ̃(k/ε²)`; `Fₚ (p>1)`
  `Ω(k^{p−1}/ε²)`; analogous for heavy hitters / empirical entropy. This is the
  ceiling cited in [§6.4](#64-communication-cost).
- **Yi–Zhang** [yi2013optimalhh] — heavy hitters / quantiles in `O((k/ε) log n)`
  with **matching lower bounds** ⇒ optimal. Recent work re-derives these with
  simpler protocols [bhattacharya2025simpleoptimal].

> *Effect on this doc:* our F1 bound (`O(k log 1/ε)`) and the `Ω(k/ε²)`
> value-monitoring ceiling are exactly these results — no change to the design,
> but the claims are now backed by tight theory.

### 7.2 Geometric monitoring after 2013: safe zones & convex decompositions

The biggest practical advance. Sharfman's covering-spheres (the `½‖ΔG_i‖` ball
in [§6.4(d)](#64-communication-cost)) has been superseded:

- **Safe zones** [keren2014safezones] — replace the sphere with an arbitrary
  **convex safe region** per site; covering spheres are a special case.
- **Convex decompositions** [lazerson2015convex] (VLDB 2015) — decompose the
  admissible region; **provably never worse than covering spheres**, often
  several × tighter.
- **Lightweight monitoring** [lazerson2016lightweight] (KDD 2016) — much cheaper
  *local* tests, addressing the per-edge CPU worry of our Weyl ball test.
- **Composable safe zones via convex analysis** [samoladas2017composable]
  (ICDT 2017) — safe zones that compose across multiple queries.
- **Sketch-based geometric monitoring** [garofalakis2013sketchgm] — geometric
  monitoring *on top of* linear sketches, the formal template for running GM over
  our covariance/Count-Sketch state.

> *Effect on this doc:* the alert plane ([§8](#8-alert-plane-hook-geometric-monitoring))
> should use **safe zones / convex decomposition**, not covering spheres. This
> lowers both `N_sync` and the per-edge local-test cost for F2/F3; the
> covering-spheres `ρ_safe` in [§6.4(d)](#64-communication-cost) is an upper
> bound on what the modern construction achieves.

### 7.3 Distributed PCA / covariance-sketch communication: the backbone for this doc

The survey omits matrix problems entirely; this is the line that makes our
covariance-shipping choice *provably* right, not just convenient:

- **Boutsidis–Woodruff–Zhang (STOC 2016)** [boutsidis2016optimalpca] — optimal
  distributed PCA with communication **independent of `n` and `d`** (`O(skd/ε)`
  bits-scale), with **matching lower bounds**.
- **Communication-Efficient Distributed Covariance Sketch (JMLR 2021)**
  [huang2021covsketch] — `s` sites each hold a matrix; build a covariance sketch
  at the coordinator with **communication-optimal** protocols and lower bounds.
  This is essentially the rigorous, optimal version of [§4](#4--edge--backend-split)
  / [§6.4](#64-communication-cost).
- **Frequent Directions optimality** [ghashami2016fd] — FD is space-optimal;
  robust/parameterized variants exist.

> *Effect on this doc:* cite [huang2021covsketch] and [boutsidis2016optimalpca]
> as the optimality targets for [§4.2](#42-large-d-random-projection--backend-fd)
> and the projected-regime communication, replacing the hand-derived path-length
> bound as the *reference* point.

### 7.4 The modern descendants (context, not yet adopted)

Where CDM grew up; relevant as future directions, not v1 dependencies:

- **Communication-efficient distributed/federated mean estimation**
  [suresh2017distributedmean] — the value-monitoring problem reborn under bit
  budgets; its quantization/sparsification toolbox applies *byte-level* on top of
  our covariance shipment.
- **Differential privacy under continual observation** [dwork2010continual] and
  near-optimal **matrix-factorization mechanisms** [henzinger2023continual] — a
  whole new axis: monitor while guaranteeing per-tenant privacy. A candidate v2
  direction if telemetry aggregation crosses tenant boundaries.

---

## 8. Alert-plane hook (geometric monitoring)

For the **last-`T`, real-time** alert plane, edges stay silent unless the score
could cross `τ`, per [§6.4(d)](#64-communication-cost). The monitored state is
the covariance `C` (additive ⇒ the convex-region machinery applies). We use
**safe zones / convex decomposition** [lazerson2015convex, keren2014safezones]
(not covering spheres — see [§7.2](#72-geometric-monitoring-after-2013-safe-zones--convex-decompositions))
to derive each edge's local admissible region. The residual `r_r(C)` is
**spectral — non-linear, non-smooth**; the local test certifies the safe region
stays one side of `τ` via the **Weyl bound** (`|Δλ_i| ≤ ‖E‖₂`). Sliding `last-T`
covariance via smooth histograms over the additive moments [braverman2007smooth].
This is intentionally a **hook**, not a spec — the CDM integration (control-plane
back-channel, slack redistribution, exactly-once violation messages) is the next
discussion.

---

## 9. Windowing integration (the `[a,b]` payoff)

`(n, s, Q)` are additive and subtractable, so this family slots into the
**linear / prefix-difference** branch of the windowing substrate
([linear-vs-non-linear split](continuous-windows-related-work.md#linear-vs-non-linear-families)):

| Query shape | Mechanism | Cost |
| --- | --- | --- |
| Tumbling | sum the slice's moments | `Θ(d²)` |
| Sliding "last `T`" | suffix sum (or EH for bounded space) | `Θ(d²)` |
| Arbitrary `[a,b]` | **prefix-difference** of two cumulative moment-sets | `Θ(d²)`, `O(1)` slices |

Centering and eigendecomposition happen **after** assembling the window. The
subtraction's numerical caveats are quantified in [§6.3(ii)](#63-accuracy-guarantees):
re-symmetrize and clip negative eigenvalues; accumulate in float64 / Kahan; fix
the standardization scale per query window.

---

## 10. Requirements mapping

| Requirement (related-work doc) | How this family satisfies it |
| --- | --- |
| **Edge memory ∝ useful info** | whole-stream `Θ(d²)`, cardinality-independent ([§6.6](#66-edge-memory-cost)) |
| **Hot path `O(1)`-ish** | `Θ(d²)` (or `Θ(md+m²)`), **no SVD**; subsample knob ([§6.5](#65-edge-computation-cost)) |
| **Summaries, never raw** | ships moments `(n,s,Q)`, never rows |
| **Delta-encoded** | moments additive ⇒ deltas track change |
| **Mergeable, non-compounding error** | exact (small `d`) or bounded ([§6.3](#63-accuracy-guarantees)) |
| **Arbitrary `[a,b]`** | linear/subtractable ⇒ prefix-difference |
| **Exactly-once ingest** | additive `Q` is **not idempotent** — same seq-number/dedup as other linear families |
| **Query-driven config** | control plane sets `d`, metric set, `D_max`, `m`, seed, `r`, `τ`-grid, threshold/accuracy mode |

---

## 11. Non-goals (v1)

- Per-series covariance (cardinality-proportional; deferred).
- Edge-side SVD / edge-side FD (rejected: heavy + non-subtractable).
- Causal / lagged correlation (only contemporaneous `a_t a_tᵀ`).
- Automatic metric selection — the monitored `d`-set is operator/query-specified.
- Differential privacy (a v2 axis — see [§7.4](#74-the-modern-descendants-context-not-yet-adopted)).

---

## 12. Open questions — for the monitoring-integration discussion

1. **Row construction & standardization.** Alignment to a common tick and scaling
   (z-score with what reference μ/σ? robust scaling?) — wrong scaling makes
   covariance meaningless and changes `‖G‖`, hence every cost bound above.
2. **Baseline maintenance.** Is `C^ref` a trailing window, an EWMA, or a learned
   normal — and does it live only at the backend, or must edges hold it for the
   geometric alert plane?
3. **`D_max` and `m`.** Where is the exact→projection crossover; what `m` meets
   the `(1±ε)` residual target of [§6.3(iii)](#63-accuracy-guarantees), measured
   against the optimal targets in [§7.3](#73-distributed-pca--covariance-sketch-communication-the-backbone-for-this-doc)?
4. **Alert vs. analytics planes.** Does the residual feed only registered
   standing alerts (geometric, last-`T`), only ad-hoc `[a,b]` analytics, or both
   over the shared moment substrate?
5. **Geometric local-test cost.** Is the safe-zone/Weyl ball test cheap enough on
   the edge ([§7.2](#72-geometric-monitoring-after-2013-safe-zones--convex-decompositions)),
   or do we fall back to filter/periodic emission for this family?
6. **Subsample rate `s`.** How much hot-path budget ([§6.5](#65-edge-computation-cost))
   do we spend, and how does the resulting variance compose with the accuracy
   budget?

---

## References

Citation keys are placeholders; bind full entries when integrating into the paper.

- `liberty2013fd` — E. Liberty, [Simple and Deterministic Matrix Sketching](https://doi.org/10.1145/2487575.2487623), KDD 2013 (Frequent Directions).
- `ghashami2016fd` — Ghashami, Liberty, Phillips, Woodruff, [Frequent Directions: Simple and Deterministic Matrix Sketching](https://doi.org/10.1137/15M1009718), SICOMP 2016 (mergeability, space-optimality).
- `halko2011randomized` — Halko, Martinsson, Tropp, [Finding Structure with Randomness: Probabilistic Algorithms for Constructing Approximate Matrix Decompositions](https://doi.org/10.1137/090771806), SIAM Review 2011 (randomized low-rank / projected PCA).
- `huang2007anomaly` — Huang, Nguyen, Garofalakis, Hellerstein, Joseph, Jordan, Taft, [Communication-Efficient Online Detection of Network-Wide Anomalies](https://doi.org/10.1109/INFCOM.2007.24), IEEE INFOCOM 2007.
- `lakhina2005` — Lakhina, Crovella, Diot, [Mining Anomalies Using Traffic Feature Distributions](https://doi.org/10.1145/1080091.1080118), ACM SIGCOMM 2005.
- `sharfman2006geometric` — Sharfman, Schuster, Keren, [A Geometric Approach to Monitoring Threshold Functions over Distributed Data Streams](https://doi.org/10.1145/1142473.1142508), SIGMOD 2006.
- `cormode2013survey` — Cormode, [The Continuous Distributed Monitoring Model](https://doi.org/10.1145/2481528.2481530), SIGMOD Record 2013.
- `cormode2008functional` — Cormode, Muthukrishnan, Yi, [Algorithms for Distributed Functional Monitoring](https://doi.org/10.1145/1921659.1921667), SODA 2008 / ACM TALG 2011.
- `olston2003filters` — Olston, Jiang, Widom, [Adaptive Filters for Continuous Queries over Distributed Data Streams](https://doi.org/10.1145/872757.872825), SIGMOD 2003.
- `liu2012nonmonotonic` — Liu, Radunović, Vojnović, [Continuous Distributed Counting for Non-monotonic Streams](https://doi.org/10.1145/2213556.2213600), PODS 2012.
- `woodruff2012tight` — Woodruff, Zhang, [Tight Bounds for Distributed Functional Monitoring](https://doi.org/10.1145/2213977.2214063), STOC 2012.
- `yi2013optimalhh` — Yi, Zhang, [Optimal Tracking of Distributed Heavy Hitters and Quantiles](https://doi.org/10.1007/s00453-011-9584-4), Algorithmica 2013 (PODS 2009).
- `bhattacharya2025simpleoptimal` — [Simple and Optimal Algorithms for Heavy Hitters and Frequency Moments in Distributed Models](https://www.researchgate.net/publication/392720890), 2024/25.
- `keren2014safezones` — Keren, Sharfman, Schuster, Korman, [Geometric Monitoring of Heterogeneous Streams / Safe Zones for Monitoring Distributed Streams](https://doi.org/10.1109/TKDE.2013.18), IEEE TKDE 2014.
- `lazerson2015convex` — Lazerson, Sharfman, Keren, Schuster, Garofalakis, Samoladas, [Monitoring Distributed Streams using Convex Decompositions](http://www.vldb.org/pvldb/vol8/p545-lazerson.pdf), VLDB 2015.
- `lazerson2016lightweight` — Lazerson, Keren, Schuster, [Lightweight Monitoring of Distributed Streams](https://doi.org/10.1145/2939672.2939820), KDD 2016.
- `samoladas2017composable` — Samoladas, Garofalakis, [Distributed Query Monitoring through Convex Analysis: Towards Composable Safe Zones](https://doi.org/10.4230/LIPIcs.ICDT.2017.14), ICDT 2017.
- `garofalakis2013sketchgm` — Garofalakis, Keren, Samoladas, [Sketch-based Geometric Monitoring of Distributed Stream Queries](https://doi.org/10.14778/2536222.2536233), VLDB 2013.
- `boutsidis2016optimalpca` — Boutsidis, Woodruff, Zhong, [Optimal Principal Component Analysis in Distributed and Streaming Models](https://doi.org/10.1145/2897518.2897646), STOC 2016.
- `huang2021covsketch` — Huang, Lin, Zhang, Zhang, [Communication-Efficient Distributed Covariance Sketch, with Application to Distributed PCA](https://www.jmlr.org/papers/volume22/20-705/20-705.pdf), JMLR 2021.
- `suresh2017distributedmean` — Suresh, Yu, Kumar, McMahan, [Distributed Mean Estimation with Limited Communication](https://proceedings.mlr.press/v70/suresh17a.html), ICML 2017.
- `dwork2010continual` — Dwork, Naor, Pitassi, Rothblum, [Differential Privacy under Continual Observation](https://doi.org/10.1145/1806689.1806787), STOC 2010.
- `henzinger2023continual` — Henzinger, Upadhyay, Upadhyay, [Almost Tight Error Bounds for Differentially Private Continual Counting / Matrix-Factorization Mechanisms](https://doi.org/10.1137/1.9781611977554.ch200), SODA 2023.
- `braverman2007smooth` — Braverman, Ostrovsky, [Smooth Histograms for Sliding Windows](https://doi.org/10.1109/FOCS.2007.55), FOCS 2007.
- `agarwal2013mergeable` — Agarwal et al., [Mergeable Summaries](https://doi.org/10.1145/2500128), PODS 2012 / ACM TODS 2013.
- `achlioptas2003jl` — D. Achlioptas, [Database-Friendly Random Projections: Johnson–Lindenstrauss with Binary Coins](https://doi.org/10.1016/S0022-0000(03)00025-4), JCSS 2003 (the edge projection `R`).
