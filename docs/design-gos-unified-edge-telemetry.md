# GOS: A Unified Error / Threshold / Cost Framework for Distributed Edge Telemetry

**One line.** One framework that unifies error-bounded sketching, coordinated
sampling, Geometric Monitoring (GM/AutoMon), and OctoSketch-style change
transmission into a single per-cell decision — reducing to **one atomic quantity**
`e_j = k·T_j` (per-cell backend error) — with a unified relative error bound, a
tunable memory/compute/communication objective, and a closed-form
water-filling solution whose worst-case communication matches the
Woodruff–Zhang lower bound `Θ̃(k/ε²)`.

We call the construction **GOS** (Geometric-OctoSketch).

---

## 1. Context and requirements

Distributed data collection and transmission with a centralized analytics
backend. Edge telemetry must be **memory-, computation-, and
communication-efficient** under cloud economics, while the backend serves
**continuous, accurate, fresh** queries.

- **Memory efficiency.** Cloud memory instances are priced above CPU instances.
  With high-cardinality, high-frequency time-series metrics from many
  distributed services, allocating *one sketch per series per window* puts heavy
  memory pressure on the data-source nodes. Memory must be controlled across the
  series×window space.
- **Computation efficiency.** Per-sample sketch update work at the edge must stay
  within line-rate CPU budgets on shared instances.
- **Communication efficiency.** Cross-AZ egress is billed per byte; transmission
  must be minimized.
- **Continuous & accurate queries.** Monitoring queries are *long-running* over
  the stream, not the database "on-demand" model. We care about the
  **freshness/timeliness** gap between when a sample is generated at the source
  and when it can be queried accurately at the backend.

These four axes are exactly the decision variables and constraints of the
optimization below.

---

## 2. Positioning: what each prior line gives, and what it lacks

All four lines are instances of "keep the local drift inside a **safe zone**,
communicate on violation." They differ in the *shape* of the safe zone and in
*what* they bound. (Attribution matters; see §10.)

| Line | Bounds | Safe-zone shape | Continuous query? | Gap |
|---|---|---|---|---|
| **Error-bounded sketching** (Count-Sketch [Charikar+02], CMY [Cormode+08]) | one function, per window | — | window granularity only | intra-window staleness |
| **Geometric Monitoring** (Sharfman–Schuster–Keren [SIGMOD'06]; AutoMon [Sivan+ SIGMOD'22]) | one scalar `f(x̄)` | DC-quadric (ball / slab / general) | that **one function** only | not per-cell; not query-general |
| **OctoSketch** [Zhang+ NSDI'24] | **every counter cell** `|ΔC[j]|<T` | axis-aligned **box** | whole sketch, **any query, any time** | function-agnostic (uniform T) |
| **Coordinated sampling** (NitroSketch; ASAP `AllocateSampleRates`) | edge CPU / counts | per-site rate `p_i` | — | ingestion-side only |
| **Lower bound** (Woodruff–Zhang [STOC'12]) | `F_p` continuous, `(1±ε)` | — (yardstick) | — | it is a *limit*, not a protocol |

Key facts used below:
- **GM's scalar safe zone is a special case of the box** (OctoSketch box = GM with
  an `L∞` safe zone), and **linear functions (sum/count/point) = GM with `λ=0` →
  slab**, **`F_2` = GM with constant Hessian `2I` → ball**. So *counter-based* and
  *`F_2`* monitoring are the same monitor at different Hessians (§7).
- **OctoSketch gives the "whole sketch continuously queryable" property that GM
  does not** — because it bounds every cell, not one function.
- **GOS = adopt water-filling (classic; already used in ASAP for sampling) to
  allocate *per-cell* thresholds, weighted by GM/AutoMon's gradient `∇f`, capped
  by OctoSketch's query bound.** The optimization tool is off-the-shelf; the
  contribution is the *combination*.

---

## 3. Model and decision variables

`k` edge sites, one backend/coordinator. Site `i` observes a substream with true
frequency vector `f_i`; the global signal is `f = Σ_i f_i`. Each site maintains a
**linear sketch** `x_i = L f_i ∈ ℝⁿ`, `n = d·w` (Count-Sketch: `d` rows of ±1
hashing, `w` buckets). Sketches merge additively: ideal global `C = Σ_i x_i = L f`.

**Workload.** A set `Q` of linear queries `q(f)=⟨a_q,f⟩` (point, sum,
heavy-hitter), each answered by a sketch estimator `q̂=⟨r_q,C⟩`; plus monitored
non-linear functionals `f_m(f)` (e.g. `F₂=‖f‖₂²`, entropy, ratios) answered by
`f̂_m = F_m(C)`.

**Decision variables** (per metric group):

| Variable | Meaning | Trades |
|---|---|---|
| `(d, w)` | sketch shape | accuracy ↔ memory |
| `G` | # sketch instances (grouping over series) | accuracy ↔ **memory** |
| `p_i ∈ (0,1]` | per-site sampling rate | edge **CPU** ↔ accuracy |
| `T_j ≥ 0` | per-cell transmission threshold | **communication**/freshness ↔ accuracy |
| flags | delta-vs-full, geometric-refs, uniform-vs-aniso | comm ↔ edge/coord memory |

---

## 4. Unified error bound

The backend holds `Ĉ(t)`, perturbed from the ideal `C(t)` by three **independent**
sources:

1. **Sketch** compression `L`: `|⟨r_q,C⟩ − q(f)| ≤ ε_sk‖f‖₂` w.p. `1−δ`, with
   `ε_sk = Θ(1/√w)`, `δ = 2^{−d}`.
2. **Sampling** `{p_i}`: site `i` admits w.p. `p_i`; the `1/p_i`-rescaled sketch is
   unbiased with per-cell variance. Perturbation `η = Σ_i η_i`, `𝔼η = 0`,
   `Var(η[j]) = Σ_j^{sa}({p_i}) ≈ Σ_i (1−p_i)/p_i · V_{ij}`, where `V_{ij}` is
   cell-`j` activity at site `i`.
3. **Staleness** `{T_j}`: site `i` withholds cell `j` until its accumulated change
   reaches `T_j`, so `|Ĉ[j] − (C+η)[j]| ≤ k T_j =: e_j` **at all times**
   (deterministic — this is the atomic quantity).

**Theorem 1 (unified relative error).** For any query `q` and any time `t`, w.p.
`≥ 1 − 3δ`:

```
              ┌ sketch ┐   ┌──────── sampling ────────┐   ┌──── staleness ────┐
|q̂(t) − q(f)| ≤ ε_sk‖f‖₂ + z_δ·√( Σ_j r_{q,j}²·Σ_j^{sa} ) + k·Σ_j |r_{q,j}|·T_j
```

Dividing by `‖f‖₂` gives the **relative** budget split (random parts in
quadrature; deterministic staleness adds linearly):

> **√(ε_sk² + ε_sa²) + ε_st ≤ ε_q.**

For a monitored non-linear `f_m` (`g_m = ∇f_m`, Hessian spectral bound `λ_m` from
AutoMon/ADCD), a Taylor + DC bound gives

```
|f̂_m − f_m| ≤ |g_m|ᵀ(ε_sk + ε_sa terms) + k·Σ_j|g_{m,j}|T_j + ½·λ_m·k²‖T‖₂²  ≤  ε_m·|f_m|.
```

This holds **continuously**, not only at window boundaries — the OctoSketch
"online accuracy at any query time" property, here generalized to arbitrary
queries and to gradient-weighted function monitoring.

### Freshness

Cell `j` (activity `V_j = Σ_i V_{ij}`) reaches `T_j` after time `T_j / V_j`, so its
max staleness age is `Δ_j = T_j / V_j`. To guarantee query freshness `Δ*`:
`T_j ≤ V_j Δ*`. (Quiet cells have large `Δ_j` even at small `T_j`, so freshness
binds them → periodic heartbeat.)

---

## 5. Cost models (per unit time)

```
Memory_edge  = m·G·n·( 1 + 1{delta}[acked snapshot] + 1{aniso}[threshold vec] )
Comp_edge    = c_u·Σ_i p_i·rate_i           (admitted updates — sampling cuts this)
             + c_s·Σ_j V_j/T_j              (uploads      — thresholds cut this)
Comm         = b·Σ_j V_j/T_j  (+ broadcast for geometric)
Cost_coord   = m·n·(1 + k·1{geo})           (running merge + per-edge refs)
             + c_a·Σ_j V_j/T_j              (incremental apply_delta)
```

**Structural insight.** `Comm`, the upload part of `Comp_edge`, and the apply part
of `Cost_coord` are all `∝ Σ_j V_j/T_j` → they collapse into one effective weight
`W = w_c·b + w_e·c_s + w_b·c_a`. Hence the *cost weights do not change the optimal
threshold shape* — the accuracy constraint pins `T_j`; the weights instead select
the **structural knobs** (uniform-vs-aniso, delta-vs-full, sampling `p`, per-edge
refs), which is where the memory/compute tradeoffs live.

---

## 6. The optimization problem (P)

```
minimize   w_m·Memory_edge + w_e·Comp_edge + w_c·Comm + w_b·Cost_coord
over       d, w, G, {p_i}, {T_j}, structural flags
subject to
  (query,    ∀ q ∈ Q)   √(ε_sk(w)² + ε_sa({p_i})²) + k·Σ_j|r_{q,j}|T_j/‖f‖  ≤  ε_q
  (function, ∀ f_m)      k·Σ_j|g_{m,j}|T_j + ½·λ_m·k²‖T‖²                    ≤  ε_m·|f_m|
  (freshness)            T_j ≤ V_j·Δ*
  (confidence)           d ≥ log₂(1/δ)
                         p_i ∈ (0,1],  T_j ≥ 0,  w ≥ 1,  G ≥ 1
```

`w_b` (coordinator) is the least-weighted axis by requirement.

---

## 7. Solution: hierarchical decomposition + two water-fillings

**Layer A — outer (small enumeration).** `d = ⌈log₂(1/δ)⌉`; choose `w`
(`ε_sk = c/√w`) and `G` (grouping) to trade the memory term `w_m·G·n` against the
accuracy the sketch must supply. Fix structural flags. Sets residual
`ε_res² = ε_q² − ε_sk²`.

**Layer B — budget split.** Split `ε_res` between **sampling** (`ε_sa`, buys edge
CPU) and **staleness** (`ε_st`, buys communication) by a 1-D convex tradeoff of
`w_e·Comp` vs `w_c·Comm`; unique (both convex, monotone). This is the
sampling↔threshold coupling.

**Layer C — two water-fillings (same KKT tool, two variables).**

- **Sampling** (per-site; existing ASAP `AllocateSampleRates`):
  ```
  p_i ∝ √( f_i / rate_i ),  clamped to (0,1],  binding Σ_i f_i(1−p_i)/p_i ≤ V_sa(ε_sa).
  ```
- **Thresholds** (per-cell; GOS). Effective weight `W` (§5), per-cell price
  `c_j = k|g_j|` (function) or `k|r_{q,j}|` (query — take the binding constraint),
  budget `B` (relative: `B = ε_m‖Ĉ‖²` for F₂):

  ```
              ┌ water-filling ┐   ┌──── clamps ────┐
  T_j = clamp( (B/Σ_ℓ√(c_ℓ V_ℓ))·√(V_j/c_j),  T_j^floor,  min(T_q, V_j·Δ*) )
  ```

  with **sampling-coupling floor** `T_j^floor = √( V_j(1−p)/p )` ("don't transmit
  finer than you sample"), **query cap** `T_q = ε_q‖Ĉ‖/(k·s_q)`, **freshness cap**
  `V_j·Δ*`. Clamped cells release budget → box water-filling redistributes (a few
  iterations).

**Reading.** `T_j ∝ √(V_j/|g_j|)`: high-activity cells get larger thresholds (don't
chase high-frequency noise); cells the monitored function is sensitive to
(`|g_j|` large) get smaller thresholds (report early); everything capped by the
universal query cap and freshness.

### Closed forms

- **F₂, isotropic** (`g_j = 2Ĉ_j`, `λ=2`, uniform, relative): `T = ε‖Ĉ‖ / (2k√(dw))`
  — adaptive: scales with the current norm.
- **F₂ threshold-alert version** (monitor `F₂ ≥ τ`, one-sided band): `T = (1/k)√((1−ε)τ/w)`.
- **Linear `f` (sum/count/point)**: `λ = 0` → the curvature term vanishes → box
  degenerates to a **slab** = the classic CMY slack countdown.

### Unification of counter-based and F₂

Both are `f(x̄)` vs a band `[L,U]`, with the safe zone from the DC bound; the only
difference is the Hessian eigenvalue: `λ=0` → slab (counter-based), `λ=const·I` →
ball (F₂), general → ADCD quadric. One monitor, one edge check
`isLocallySafe(Δ, x₀, ∇f, λ, L, U)`; keep the scalar-countdown fast-path for the
linear case (no reference-vector broadcast needed).

### Threshold band vs tracking band

- **Threshold/alert**: one-sided band `[−∞, τ]` → fire on crossing.
- **Continuous ε-query**: moving band `[f(x₀)−ε, f(x₀)+ε]` → backend answers
  `f(x̄)=f(x₀)±ε` at all times. Same monitor, different band — this is how
  threshold monitoring and continuous approximate querying unify (Cormode–
  Garofalakis continuous querying = GM with a tracking band).

---

## 8. Optimality vs the Woodruff–Zhang lower bound

At the optimum, communication `Σ_j V_j/T_j` with `w ∝ 1/ε²` (necessary sketch
width) gives worst-case `Θ̃(k/ε²)` — **matching the Woodruff–Zhang STOC'12 tight
lower bound** for continuous `(1±ε)` `F₂` monitoring. Consequences:

- The `1/ε²` and the linear-in-`k` are **fundamental**; no protocol (GM, AutoMon,
  OctoSketch, GOS) beats `k/ε²` adversarially. GOS's savings are **data-dependent**
  (small `V_j/‖Ĉ‖` on stable streams).
- Report GOS's measured bytes as a **fraction of `k/ε²`** — a stronger baseline
  than comparing to naive centralization.
- The relative-error caveat: relative bounds require the norm bounded below
  (WZ tightness / OctoSketch `L1 > ε⁻¹k'τ`); relative error on a near-zero signal
  is fundamentally not cheap.

---

## 9. How it meets the four requirements

| Requirement | Mechanism |
|---|---|
| **Memory efficiency** | `(w,G)` + `w_m`: grouping `G` avoids one sketch per series; `w=Θ(1/ε²)` is the WZ-minimum; delta/aniso flags dropped under high `w_m` (no snapshot / threshold vector) |
| **Computation efficiency** | `w_e`: sampling `p_i` (fewer updates) + threshold size (fewer uploads); both fold into one effective weight |
| **Communication efficiency** | `w_c`: per-cell water-filling thresholds + geometric silence; `Θ̃(k/ε²)` worst case, far less on stable data |
| **Continuous, accurate, fresh** | Theorem 1 bounds every query at **any** `t`, **relative**; freshness cap `T_j ≤ V_jΔ*` bounds staleness age; whole sketch queryable (OctoSketch), monitored `f_m` tighter (GM/AutoMon) |

---

## 10. What is adopted vs contributed (attribution)

- **Water-filling** — classic (information theory / convex optimization; optimal
  power allocation across parallel channels). Already used in ASAP for sampling
  (`p_i ∝ √(f_i/rate_i)`). *Adopted, not contributed.*
- **Per-cell change transmission with a threshold** — OctoSketch [Zhang+ NSDI'24].
  *Adopted.*
- **Function safe zone via DC decomposition of the Hessian (ADCD), gradient/
  Hessian bounds** — AutoMon [Sivan+ SIGMOD'22]; Geometric Monitoring [Sharfman+
  SIGMOD'06]. *Adopted.*
- **Coordinated sampling** — NitroSketch; ASAP's own `AllocateSampleRates`.
  *Adopted.*
- **Lower bound `Θ̃(k/ε²)`** — Woodruff–Zhang [STOC'12]. *Yardstick.*
- **GOS (this doc)** — casts *per-cell threshold allocation* as water-filling with
  **gradient-derived weights** (GM/AutoMon) and a **per-cell query cap**
  (OctoSketch), coupled to **per-site sampling** through a shared ε-budget and a
  granularity floor `T_j ≳ √(V_j(1−p)/p)`, inside one **tunable
  memory/compute/communication objective**. *The synthesis is the contribution;
  the optimization tools are off-the-shelf.*

---

## 11. Implementation notes (controller synthesizes, edge executes)

Everything expensive is a **controller (backend) decision**; the edge only
executes a fixed per-cell comparison.

- **Controller** (offline, per registered metric/query): runs ADCD (AD → Hessian
  eigenvalue bounds → `∇f, λ`), estimates `{V_j}` from the workload, solves (P)'s
  layers A–C, emits `(d, w, G, {p_i}, {T_j}, flags)` via OpAMP. This slots into the
  existing controller multi-objective (`controller-optimization-problem.md` SP-6:
  `min w_bw·bw + w_cpu·cpu + w_mem·mem + …`) — GOS thresholds are new decision
  variables there.
- **Edge**: maintain sketch + acked snapshot; per flush, upload cells with
  `|ΔC_j| ≥ T_j` as a sparse delta; run one generic `isLocallySafe` for monitored
  functions. No AD, no optimization at the edge.
- **Backend**: `apply_delta` into a running merge (`O(#delta cells)`), keeping the
  global sketch continuously queryable within the Theorem-1 envelope, surfaced in
  the `accuracy: ε=…` response annotation.

**Ties to existing code:**
- `ASAPQuery-backend/control_plane/src/epsilon_alloc.rs` — extend to the ε-budget
  split `ε² = ε_sk² + ε_sa² + ε_st²`.
- `ASAPCollector/asap-precompute-go/monitor/sampling_alloc.go` (`AllocateSampleRates`)
  — the sampling water-filling (already present).
- new `threshold_alloc` — the per-cell threshold water-filling (this doc's §7C).
- `data_plane/src/monitor/f2_coord.rs`, `asap-precompute-go/monitor/f2engine.go`
  — generalize the F₂-specific ball to the `(∇f, λ)` DC safe zone; use the relative
  radius `ε‖Ĉ‖/(2k√(dw))`.
- reuse `asap_sketchlib` `CountSketchDelta` + `compute_delta`/`apply_delta`
  (byte-parity Go/Rust) for the sparse per-cell delta wire format.

---

## 12. Open problems / next steps

1. **Anisotropic delta broadcast** — encode `ΔC_ref` as sparse cells (OctoSketch on
   the coordinator→edge path); removes the `O(k)` broadcast amplification.
2. **Relative-error under small norm** — heartbeat / additive floor when `‖Ĉ‖` is
   small (WZ / OctoSketch fundamental limit).
3. **Verified eigenvalue bounds** — AutoMon's numerical `λ` may miss the true
   extreme → reserve an `ε_eig` slice of the budget or use interval bounds.
4. **Empirical validation** — measure achieved communication as a fraction of the
   WZ `k/ε²`, sweep `(w_m, w_e, w_c)` to trace the Pareto surface.

---

### References (attribution)

- G. Cormode, S. Muthukrishnan, K. Yi. *Algorithms for Distributed Functional Monitoring.* SODA 2008 / ACM TALG 2011.
- M. Charikar, K. Chen, M. Farach-Colton. *Finding Frequent Items in Data Streams* (Count-Sketch). ICALP 2002.
- I. Sharfman, A. Schuster, D. Keren. *A Geometric Approach to Monitoring Threshold Functions over Distributed Data Streams.* SIGMOD 2006.
- H. Sivan, M. Gabel, A. Schuster. *AutoMon: Automatic Distributed Monitoring for Arbitrary Multivariate Functions.* SIGMOD 2022.
- Y. Zhang, P. Chen, Z. Liu. *OctoSketch: Enabling Real-Time, Continuous Network Monitoring over Multiple Cores.* NSDI 2024.
- D. Woodruff, Q. Zhang. *Tight Bounds for Distributed Functional Monitoring.* STOC 2012 (arXiv:1112.5153).
- Z. Liu et al. *NitroSketch.* SIGCOMM 2019 (coordinated update sampling).
