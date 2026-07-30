# GOS: A Unified Error / Threshold / Cost Framework for Distributed Edge Telemetry

**One line.** One framework that unifies error-bounded sketching, coordinated
sampling, Geometric Monitoring (GM/AutoMon), and OctoSketch-style change
transmission into a single per-cell decision — reducing to **one atomic quantity**
`e_j = k·T_j` (per-cell backend error) — with a unified relative error bound, a
tunable memory/compute/communication objective, and a closed-form
water-filling solution whose worst-case communication matches the
Woodruff–Zhang lower bound `Θ̃(k/ε²)`.

We call the construction **GOS** (Geometric-OctoSketch).

> **Math lives elsewhere.** This doc states *what's true and why it matters*,
> in prose. Every formula, derivation, and proof is in
> [`sampling-cdm-gos-derivations.md`](./sampling-cdm-gos-derivations.md)
> ("derivations" below) — section references point there. Treat this doc as
> the map, that one as the territory.

---

## 1. Context and requirements

Distributed data collection with a centralized analytics backend, under four
simultaneous constraints:

- **Memory** — cloud memory is priced above CPU; one sketch per series per
  window doesn't scale across a high-cardinality fleet.
- **Computation** — per-sample update work at the edge must stay within
  line-rate CPU budgets on shared instances.
- **Communication** — cross-AZ egress is billed per byte.
- **Freshness** — monitoring queries are long-running over the stream, not
  one-shot "on demand." What matters is the gap between when a sample is
  generated and when it's queryable at the backend, not just query latency
  once landed.

These four axes are exactly the decision variables and constraints of the
optimization in §6.

---

## 2. Positioning: what each prior line gives, and what it lacks

All four lines below are instances of "keep the local drift inside a **safe
zone**, communicate on violation." They differ in the *shape* of the safe
zone and in *what* they bound. (Attribution: §10.)

| Line | Bounds | Safe-zone shape | Continuous query? | Gap |
|---|---|---|---|---|
| **Error-bounded sketching** (Count-Sketch, CMY) | one function, per window | — | window granularity only | intra-window staleness |
| **Geometric Monitoring** (GM/AutoMon) | one scalar `f(x̄)` | DC-quadric (ball / slab / general) | that one function only | not per-cell; not query-general |
| **OctoSketch** | every counter cell `\|ΔC[j]\|<T` | axis-aligned box | whole sketch, any query, any time | function-agnostic (uniform T) |
| **Coordinated sampling** (NitroSketch; ASAP `AllocateSampleRates`) | edge CPU / counts | per-site rate `p_i` | — | ingestion-side only |
| **Lower bound** (Woodruff–Zhang) | `F_p` continuous, `(1±ε)` | — (yardstick) | — | it's a limit, not a protocol |

Key facts that make the unification possible:

- **GM's scalar safe zone is a special case of OctoSketch's box** (the box is
  GM with an `L∞` safe zone); linear functions (sum/count/point) are GM with
  `λ=0` → a slab; `F_2` is GM with a constant Hessian `2I` → a ball. Counter-based
  and `F_2` monitoring are the same monitor at different Hessians (§7).
- **OctoSketch gives "whole sketch continuously queryable"; GM alone doesn't** —
  because it bounds every cell, not one function.
- **GOS = water-filling (classic, already used in ASAP for sampling) to
  allocate per-cell thresholds, weighted by GM/AutoMon's gradient `∇f`, capped
  by OctoSketch's query bound.** The optimization tool is off-the-shelf; the
  contribution is the combination.

---

## 3. The SDK↔collector sampling split (row-admission)

The sampling knob is **per-row admission decided at the SDK**, not a
whole-sketch coin flip at the collector — and the SDK never hashes. A
Count-Sketch/CMS update touches `d` rows; the natural sampled unit is a
*candidate row update* `(item, row)`, not the raw item.

**Three design points**, each doing real work:

1. **Geometric skip-sampling, not per-row coin flips.** The SDK draws one
   geometric gap per *admitted* update and decrements a skip-counter, so the
   expensive RNG cost scales with admits, not with the unsampled row count —
   "always line rate." (`sketchlib-go/common.GeometricSampler`; math and cost
   accounting: derivations §5.)
2. **The SDK decides *which rows*, never *which columns*.** Hashing
   (row → column) happens only for admitted rows, and only at the collector.
3. **Only admitted samples cross the wire** — a sample admitting no row is
   dropped at the source, so the collector skips deserialization and hashing
   for it entirely.

**Applicability.** Update-sampling with `1/p` inverse-probability weighting
is only unbiased for *additive* counters. So the SDK row-samples
**CountMinSketch, CountSketch, and DDSketch** (the `d=1` whole-item case);
**KLL** (non-linear random compaction) and **HLL** (idempotent register-max)
are left unsampled by design — dropping or reweighting either breaks its
guarantee.

**Why per-row beats whole-item at equal edge cost.** Count-Sketch's `(ε,δ)`
guarantee comes from the median over `d` rows, which only concentrates
sampling error the same way it concentrates collision error if each row's
admission is independent. Per-row admission gives that independence, so
sampling noise lands *inside* the median's `δ` guarantee. Whole-item
admission shares one coin across all `d` rows, so a dropped key is missing
from every row at once — an irreducible common-mode term the median cannot
average away. Same edge CPU, strictly better estimator; full derivation
(including why the two variance terms compose in quadrature) in
derivations §3–§5, and design consequence in §4 below.

**Status.** Implemented as designed, on the **SDK-build path**: the OTLP
metrics SDK's `CountSketch`/`CountMinSketch`/`DDSketch` aggregators host a
per-series sampler (`sample_p` knob) and route inserts through
`UpdateStringSampledPerRow` / `InsertWithHashSampledPerRow` (per-row) or the
sketch's built-in `WithSampleP` (DDSketch's `d=1` case). The
**collector-build** path (raw datapoints reach the edge) realizes the
equivalent saving earlier — wire-thinning whole datapoints before decode via
`otlpfilter` — with the collector wrapper applying the same per-row `1/p`
weighting on survivors.

**Single-location sampling.** The design allows exactly one sampling stage
per series, enforced not by configuration discipline but by making the
admission decision itself a **pure, stateless function** of shared inputs
(`admit(seed, occurrence, row) ⇔ U(seed, occurrence, row) < p`, a
splitmix64-derived uniform — `sketchlib-go/common.ConsistentAdmit`) rather
than RNG state. Any pipeline stage that recomputes the decision gets the
*same* admitted set, so re-evaluating it is idempotent — nothing stochastic
is left to re-draw. This is what lets the SDK-build path (decision owned by
the SDK aggregator) and the collector-build path (decision re-derived from
wire-visible `time_unix_nano`) agree without coordination, and it's what a
misconfigured "sample at both stages" plan degrades to (same set twice) 
instead of squaring the drop rate. Contract tests pin filter-survivors ≡
stateless-recomputation ≡ wrapper-touches agreement.

---

## 4. Unified error bound

The backend's reconstructed state diverges from the true state through three
sources: sketch collision error, sampling error, and staleness (unsynced
deltas). The first two are **random** and land inside the *same* median-of-`d`
step (§3), so they compose **in quadrature**; staleness is a **deterministic,
worst-case** bound (each cell can be off by at most its threshold `T_j`), so
it **adds linearly** on top — it cannot be RMS-combined with a random tail.

> **√(ε_sk² + ε_sa²) + ε_st ≤ ε_q**

This holds **continuously**, not only at window boundaries — OctoSketch's
"queryable at any time" property, generalized here to arbitrary queries and
to gradient-weighted monitoring of non-linear functionals (F₂, entropy,
ratios — via a Taylor + DC/Hessian bound, same shape with an added curvature
term). Formal statement (Theorem 1), the generic three-term error
decomposition it's built from, and the non-linear extension: derivations §3
and §12.

### Freshness

A cell only reaches its threshold `T_j` after enough activity accumulates
(`V_j`, its per-window rate), so its own worst-case staleness age is
`T_j / V_j` — quiet cells stay "fresher-looking" only because they change
less, not because they sync faster; a low-activity cell can still go stale
if `T_j` isn't capped. Guaranteeing a query-facing freshness target `Δ*`
means capping every cell's threshold at `V_j·Δ*`. This cap is one of the
three clamps in §7's water-filling solution.

---

## 5. Cost models

Four cost terms, one per requirement in §1:

| Term | What it counts | Dominant driver |
|---|---|---|
| `Memory_edge` | Sketch state at each site, times group count `G`, plus optional delta/aniso bookkeeping | Sketch width `w` and shape `(d,w)`; grouping `G` |
| `Comp_sdk` | SDK-side geometric-sampler RNG | `∝ Σ_r p_{i,r}` (admits), **not** the unsampled row count |
| `Comp_coll` | Collector hashing/update (admitted rows only) + delta uploads | `∝ Σ_r p_{i,r}` for hashing; `∝ Σ_j V_j/T_j` for uploads |
| `Comm` | Edge→backend bytes | `∝ Σ_j V_j/T_j`, scaled down by the fraction of samples that admit no row at all |
| `Cost_coord` | Backend running-merge + incremental delta apply | `∝ Σ_j V_j/T_j` |

**The one structural insight that matters:** the upload term inside
`Comp_coll`, all of `Comm`, and the apply term inside `Cost_coord` are *all*
proportional to `Σ_j V_j/T_j`. They collapse into one effective weight
`W = w_c·b + w_e·c_s + w_b·c_a`. Consequence: **the cost weights don't change
the shape of the optimal threshold** — the accuracy constraint alone pins
`T_j` (§7); the weights instead choose the *structural* knobs (uniform vs.
anisotropic, delta vs. full, sampling rate `p`, per-edge reference vectors),
which is where the actual memory/compute/communication tradeoffs live.

---

## 6. The optimization problem

Minimize the weighted sum of the four §5 cost terms over the sketch shape
`(d,w)`, grouping `G`, per-site sampling rates `{p_i}`, and per-cell
thresholds `{T_j}`, subject to: every query's accuracy bound
(§4's `√(ε_sk²+ε_sa²) + staleness ≤ ε_q`), every monitored function's
accuracy bound, the freshness cap `T_j ≤ V_j·Δ*`, and confidence
`d ≥ log₂(1/δ)`. Full constraint set: derivations §7 (intro) and
`controller-optimization-problem.md` SP-6, which this slots into as
additional decision variables. `w_b` (coordinator cost) is the
least-weighted axis by requirement — the backend is not where the fleet's
cost pressure lives.

---

## 7. Solution: hierarchical decomposition + two water-fillings

Solving the full problem directly is intractable; it decomposes into three
layers that can each be solved cheaply:

**Layer A — outer, small enumeration.** Fix `d = ⌈log₂(1/δ)⌉`, then choose
sketch width `w` (sets `ε_sk`) and grouping `G` to trade the memory term
against the accuracy the sketch must supply.

**Layer B — budget split.** Staleness is deterministic, so it comes off the
top of the accuracy budget `ε_q` **linearly first**; what's left splits
between sketch and sampling error **in quadrature**. The staleness fraction
itself is a 1-D convex tradeoff (more staleness tolerance → looser
thresholds → less communication, but a smaller random budget → tighter
sampling → more edge CPU) — unique because both sides are monotone. Code:
`epsilon_alloc.rs`'s `split_budget`.

**Layer C — two water-fillings, same tool, two variables:**

- **Sampling (per-site).** The textbook per-key water-filling
  (`p_i ∝ √(f_i/rate_i)`) has been **retired for sketch sampling**: a
  sketch's point/L2 error is bounded by the sketch's *norm*, not any single
  key's frequency, so a per-key rate doesn't buy the accuracy the formula
  assumes. What's actually implemented is a simpler whole-sketch ε-floor,
  `p_i = 1/(1+ε²·rate_i)` — see `sampling_alloc.rs`'s `epsilon_sample_floor`.
  The per-key formula remains valid only when a key is counted exactly
  *outside* the sketch, where sampling it would be pointless anyway.
- **Thresholds (per-cell — this is GOS).** A closed-form water-filling over
  `T_j`, weighted by activity `V_j` and priced by query/function sensitivity
  `c_j` (`k|r_{q,j}|` or `k|g_j|`), clamped by three things: a
  **sampling-coupling floor** (don't transmit finer than you sampled —
  `T_j ≳ √(V_j(1−p)/p)`), a **query cap**, and the **freshness cap** from
  §4. Clamped cells release budget back to the pool; a few redistribution
  passes converge it. Closed form, the Lagrangian derivation, and the
  clamped iterative solution: derivations §7 and Theorem 2 (§12). Code:
  `sketches/gos_threshold.go` (Go) / `threshold_alloc.rs` (Rust).

**Reading the result without the algebra:** high-activity cells get bigger
thresholds (don't chase noise on a cell that changes constantly anyway);
cells the monitored function is *sensitive to* get smaller thresholds
(report those early); everything is capped by the query's own accuracy
budget and by the freshness target. The counter-based (linear) and F₂
(quadratic) cases are the *same* monitor at different Hessians — `λ=0`
degenerates the general quadric safe-zone to a slab (the classic CMY
countdown); `λ=const·I` gives a ball. One `isLocallySafe` check serves both.

### Threshold vs. tracking band

The same per-cell mechanism serves two different query modes, depending only
on which band is checked: a one-sided band `[−∞, τ]` gives
threshold/alerting semantics (fire on crossing); a symmetric moving band
`[f(x₀)−ε, f(x₀)+ε]` gives continuous approximate-query semantics (the
backend can answer `f(x̄)=f(x₀)±ε` at any time). This is how alerting and
continuous querying — historically separate literatures — turn out to be one
mechanism with a different band. Derivations §10.

---

## 8. Optimality vs. the Woodruff–Zhang lower bound

`Σ_j V_j/T_j` (§5) is an upload *rate*; the Woodruff–Zhang bound
`Θ̃(k/ε²)` is *total* communication to maintain one continuous `(1±ε)` `F_2`
estimate across `k` sites. Normalized the same way (derivations, and the
"one-round" unit `k·S` in [`gos-eval-results.md`](gos-eval-results.md) §3),
GOS's worst case matches WZ exactly — the `1/ε²` scaling and the linear-in-`k`
factor are fundamental, and no protocol in §2's family beats them
adversarially. GOS's actual savings are **data-dependent**: they come from
`V_j/‖Ĉ‖` being small on real, non-adversarial streams, not from beating the
bound. Report measured bytes as a *fraction* of `k/ε²` — a meaningfully
stronger baseline than comparing against naive centralization. One caveat
that doesn't go away: relative-error bounds need the norm bounded away from
zero (both here and in the WZ tightness construction) — relative error on a
near-zero signal is fundamentally not cheap, at any layer of this stack.

---

## 9. How it meets the four requirements

| Requirement | Mechanism |
|---|---|
| **Memory** | Grouping `G` avoids one sketch per series; `w=Θ(1/ε²)` is the WZ-minimum sketch width; delta/aniso bookkeeping drops under high memory weight |
| **Computation** | Sampling `p_i` (fewer updates) and threshold size (fewer uploads) fold into one effective weight (§5) |
| **Communication** | Per-cell water-filling thresholds + geometric silence; `Θ̃(k/ε²)` worst case, far less on stable data (§8) |
| **Continuous, accurate, fresh** | Theorem 1 bounds every query at any time, relative; freshness cap bounds staleness age; whole sketch stays queryable, monitored functions get a tighter (gradient-weighted) bound |

---

## 10. What is adopted vs. contributed (attribution)

- **Water-filling** — classic (information theory / convex optimization).
  Already used in ASAP for sampling (`p_i ∝ √(f_i/rate_i)`). *Adopted.*
- **Per-cell change transmission with a threshold** — OctoSketch [Zhang+
  NSDI'24]. *Adopted.*
- **Function safe zone via DC decomposition (ADCD), gradient/Hessian
  bounds** — AutoMon [Sivan+ SIGMOD'22]; Geometric Monitoring [Sharfman+
  SIGMOD'06]. *Adopted.*
- **Coordinated sampling + geometric skip-sampling** — NitroSketch [Liu+
  SIGCOMM'19]; ASAP's own `AllocateSampleRates` and
  `sketchlib-go/common.GeometricSampler`. *Adopted.*
- **Lower bound `Θ̃(k/ε²)`** — Woodruff–Zhang [STOC'12]. *Yardstick.*
- **GOS (this doc)** — casts per-cell threshold allocation as water-filling
  with gradient-derived weights (GM/AutoMon) and a per-cell query cap
  (OctoSketch), coupled to per-site sampling through a shared ε-budget and a
  granularity floor, inside one tunable memory/compute/communication
  objective. *The synthesis is the contribution; the optimization tools are
  off-the-shelf.*

---

## 11. The 2026-07 redesign: insert-time detection, no sub-window, for all 6 families

§1–10 establish *what threshold to use*. This section is about *when
transmission actually happens* — replacing a periodic sub-window tick with
per-insert detection, uniformly across Sum, CMS, CountSketch, DDSketch, KLL,
and HLL, on the existing OTLP/`SketchEnvelope` wire.

**The unified mechanism.** Check at insert time whether the
accumulated-since-last-sync delta crosses the cell's threshold (§7 / §4;
exact per-family formulas: derivations §8). If it crosses, that delta needs
to reach the backend — the sole purpose is keeping the backend's
reconstructed state accurate. Alerting and every other query-time decision
happen entirely at the backend against that synced state; the edge no
longer decides to alert. This **retires the old, separate "continuous
monitoring" alerting path** (`monitor.Engine.Observe` →
`sendReportLocked`) — Sum becomes just another family running the same
insert-time check. `Engine.Observe` today conflates two things that must be
split, not deleted together: `obsCount++` (rate tracking, feeds the
coordinator's sampling-rate grant — keep) and the alerting check that calls
`sendReportLocked` (retire).

**Wake-on-demand flush.** The naive reading — "insert-time means bypass the
pipeline, send out-of-band" — was considered and rejected. The OTLP export
chain (SDK `PeriodicReader` → collector processor → exporter) is unchanged;
only *when a flush cycle runs* changes, from purely timer-driven to
timer-**or**-woken:

```go
for {
    select {
    case <-ticker.C:  // slow fallback, in case a wake is ever missed
        flush()
    case <-wakeCh:     // fired the instant something crosses threshold
        flush()
    }
}
```

The insert path pushes a non-blocking wake (`select { case wakeCh <- struct{}{}: default: }`)
and `flush()` keeps using the existing snapshot/delta machinery unchanged —
only the trigger changed. This makes the old periodic full-matrix
divergence pre-check redundant (an empty dirty-set at flush time already
means "nothing worth sending," computed once per crossing instead of by a
periodic O(dw) scan) — that whole mechanism is dead code to delete, not a
fallback to keep. CMS's local point-query read retires alongside it: once
cells reset in place at insert time, a local `min`-read is corrupted by any
recently-reset row; all point/alert reads move to the backend's copy.

**Per-family mechanism** (math: derivations §8):

| Family | Detection unit | Reset on send? | Note |
|---|---|---|---|
| CountSketch (isotropic) | matrix cell | zero it | norm tracked incrementally, O(1) |
| CountMinSketch | matrix cell | zero it | same mechanism, L1-scale threshold |
| DDSketch | bucket count | zero it | bucket count tracked incrementally, no extra config |
| Sum | scalar | zero it (subtract reported amount) | degenerate 1-cell case |
| KLL | whole sketch | full reset | trigger is `Count() ≥ εN`, not per-cell — same existing disjoint-segment mechanism, re-triggered by count instead of a timer |
| HLL | register | never (MAX-merge is idempotent) — only clears a dirty flag | trigger is on the *linearized* register value, not the raw one |

Backend reconstruction is unaffected for every additive family: summing
every fragment ever received for a cell telescopes to the true cumulative
value regardless of how many times, or when, it was reset — which is why
resets can happen asynchronously per cell without breaking correctness.

**Cold start is intentional, not a bug.** Every threshold scales with an
accumulated quantity that starts near zero, so the first few inserts cross
it almost immediately — the backend gets a usable estimate fast rather than
waiting for data to build up. No floor is needed to suppress this.

**Open, not blocking:**
- **Anisotropic CountSketch's per-cell activity** needs redefining under
  async per-cell resets (the old "diff against one snapshot" definition
  assumed synchronized flushes). Leading candidate: a per-cell EMA of `|Δ|`
  — cheap, reset-timing-independent — but it's a heuristic replacing the
  derivation's exact `Activity_j=V_j`, unverified against §7's guarantee.
  Its water-filling solve also still needs a periodic O(dw) pass, unlike
  every other family here.
- **DDSketch's unbounded bucket-array growth** on outlier values is a real
  memory-safety gap, independent of this redesign (tracked separately,
  sketchlib-go#72).
- **HLL's small-cardinality regime** — the register-change accuracy proof
  assumes "sufficiently large" cardinality; behavior with mostly-zero
  registers early in a window is unverified. A candidate fix (always send a
  register's first nonzero write) is proposed, unverified.

---

## 12. Implementation notes (controller synthesizes, edge executes)

Everything expensive is a controller decision; the edge only executes a
fixed per-cell comparison. The controller runs the ADCD/eigenvalue-bound
analysis, estimates `{V_j}` from the workload, solves §7's layers, and ships
**scalars** (`ε_delta`, sites, aniso flag) via OpAMP — not the full per-cell
`{T_j}` vector; the edge reconstructs `{T_j}` locally from those scalars
plus its own live `{V_j}` (`sketches/gos_threshold.go`), so the `O(d·w)`
vector never crosses the wire. The edge maintains the sketch, recomputes
`{T_j}` per flush (or, under §11, per insert), and uploads only cells that
crossed. The backend applies each delta into a running merge
(`O(#delta cells)`) and annotates responses with the resulting
`accuracy: ε=…`.

**Status as of this writing** — the pieces exist but the loop is **not
fully wired end-to-end**:
- **Live today:** the scalar CDM loop (register → grant → countdown →
  report → alert) and the sampling grant path (`Grant.SampleP` →
  `otlpfilter` → wrapper `WithSampleP`).
- **Implemented but unreachable from a production config:** the control
  plane derives and emits the scalar GOS knobs, and the edge consumes them
  (`applyGosMode` → `gosThresholdMatrix`) — but no collector processor yet
  parses those YAML keys into config, so the per-cell delta-gating path
  only runs under test/eval, not production. The sampling↔threshold
  coupling floor (§7) is similarly implemented but inert — its only
  production-shaped caller hardcodes `SampleP=1`. Closing this (a knob
  parser + threading the granted `p` into `GosParams`) is the remaining hop.

---

## 13. Open problems

1. **Anisotropic delta broadcast, partially done.** The coordinator→edge
   sparse-cell encoding is implemented and measured (removes the `O(k)`
   broadcast amplification — see `gos-eval-results.md` §2). Still open:
   giving that broadcast gate anisotropic per-cell thresholds, matching what
   the edge→coordinator upload path already does.
2. **Relative error under small norm** — needs a heartbeat/additive floor
   when `‖Ĉ‖` is small (a WZ/OctoSketch fundamental limit, not a bug).
3. **Verified eigenvalue bounds** — AutoMon's numerical `λ` may miss the
   true extreme; reserve an `ε_eig` budget slice or use interval bounds.
4. **Empirical validation** — measure achieved communication as a fraction
   of `k/ε²`, sweep the cost weights to trace the Pareto surface.

---

### References (attribution)

- G. Cormode, S. Muthukrishnan, K. Yi. *Algorithms for Distributed Functional Monitoring.* SODA 2008 / ACM TALG 2011.
- M. Charikar, K. Chen, M. Farach-Colton. *Finding Frequent Items in Data Streams* (Count-Sketch). ICALP 2002.
- I. Sharfman, A. Schuster, D. Keren. *A Geometric Approach to Monitoring Threshold Functions over Distributed Data Streams.* SIGMOD 2006.
- H. Sivan, M. Gabel, A. Schuster. *AutoMon: Automatic Distributed Monitoring for Arbitrary Multivariate Functions.* SIGMOD 2022.
- Y. Zhang, P. Chen, Z. Liu. *OctoSketch: Enabling Real-Time, Continuous Network Monitoring over Multiple Cores.* NSDI 2024.
- D. Woodruff, Q. Zhang. *Tight Bounds for Distributed Functional Monitoring.* STOC 2012 (arXiv:1112.5153).
- Z. Liu, R. Ben-Basat, G. Einziger, Y. Kassner, V. Braverman, R. Friedman,
  V. Sekar. *NitroSketch: Robust and General Sketch-Based Monitoring in Software
  Switches.* SIGCOMM 2019.
