# Continuous Monitoring over Tumbling Windows — Cost Analysis of the Existing Sketch Families

<!-- Design metadata -->

## TL;DR

Cost and error model for summary families under continuous tumbling-window monitoring.

**Status:** active

**MVP relationship:** in-scope.

This document is design-level: it defines scope, behavior, constraints, and trade-offs; implementation details are intentionally out of scope.


> **Design-doc / paper section** — theory only. For each sketch family we
> already ship (Sum, DDSketch, KLL, Count-Min, Count-Sketch, HyperLogLog),
> derive, under the **continuous distributed monitoring (CDM)** model
> [cormode2013survey] with a **tumbling-window backend**, the four costs:
> **accuracy bound**, **edge memory/space**, **edge compute complexity**, and
> **communication cost**. Citation keys are placeholders; see
> [references](#references).
>
> Companion to [continuous-windows-related-work.md](continuous-windows-related-work.md).
> The multivariate/covariance family is intentionally **out of scope** here.

---


- A **tumbling window is the degenerate-optimal case for CDM**: each window is an
  independent monitoring epoch with a **known horizon** and a **reset** at the
  boundary — no two-window decomposition, no persistent prefix store, and
  **error does not compound** (mergeability), so a window's answer carries
  exactly the base sketch error `ε`.
- Two emission disciplines, analyzed separately:
  1. **Boundary emission** (answer-at-close): each edge ships one sketch per
     window → communication `Θ(k·S)` per window, `S` = sketch size. This is the
     baseline and is already minimal for "read at the boundary."
  2. **Continuous intra-window monitoring** (early alert / fresh-before-close):
     run a CDM protocol inside each window, reset at the boundary. Pays off only
     when the window `T_w` ≫ the freshness target.
- **Additive families** (Sum, CMS, Count-Sketch, DDSketch buckets) merge by
  summation `O(size)` and let a *linear* threshold collapse to a **scalar
  countdown**; **non-additive** families (KLL selection, HLL register-max) need
  a structural merge and cannot linearize.
- Within a window, **additive non-negative aggregates are monotone** ⇒ the clean
  `O(k log τ/k)` countdown bound applies; **quantile/heavy-hitter/ratio**
  functions stay non-monotone even within a window.

---

## 1. Model, notation, and the two disciplines

**CDM + tumbling.** `k` edges (observers) each summarize a local substream; a
coordinator (backend) answers queries aligned to fixed tumbling boundaries of
size `T_w`. Over a horizon `W` there are `W/T_w` windows. Each window is an
**independent** CDM instance: accumulate within the window, answer/merge at the
boundary, reset.

| Symbol | Meaning |
| --- | --- |
| `k` | number of edges |
| `T_w`, `W` | tumbling window size; total horizon (`W/T_w` windows) |
| `N` | items per window across all edges (`N_i` per edge, `N = Σ N_i`) |
| `ε`, `δ` | sketch accuracy parameter; failure probability |
| `α` | DDSketch relative accuracy |
| `U` | value/key universe (`v_max/v_min` dynamic range for DDSketch) |
| `S` | wire size of one sketch (family-specific, derived below) |
| `τ` | alert threshold (continuous) / freshness target |

**Discipline A — boundary emission (one-shot per window).** Edge builds the
window sketch, ships it once at the boundary; backend merges the `k` sketches.
This is the CDM *degenerate* case (a single "report" per window). Communication
per window `= k·S`.

**Discipline B — continuous intra-window monitoring.** Inside a window the edges
run a threshold/value-monitoring protocol with communication suppression, reset
at the boundary. Only relevant when `T_w` ≫ freshness target; otherwise A
dominates and B adds nothing.

> **Accuracy under tumbling is clean.** A window answer is the merge of the
> per-edge per-window sketches. By mergeability [agarwal2013mergeable] the merge
> **does not compound error**, so the answer carries exactly the base error `ε`
> (or `α`) — *no* boundary error `ε_w` (unlike bounded-space sliding/`[a,b]`).
> Every "accuracy bound" below is therefore just the base sketch guarantee,
> preserved across the edge merge.

The general communication shapes we will instantiate per family:

```
Discipline A (boundary):        Comm = (W/T_w) · k · S
Discipline B (continuous):
  • linear threshold f=⟨w,·⟩:   Comm = (W/T_w) · O(k log 1/ε)      scalars  (countdown)
                                 lower bound  Ω(k log(τ/k)) per window (deterministic)
  • value/quantile/HH tracking: Comm = (W/T_w) · O((k/ε) log N)    (Yi–Zhang, matching)
  • count (sum) within (1±ε):   Comm = (W/T_w) · O((√k/ε) log N)   (Huang–Yi–Zhang, randomized)
  • distinct (F0)/F2 tracking:  Comm = (W/T_w) · Õ(k/ε²)           (Woodruff–Zhang bound)
```

These problem-level bounds are *sketch-agnostic*; per family we specialize `S`
and note whether the family can realize the linear-threshold collapse.

---

## 2. Per-family derivations

For each: **accuracy** (base guarantee, preserved by merge) · **edge memory**
`S` · **edge compute** (hot path / merge) · **communication** (A and B).

### 2.1 Sum / Count (additive, monotone)

```
Accuracy:    exact (additive merge is exact up to float; threshold ε-relaxable)
Edge memory: S = O(1)   (one accumulator per series; whole-stream O(1))
Compute:     hot path O(1) per sample (increment);   merge O(k)
Communication:
  A (boundary):   (W/T_w)·O(k)             words
  B (threshold "window sum > τ", early alert):
     within a window the sum is MONOTONE ↑ ⇒ clean countdown:
        per window  O(k log 1/ε)  (approx),   Ω(k log(τ/k))  (det. lower bound)
  B (value, track sum within (1±ε) through the window):
        per window  O((√k/ε) log N)  randomized [huang2012randomized]
                    O((k/ε)  log N)  deterministic
```

Sum is the textbook case: tumbling restores monotonicity, so the early-alert
countdown is `n`-independent and optimal.

### 2.2 DDSketch — quantiles, relative error `α` (additive buckets)

```
Accuracy:    relative-error α on the quantile VALUE: |q̂−q|/q ≤ α
             (bucket counts are additive ⇒ merge preserves α, no compounding)
Edge memory: S = B = O((1/α)·log(v_max/v_min))   buckets   (sparse in practice)
Compute:     hot path O(1) per sample (bucket index = ⌈log_γ v⌉, increment);
             merge O(k·B)  (add bucket vectors)
Communication:
  A (boundary):   (W/T_w)·O(k·B)            words   (delta-encode bytes ↓)
  B (threshold "p_φ in window > v*"):
     the condition is a LINEAR functional of bucket counts:
        g(c) = Σ_{b≤j*} c[b] − φ·Σ_b c[b] ≷ 0     (j* = bucket(v*))
     ⇒ a single scalar countdown:  per window  O(k log 1/ε)
     CAVEAT: g has a negative coefficient ⇒ NON-monotone even within a window
             ⇒ use the ε-relaxed / value-monitoring variant, not the clean
               monotone bound.
  B (value, track p_φ within α through the window):
        per window  O((k/ε) log N)   [yi2013optimalhh]  (matching)
```

DDSketch is the *additive* quantile family: it realizes the linear-threshold
collapse to a scalar (cheap early alert), unlike KLL.

### 2.3 KLL — quantiles, additive rank error `ε` (non-additive)

```
Accuracy:    additive rank error: Pr[|rank(q̂)−rank(q)| ≤ εN] ≥ 1−δ
             (mergeable [karnin2016kll], no compounding)
Edge memory: S = O((1/ε)·log log(1/(εδ)))   — space-OPTIMAL for quantiles,
             ≪ DDSketch for tight ε
Compute:     hot path O(1) amortized per sample;   merge O(k·S) (compaction)
Communication:
  A (boundary):   (W/T_w)·O(k · (1/ε) log log(1/εδ))   words
  B (continuous): KLL is NON-additive ⇒ cannot linearize a quantile threshold to
                  a scalar; the problem-level bound still holds
                  per window  O((k/ε) log N)   but the protocol must ship sketch
                  updates rather than a single scalar — strictly costlier
                  constant factors than DDSketch for the SAME guarantee class.
```

**Trade vs DDSketch:** KLL wins on *memory* (Discipline A boundary size) for
tight `ε`; DDSketch wins on *continuous monitoring* (Discipline B) because it is
additive and linearizable.

### 2.4 Count-Min (CMS) — frequency, `L1` error (additive)

```
Accuracy:    f̂(x) ≤ f(x) + ε·N  w.p. 1−δ      (one-sided over-estimate, L1)
             width w=⌈e/ε⌉, depth r=⌈ln 1/δ⌉; additive merge, no compounding
Edge memory: S = w·r = O((1/ε)·log(1/δ))   counters   (universe-independent)
Compute:     hot path O(r)=O(log 1/δ) per sample (r hashes+increments);
             merge O(k·w·r)
Communication:
  A (boundary):   (W/T_w)·O(k · (1/ε) log(1/δ))   words
  B (point threshold "f(x) in window > τ", fixed x):
     a single CMS cell-readout is a sum of counts ⇒ MONOTONE within window
        per window  O(k log 1/ε)   (clean countdown, n-independent)
  B (track all φ-heavy-hitters):
        per window  O((k/ε) log N)  [yi2013optimalhh]  (matching)
```

### 2.5 Count-Sketch (CS) — frequency, `L2` error (additive)

```
Accuracy:    f̂(x) = f(x) ± ε·‖f‖_2  w.p. 1−δ   (unbiased, L2 — better on skew)
             width w=O(1/ε²), depth r=O(log 1/δ); additive merge, no compounding
Edge memory: S = w·r = O((1/ε²)·log(1/δ))   counters   (1/ε² vs CMS's 1/ε)
Compute:     hot path O(r)=O(log 1/δ) per sample;   merge O(k·w·r)
Communication:
  A (boundary):   (W/T_w)·O(k · (1/ε²) log(1/δ))   words
  B (point threshold): per-item readout is signed but a linear functional ⇒
        per window  O(k log 1/ε) (with the non-monotone caveat of §2.2)
  B (track φ-heavy-hitters by L2): per window  O((k/ε) log N)
```

CS costs a `1/ε` factor more memory than CMS for the stronger `L2` guarantee;
both are additive and linearizable.

### 2.6 HyperLogLog (HLL) — distinct count, relative error (non-additive)

```
Accuracy:    relative std-error ≈ 1.04/√m ⇒ for (1±ε):  m = O(1/ε²) registers
             (mergeable by register-wise MAX — NON-additive, but no compounding)
Edge memory: S = m registers ≈ O((1/ε²)·log log N) bits   (per series — this IS
             the cardinality-proportional floor if scoped per-series)
Compute:     hot path O(1) per sample (one register max-update);  merge O(k·m)
Communication:
  A (boundary):   (W/T_w)·O(k·m) = (W/T_w)·O(k/ε²)   words
  B (threshold "distinct in window > τ"):
     distinct count is MONOTONE ↑ within a window (inserts only) ⇒ thresholdable,
     BUT it is not a simple sum (dedup) and HLL is register-MAX (non-additive):
        per window  Õ(k/ε²)   [woodruff2012tight] (F0 tracking; non-linearizable)
```

HLL mirrors KLL: non-additive ⇒ no scalar collapse, and its `1/ε²` memory is the
dominant per-series cost.

---

## 3. Summary

### 3.1 Per-window costs

| Family | Accuracy (preserved by merge) | Edge memory `S` | Hot-path / sample | Merge (k sketches) | Boundary comm / window | Continuous comm / window |
| --- | --- | --- | --- | --- | --- | --- |
| **Sum** | exact | `O(1)` | `O(1)` | `O(k)` | `O(k)` | `O(k log 1/ε)` (mono) |
| **DDSketch** | rel. `α` (value) | `O((1/α)log U)` | `O(1)` | `O(kB)` | `O(kB)` | `O(k log 1/ε)`\* / `O((k/ε)log N)` |
| **KLL** | add. `ε` (rank) | `O((1/ε)log log 1/εδ)` | `O(1)` am. | `O(kS)` | `O(kS)` | `O((k/ε)log N)`, non-lin. |
| **CMS** | `+εN` (L1) | `O((1/ε)log 1/δ)` | `O(log 1/δ)` | `O(kwr)` | `O(k/ε·log 1/δ)` | `O(k log 1/ε)` (pt) / `O((k/ε)log N)` |
| **Count-Sketch** | `±ε‖f‖₂` (L2) | `O((1/ε²)log 1/δ)` | `O(log 1/δ)` | `O(kwr)` | `O(k/ε²·log 1/δ)` | `O(k log 1/ε)`\* / `O((k/ε)log N)` |
| **HLL** | rel. `ε` | `O((1/ε²)loglog N)` | `O(1)` | `O(km)` | `O(k/ε²)` | `Õ(k/ε²)`, non-lin. |

\* linear-threshold collapse applies but is **non-monotone within the window**
(negative coefficient) — use the ε-relaxed / value variant.

Total over the horizon = **per-window × `W/T_w`**.

### 3.2 Cross-cutting observations

1. **Boundary emission is the operative cost.** For a tumbling backend read at
   the boundary, total communication is `(W/T_w)·k·S`, dominated by sketch size
   `S`. The cost ranking is therefore the size ranking:
   `Sum ≪ DDSketch ≈ KLL ≈ CMS ≪ Count-Sketch ≈ HLL` (the last two carry `1/ε²`).
2. **Continuous monitoring only earns its keep when `T_w` ≫ freshness target.**
   Otherwise Discipline A already gives a fresh answer every window and B is
   redundant.
3. **Additivity decides the cheap path.** Sum, CMS, Count-Sketch, DDSketch
   buckets are additive ⇒ linear thresholds collapse to a **scalar countdown**
   (`O(k log 1/ε)`, `n`-independent). KLL and HLL are non-additive ⇒ continuous
   monitoring must ship sketch updates (`O((k/ε)log N)` / `Õ(k/ε²)`).
4. **Tumbling restores monotonicity — but only for additive non-negative
   aggregates** (count, sum, point-frequency, distinct). Quantile / heavy-hitter
   / ratio thresholds remain non-monotone even within a window, so they fall to
   the value-monitoring bound, not the clean countdown.
5. **No error compounding.** Every per-window answer carries exactly the base
   `ε`/`α`; tumbling avoids the additive boundary error that bounded-space
   sliding/`[a,b]` incurs.
6. **Memory is `N`- and `T_w`-independent** for every family (the point of
   sketches) — a larger tumbling window costs more *communication frequency
   amortization benefit*, not more edge memory.

---

## References

Citation keys are placeholders; bind full entries when integrating into the paper.

- `cormode2013survey` — Cormode, [The Continuous Distributed Monitoring Model](https://doi.org/10.1145/2481528.2481530), SIGMOD Record 2013.
- `cormode2008functional` — Cormode, Muthukrishnan, Yi, [Algorithms for Distributed Functional Monitoring](https://doi.org/10.1145/1921659.1921667), SODA 2008 / ACM TALG 2011 (countdown bounds).
- `yi2013optimalhh` — Yi, Zhang, [Optimal Tracking of Distributed Heavy Hitters and Quantiles](https://doi.org/10.1007/s00453-011-9584-4), Algorithmica 2013 (PODS 2009) — `O((k/ε)log n)` matching.
- `huang2012randomized` — Huang, Yi, Zhang, [Randomized Algorithms for Tracking Distributed Count, Frequencies, and Ranks](https://doi.org/10.1145/2213556.2213588), PODS 2012 — `O(√k/ε·log n)` count.
- `woodruff2012tight` — Woodruff, Zhang, [Tight Bounds for Distributed Functional Monitoring](https://doi.org/10.1145/2213977.2214063), STOC 2012 — `k/ε²` lower bounds (F0, F2).
- `agarwal2013mergeable` — Agarwal et al., [Mergeable Summaries](https://doi.org/10.1145/2500128), PODS 2012 / ACM TODS 2013 (no-compounding merge).
- `masson2019ddsketch` — Masson, Rim, Lee, [DDSketch](https://doi.org/10.14778/3352063.3352135), VLDB 2019.
- `karnin2016kll` — Karnin, Lang, Liberty, [Optimal Quantile Approximation in Streams](https://doi.org/10.1109/FOCS.2016.17) (KLL), FOCS 2016.
- `cormode2005cm` — Cormode, Muthukrishnan, [The Count-Min Sketch and its Applications](https://doi.org/10.1016/j.jalgor.2003.12.001), J. Algorithms 2005.
- `charikar2002frequent` — Charikar, Chen, Farach-Colton, [Finding Frequent Items in Data Streams](https://doi.org/10.1007/3-540-45465-9_59) (Count-Sketch), ICALP 2002.
- `flajolet2007hyperloglog` — Flajolet et al., [HyperLogLog](https://doi.org/10.46298/dmtcs.3545), AofA 2007.
