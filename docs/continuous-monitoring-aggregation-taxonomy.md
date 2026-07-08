# Continuous Monitoring — Aggregation Taxonomy & the `[t1,t2]` Window Question

> **Part of the CDM/GOS theory set.** The canonical, unified derivations live in [`sampling-cdm-gos-derivations.md`](sampling-cdm-gos-derivations.md) — start there. This doc is retained for the full per-series×window / series×timestamp / series×window taxonomy and the (agg_id, group_key) monitor-keying rule, which the canonical doc does not reproduce.


> Design note. Companion to
> [continuous-monitoring-tumbling-cost-analysis.md](continuous-monitoring-tumbling-cost-analysis.md)
> (the per-family cost theory) and
> [continuous-windows-related-work.md](continuous-windows-related-work.md)
> (the windowing substrate). This note pins down **which aggregation shapes need
> a continuous-monitoring protocol, which don't, and what model each uses**, and
> records the open question for arbitrary `[t1,t2]` sub-window queries.

## TL;DR

Continuous monitoring is **not one model**. It depends on which axis you
aggregate over:

| # | Aggregation shape | Monitoring model | Coordinator? |
| - | --- | --- | --- |
| 1 | **per-series × window** (one series, over the open tumbling window) | local ε·‖f‖ delta-emission (I-frame/P-frame) | **no** |
| 2 | **series × timestamp** (spatial fan-in at an instant) | mergeable sparse-sketch merge | no (no delta) |
| 3 | **series × window** (2D: spatial **and** temporal, fused) | CMY slack-countdown, `k`-aware | **yes** |

The backend stores **tumbling windows**, so monitoring always runs **within a
finite window** (one epoch, reset at the boundary) — never the unbounded-stream
functional-monitoring problem.

## 1. Per-series × window — local, no coordinator

Goal: make the **most recent OPEN (incomplete) tumbling window** queryable to a
bounded ε, while closed windows stay exact (the full sketch ships at the
boundary). There is **no distributed coordinator** here: a single series knows
its own threshold. It emits a delta to the backend whenever its local sketch has
diverged from the backend's last-acked copy by more than the **functional's own
error norm × ε**:

| family | norm | threshold |
| --- | --- | --- |
| Sum / Count, Count-Min | L1 (`N`) | `ε·N` |
| Count-Sketch | L2 (`‖f‖₂`) | `ε·‖f‖₂` |
| DDSketch | relative `α` | per-value `α` |
| KLL | rank (`εN`) | `εN` |
| HLL | relative (`1.04/√m`) | register divergence |

This is **Cormode-2005 sketch+prediction tracking** (the I-frame/P-frame delta
discipline), and it reuses the existing `delta_transmission` machinery. Its
elegance: it composes the *streaming-sketch* error and the
*transmission-suppression* error at the **same ε**, so the open window is
queryable at the sketch's native accuracy. The `k`-aware CMY coordinator is
**overkill** for this case (and `k=1` collapses to exactly this).

## 2. Series × timestamp — spatial merge, no monitoring

Aggregating across many edges at one instant is a **mergeable-summary fan-in**:
each edge ships its sparse partial sketch observing part of the stream; the
backend merges. Every partial is needed for accuracy, so there is nothing to
suppress → **no delta transmission, no monitoring protocol**.

## 3. Series × window — 2D, `k`-aware coordinator

This is the genuine [CDM](https://doi.org/10.1145/2481528.2481530) case: `k`
edges each contribute to one **global windowed functional**, and we bound
communication while tracking it to ε. The coordinator allocates each edge a
slack `Δ/(2k)` (`Δ = τ − global_estimate`), so `Σ slack = Δ/2` and
`true_global < (τ+estimate)/2 < τ` while `estimate < τ` (no missed crossing); it
fires at `Δ ≤ ε·τ`. See the implementation in `asap-precompute-go/monitor` (edge)
and `ASAPQuery-backend/data_plane/src/monitor` (coordinator).

**Departure from the classic CDM model.** Cormode-2013 assumes each site owns
**one** time series. Here each collector ingests and processes **many** raw
series, so the edge first does a **local cross-series fold** within a group
(e.g. `sum by (zone)` collapses all raw points of a zone into one combined
series) and *then* participates in the series×window monitor as a single "site"
reporting that group's combined local value. The coordinator sums these
per-group combined values across the `k` edges that carry the group. **PromQL
does not express this fused 2D continuous monitor today** — it is a new query
shape.

> **Caveat (series-axis analogue of policy A, see §4):** the local fold makes the
> **group** the finest answerable granularity in the series dimension. For
> additive functionals this is a pure win (`‖f_group‖₁ = Σ‖f_i‖₁`, no loss); for
> L2 / quantile / distinct workloads that need *per-raw-series* answers, folding
> first reintroduces a wrong-norm error — emit per-subgroup deltas instead.

## Implementation: `(agg_id, group_key)` monitor keying

The monitor state is keyed by **`(agg_id, group_key)`**, not `agg_id` alone:

- **CMS point-frequency**: `group_key` = the point key `x` (whole-stream over the agg).
- **Sum / linear-buckets**: `group_key` = the canonical encoding of the series'
  `AggregateBy` label tuple (`zone=z0`), so per-group series get **independent**
  monitor state + slack + threshold instead of colliding on one agg-wide slot.
  The edge has already folded all raw series of a group into this entry (§3), so
  the value is the group's combined local aggregate — exactly the per-`(agg,
  group)` quantity the coordinator sums across edges.

Edge: `groupKeyBytes(entry.Labels)` in `precompute.go`, threaded through the
window hook. Coordinator: state map keyed `(agg_id, key)`; for full multi-group
support the coordinator should accept any `key` under a configured `agg_id`
(per-key state) rather than requiring one `monitors:` entry per group — a
follow-up.

## Open question: arbitrary `[t1,t2]` sub-window queries

Beyond tumbling, we want any `[t1,t2]` range query, with edges still emitting
ε·‖f‖-thresholded deltas. Two thresholding policies:

- **(A) Cumulative** — threshold against the running `ε·‖f_total‖`. A range query
  is recovered as a **difference of two cumulative reads** `S(t2) − S(t1)`. Each
  endpoint carries error up to `ε·‖f_total‖`, so the slice's **absolute** error
  is fixed at the cumulative scale → **relative** error `~ε·‖f_total‖/‖f_[t1,t2]‖`
  **diverges as the slice shrinks** (a differencing / wrong-norm problem, not a
  single boundary residual). And it is **structurally impossible** for
  non-subtractable summaries (HLL, and effectively quantiles — you cannot
  subtract a KLL).
- **(B) Per-pane** — partition time into panes, store mergeable per-pane
  summaries, answer `[t1,t2]` by **merging covering panes**. Accuracy then rests
  on **mergeable summaries with no error compounding**
  ([Agarwal et al.](https://doi.org/10.1145/2500128)), *not* on "errors add to
  `Σε‖f_pane‖`" (that over/under-states it): DDSketch/HLL keep their relative
  guarantee structurally under merge; KLL gives `εN_[t1,t2]` for free;
  Count-Sketch gives `ε‖f_[t1,t2]‖₂` from the merged sketch (better than the
  additive bound). This is essentially [ECM-sketch](https://doi.org/10.14778/2350229.2350252)
  (Count-Min cells as exponential histograms).

**Recommendation (do not run uniform fine panes):**

1. Use an **exponential-pane smooth-histogram** layout
   ([Datar et al.](https://doi.org/10.1137/S0097539701398363) EH +
   [Braverman–Ostrovsky](https://doi.org/10.1109/FOCS.2007.55) smooth
   histograms): `O(log N)` panes, geometrically coarsening with age →
   `(1±ε)` arbitrary-range accuracy at `O((1/ε)log N)` space and near-(A)
   communication, strictly dominating uniform (B).
2. **Store mergeable per-pane summaries; merge at query time.** Lean on the
   no-compounding theorem rather than summing per-pane error budgets.
3. **Keep ε fixed across panes; adapt only granularity** (lazy, age-bounded,
   dyadic coarsening where queries cluster). *Never* adapt the error budget —
   that yields non-stationary, straddle-dependent errors. Merges are
   irreversible, so adaptivity preserves the *accuracy bound* but can cost
   *resolution* on an unanticipated fine query.
4. **Reserve (A) for its sweet spot:** whole-window / large-slice monitoring of
   subtractable additive functionals on bursty series (where
   `‖f_[t1,t2]‖ ≈ ‖f_total‖` for the queries that occur). Given the tumbling
   horizon and "current-window-so-far" alerting, (A) is often genuinely cheaper
   and equally accurate.

**Unifying principle:** it is not "errors add." It is *"mergeable summaries don't
compound, but merges are irreversible and **absolute error is fixed at the norm
you threshold against**."* Both (A)-in-time and the §3 collector fold-in-series
fail for the same reason — thresholding against a norm coarser than the query's
slice — so the **time-pane granularity** and the **series-group granularity** are
the same decision applied to two different axes, and both must follow the query
workload.

## Communication / space summary

| | space | comms | arbitrary `[t1,t2]` |
| --- | --- | --- | --- |
| (A) cumulative | `O(1)` sketches | low (gap grows → crossings rarer) | small slices lose relative accuracy; impossible for HLL/quantile |
| (B) uniform panes | `O(N/pane · sketch)` | high (`k`× more deltas) | accurate but wasteful |
| **exp-pane smooth-hist** | `O((1/ε) log N)` | near-(A) | **`(1±ε)` accurate** |

## References

- Cormode 2013, *The Continuous Distributed Monitoring Model* — the §3 `k`-aware case.
- Cormode et al. 2005, *Sketching streams through the net* — the §1 prediction-tracking delta.
- Agarwal et al. 2013, *Mergeable Summaries* — no-compounding merges (the load-bearing result for `[t1,t2]`).
- Datar et al. 2002, *Maintaining stream statistics over sliding windows* (exponential histograms) + the `Ω((1/ε)log²N)` lower bound.
- Braverman & Ostrovsky 2007, *Smooth histograms*.
- Papapetrou et al. 2012, *ECM-sketch*.
