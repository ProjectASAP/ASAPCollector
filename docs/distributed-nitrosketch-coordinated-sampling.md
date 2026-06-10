# Distributed NitroSketch — coordinator-allocated update-sampling across edge collectors

> Design note. Extends [NitroSketch](https://doi.org/10.1145/3341302.3342076)
> (per-sketch update-sampling, single stream) to a multi-edge setting using the
> coordinated-sampling idea from the
> [CDM survey](https://doi.org/10.1145/2481528.2481530), to cut **edge CPU**
> while holding backend query accuracy. Companion to
> [continuous-monitoring-aggregation-taxonomy.md](continuous-monitoring-aggregation-taxonomy.md)
> and [continuous-monitoring-tumbling-cost-analysis.md](continuous-monitoring-tumbling-cost-analysis.md).

## Context

Each edge collector builds per-metric sketches over its local stream and ships
them to the backend, which merges them. The per-item update cost — for
Count-Min / Count-Sketch, `d` rows of (hash-index derive + counter writes) per
item — is a real edge-CPU cost at high rates. **NitroSketch** cuts it by sampling
the sketch *insertions* at probability `p` (geometric skip, so RNG cost is
amortized) and rescaling counters by `1/p`; ASAP already implements this
(`sketchlib-go/common/sampling.go` `GeometricSampler`, wired into Count-Min and
DDSketch via `WithSampleP`, stamped on `SketchEnvelope.sample_p`).

NitroSketch is **single-stream**: it picks one `p` blind to the fleet, and the
current code does exactly that — one `sample_p` per metric from config, applied
identically on every edge. This note describes **distributed NitroSketch**: let
the CDM coordinator (the slack-grant channel in `asap-precompute-go/monitor` +
the backend coordinator) hand each edge a *different* `p_i`, chosen from the
global picture to minimize total edge CPU at a target accuracy.

## Core idea

The coordinator already sees per-edge registrations/reports and the query's ε.
Have it solve, per monitored aggregate:

```
minimize   C(p) = Σ_i  rate_i · p_i · c_update        (expected admitted updates × per-update cost)
subject to Var_merged(p_1..p_k) ≤ (ε·‖f‖)²            (merged estimator accuracy)
           0 < p_i ≤ 1
```

`rate_i` = edge `i`'s items/window; `c_update` = the family's per-item update
cost. The coordinator grants `p_i` over the **same reverse channel and `Grant`
message** it uses for slack today — add a `SampleP` field to `monitor.Grant`,
deliver via `OnGrant`, apply by re-`WithSampleP`-ing the live wrapper at the next
`EpochReset` (the wrappers already re-apply sampler config across `Reset`).
**Never change `p` mid-window** — the `1/p` rescale assumes both merge operands
share `sample_p`.

## Why distributed changes the calculus

For **linear** sketches (Count-Min L1, Count-Sketch L2) the sketch is linear in
the stream and update-sampling injects variance into each edge's estimate. For a
point-frequency estimate of key `x`, edge `i` contributes an unbiased
`f̂_i = admitted/p_i` with sampling variance ≈ `f_i(x)·(1−p_i)/p_i`. Because the
merge is additive (`f̂ = Σ_i f̂_i`) and the per-edge RNGs are independent, the
merged sampling variance is a **sum over edges**:

```
Var_merged(x) ≈ Σ_i  f_i(x) · (1 − p_i)/p_i .
```

This is the lever single-stream NitroSketch can't see: **variance is a separable
sum, and CPU is also a sum, but the trade-off rate differs per edge.** Minimizing
`Σ rate_i·p_i` subject to `Σ f_i(1−p_i)/p_i ≤ V` is convex; the KKT interior
solution is

```
p_i*  ∝  √( f_i / rate_i )        (clamped to (0, 1])
```

i.e. **sample aggressively (small `p_i`) on high-rate edges** — the local law of
large numbers already gives accuracy there and dropped updates are cheap — and
keep `p_i ≈ 1` on low-rate edges, where every item matters and there's no CPU to
save anyway. By Cauchy–Schwarz the coordinated cost is `≤` the uniform-`p` cost,
with the gap growing as the per-edge rate distribution's coefficient of variation
grows. **Flat fleets gain ~0; skewed fleets (a few hot collectors, a long quiet
tail — the realistic telemetry case) is where it pays.**

## Per-family applicability

| family | update-sampling | rescale | merge composition | verdict |
| --- | --- | --- | --- | --- |
| **Count-Sketch** | geometric (framework exists, wrapper TODO) | `×1/p_i`, unbiased median-of-rows | L2 variance adds; sign-hash zero-mean under sampling | **best first target** — clean two-sided unbiased estimator, `d` rows saved/item |
| **Count-Min** | geometric (wired) | `×1/p_i` | `Σ f_i(1−p_i)/p_i` + collision | high value, but **one-sided min-estimate** — sampling's downward noise can drop a heavy hitter below the no-underestimate threshold; subtler than Count-Sketch. *(Backend `1/p` rescale was missing — fixed separately.)* |
| **DDSketch** | geometric (wired+rescaled on Count) | Count `×1/p`; **quantiles not rescaled** (rank-preserving) | bucket-add; sampling thins N → rank error `~1/√(p·N)` | helps Count; for quantiles coordinate `p_i` so merged effective N `Σ p_i·N_i` meets the rank target |
| **HLL** | hash-threshold only | `×1/p` on cardinality | **non-additive (max)** | **exclude** — saves ~0 CPU (hash needed for the register index regardless) and dropping rare large-leading-zero items is pure downward bias |
| **KLL** | none | rank-preserving | KLL-merge; sampled = thinned-stream KLL | low value — buffer insert already cheap; sampling costs rank accuracy for little CPU |
| **Sum/count** | none | `×1/p` | `Σ (1−p_i)/p_i·E[x²]_i` | trivial but cheapest per-update → least to gain; variance grows fast for heavy tails |

**Helps most:** Count-Sketch and Count-Min (high per-update `d`-row cost, linear,
clean rescale). **Wrong/marginal:** HLL, Sum, KLL.

## Coordination protocol

Reuse the CDM machinery almost verbatim (`monitor/types.go`, `engine.go`):
- **Edge reports** (extend `Report`): observed `rate_i` (admitted/window) — CMS/
  Count-Sketch already track L1/L2 incrementally, so it's free.
- **Coordinator grants** (extend `Grant` with `SampleP`): solve the optimization
  from `{rate_i}` + the authoritative `Epsilon`; allocate `p_i* ∝ √(f_i/rate_i)`.
- **Edge applies** in `OnGrant`, deferred to the next `EpochReset` (never
  mid-window).

Two modes: **static** (compute `p_i` once from declared/historical rates at plan
time — captures most of the win if rates are stable) and **adaptive** (start
`p_i=1`, tighten on edges that report as hot — NitroSketch's "start coarse,
refine," but driven by the fleet picture, over the existing round loop).

## Interaction with the threshold-driven sub-window emitter

Update-sampling (CPU axis) and the sub-window ε·‖f‖ emit-gate (bandwidth axis,
see the taxonomy doc) are **orthogonal** — one decides how many items touch the
counters, the other how often the sketch crosses the wire. They interact at one
point: **update-sampling injects variance into the very value the emit-gate (and
any threshold monitor) tests.** The coordinator should treat them as one error
budget — split `ε² = ε²_sample + ε²_emit` and keep `(1−p_i)/p_i·f_i ≪ slack_i²`
so sampling variance doesn't swamp the emit/alert decision.

## Honest assessment

- **Real but conditional.** Beats per-edge-independent NitroSketch *only* when
  `rate_i` is skewed; the win scales with the rate CV. Measure the actual
  per-collector rate distribution before building.
- **Failure modes:** (1) point queries on a rare key that lives only on a hot
  edge with small `p_i` concentrate all the variance there — allocate on the
  queried key's `f_i(x)` when the query is known (the coordinator holds `Key`).
  (2) Count-Min's one-sided error composes worse with sampling than Count-Sketch
  → prefer Count-Sketch as the first target. (3) Static allocation goes stale;
  adaptive lags one round. (4) If alloc (not the hash+counter loop) is the real
  CPU bottleneck, or the fleet is flat, the coordination win won't clear the
  complexity — ship a good static per-family `p` instead.

## Recommendation

Prototype **narrowly**: Count-Sketch on the highest-rate/high-cardinality metric,
on a deliberately **skewed** synthetic fleet (e.g. 1 edge at 100k/win, 9 at
1k/win), validating `p_i ∝ √(f_i/rate_i)` against uniform-`p` on (edge CPU, merged
accuracy). Prereq: the CMS/HLL backend `1/p` rescale fix (separate). Hooks:
`SampleP` on `monitor.Grant`, set in the coordinator's grant computation, consumed
in `OnGrant`/`EpochReset`, applied via the existing `WithSampleP` wrappers — no
new transport (rides the reverse gRPC channel already built).

## References
- Liu et al. 2019, *NitroSketch* (SIGCOMM) — per-sketch update-sampling + adaptive `p`.
- Cormode 2013, *The Continuous Distributed Monitoring Model* — coordinated distributed sampling over `k` sites.
- ASAP code: `sketchlib-go/common/sampling.go` (geometric sampler), `…/operators/dd_sketch_accumulator.rs` (`sample_p` rescale), `asap-precompute-go/monitor` (CDM grant channel).
