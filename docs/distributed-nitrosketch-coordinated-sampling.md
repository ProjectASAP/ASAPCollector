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

## Combining with the CDM threshold — the joint bound

Update-sampling (CPU axis) and the CDM ε·‖f‖ emit-gate / slack-countdown alert
(bandwidth + monitoring axis) are largely **orthogonal** — one decides how many
items touch the counters, the other how often/whether the sketch crosses the
wire. They interact at exactly one point: **update-sampling injects variance into
the very value the CDM threshold tests.** When both run at the edge, the edge
feeds the CDM mechanism an unbiased-but-noisy `f̂` (the 1/p-rescaled sampled
sketch) with sampling error

```
ε_s  ≈  √( (1−p) / (p·N) )      (relative, w.h.p.; from Var[f̂] = f(1−p)/p)
```

**(A) ε-approximation mode (open-window freshness / value tracking) — bound holds
additively.** The emit-gate keeps the backend within `ε_cdm·‖f‖` of the *local*
sketch; the local sketch is within `ε_s` (sampling) + `ε_sk` (native sketch
error) of truth. By the triangle inequality

```
‖backend − true‖  ≤  ε_sk + ε_s + ε_cdm        (random parts in quadrature)
```

so the guarantee **still holds**, as the sum of three sources. Because
`ε_s = √((1−p)/(pN))` *shrinks with N*, **more samples per series make sampling
near-free**: at high N the combined bound → `ε_sk + ε_cdm`.

**(B) Threshold/alert mode (slack countdown, detect global > τ) — becomes
probabilistic.** The CMY no-missed-crossing safety relies on the reported value
being a faithful (one-sided) bound; a sampled `f̂` is unbiased but two-sided, so a
downward fluctuation in `Σf̂_i` can *delay* firing past the true crossing. Recover
the guarantee w.p. 1−δ by inflating the fire condition with a confidence margin:

```
fire when   Σ f̂_i  +  c_δ·√( Σ f_i(1−p_i)/p_i )  ≥  (1−ε)·τ
```

The deterministic bound becomes (1−δ)-probabilistic; the effective ε grows by the
sampling confidence interval.

**The real catch — communication.** The CDM emitter/reporter fires on threshold
*crossings*; sampling noise makes the value **jitter across the threshold**,
producing **spurious emits** — so the CDM communication/freshness bound does
**not** hold automatically and can get *worse* under sampling. It is preserved
only if the threshold band absorbs the noise, i.e. the **coupling rule**

> **`ε_cdm  ≳  ε_s = √((1−p)/(p·N))`** — never sample so hard that the sampling
> noise exceeds the CDM threshold tolerance.

Under this single condition everything composes cleanly: accuracy
`≈ ε_sk + 2·ε_cdm` (sampling absorbed within the CDM tolerance), no spurious-emit
blow-up, and alert safety holds w.p. 1−δ. This is the concrete form of the joint
budget: pick `p_i` and `ε_cdm` together so `ε_s ≤ ε_cdm`. It also closes the loop
with §B.2 — higher per-series `N` shrinks `ε_s`, *relaxing* the coupling and
letting you sample harder for the same `ε_cdm`.

## Sampling tier — why the CDM site is the edge collector

*Where* the coordinator-allocated `p` is **applied** is a separate choice from
where it is computed, and it is constrained by the CDM threshold's `k`.

- **`k` must be a stable population.** The slack countdown gives each site slack
  `Δ/(2k)` (`Monitor::slack` = `gap()/(2k)`), and the allocation `p_i ∝ √(f_i/rate_i)`
  is likewise per-site. If the "sites" were **SDK / instrumentation instances** — a
  dynamic, churning population — every join/leave perturbs both `k` (re-broadcasting
  slack) and the `p_i` re-solve, adding synchronization cost and instability.
  **Edge collectors are the stable tier; SDKs are not.** So the CDM site = the edge
  collector, for both the threshold and the sampling allocation — which is exactly
  what the as-built coupling does (`SampleP` applied in `asap_edge` via `WithSampleP`;
  the coordinator coordinates over collectors).

- **Cost of that choice.** Because the `otlpreceiver` fully decodes OTLP→pdata
  *before* `asap_edge` runs, collector-side sampling can only skip the innermost
  update loop — the measured ~3% (`0.124` vs `0.125` cores). It does not save the
  dominant per-sample decode/key, nor the SDK→collector network.

- **Capturing more without destabilizing `k`:**
  1. **In-collector pre-decode shim (preferred next step).** Sample at the *decode
     boundary* — a custom receiver/decode path that, for warm-sketch-*only* series,
     applies the geometric skip *before* materializing a datapoint's attribute map +
     value (wire-skip the dropped fraction). NitroSketch's decision is value-
     independent, so this is sound. The edge is still the collector → `k` unchanged,
     no cross-node synchronization. The win is gated by how cheaply the sketch-vs-raw
     (warm/cold) class is decodable: **metric name ≫ single routing label ≫ full
     series identity**. Requires the disjoint warm/cold routing (a series is sketched
     **xor** cold-archived) so a dropped warm sample truly has no other consumer.
  2. **SDK-source sampling via a hierarchical budget (optional, future).** To push
     sampling to the source — saving decode *and* network — without exposing the
     volatile SDK count to the global protocol: the coordinator allocates a sampling
     **budget per stable collector** (`k` = collectors), and each collector fans that
     budget out locally to its dynamic SDKs. SDK churn is absorbed at the collector;
     the global coordination only ever sees collectors. This decouples *where
     sampling executes* from *what `k` counts*. The granted `p` then rides one more
     hop (collector→SDK control push), and exports carry `p` for the backend's `1/p`
     rescale (the merged path).

- **Bottom line:** keep the CDM site = edge collector. Take incremental CPU from the
  in-collector pre-decode shim (stable, no sync cost); treat SDK-source sampling as a
  later option behind a per-collector hierarchical budget, justified only if the
  network/decode savings outweigh the control-plane fan-out complexity. Neither
  touches the per-emit delta cost — that stays the CMS empty-base fix + emit-gating.

## Filter-only push: SDK samples, collector sketches — does the guarantee survive?

The lightest source-side option: push the **query-filter + `p`** to the SDK and
keep the sketch at the collector.

- **SDK:** evaluates the filter on its **native labels** (no decode — they're
  in-memory objects), geometric-samples the matched stream at `p`, and sends only
  **admitted raw points**, each tagged with its `p`.
- **Collector:** decodes the admitted points, builds/merges the sketch, stays the
  CDM site (`k` = collectors). The coordinator allocates a **per-collector** budget;
  the collector sub-allocates `p` to its SDKs.

**Why this cuts collector CPU (where *collector-side* sampling didn't):** the
dropped `(1−p)` points **never reach the collector**, so its per-sample work —
decode + key + update — scales with the **admitted fraction `p`**, not `N`.
(Collector-side sampling only skipped the ~3% update *after* decoding; here decode
*and* update vanish for dropped points.) It does **not** change the per-emit delta
(cardinality-driven). The sketch is still built at the collector — moving *that* to
the SDK is the next option.

### Does the CDM sampling guarantee survive the SDK→collector split? **Yes.**

Sampling at the SDK instead of the collector changes *where* the geometric draw
happens, not the merged estimator:

- **Unbiased.** Each admitted point is inserted with `×1/p_j` (its source SDK's
  `p`); the sketch is linear/additive and merge is associative, so the merged
  per-key estimate is unbiased — *provided each point carries its source `p_j`*
  (the `sample_p` tag) so the collector rescales **per point**, not by a single
  global `p`.
  Stronger statement of the rescale rule: each contribution must be divided by the
  `p` that **admitted it**, **before** it is merged with contributions sampled at a
  different `p`. **Insert-upweight** (`count /= p_j` at the source) realizes this by
  construction — the rescale is local, so merge is pure addition of true-scale
  estimates. **No-upweight + backend rescale** is correct *only if* the backend
  rescales **per envelope before every merge**; merging raw admitted counts from
  different-`p` envelopes and then applying one factor is **biased**. (Under uniform
  `p` the two are identical.)
- **Variance adds under uniform `p` — partition-invariant.** For a point-frequency
  `f(x)`, *and assuming independent per-unit admission (see below)*, the merged
  sampling variance is `Σ_units f_u(x)·(1−p)/p = (1−p)/p · f(x)` — it depends only on
  the **total** `f(x)` and `p`, **not** on whether the units are SDKs or collectors.
  So `ε_s = √((1−p)/(pN))` is **unchanged** and the joint bound holds **identically —
  for uniform `p`**.
- **Heterogeneous `p_j` does NOT give the same `ε_s`.** Then the merged variance is
  `Σ_j f_j(x)(1−p_j)/p_j`, which is allocation-dependent. The guarantee transfers
  **only if the collector sub-allocation explicitly enforces the variance-sum
  budget** `Σ_j f_j(1−p_j)/p_j ≤ (ε·‖f‖)²`. A per-SDK ε-**floor** (`p_j ≥ floor`) or
  an expected-admitted-count budget is **necessary but not sufficient** — many SDKs
  near their floor can still blow the sum. Bound the sum itself.
- **Independence must be engineered, not assumed.** Variance-addition needs
  **independent admission streams per unit**. The current sampler wires a *fixed
  shared seed* per family (`cms.go` `cmsSampleSeed`, `countsketch.go`
  `countSketchSampleSeed`, `ddsketch.go` `ddSampleSeed`); copied to SDKs, they'd
  replay the **same** skip sequence → **correlated** admissions → the cross-terms
  don't vanish and variances **do not add**. SDK-source sampling MUST use an
  independent stream per `(edge_id, agg_id, window)` (e.g. `seed = hash(edge_id,
  agg_id, window_start)`) or **hash-based per-point admission** (`admit ⇔ h(item) <
  p`, decorrelated across distinct items).
- **Threshold/alert mode.** The collector reports its merged sampled value, with the
  same variance, so the slack-countdown's no-missed-crossing safety stays
  probabilistic — but as a *guarantee* `ε_s` must be the **w.h.p.** form `ε_s(δ)`
  (below), not the 1-σ value.
- **Caveat (one level down).** A rare queried key that lives on a single **hot**
  SDK with a small `p_j` concentrates all its variance there — the collector-level
  rare-key failure mode, pushed to SDK granularity. Allocate on the queried key's
  `f_j(x)` when the query is known.

### Proof — unbiasedness and partition-invariant variance

Fix a key `x`. Its global stream is `N(x) = f(x)` unit contributions, partitioned
across sampling units `u` (SDKs *or* collectors) with `f_u(x)` at unit `u`, so
`Σ_u f_u(x) = f(x)`. Each unit admits each item with probability `p_u` from an
**independent** stream (an *assumption the implementation must enforce* — see the
independence requirement above; not automatic under a shared seed), rescaling an
admitted item by `1/p_u`. (NitroSketch's geometric skip implements per-item
`Bernoulli(p_u)` — same admit probability, same first two moments.) Let `A_u(x)` =
number of admitted `x`-items at `u`:

```
A_u(x) ~ Binomial( f_u(x), p_u ),    f̂_u(x) = A_u(x) / p_u,    f̂(x) = Σ_u f̂_u(x).
```

**Unbiased.** `E[A_u(x)] = f_u(x)·p_u`, so `E[f̂_u(x)] = f_u(x)` and

```
E[f̂(x)] = Σ_u f_u(x) = f(x).                                   (1)
```

This holds for **any** partition and **any** `p_u` (down to one item per unit),
provided each admitted item is rescaled by *its own* admit probability — i.e. `p`
must be carried per point.

**Variance.** `Var[A_u(x)] = f_u(x)·p_u·(1−p_u)`, so
`Var[f̂_u(x)] = f_u(x)·(1−p_u)/p_u`. **If the per-unit admission streams are
independent**, the covariances vanish and the variances add:

```
Var[f̂(x)] = Σ_u f_u(x)·(1−p_u)/p_u.                            (2)
```

(Under a *shared seed* the cross-covariances are non-zero and (2) fails — hence the
independence requirement.)

**Partition invariance (uniform `p_u = p`).** Substituting into (2):

```
Var[f̂(x)] = (1−p)/p · Σ_u f_u(x) = (1−p)/p · f(x).             (3)
```

depends only on the **total** `f(x)` and `p` — so SDK-tier (many small `f_u`) and
collector-tier (few large `f_u`) sampling at the same `p` give **identical** merged
variance. ∎ For **heterogeneous** `p_u`, (2) is allocation-dependent and (3) does
**not** hold: splitting mass at a *fixed* `p_u` is invariant
(`f_{u1}(1−p_u)/p_u + f_{u2}(1−p_u)/p_u = f_u(1−p_u)/p_u`), so only the **assignment
of `p` to mass** matters — but matching the uniform `ε_s` then requires **enforcing
`Σ_u f_u(1−p_u)/p_u ≤ (ε‖f‖)²` directly**, which a per-unit floor does not
guarantee.

**From 1-σ to a guarantee (concentration).** Eq (4) is **one standard deviation**.
`A_u(x)` is a sum of independent bounded `[0,1]` indicators, so by Bernstein/Chernoff
`|f̂(x) − f(x)| ≤ ε_s(δ)·f(x)` with probability `≥ 1−δ`, where

```
ε_s(δ) ≈ √( 2(1−p)·ln(2/δ) / (p·N(x)) ).                       (4′)
```

Use `ε_s(δ)` (not the bare 1-σ) wherever the CDM safety is asserted *as a guarantee*.

**Corollary.** The 1-σ relative sampling error is

```
ε_s(x) = √(Var[f̂(x)]) / f(x) = √( (1−p) / (p·N(x)) ).          (4)
```

**Scope.** (1)–(4) bound the **sampling** variance of an **additive/linear**
estimator — Sum, Count-Sketch row/point estimates, DDSketch *counts*, and the
Count-Min **row counters**. The full **Count-Min point query** is `min` over rows
**plus collision bias** (one-sided, `f̂ ≥ f`): the proof covers its sampling
component, **not** the min-readout — the collision / no-underestimate interaction
with sampling's *downward* noise is the separate caveat in the per-family table
(sampling can push a heavy hitter below the threshold). For DDSketch **quantiles**
the governing quantity is the merged admitted count `Σ_u p_u·f_u = p·N` (uniform
`p`) — partition-invariant — feeding rank error `~1/√(p·N)`. **HLL** (non-additive
max) is excluded.

**Net:** for **uniform `p` with independent per-unit streams**, the guarantee is
preserved *exactly* (same `ε_s`, same joint bound) — additive sampling variance is
partition-invariant. The requirements: **carry `p` per point and rescale by the
admitting `p` before merge** (insert-upweight, or per-envelope-before-merge backend
rescale); **independent admission streams per `(edge, agg, window)`** (the
shared-seed pattern must change); for **heterogeneous `p`**, **enforce the
variance-sum budget** `Σ f_j(1−p_j)/p_j ≤ (ε‖f‖)²` (not merely a floor); and, stated
as a guarantee, use the w.h.p. `ε_s(δ)` of (4′).

## Honest assessment

- **⚠️ Empirically, the CPU win does NOT materialize in the OTel collector — but
  the bandwidth win does.** A multi-shape e2e (edge `asap-otel` + backend, CMS +
  DDSketch, sub-window + delta, `sample_p` 1.0 vs 0.25, across 8→300 series and
  120→3000 samples/s) found update-sampling left **edge CPU unchanged** (±2–8%,
  all within run noise; sometimes slightly *up* from the sampler's own overhead)
  while cutting **egress ~9%**. Reason: in this collector the per-item sketch-
  update loop is a *small fraction* of per-sample work — **OTLP decode + per-
  series key extraction + observe-framing + allocation dominate** (consistent
  with this repo's allocation-bound precompute finding, where jemalloc gave
  ~2.2×). NitroSketch's "the d-row counter loop is the line-rate bottleneck"
  holds for a dedicated sketching engine, not for a collector. **Implication:**
  the coordinated-`p_i` *bandwidth* objective is sound; the *CPU-minimization*
  objective above only pays off where the sketch-update loop genuinely dominates
  (extreme-cardinality CMS/Count-Sketch with a cheap decode path) — otherwise
  attack decode/alloc first. Reframe the objective as **egress** minimization
  unless a profile shows the counter loop dominates.
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
