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
| `p_{i,r} ∈ (0,1]` | per-**row** admission rate (SDK-side) | edge **CPU** ↔ accuracy |
| `T_j ≥ 0` | per-cell transmission threshold | **communication**/freshness ↔ accuracy |
| flags | delta-vs-full, geometric-refs, uniform-vs-aniso | comm ↔ edge/coord memory |

### 3.1 The SDK↔collector sampling split (row-admission)

The sampling knob is **per-row admission decided at the SDK**, not a whole-sketch
Bernoulli at the collector — and, crucially, **the SDK does not hash**. A
Count-Sketch/CMS update touches `d` rows, one counter per row; the natural
sampled unit is therefore a *candidate row update* `u = (item x, row r)`, not the
raw item. The pipeline is three stages with a clean responsibility split:

```
raw measurement (item x, value Δ)
  │
  ├─[SDK]  row-admission by GEOMETRIC skip-sampling (NitroSketch), NOT a coin
  │        per row. The SDK keeps a running skip-counter over the flattened
  │        candidate stream (item,row); on admit it draws one gap
  │        g ~ Geometric(p_{i,r}) and skips g candidates before the next admit.
  │        admitted rows  R(x) = { rows whose candidate index the counter lands on }.
  │        if the gap overruns all d rows  →  DROP the sample in O(1)
  │           (no per-row work, never sent, never deserialized).
  │        else send  (x, Δ, R(x))  to the agent collector.
  │
  └─[collector]  for each admitted r ∈ R(x):  compute the column h_r(x) and sign
           s_r(x), then  C[r][h_r(x)] += s_r(x)·Δ / p_{i,r}   (inverse-prob weight).
```

Three design points make this different from applying `p` at the collector:

1. **Geometric skip-sampling, not per-candidate coin flips (NitroSketch).**
   Flipping `d` coins per item costs `Θ(d·rate)` RNG. Instead the SDK draws a
   single geometric gap per *admitted* update and decrements a skip-counter, so
   the expensive RNG (`ln`, divide) is `O(1)` **amortized per admitted update** →
   `Θ((Σ_r p_{i,r})·rate)` — the expensive part (RNG `ln`/divide) scales with
   admits, not with the unsampled row count. The current implementation
   (`UpdateStringSampledPerRow`) still steps the skip-counter once per row,
   `Θ(d)` cheap integer decrements per item; the `O(1)` whole-item skip (draw the
   gap, and if it exceeds the item's `d` rows skip the item without touching each
   row) is an available optimization on the uniform-`p` flattened-stream variant,
   not yet shipped. Either way the RNG is `∝ Σ_r p_{i,r}` ("always line rate" for
   the costly draws). This is the existing `sketchlib-go/common` `GeometricSampler`.
2. **The SDK decides *which rows*, not *which columns*.** It never computes a
   hash. The hash (row → column) is the collector's job, done *only for admitted
   rows*.
3. **Only admitted samples cross the wire.** A sample that admits no row is
   dropped at the source, so the collector **skips its deserialization and
   hashing entirely** — the CPU/bandwidth saving compounds.

**SDK configuration prerequisite.** To decide row/counter admission the SDK must
know, *per series*, (a) **which sketch** that series feeds and (b) that sketch's
**configuration** — the counter fan-out per item and its dimensions. The
controller therefore pushes the `series → (sketch type, dims, {p_{i,r}})` mapping
to the SDK over the same config channel that carries the collector's plan (so the
two stay consistent: the SDK admits the exact rows the collector is prepared to
hash).

**Applicability — linear counter-array sketches only.** Update-sampling with
`1/p` weighting is unbiased **iff the counter is additive** (inverse-probability
weighting is a linear correction). So it applies to:

| Sketch | Counter fan-out / item | Admission | Why |
|---|---|---|---|
| **CountMinSketch** | `d` counters (one per row) | per-**row** | additive counters |
| **CountSketch** | `d` signed counters | per-**row** | additive (signed) counters |
| **DDSketch** | 1 bucket | per-**item** (the `d=1` case) | additive bucket counts |
| **KLL** | — | ✗ **not sampled** | non-linear *random compaction*; dropping/weighting an insert breaks the rank guarantee |
| **HLL** | — | ✗ **not sampled** | idempotent register **MAX**; a dropped max is unrecoverable (`1/p` can't correct a max), biasing cardinality low |

So the SDK runs row-admission for **DDSketch / CMS / CountSketch** and leaves
**KLL / HLL** unsampled (they emit every update).

**Implementation status (current vs this target).** Today's edge runtime
implements a *weaker* form of this design, and closing the gap is tracked work:

| | This design (target) | Current code |
|---|---|---|
| Algorithm | geometric skip-sampling | ✅ `sketchlib-go/common.GeometricSampler` |
| Weighting | `1/p` on admit | ✅ `CountSketchWrapper.UpdateString` (`count /= sampleP`) |
| Families | CMS/CS/DDSketch only | ✅ `applyGrantedSampleP` |
| **Granularity** | per-**row** admission (subset of `d` rows; hash only if ≥1 admitted) | ✅ **per-row** — `CountSketch.UpdateStringSampledPerRow` (sketchlib), wired in both the collector wrapper and the SDK aggregator |
| **Where** | SDK decides, then sends | ✅ **SDK-build path** — hosted in the OTLP SDK `CountSketch` aggregator (`opentelemetry-go-patch/.../aggregate/countsketch.go`, knob `sample_p`); collector-build path retains the wrapper fallback |

The **granularity** matches the design (per-row admission, drop-before-hash,
`1/p` weight — mirroring `asap_sketchlib/.../nitro.rs`), so the sampling term is
the tight per-row estimator of §3.2 (median decorrelation, `ε_sa` in
quadrature). The **location** is now realized on the **SDK-build path**: the
OTLP metrics SDK's `CountSketch` aggregator hosts a per-series sampler and
routes every measurement through `UpdateStringSampledPerRow`, so admission
happens at the source. Two deployment modes and what each saves:

- **SDK-build (sketch in the app).** The SDK builds the group sketch and ships
  one matrix per window. Sampling here is the source-side realization of the
  admission split: it cuts SDK hashing/update CPU (`∝ Σ_r p_{i,r}`) and keeps the
  per-row estimator; the collector then deserializes **one sketch per series**
  instead of the raw datapoint stream (the raw-datapoint deserialization saving
  is intrinsic to sketch-in-SDK). Wire volume of the sketch matrix itself is
  fixed (dense), so sampling's benefit here is CPU + estimator, not matrix bytes.
- **Collector-build (raw datapoints to the edge).** The app SDK emits raw
  `NumberDataPoint`s; the edge `otlpfilter` wire-thins **whole datapoints**
  (per-metric geometric admission) *before* decode, so dropped datapoints are
  never materialized — the bandwidth/deserialization saving lands at the edge
  receiver. The per-row `1/p` weighting is then applied by the collector wrapper
  on survivors. (Whole-item drop is the `R(x)=∅` fast path of the same geometric
  sampler.)

All three families are wired on the SDK-build path via a `sample_p` knob:
**CountSketch** and **CountMinSketch** host a per-series sampler and route
inserts through `UpdateStringSampledPerRow` / `InsertWithHashSampledPerRow`
(per-row admission, `1/p` applied in-place, wire stays exact — no downstream
rescale); **DDSketch** is the `d=1` whole-item case and uses the sketch's
built-in `WithSampleP` (raw counts, wire stamps `p`, consumer rescales `×1/p`).
`KLL`/`HLL` stay unsampled by design (§ applicability table).

### 3.1.1 Single-location sampling via consistent (stateless) decisions

The design has exactly **one** sampling stage per series — never SDK *and*
collector compounding. Rather than relying on configuration alone to prevent
double sampling, the admission decision itself is made **location-independent**:

```
admit(seed_series, occ, row) ⇔ U(seed_series, occ, row) < p        (pure function)
```

`U` is a splitmix64-derived uniform (`sketchlib-go/common.ConsistentAdmit`);
`seed_series = FNV-1a(series key) ⊕ window-start` and `occ` is the item's
**occurrence id**. Because the decision is a deterministic function of shared
inputs — no RNG state — *any* pipeline stage evaluates it and gets the identical
admitted-row set:

- **SDK-build**: the SDK aggregator's `ConsistentSampler` owns the decision
  (`occ` = per-series measurement counter). Downstream sees only sketch frames;
  nothing to re-sample.
- **Collector-build**: the raw datapoints cross the wire, and `occ` is taken
  from a **wire-visible identity** (the datapoint's `TimeUnixNano`, plus a
  within-batch ordinal for ties). Then the wire-level `otlpfilter` and the
  collector wrapper become **two views of the same decision**: the filter drops
  a datapoint iff *no* row admits (`R(x)=∅`, the `∏_r(1−p)` fast path, checked
  by evaluating the same `d` decisions on the wire bytes), and the wrapper
  re-evaluates the identical per-row admissions on survivors and applies the
  `1/p` weight once. Re-evaluation is **idempotent** — recomputing a decision
  never dilutes twice, because there is nothing stochastic left to re-draw.

Enforcement remains one plan-level bit (`sample_at: sdk | collector`; the
unselected side sees `p=1`), but consistency no longer *depends* on it: a
misconfigured extra evaluation reproduces the same admitted set instead of
squaring the sampling rate. `occ` must vary per **occurrence** — never per key
alone, or a key's admissions become all-or-nothing and the §3.2 per-row
decorrelation collapses to whole-key sampling.

**Window-start seed salt.** The per-series seed is XOR-salted with the window
start (refreshed each delta-temporality collect). Without it, a series
recreated each window (occurrence counter rewound) would repeat the identical
admission pattern every window — per-window unbiased, but with sampling errors
correlated across windows instead of averaging out. (The collector-build wire
identity gets this for free: `TimeUnixNano` never repeats across windows.)

**Fractional counts on the wire.** Per-row `1/p` weighting makes cells and
deltas non-integral, so the proto wire carries them losslessly: full frames
switch to packed-float64 `counts_float` when any cell is fractional (integral
matrices keep the compact sint64 wire, byte-identical to before), and sparse
deltas ride the new `d_counts_float` field under the same rule. The SDK's
`payloadFor` additionally falls back to a full frame on any delta error, so an
export can never wedge a series. (The msgpack-heap delta wire keeps its i64
contract with the Rust backend and is unaffected.)

**Unbiasedness.** For an admitted row-update the collector applies weight
`1/p_{i,r}`; since `E[Z_r · 1/p_{i,r}] = 1`, each row-`r` sub-sketch is an
unbiased estimator of `f`. The Count-Sketch readout `median_r s_r(x)·C[r][h_r(x)]`
keeps its `(ε_sk, δ)` guarantee; sampling only inflates the per-row variance.

**Variance / `ε_sa`.** An admitted counter carries weight `1/p_{i,r}`, so its
contribution to the row-`r` cell variance is `(1−p_{i,r})/p_{i,r}·Δ²`. Summed over
a cell's traffic this is the sampling perturbation `Σ_r^{sa}` of §4; the
median-of-`d` rows turns it into the effective relative sampling error `ε_sa`.

**Costs (the whole point).**

| | per raw item | scales with |
|---|---|---|
| **SDK CPU** | 1 geometric gap (RNG) per *admit* + `Θ(d)` cheap skip decrements per item | RNG `∝ (Σ_r p_{i,r})·rate`; bookkeeping `Θ(d·rate)` |
| **Collector CPU** | `|R(x)| ≈ Σ_r p_{i,r}` hashes + updates | `(Σ_r p_{i,r}) · rate` (vs `d` unsampled) |
| **Bandwidth / collector deserialization** | 0 if `R(x)=∅` | drop prob `∏_r(1−p_{i,r})` = `(1−p)^d` uniform |

So `p_{i,r}` is a **joint edge-CPU + bandwidth** lever: lowering it cuts collector
hashing (`Σ_r p_{i,r}`), cuts wire volume, and lets the SDK stay at line rate with
only coin flips. The accuracy cost is the `ε_sa` term below; §7 allocates the
sampling budget against it. **Today the rate is per-site** (`p_{i,r}=p_i`,
`AllocateSampleRates` gives one `p_i` per site with `p_i ∝ √(f_i/rate_i)`);
per-row rate differentiation (a larger `p` for high-sensitivity rows) is a future
refinement — the current benefit is the per-row *admission* decorrelation of §3.2,
not per-row rate tuning.

### 3.2 Error and threshold-allocation math under per-row SDK sampling

**Per-row estimator.** With per-row admission, row `r`'s cell is
`C[r][c] = Σ_{x:h_r(x)=c} s_r(x)·Δ_x·(Z_{x,r}/p_{i,r})`, `Z_{x,r} ~ Bernoulli(p_{i,r})`
**independent across rows**. The row estimate `X_r = s_r(y)·C[r][h_r(y)]` is
unbiased (`E[Z_{x,r}/p_{i,r}]=1`); the point estimate is `f̂(y)=median_r X_r`.

**Per-row variance = collisions + sampling.**
```
Var[X_r] ≈  F₂/w                     (Count-Sketch hash collisions, F₂=‖f‖₂²)
         +  (1−p_r)/p_r · S₂,r(y)    (sampling; S₂,r(y)=Σ Δ² routed through y's row-r cell)
```

**The decisive point — median decorrelation (why per-row ≠ per-item).** The
Count-Sketch `(ε,δ)` guarantee comes from the **median over `d` rows**: if each
row fails w.p. `≤ ⅓` *independently*, the median fails w.p. `2^{−Θ(d)} = δ`. That
independence is exactly what the two schemes do or do not give:

- **Per-row admission (target):** `Z_{x,r}` is independent across `r`, so the
  `X_r` are independent → the median concentrates **both** the collision **and**
  the sampling error → sampling error lands **inside** the `δ` guarantee.
- **Whole-item admission (current):** one `Z_x` shared by every row. A key `y`'s
  own contribution is `Δ_y·Z_y/p`, *identical in all rows*; if `y` is dropped
  (`Z_y=0`) it is missing from **all** rows at once and the median cannot recover
  it. The queried key's own contribution `Δ_y·(Z_y/p − 1)` (scale
  `∝ f(y)·√((1−p)/p)`) is **identical in every row**, so this part is
  **common-mode** — it **survives the median** and is **not** reduced by `d`.
  (Cross-key collision noise carries independent row signs `s_r(·)` and does
  partially decorrelate even here; the un-reducible piece is the key's own
  term.) It must be bounded *before* the median and cannot be credited with the
  `d`-fold amplification.

> **Result.** At equal admitted work (`E[rows]=d·p` per item ⇒ same edge CPU),
> per-row sampling keeps the sampling error inside the median's high-probability
> envelope; whole-item sampling leaves it as an irreducible common-mode penalty.
> Per-row is the **strictly better estimator** — this, not the relocation, is the
> reason to push the decision into the SDK.

**Effective `ε_sa` (composition).** Under per-row independence the sampling
*variance* folds into the **same** `median_r` step as the collision variance —
`Var[X_r] = F₂/w + (1−p)/p·S₂,r` under one median tail — so it composes **in
quadrature** with `ε_sk`: `|f̂(y)−f(y)| ≤ √(ε_sk²+ε_sa²)·‖f‖₂` w.p. `1−δ`, with
`ε_sa = Θ(√((1−p)/(p·w)))` (uniform `p`). This is exactly the random part of
Theorem 1 (§4). Whole-item admission instead leaves a **common-mode** term that
survives the median and adds to `ε_sk` **linearly** — strictly looser at equal
edge cost.

**Threshold-allocation coupling (row-dependent floor).** The delta gate `T_j` for
cell `j=(r,c)` sits over a *row-`r`-subsampled* counter, so the
"don't-transmit-finer-than-you-sample" floor of §7 is **row-indexed** by that
row's admission rate:
```
T_j ≥ T_j^floor = √( V_j · (1−p_i)/p_i )      (rate is per-site p_i, admission per-row)
```
The GOS water-filling is unchanged in form; the ε-budget is split by §7 Layer B
(staleness `ε_st` peeled linearly, then `ε_sk²+ε_sa² = (ε_q−ε_st)²`), and this
floor closes the sampling↔threshold coupling.

**Status.** The per-row *estimator* benefit above is **realized** in code via
`UpdateStringSampledPerRow`, so `ε_sa` sits inside the `1−δ` median guarantee. The
*location* move is realized on the **SDK-build path** — the OTLP SDK `CountSketch`
aggregator hosts the sampler (§3.1) — so admission happens at the source; the
collector-build path realizes the pre-deserialization saving at the edge
`otlpfilter` (whole-datapoint wire-thinning). CMS/DDSketch SDK-build hosting is
the remaining follow-up.

---

## 4. Unified error bound

The backend holds `Ĉ(t)`, perturbed from the ideal `C(t)` by two **random**
sources that the Count-Sketch median absorbs together, plus one **deterministic**
staleness term that adds on top:

1. **Sketch + sampling (random, one median).** Per row `r`, the cell estimate
   carries hash-collision variance `≈ F₂/w` *and* per-row sampling variance
   `≈ (1−p_i)/p_i · S₂,r` (§3.2 — the `1/p_i` weight is unbiased; per-row
   admission keeps the rows independent). Because both live in the **same**
   `median_r` step, they compose in quadrature into one high-probability tail:
   `|q̂ − q(f)| ≤ √(ε_sk² + ε_sa²)·‖f‖₂` w.p. `1−δ`, with `ε_sk=Θ(1/√w)`,
   `ε_sa=Θ(√((1−p)/(p·w)))`, `δ=2^{−Θ(d)}`. (Sampling rate is per-site `p_i`;
   admission is per-row-independent, i.e. `p_{i,r}=p_i` uniform across rows.)
2. **Staleness `{T_j}` (deterministic).** Site `i` withholds cell `j` until its
   accumulated change reaches `T_j`, so `|Ĉ[j] − (C+η)[j]| ≤ k T_j =: e_j` **at
   all times** — a worst-case bound, *not* a random variable, so it **adds
   linearly** (it cannot be RMS-combined with the random tail).

**Theorem 1 (unified relative error).** For any query `q` and any time `t`, w.p.
`≥ 1 − δ`:

```
              ┌──── random (one median) ────┐   ┌──── staleness (linear) ────┐
|q̂(t) − q(f)| ≤ √(ε_sk² + ε_sa²)·‖f‖₂        +   k·Σ_j |r_{q,j}|·T_j
```

Dividing by `‖f‖₂` gives the **relative** budget: the random part (sketch ⊕
sampling in quadrature) plus the deterministic staleness, linearly:

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
Comp_sdk     = c_rng·(Σ_r p_{i,r})·rate     (geometric skip-sampling: O(1)/admit RNG)
Comp_coll    = c_h·(Σ_r p_{i,r})·rate       (hash + update — only admitted rows)
             + c_s·Σ_j V_j/T_j              (uploads       — thresholds cut this)
Comm         = b·Σ_j V_j/T_j  (+ broadcast for geometric)
             × (1 − ∏_r(1−p_{i,r}))         (samples with no admitted row aren't sent)
Cost_coord   = m·n·(1 + k·1{geo})           (running merge + per-edge refs)
             + c_a·Σ_j V_j/T_j              (incremental apply_delta)
```

The edge CPU is split across the two runtimes: the **SDK** pays only the
geometric-sampler RNG (`Comp_sdk`, `O(1)` per admit, no hashing), and the **agent
collector** pays the hashing/update (`Comp_coll`) *only for admitted rows* plus
the delta uploads. Lowering `p_{i,r}` cuts SDK RNG, collector hashing, and wire
volume together — one lever, three savings.

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
accuracy the sketch must supply.

**Layer B — budget split (staleness peeled linearly first).** Because staleness
is deterministic it comes off the top of `ε_q` **linearly** (Theorem 1), *then*
the remaining random budget is split in **quadrature**:

1. choose `ε_st ∈ [0, ε_q − ε_sk]` — the edge-CPU↔communication knob (larger
   `ε_st` ⇒ looser thresholds ⇒ less comm, but a smaller random budget ⇒ tighter
   sampling ⇒ more CPU); pick it by the 1-D convex tradeoff of `w_e·Comp` vs
   `w_c·Comm` (unique — both monotone);
2. the random budget after the linear peel is `ε_rand = ε_q − ε_st`;
3. split it in quadrature: `ε_sa² = ε_rand² − ε_sk²` (sketch ⊕ sampling), which
   requires `ε_sk ≤ ε_rand`.

So the composition is `√(ε_sk² + ε_sa²) + ε_st = ε_q`, matching §4 — **not** a
three-way quadrature. The code's `split_budget(ε_q, ε_sk, w_edge, w_comm)`
implements exactly this linear peel (`ε_st = t·(ε_q−ε_sk)`,
`ε_sa = √((ε_q−ε_st)²−ε_sk²)`, `t = w_comm/(w_edge+w_comm)`).

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
  The whole-sketch `F₂` readout here is the **mean-of-rows** estimator
  `F̂₂ = ‖C‖²/d` (each row's `‖C_r‖²` is an unbiased `F₂` estimate; averaging the
  `d` independent rows reduces its variance by `1/d`) — *not* the median-of-rows
  point-query estimator of §3.2. This is deliberate: the geometric safe-zone is a
  **ball** `‖C‖ ≤ √(d(1−ε)τ) ⇔ ‖C‖²/d ≤ (1−ε)τ`, so edge silence and coordinator
  alert test the identical functional (`all-sites-safe ⟺ F̂₂ < (1−ε)τ`). The two
  estimators serve different readouts — median for individual-key location
  (robust tail), mean for the aggregate energy the ball bounds (variance
  reduction) — and must not be conflated.
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

**Rate vs total.** `Σ_j V_j/T_j` is an upload *rate*; the WZ `Θ̃(k/ε²)` is the
*total* communication to maintain one continuous `(1±ε)` `F₂` estimate. Compare
them over a fixed horizon of bounded total change: with `w ∝ 1/ε²` (the necessary
sketch width) each sketch is `Θ(1/ε²)` and `k` sites must each be represented, so
the total is `Θ̃(k/ε²)` — **matching the WZ STOC'12 tight lower bound** (bits vs
words absorbed in the `Θ̃`). The measured normalization in
[`gos-eval-results.md`](gos-eval-results.md) §3 uses the "one-round" unit `k·S`
for exactly this comparison. Consequences:

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
- **Coordinated sampling + geometric skip-sampling** — NitroSketch [Liu+
  SIGCOMM'19]; ASAP's own `AllocateSampleRates` (rate allocation) and
  `sketchlib-go/common.GeometricSampler` (the `O(1)`-amortized skip sampler).
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
- Z. Liu, R. Ben-Basat, G. Einziger, Y. Kassner, V. Braverman, R. Friedman,
  V. Sekar. *NitroSketch: Robust and General Sketch-Based Monitoring in Software
  Switches.* SIGCOMM 2019. (Geometric skip-sampling: one RNG draw per admitted
  update — `O(1)` amortized, "always line rate" — with inverse-probability
  weighting; the SDK-side row-admission sampler here is this scheme.)
