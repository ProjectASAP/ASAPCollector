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

**Implemented as designed**, hosted on the SDK-build path (OTLP SDK
`CountSketch` aggregator, knob `sample_p`, routing through
`UpdateStringSampledPerRow`) with per-row admission, drop-before-hash, and
`1/p` weighting — mirroring `asap_sketchlib/.../nitro.rs`, giving the
sampling term the tight per-row estimator of §3.2 (median decorrelation,
`ε_sa` in quadrature). Two deployment modes exist, with different savings:

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

**CountSketch** and **CountMinSketch** route inserts through
`UpdateStringSampledPerRow` / `InsertWithHashSampledPerRow` (per-row
admission, `1/p` applied in-place, wire stays exact — no downstream rescale);
**DDSketch** is the `d=1` whole-item case via the sketch's built-in
`WithSampleP` (raw counts, wire stamps `p`, consumer rescales `×1/p`).
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

- **SDK-build** (implemented): the SDK aggregator's `ConsistentSampler` owns
  the decision (`occ` = per-series measurement counter, seed = FNV(attrs) ⊕
  window start). Downstream sees only sketch frames; nothing to re-sample.
- **Collector-build** (implemented): the raw datapoints cross the wire, and
  the identity is wire-visible — **seed = `common.SeedForMetric(name)`**
  (canonical FNV-1a-64 of the metric name) and **occ = `time_unix_nano / 1e6`
  in milliseconds**, chosen because the decoded `Observation.TimestampMs` the
  wrapper sees is ms-resolution, so both stages compute the *same integer*.
  The wire-level `otlpfilter` and the collector wrapper are then **two views
  of the same decision**:
  - `otlpfilter.SampleState.SetParams(name → {P, Rows})` configures per-metric
    `(p, d)`; the filter reads `time_unix_nano` (field 3, fixed64) from the
    opaque datapoint bytes and drops it iff *no* row admits (`R(x)=∅`, the
    `(1−p)^d` fast path) — never decoding attributes or values.
  - the runtime threads `(obs.Metric, obs.TimestampMs)` to the sketch via the
    optional `precompute.SampleIdentitySetter` interface (one cached assert
    per series entry, in `recordLocked`); the wrappers re-evaluate the
    identical per-row admissions on survivors and apply the weight once:
    **CountSketch/CMS** per-row with in-place `1/p` (CMS's internal whole-item
    sampler is switched off — the wire envelope stays exact, no downstream
    rescale); **DDSketch** is the `d=1` case — admission moves out of the
    sketch into the wrapper's consistent decision, and `SetWireSampleP` keeps
    the raw-counts + envelope-`p` convention for the consumer's `×1/p`.

  Re-evaluation is **idempotent** — recomputing a decision never dilutes
  twice, because there is nothing stochastic left to re-draw. Contract tests
  pin this: filter survivors ≡ stateless recomputation ≡ wrapper touches
  (`otlpfilter.TestConsistentDecisionsMatchSurvivors`,
  `sketches.TestCSWrapper_ConsistentAgreesWithFilter`,
  `TestDDSketchWrapper_ConsistentD1`).

**Timestamp edge cases.** A datapoint with absent/zero `time_unix_nano` is
passed through by the filter (fail-open — no wire identity to decide on) and
sampled by the wrapper's per-series occurrence-counter fallback instead —
exactly once end-to-end either way. Datapoints of the same metric sharing one
millisecond share decisions (same `(seed, occ)`): per-occurrence unbiasedness
holds, but errors within a same-ms burst are correlated — acceptable, and the
reason `occ` granularity is ms (exact cross-stage agreement) rather than ns.

**Grant plumbing (implemented).** The coordinator's per-round sampling grant
reaches the wire filter automatically: `Grant.SampleP` arrives on the monitor
channel → `monitor.Engine.OnGrant` stores it and fires `SetSampleGrantHook`
(accepted grants only — stale-epoch/unknown are dropped, mirroring
`grantedSampleP`) → the `asap_edge` processor's hook (`warm_sketch.go`) does
`otlpfilter.Default().Upsert(inputMetricName, {P: p, Rows: d})`, with `d` from
the family config (`wireSampleRows`: CS/CMS matrix rows, DDSketch 1; Sum/KLL/
HLL never installed — same gating as `applyGrantedSampleP`). `p≤0`/`p≥1`
grants withdraw the entry (no thinning). The `asap_otlp` receiver reads the
same process-wide `otlpfilter.Default()` state (its factory defaults to it),
so a single-pipeline collector needs zero extra wiring. Note `AggID =
FNV-1a-64(metric)` is the SAME function as `SeedForMetric` — the grant key and
the sampling seed are one identity.

Enforcement remains one plan-level bit (`sample_at: sdk | collector`; the
unselected side sees `p=1`), but consistency no longer *depends* on it: a
misconfigured extra evaluation reproduces the same admitted set instead of
squaring the sampling rate. `occ` must vary per **occurrence** — never per key
alone, or a key's admissions become all-or-nothing and the §3.2 per-row
decorrelation collapses to whole-key sampling. The filter's `Rows` must equal
the target sketch's row count (plan consistency). A mismatch is asymmetric:
`Rows` too LARGE is safe (the filter keeps extra points the wrapper then skips
itself — wasted wire bytes, no bias); `Rows` too SMALL over-drops (the filter
discards points whose higher rows the wrapper would have admitted — mass the
`1/p` weight cannot recover, biasing estimates low). When in doubt, round up.

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
sampling budget against it. **Today the rate is per-site** (`p_{i,r}=p_i`): the
coordinator gives one `p_i` per site via the whole-sketch ε-floor
`p_i = 1/(1+ε²·rate_i)` (see §7C — the per-key `√(f_i/rate_i)` water-filling is
retired for sketch sampling). Per-row rate differentiation (a larger `p` for
high-sensitivity rows) is a future refinement — the current benefit is the
per-row *admission* decorrelation of §3.2, not per-row rate tuning.

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

- **Sampling** (per-site). The per-key KKT water-filling
  `p_i ∝ √(f_i/rate_i)` (binding `Σ_i f_i(1−p_i)/p_i ≤ V_sa(ε_sa)`) is the
  general form, but it has been **retired for sketch sampling**: a sketch
  point/L2 estimate's error is bounded by the sketch *norm*, not a single key's
  `f(x)`, so the accuracy a per-edge `p_i` buys is only "keep this edge's L2
  contribution within ε." The **implemented** allocation is therefore the
  whole-sketch ε-floor
  ```
  p_i = 1 / (1 + ε²·rate_i),   clamped to (0,1]
  ```
  (`data_plane monitor::coordinator::allocate_p` → `epsilon_sample_floor`; see
  derivations §5). The `√(f/rate)` split remains valid only when a key is
  exact-counted *outside* the sketch — where sampling that one counter is
  pointless anyway.
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

## 11. The 2026-07 redesign: no sub-window, insert-time detection for all 6 families

Everything above (§1-10) establishes *what threshold to use*. This section is
about *when transmission actually happens* — a mechanism-level redesign that
applies uniformly across Sum, CMS, CountSketch, DDSketch, KLL, and HLL,
replacing the periodic sub-window tick with per-insert detection while
keeping the existing OTLP/`SketchEnvelope` wire and pipeline unchanged.

### The unified model

One mechanism for every family, isotropic case: **check at insert time
whether the accumulated-since-last-sync delta crosses the family's threshold
(§8 of `sampling-cdm-gos-derivations.md`); if it does, that cell's (or
scalar's) delta needs to reach the backend.** The sole purpose is data
synchronization — keeping the backend's reconstructed state accurate. Alerting
and any other query-time decision is made entirely at the backend against
that synced state; the edge no longer makes alerting decisions itself. This
retires the previously-separate Discipline B "continuous monitoring" path
(`monitor.Engine.Observe` → immediate `sendReportLocked` over
`monitor/grpcclient`) as a distinct alerting mechanism — Sum becomes just
another family running the same insert-time check, synced over the normal
`SketchEnvelope` pipeline like everything else. `Engine.Observe` today
conflates two things in one function: `obsCount++` (rate tracking, feeding
the coordinator's `SampleP` grant negotiation) and the
`value-baseline>=slack` alerting check (which calls `sendReportLocked`). Only
the *alerting* half retires; `obsCount`/rate-tracking must be preserved (it
feeds a genuinely separate concern — sampling-rate negotiation, not data
sync) — these two halves need to be split apart, not deleted together.

### Wake-on-demand flush (keeps the existing OTLP pipeline)

The naive read of "insert-time, no sub-window" is "bypass the pipeline
entirely, send out-of-band" — this was considered and rejected. The OTLP
export chain (SDK `PeriodicReader` → collector processor → exporter) stays
exactly as-is; only *when a flush cycle runs* changes, from purely
timer-driven to timer-**or**-woken:

```go
for {
    select {
    case <-ticker.C:   // slow fallback cadence, in case a wake is ever missed
        flush()
    case <-wakeCh:      // fired the instant something crosses threshold
        flush()
    }
}
```

Insert path, non-blocking (never waits for the flush loop):

```go
select {
case wakeCh <- struct{}{}:
default: // a wake is already pending; nothing to add
}
```

`flush()` keeps using the existing `SnapshotCache`/`ComputeDeltaAgainst`
machinery (full-frame fallback on cold start, the "never emit a delta larger
than a full frame" clamp) unchanged — the only thing that changed is the
trigger. This makes **Gate 1 (the old `subWindowShouldEmit` /
`L2DivergenceSinceEmit` divergence pre-check / `ackedCells`) redundant**: an
empty `dirtySet` at flush time IS "nothing was worth sending," computed once
per crossing instead of by a periodic O(dw) full-matrix scan. All of that
(`ackedCells`, `subWindowDivergence`, `subWindowShouldEmit`) is dead code
under this design and should be deleted, not kept as a fallback path.

CMS's local point-query read (`ThresholdConfig.Functional: cms_point`)
retires alongside Discipline B's alerting: once cells reset in place at
insert time, a `min`-based local read is corrupted by any single recently-
reset row. All point/alert reads move to the backend's reconstructed copy.

### Per-family cell / threshold / reset semantics

See `sampling-cdm-gos-derivations.md` §8 for the derivations; summary of the
*mechanism* (not the math) each family needs:

| Family | Detection unit | Reset on send? | Notes |
|---|---|---|---|
| CountSketch (isotropic) | matrix cell | zero it | `normSqAll` tracked incrementally (`+= 2·old·Δ+Δ²`), O(1) |
| CountMinSketch | matrix cell | zero it | same mechanism, $L_1$-scale threshold (§8.2) |
| DDSketch | bucket count | zero it | bucket count `B` tracked incrementally too (+1 on genuinely new bucket), no config constant needed (§8.4) |
| Sum | scalar | zero it (subtract reported amount) | degenerate 1-cell case |
| KLL | whole sketch (no per-cell structure) | full `Reset()` | trigger is `Count() >= εN`, not a per-cell check; already the existing disjoint-segment mechanism, just re-triggered by count instead of a timer |
| HLL | register | **never** — MAX-merge is idempotent, only clear a dirty flag | trigger is $\lvert 2^{C'}-2^{C}\rvert \ge 2^{\tau}$ on the linearized value, not raw register value (§8.7) |

Backend reconstruction is unchanged for the additive families (Sum/CMS/CS/
DDSketch): summing every fragment ever received for a cell — regardless of
how many times or when it was individually reset — telescopes to the true
cumulative value (`v_1+v_2+...+v_n+v_{residual}` = true total). This is why
resets can happen asynchronously, at different times per cell, without
breaking correctness — it only affects when transmission happens, never
what the backend eventually reconstructs.

### Cold start is a feature, not a bug

Every family's threshold scales with an accumulated quantity (`‖Ĉ‖`, `N`,
`R`) that starts near zero at window start, so the very first few inserts
cross threshold almost immediately. This is intentional: it gets the backend
a usable initial estimate as fast as possible, rather than waiting for data
to accumulate before syncing anything. No floor/minimum-threshold mechanism
is needed to suppress this.

### Open items (not blocking)

- **Anisotropic CountSketch's `Activity_j`** needs redefinition. The old
  `Activity_j=|current-prev|` assumed a single, uniformly-timed `prev`
  snapshot; under per-cell async reset, different cells' "since last touch"
  windows are no longer comparable, and naively diffing against any
  snapshot double-counts/under-counts around individual cell resets. A
  per-cell EMA of `|Δ|` (`activityRate[r][c] = decay·activityRate[r][c] +
  (1-decay)·|Δ|`, updated every insert) is the leading candidate — cheap,
  reset-timing-independent — but it replaces the derivation's exact
  `Activity_j=V_j` with a heuristic, and whether the §7 closed-form
  water-filling solution still carries the same error guarantee under that
  substitution has not been checked. The anisotropic water-filling solve
  itself also still requires a periodic $O(dw)$ pass (unlike every other
  family here) — per-cell detection at insert time only avoids the
  "decode a serialized `prev`" cost, not the joint solve.
- **DDSketch's unbounded contiguous bucket-array growth** on outlier values
  is a real memory-safety gap, independent of this redesign — tracked as
  sketchlib-go#72. The dynamically-tracked `B` used in the threshold formula
  above does not require fixing this; it is a separate, likely
  higher-priority issue.
- **HLL's small-cardinality regime**: the register-change adapter's accuracy
  proof (OctoSketch's Appendix B) is stated for "sufficiently large"
  cardinality; behavior when most registers are still at 0 (early in a
  window) has not been separately verified. A candidate mitigation (always
  send a register's first-ever nonzero write unconditionally) is proposed
  but unverified.

---

## 12. Implementation notes (controller synthesizes, edge executes)

Everything expensive is a **controller (backend) decision**; the edge only
executes a fixed per-cell comparison.

- **Controller** (offline, per registered metric/query): runs ADCD (AD → Hessian
  eigenvalue bounds → `∇f, λ`), estimates `{V_j}` from the workload, solves (P)'s
  layers A–C, emits `(d, w, G, {p_i}, scalar GOS knobs, flags)` via OpAMP. This
  slots into the existing controller multi-objective
  (`controller-optimization-problem.md` SP-6:
  `min w_bw·bw + w_cpu·cpu + w_mem·mem + …`) — GOS thresholds are new decision
  variables there. **Note:** the controller ships *scalars*
  (`ε_delta`, sites), **not** a full per-cell vector `{T_j}`; the isotropic
  case (§11, all 6 families) reconstructs its single scalar `T` from those
  plus live sketch state, so no vector ever crosses the wire. The anisotropic
  per-cell `{T_j}` water-filling described here is CountSketch-only and, per
  §11's open items, not currently implemented.
- **Edge**: maintain sketch + acked snapshot; per flush, recompute `{T_j}` from
  the pushed scalars + local `{V_j}`, upload cells with `|ΔC_j| ≥ T_j` as a sparse
  delta; run one generic `isLocallySafe` for monitored functions. No AD, no
  water-filling solve at the edge — only the closed-form threshold evaluation.
  **Superseded for the isotropic case by §11**: there is no longer a
  periodic "per flush" cadence or a separate acked-snapshot copy — the
  per-cell comparison happens at insert time and the cell resets in place
  on send. This paragraph's *acked-snapshot, periodic-flush* model still
  describes anisotropic CountSketch's water-filling solve, which §11 flags
  as an open item, not yet redesigned.
- **Backend**: `apply_delta` into a running merge (`O(#delta cells)`), keeping the
  global sketch continuously queryable within the Theorem-1 envelope, surfaced in
  the `accuracy: ε=…` response annotation.

**Ties to existing code:**
- `ASAPQuery-backend/control_plane/src/epsilon_alloc.rs` — the ε-budget split
  (linear staleness peel, then quadrature split: `√(ε_sk² + ε_sa²) + ε_st = ε_q`,
  §7 Layer B — not a three-way quadrature).
- `ASAPQuery-backend/data_plane/src/monitor/sampling_alloc.rs`
  (`epsilon_sample_floor`) — the live whole-sketch sampling floor.
- `asap_sketchlib` `CountSketchDelta` + `compute_delta`/`apply_delta`
  (byte-parity Go/Rust) — the sparse per-cell delta wire format.

---

## 13. Open problems / next steps

1. **Anisotropic delta broadcast** — *partially done.* The **sparse-cell**
   encoding of `ΔC_ref` on the coordinator→edge path is implemented and measured
   (`CRefUpdate::Delta`; removes the `O(k)` broadcast amplification — see
   gos-eval-results.md §2). Still **open:** the broadcast gate is currently
   isotropic (ships every changed cell, `Δ ≠ 0`); giving it *anisotropic per-cell
   thresholds* (the §7C water-filling, as already done on the edge→coordinator
   upload path via `ComputeDeltaPerCell`) is the remaining work.
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
