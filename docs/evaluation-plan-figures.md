# Evaluation plan — §6 figures & tables (with example data)

Companion to [`paper-outline.md`](paper-outline.md) and
[`sdk-cost-evaluation.md`](sdk-cost-evaluation.md). Lays out **every planned §6
figure/table**, populated with the **real measured numbers we already have**
(`✅ done`), partial evidence (`◐ partial`), or a mock layout for the gaps
(`◻ gap`). Real numbers are from this codebase's runs (2026-06-11/12): the
`p × ε_cdm` matrix (`/tmp/cdmsamp`), the Google-cluster-2019 accuracy sweep
(`/tmp/gct-sweep-results.json`), the sampling×delta 2×2 (`/tmp/2axis`), and the
CDM validation recorded in
[`distributed-nitrosketch-coordinated-sampling.md`](distributed-nitrosketch-coordinated-sampling.md).

Legend: ✅ measured · ◐ partial (some data, needs completion) · ◻ not yet run.

---

## 0. The headline

> **Across equivalent query coverage, ASAP's total resource (edge + wire + backend +
> storage) sits on a strictly better accuracy-vs-cost Pareto than raw forwarding — and
> two new orthogonal knobs, SDK update-sampling (`p`) and CDM ε-gated delta emission
> (`ε_cdm`), push that frontier further, with a proof that bounds the cost.**

**Routing is disjoint:** every series is *either* warm-sketched (where sampling +
CDM apply) *or* cold-archived to Gorilla (lossless, exact/historical) — never both.
So the Pareto's "total cost" is the **sum of the two partitions' costs** (warm-half +
cold-half, Fig 11), and **the controller allocates the partition** (which series →
sketch[type,`W`,`L`,`p`] vs cold-Gorilla) from the query set (Fig 12).

The contribution stack, from system to evidence:
1. sketch-across-the-lifecycle + the `(W,L,agg_type)` planner (the base system);
2. **coordinated SDK sampling** (`p_i ∝ √(f_i/rate_i)`, ε-floored) — new cost axis;
3. **CDM** — ε-gated sub-window delta emission (open-window freshness) + slack-countdown alert;
4. the **joint bound** `ε_sk + ε_s + ε_cdm` (proof) tying accuracy to cost.

---

## Fig 1 — HEADLINE: accuracy-vs-cost Pareto, swept over `(W, L, agg, p, ε_cdm)`  ◐
**Claim:** ASAP dominates raw; sampling + CDM extend the frontier.
**Layout:** scatter, x = total cost (edge CPU + wire bytes/s, normalized to raw=1.0),
y = query accuracy (1 − rel-err). One marker per operating point; the **Pareto
frontier** highlighted; the **raw baseline** at (1.0, 1.0); arrows showing `p↓` and
`ε_cdm↑` sliding *left* (cheaper) at ~constant `y`.

```
acc 1.00 │ raw●                      ← baseline (cost 1.0)
    0.99 │      ◆ sketch(p=1,ε=0)
    0.98 │        ◆ +ε_cdm=0.1
    0.98 │          ◆ +ε_cdm=0.2
    0.98 │            ◆ +p=0.5
    0.96 │               ◆ +p=0.25   ← frontier pushed left, acc still ≈α
         └────────────────────────────────────  cost (× raw)
           0.3      0.5      0.7    1.0
```
**Have:** the corner points (accuracy per `p`; egress per `ε_cdm`; ingest per `p`).
**Need:** one combined sweep run producing the full matrix → one frontier. **◐**

---

## Fig 2 — Bandwidth ablation: time × label × encoding (+ sampling)  ◐
**Claim (paper-outline §6.2):** `bw reduction = time-factor(W) × label-factor(L) ×
encoding-factor`, each independently measurable; **sampling adds a 4th factor (`p`).**
**Layout:** 4 panels (or 4 curves), each *one axis swept, others fixed*, y = wire
bytes/s vs `raw-buffer`, mean + P99 band.
**Have:** the `p` factor (ingest ∝ p, below) + the encoding/delta factor (Table 1).
**Need:** the W and L sweeps from `sdk-cost-evaluation.md`'s §6.2a/b. **◐**

---

## Table 1 — CDM ε-gate cuts egress on stable series  ✅
**Claim:** the ε threshold suppresses emission for stable series (open-window CDM),
cutting egress vs fixed-interval (`ε=0`). Mixed 70% stable / 30% bursty workload.

| `ε_cdm` | sub-window emits / window | egress bytes / window | vs ε=0 |
|---|---|---|---|
| 0 (fixed) | 1100 | 56 438 | 1.00× |
| 0.1 | 926 | 37 612 | 0.67× |
| 0.2 | **717** | **30 333** | **0.54×** |

*(p=0.25 arm: 999 → 511 emits across ε=0→0.2.)* **✅ measured (`/tmp/cdmsamp`).**

---

## Fig 3 — Query accuracy stays in the ε-envelope; small-N degradation matches the bound  ✅
**Claim:** every answer falls inside `ε_sk + ε_s + ε_cdm`; the only degradation under
aggressive `p` is the **predicted** `ε_s = √((1−p)/(pN))` on small-N series.
**Layout:** (a) rel-err vs `p` on the real 2019 Google trace (pooled, high-N);
(b) per-series rel-err vs series count `N` at `p=0.25`, with the `ε_s(N)` curve overlaid.

**(a) pooled accuracy sweep (Google-cluster-2019 `cpu_rate`, 50k pts):** ✅
| `p` | p99 rel-err | p50 rel-err | ingest pts/s |
|---|---|---|---|
| 1.0 | 0.34% | 0.29% | 1667 |
| 0.5 | 0.34% | 0.29% | 832 |
| 0.25 | 1.69% | 0.29% | 417 |
| 0.1 | 1.86% | 1.23% | 165 |

**(b) per-series at `p=0.25`:** high-N (n≥165) rel-err 0.6–1.7% ≈ `α`; low-N (n≈44)
rel-err ~15–18% — matches `ε_s=√(0.75/(0.25·44))≈0.26`. **The bound predicts exactly
where it breaks**, and the coordinator's ε-floor `1/(1+ε²·rate)` is what prevents it.
**✅ measured (`/tmp/gct-sweep-results.json`, `/tmp/2axis`).**

---

## Fig 4 — Open-window freshness (CDM ε-gate vs fixed-window)  ✅
**Claim:** sub-window emission keeps the *open* window queryable within ε; the
fixed-window baseline is blind until the boundary seals.
**Layout:** time-series, x = time within a 30 s window, y = queried p99; two lines —
**sub-window ON** (climbs as bursts land) vs **fixed-window OFF** (no data → flat/blank).

```
p99  1000│                         ●ON (982 @ t=26s)
      600│              ●ON
      400│   ●ON(399)
        0│●─────────────────────────×── OFF: "No result" for ~28s, then seals
         └──────────────────────────────  t (s)   0    10    20    30↑seal
```
**✅ measured** — ON live throughout; OFF returns "No result" ~28 s of every 30 s window.

---

## Fig 5 — Threshold alert (slack-countdown) + concurrent coordinated `p`  ✅
**Claim:** the coordinator fires the global-threshold alert exactly once within ε,
*while* simultaneously running coordinated sampling.
**Layout:** time-series, y = global estimate, horizontal line at `τ=24000` and
`(1−ε)τ=22800`; alert marker at the crossing; an inset/second axis showing each
edge's granted `p`.

```
global│ τ=24000 ─────────────────────────
22987 │                         ⚡ALERT (22987 ≥ 22800)
      │                    ╱
      │          ╱──╱
        └────────────────────────  per-epoch
grants: hot edge p=1.0→0.032 (sampled down) · quiet edge p=1.0 (kept)
```
**✅ measured** — fired once/epoch at `global=22987 ≥ (1−ε)τ=22800`; quiet below τ.

---

## Table 2 — Sampling × delta compose (the two-axis benefit)  ✅
**Claim:** `p` cuts ingest, `ε_cdm`/delta cuts egress, **orthogonally**, accuracy held.

| emission | `p` | egress KB/win (steady) | ingest adm/s | edge CPU | p99 rel-err |
|---|---|---|---|---|---|
| full | 1.0 | 162.6 | 9350 | 3.4% | 0.014 |
| full | 0.25 | 158.9 | 2335 | 1.0% | ~0.18* |
| delta | 1.0 | **80.0** | 9350 | 3.7% | 0.015 |
| delta | 0.25 | 72.9 | 2335 | 1.3% | ~0.15* |

\* per-series small-N (the `ε_s` tail of Fig 3b); pooled stays ≈α. Delta ≈ **2×
steady** egress cut (mean ~1.3× after first-window + periodic full re-sync).
**✅ measured (`/tmp/2axis`).**

---

## Fig 6 — Edge CPU / memory: sketch vs raw, + long soak  ◻
**Claim (dims 2–3):** sketch edge CPU/RSS bounded vs raw-forwarding; no leak over 24 h.
**Layout:** (a) stacked CPU bar per baseline (raw `b0` / gzip `b1` / sketch `b3`);
(b) RSS-over-time line, 24 h, slope-based leak verdict.
**Note (honest framing):** this is the **sketch-vs-raw** CPU story — *not* a
sampling-CPU claim. Sampling's win is bandwidth/ingest; the edge-CPU lever is
sketch-vs-raw **+ the CMS empty-base delta opt (−63%)**. **◻ need the raw baseline + soak.**

---

## Fig 7 — Query latency CDF: PromQL-native vs sketch-answered  ◻
**Claim (dim 5):** warm-tier p50/p99 production-usable; cold fallback ≤2×.
**Layout:** latency CDF, lines for B0 (native) vs B3 (sketch warm) vs cold-fallback.
**◻** — `metricsql_replay.py` exists; needs a run at fixed QPS.

---

## Fig 8 — Cross-layer placement: same `agg_type` at SDK / agent / backend  ◐
**Claim (§6.3):** placement doesn't change correctness, but shifts the CPU/mem
tradeoff; **and there's a `k`-stability constraint** on where coordination lives.
**Layout:** stacked CPU/mem-per-layer bars for the three placements.
**Have:** the design analysis + proof (sampling at SDK = zero-decode but `k` churns →
coordinate at the stable collector tier via a hierarchical budget). **Need:** the
per-layer CPU/mem bars. **◐**

---

## Fig 9 — Coordinated vs uniform `p` on a skewed fleet  ◐
**Claim:** `p_i ∝ √(f_i/rate_i)` beats uniform-`p` at equal merged variance, with the
gap ∝ the rate CV; the win **only appears on skewed fleets** (multi-edge).
**Layout:** total edge work (or wire bytes) for coordinated vs uniform across rate-CV.
**Have:** the differentiated grant — hot edge `p=0.0065`, quiet `p=1.0` (ε-floors
0.0079 vs 0.444); single-edge ⇒ p=1 by design. **Need:** the CV sweep on ≥2 edges. **◐**

---

## Fig 10 — Scaling: N ∈ {1, 10, 100} agents  ◻
**Claim:** bw/CPU per agent stays flat as the fleet grows (B3).
**◻** — all results single-node so far; needs multi-host or a sim.

---

## Fig 11 — Cold-tier (Gorilla) overhead — the *disjoint* cold half  ◻
**Routing model:** **disjoint** — a series is *either* warm-sketched (sampling + CDM
apply) *or* cold-archived to Gorilla (lossless, exact/historical), **never both**.
So the **total-resource Pareto = warm-half cost + cold-half cost**, partitioned
across series; the cold half is the *entire* cost of the non-sketched series and
sampling/CDM never touch it.
**Claim:** the cold path's overhead is bounded and is the *price of exact/historical
replay*; the lifecycle win is largest on **storage**.
**Overhead axes (edge encodes Gorilla-XOR `ASAPFRG1` fragments → merger builds
block+index → S3):**

| component | layer | measured as |
|---|---|---|
| encode CPU | edge | XOR-encode per sample (`fragment`) vs FOR+delta (`intchunk`, more CPU) |
| fragment RSS | edge | per-window batch before ship |
| ship bytes | edge→merger | cold wire bytes (separate from warm sketch egress) |
| merger CPU/IO | merger | block+index build + S3 PUT (decode-free ingest, PR #354) |
| **storage** | S3/MinIO | **bytes/series/day — the dimension the 5 dims underweight** |

**Cold-codec ablation** (`fragment` vs `intchunk` vs `intchunk`+zstd): per prior
`intchunk` verdict, `intchunk` saves ~2.3× wire bytes but costs more encode CPU/mem
(2.3×, not the designed 4.8×, because it dropped the zstd the real VM path uses).
**Layout:** (a) cold overhead arm added to the CPU/mem/bandwidth bars (Fig 6/2);
(b) a **storage figure** — bytes/series/day for raw vs gorilla-cold vs sketch-warm
(the raw/cold blow-up: a prior long-run hit ~496 GB disk); (c) the codec ablation.
**◻ to measure** (this session ran `cold: enabled=false` throughout). Prior data:
`asap-gorilla-go/gorilla_edge_bench_test.go`, `GORILLA_MERGER_DESIGN.md`,
`docs/design-archive-tier.md`. **Net cold into the disjoint Pareto.**

---

## Fig 12 — Controller allocation: routing + sketch + sampling matches the ideal  ◐
**Claim (extends paper-outline's planner-match):** the controller parses the query
set and **allocates, per metric/series, over a three-part space** — and that
allocation matches the hand-tuned ideal within X%.
The controller's optimizer (`control_plane/src/optimizer/`) already does the sketch
part — **bind rules** (`BindHllOnCardinality`, `BindKllOnQuantile`, …) + a **cost
model** (`cost/tco.rs`, `cost/wire.rs`: "family beats raw iff low-cardinality OR
high-sample-per-window"). This requirement extends the *same* optimizer to allocate:

1. **disjoint warm-sketch vs cold-Gorilla routing** — sketch if the queries on a
   series are approximate/aggregate (quantile/topk/cardinality/sum) *and* the cost
   model says sketch beats raw; **cold** if any query needs exact/historical replay
   on it. (The wire/TCO cost model is the decision function.)
2. **sketch type + `(W, L, agg_type)`** — the existing bind-rules + cost output.
3. **sampling `p`** — which sketches are sampling-eligible (policy from the
   controller) + the coordinated runtime allocation `p_i ∝ √(f_i/rate_i)` (data_plane
   coordinator, ε-floored).

**Layout:** match-rate curve — controller's `{routing, sketch, p}` choice vs the
ground-truth-optimal over synthetic query sets; a confusion-style breakdown of which
axis the controller gets wrong (routing? sketch type? `p`?).
**◐** — bind-rules + cost model exist (the sketch axis); the **disjoint-routing** and
**sampling-eligibility** allocation are the extension to build + evaluate.

---

## Threats & answers (reviewer-facing)

| threat | answer (and evidence) |
|---|---|
| "Sampling doesn't cut CPU" | **Don't claim it does.** CPU = sketch-vs-raw (Fig 6) + CMS empty-base −63%; sampling's win is bandwidth/ingest (Fig 2, Table 2). Profile breakdown backs it (decode + per-emit delta dominate; update loop ~3%). |
| "Delta queries are fragile" (fresh-edge-per-arm) | Real gap. Scope as long-lived edges / no mid-stream producer churn, **or** fix the per-series-base reset on producer gap. (Also: a Kafka-ordered transport would make delta delivery robust — future-work note.) |
| "Small-N accuracy degrades under aggressive `p`" | **Predicted, not surprising** — `ε_s=√((1−p)/(pN))`; high-N ≈α, low-N degrades as the formula says (Fig 3b), and the coordinator ε-floor bounds it. A *validated bound*, not a failure. |
| "Single node only" | Scaling (Fig 10) + coordinated-vs-uniform (Fig 9) need multi-edge; flag as the main missing axis. |
| "Per-emit delta cost" | Honest: cardinality-driven, addressed by the CMS empty-base opt, independent of sampling. |

---

## Status summary

| § | figure/table | status |
|---|---|---|
| 6.1 | accuracy in ε-envelope (Fig 3) | ✅ real-dataset |
| 6.1 | CDM ε-gate egress (Table 1) | ✅ |
| 6.1 | sampling × delta (Table 2) | ✅ |
| 6.1 | open-window freshness (Fig 4) | ✅ |
| 6.1 | threshold alert (Fig 5) | ✅ |
| 6.2 | bandwidth ablation W×L×enc×p (Fig 2) | ◐ (p + encoding done; W, L to run) |
| 6.headline | Pareto (Fig 1) | ◐ (corners done; one combined sweep) |
| 6.2 | edge CPU/mem + soak (Fig 6) | ◻ |
| 6.4 | query latency CDF (Fig 7) | ◻ |
| 6.3 | cross-layer placement (Fig 8) | ◐ (design+proof; bars to run) |
| 6.x | coordinated vs uniform (Fig 9) | ◐ (differentiation shown; CV sweep) |
| 6.x | scaling N∈{1,10,100} (Fig 10) | ◻ |
| 6.2/6.storage | **cold-tier (Gorilla) overhead, disjoint** (Fig 11) | ◻ (cold disabled all session — measuring now) |
| 6.5 | **controller allocation: routing+sketch+`p`** (Fig 12) | ◐ (bind-rules+cost exist; routing+sampling alloc = extension) |
| 6.x | drift / resilience | ◻ |

**Bottom line:** the *accuracy + CDM + composition* half of §6 already has real,
defensible numbers (incl. a real-workload trace). The *cost-baseline* half (raw-vs-
sketch CPU/mem, latency CDF) and the *multi-node* half (scaling, coordinated-vs-
uniform CV sweep) are the runs that remain — and the multi-node axis is the one the
coordinated-sampling claim most needs.
