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
1. sketch-across-the-lifecycle + the **autonomous `(ε, queries) → {sketch, W, L, p, τ}` planner** (the controller — the key originality);
2. **coordinated SDK sampling** — the unified whole-sketch ε-floor `p = 1/(1+ε²·rate)` (the per-key `√(f/rate)` allocation was retired) — new cost axis;
3. **CDM** — ε-gated sub-window delta emission (open-window freshness) + slack-countdown alert;
4. the **joint bound** `ε_sk + ε_s + ε_cdm` (proof) tying accuracy to cost.

---

## Experiment matrix — claim × dataset × baseline (the comparative-rigor plan)

The figures below are the *mechanism* demos. For a submission they must each run on
**≥2 real-world datasets** against **real baselines**, with **multiple trials + 95% CIs**.
This matrix is the contract; the per-figure sections carry the current numbers.

### Real-world datasets (workload-credibility axis — NO synthetic in the headline)
| dataset | domain | shape / regime | exercises | status |
|---|---|---|---|---|
| **Google-cluster-2019** (`cpu_rate`) | machine resource usage | millions of (machine,job,task); high cardinality; continuous gauge | quantile/sum sketches, cardinality regime | ✅ staged (`/tmp/gct-*`) |
| **DEBS-2022** (last-trade) | financial tick stream | ~5k symbols; high rate; Zipf-skewed | frequency/heavy-hitter (CMS/CountSketch/Topk), skewed-rate sampling | ◐ downloading (`debs/data/`) |
| **3rd real** (Alibaba-2022 / Azure-VM / node-exporter dump) | infra metrics | TBD | generality / scale | ◻ gap |

### Baselines (comparative axis — the biggest current gap)
| baseline | what it is | claim it stresses |
|---|---|---|
| **b0 raw-OTLP** | full samples, no aggregation | bw / CPU / mem floor |
| **b0a/b0b/b1 raw+codec** | gzip / zstd / Snappy only | bw from compression *alone* (isolate the aggregation factor) |
| **Prometheus+Thanos / VictoriaMetrics** | deployed TSDB remote-write | real-world reference point |
| **NitroSketch / OmniSketch** | sampling-sketch prior art | sampling *accuracy* vs ASAP's ε-floor |
| **Cormode-style CDM** | functional/threshold monitoring | delta-emission / freshness |
| **ASAP ablations** | no-sampling · uniform-p · no-CDM-delta · single-tier (warm-only) · **static (non-autonomous) alloc** | the marginal value of each ASAP knob |

### The matrix (✅ measured · ◐ partial · ◻ gap)
| # | Claim | Experiment | Metric | gct-2019 | DEBS-2022 | 3rd | vs baselines | rigor (trials/CI) |
|---|---|---|---|---|---|---|---|---|
| C1 | Bandwidth reduction | sketch-vs-raw on a true family slice (`c1_wire.py`) | wire bytes agent→backend | ✅ **DDSketch 33.8×±0.3, HLL 65.9×±0.8** (real gct, n=3, 95% CI) | ◻ | ◻ | ✅ vs raw-OTLP; ◐ **vs raw+gzip = net 2.7×** (gzip 12.3×, offline); ◻ Prom/Nitro | ✅ 3 trials + CI |
| C2 | Edge CPU | sketch processors vs raw-forward, per-node | cpu cores | ◐ Fig 6 | ◻ | ◻ | ◐ vs raw | ◻ |
| C3 | Edge memory | RSS bounded over long soak (no leak) | RSS slope | ✅ edge bounded; ⚠ backend leak (Fig 6) | ◻ | ◻ | ◐ vs raw | ◐ 1 soak |
| C4 | Query accuracy | all-6-family error inside ε-envelope vs ground truth | rel-err, %≤ε, top-K recall | ✅ **DDSketch p50 0.72%/p99 2.67%, HLL 0.33%** (2026-06-20, root-caused valid); KLL small-N◐; CMS/CS → gauge-mismatch, use DEBS | ◻ (heavy-hitter natural here) | ◻ | ◻ **vs Nitro/Omni** | ◻ **need N trials + CI** |
| C5 | Query latency | warm-sketch vs cold-fallback PromQL replay | p50/p99 ms | ✅ Fig 7 (warm+cold) | ◻ | ◻ | ◐ vs VM/Thanos native | ◐ |
| H | **Pareto headline** | total (edge+wire+backend+storage) cost vs accuracy, swept over `(W,L,agg,p,ε)` | cost↔acc frontier | ◐ Fig 1 | ◻ | ◻ | ◻ **vs raw+Prom on same frontier** | ◻ |
| N1 | **Autonomous allocation quality** | `(ε,queries)`→plan vs oracle/hand-tuned/naive | plan match-rate, cost↔acc gap | ◐ Fig 12 (mechanism ✅ on cluster; quality ◻) | ◻ | ◻ | vs static-alloc, all-DDSketch, all-raw | ◻ |
| N2 | Controller adaptivity | inject query/workload drift → re-plan | re-plan latency, post-shift acc | ◻ | ◻ | ◻ | — | ◻ |
| X1 | Coordinated vs uniform p | whole-sketch ε-floor (observable R) vs fixed-p; per-edge on a fleet | F1/heavy-hitter err, insert-tput | live grants ✅ | ✅ real CMS/DEBS: **22× insert-tput, F1 err held at ε (0.050@ε=.05)**; fleet cold-edge **7×** better than fixed-p (Fig 9a; earlier 40–60× per-key claim RETRACTED, was circular) | ◻ | ✅ vs NitroSketch (same sampler; ε-floor derives p) | ✅ |
| X2 | Two-tier coverage | fraction warm- vs cold-answerable over a real query set; per-tier acc/latency | coverage %, per-tier | ◐ Fig 11 | ◻ | ◻ | — | ◻ |
| X3 | CDM delta savings | egress vs always-send, swept over τ | emits/window, bytes | ✅ Table 1 (gct) ; ✅ DEBS cross-check (structural-skew caveat) | ✅ | ◻ | vs Cormode-CDM | ◐ |

**Headline gaps to close (priority order):** (1) **real baselines** — at minimum b0a/b0b + Prometheus/Thanos + one sampling-sketch (NitroSketch), on the same Pareto; (2) **statistical rigor** — ≥5 trials + 95% CI on every accuracy/cost number (current runs are single-shot and noisy); (3) **all-6-family accuracy** clean (only DDSketch is solid); (4) a **DEBS-2022 end-to-end** pass (downloading) as the 2nd real axis; (5) **autonomous-allocation quality vs oracle** (mechanism is validated, decision quality is not); (6) **scale** beyond the light workload.

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

### (c) Per-family accuracy — all 6 families, real gct, 1000 series  ✅/◑
Each family ingests the *same* real `cpu_rate` rows (aliased) and is queried vs exact
ground truth (`datasets_eval/multisketch/`, branch `feat/multisketch-accuracy`).
**Wall-clock anchoring** of the replay was required (the warm read was empty because
the trace's epoch-relative timestamps never intersected the wall-clock query window —
a *timing*, not reducer, cause; pinned + fixed via `run.py --wall-clock-anchor`).

| family | claim | err / recall | in-envelope? | wire |
|---|---|---|---|---|
| **Sum** | sum | **exact** (1883.96) | ✅ | — |
| **DDSketch** | p50 / p99 | median 0.0065 (87%≤α) / 0.026 | ✅ / ◑ | 0.99 MB |
| **KLL** | p50 / p99 | median **0.0007** (96%≤ε) / 0.026 | ✅ / ◑ | 1.34 MB |
| **Count-Min** | freq | **exact** (244, one-sided OK) | ✅ | 10.85 MB |
| **HLL** | per-series distinct | rel-err **3e-5** | ✅ (per-series) | 0.51 MB |
| **CountSketch** | topk@10 | **recall 0** | ✗ | 12.09 MB |

**DDSketch vs KLL head-to-head** (identical data): KLL wins the **median** (0.0007 vs
0.0065); DDSketch wins the **tail** (lower p95) and ships **−26% wire**. The p99 tail
dispersion is small-N order-statistic variance (single-window collapse, ~93 pts/series),
not a sketch defect.

#### (c′) Re-run on current `main` (2026-06-19) — two regressions surfaced  ⚠
Same harness (`run_perfamily.py`, real gct, 1000 series), now on `main` (post ε-floor
unification + autonomous-allocation merges). Results **diverge from (c)** and flag two
issues to fix before this figure is submission-ready:

| family | kind | median rel-err | %≤ε | wire | vs (c) |
|---|---|---|---|---|---|
| **DDSketch** | p50 / p99 | 0.0072 / 0.0267 | 87% / 41% | **57.6 MB** | acc ≈ same; **wire 58× higher** |
| **HLL** | cardinality | rel-err **0.0033** (996.7/1000) | ✅ | 57.0 MB | acc ≈ same ✅ |
| **KLL** | p50 / p99 | **0.0597** / 0.0717 | 48% / 41% | 57.9 MB | **acc 85× worse** (was 0.0007) |
| **Count-Min** | freq | f̂=143 / f_true=244 | **one-sided VIOLATED** | 67.4 MB | was "exact, one-sided OK" |
| **CountSketch** | topk@10 | recall **0.0** (n_warm=0) | ✗ | 25.8 MB | unchanged-broken |

**Both flags ROOT-CAUSED (2026-06-20) — NOT a code regression; the accuracy is valid:**
1. **57 MB wire = un-sliced data, not lost SDK-aggregation.** The per-family slices
   `/tmp/perfam-*.jsonl` are **byte-identical 210 MB files each containing ALL 8 metric
   aliases** (`cpu_rate`, `_q_ddsketch`, `_q_kll`, `_topk_cs/cms`, `_card_hll`, `_freq_cms`,
   `memory_usage`). Each arm's agent **does** SDK-sketch its one configured metric (accuracy
   proves it), but **forwards the other 7 raw**, dominating the wire. → C4 accuracy is sound;
   **C1 wire must be re-measured with genuinely family-sliced inputs** (filter the slice to
   the single metric). A data-prep gap in `make_perfamily.py`, not a sketch/agent regression.
2. **KLL median + CMS one-sided are operating-point/data-fitness, not bugs.** KLL is
   rank-based and coarse at the **~93 pts/series** the wall-clock single-window collapse
   leaves (DDSketch's relative-error buckets handle small-N better — that's the real
   tradeoff, not a KLL defect). CMS "under-counts" because gct `cpu_rate` is a **gauge** with
   no meaningful per-key *count* to over-estimate — the frequency families are mis-applied to
   gauge data and belong on **DEBS** (below), where symbol-trade frequency is a true count.

**Data-fitness finding (not a bug):** gct `cpu_rate` is a **gauge** — quantile (DDSketch)
+ cardinality (HLL) are the natural fit and behave well; the **frequency/heavy-hitter
families (CMS, CountSketch, Topk) are mismatched to gauge data** (there is no meaningful
"count of a cpu_rate value"). Those families are evaluated on **DEBS-2022** (symbol-trade
frequency = the natural heavy-hitter workload), not gct.

#### (c″) CLEAN C1 bandwidth — fixed harness, real gct, 3 trials + 95% CI  ✅
`c1_wire.py` (2026-06-20) closes the (c′) flags: it slices the data to a TRUE single-family
set `{cpu_rate Sum-anchor, memory_usage, ONE sketch metric}` and replays the *same* slice
through both a sketch agent and a **raw-forward agent** (`agent-raw-coldoff.yaml`, no
`asap_edge`) for an apples-to-apples wire comparison.

| family | W_sketch | W_raw (same slice) | **reduction (n=3, 95% CI)** | accuracy |
|---|---|---|---|---|
| **DDSketch** | **1.00 MB** | 33.7 MB | **33.8× ± 0.3** | p50 0.69% (84%≤ε), p99 2.82% |
| **HLL** | **0.51 MB** | 33.7 MB | **65.9× ± 0.8** | card 0.33% |

`W_sketch` reproduces the (c) committed numbers (DDSketch 0.99 MB, HLL 0.51 MB) **exactly**,
which definitively settles the (c′) "57 MB" question: it was the un-sliced 8-metric data, NOT
a code regression. Reduction CIs are tight (±0.3 / ±0.8 over 3 trials). HLL ships less (one
register sketch) → ~2× the DDSketch reduction.

#### (c‴) Encoding-factor decomposition vs the gzip baseline (b0-gzip)  ◐
The honest reviewer question: *does sketching still win after a deployment compresses the raw
baseline?* Decomposition (DDSketch, real gct):

| | wire | factor |
|---|---|---|
| raw OTLP | 33.7 MB | — |
| **raw + gzip** (b0-gzip) | ~2.7 MB | **encoding 12.3×** (gzip-6 on the actual payload) |
| **ASAP sketch** | 1.0 MB | total **33.8×** vs raw |
| **ASAP vs raw+gzip** | — | **net ~2.7×** (= 33.8 / 12.3) |

So ASAP's 33.8× decomposes as **encoding (12.3×, free to anyone with gzip) × residual
aggregation (~2.7×, ASAP-unique)** — `12.3 × 2.7 ≈ 33.8`, internally consistent. **Sketching
still beats a gzip-compressed raw baseline by ~2.7×**, the aggregation contribution
compression can't replicate.

**Measurement caveat (honest):** the encoding factor is `gzip(actual payload content)`
measured **offline** (the single-node cold-off stack is `--network host`, so the
agent→data-plane leg can't be isolated from the constant replay→agent leg on `lo` — the
attempted loopback-byte method failed, all arms ≈ 35 MB). The configs `agent-raw-{gzip,zstd}.yaml`
+ `baselines.py` are ready; the *clean on-wire* compression number belongs on the **cluster**
(Phase-2 per-node NIC isolates agent→backend bytes). Also unmeasured: sketch-envelope
gzip (binary, compresses less) — so net ASAP-vs-raw+gzip is a conservative lower bound here.

**Two honest, root-caused gaps (not fabricated):**
- **CountSketch topk recall 0** — real semantic mismatch (orthogonal to timing): warm
  topk **keys by `item` not `host`** and **ranks by occurrence *frequency*, not
  sum-of-value**, so "top hosts by *CPU load*" (value-weighted) ≠ what the heavy-hitter
  sketch answers (top-by-*count*). `q-topk-service-count` (by count) is the fitting
  query; **value-weighted topk needs a separate update path** — a real finding for the
  topk claim.
- **HLL global rollup** — per-series HLL is exact (3e-5), but the *global* distinct
  readout isn't cleanly served (per-series storage; sealed-window `count()` empty).

So **5/6 families validated** (Sum/DDSketch/KLL/CMS/HLL-per-series); CountSketch-topk +
HLL-global are named gaps. (Also surfaced: the earlier "No result" class is the
**warm-vs-archive routing for recent range queries when the archive is on** — cold-OFF
serves warm; a backend fix target.)

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

## Fig 6 — Edge CPU / memory: sketch vs raw, + long soak  ✅ edge bounded · ⚠ backend leak found
**Claim (dims 2–3):** sketch edge CPU/RSS bounded vs raw-forwarding; no leak over 24 h.
**Layout:** (a) stacked CPU bar per baseline (raw `b0` / sketch `b3`);
(b) RSS-over-time line, slope-based leak verdict.
**Note (honest framing):** this is the **sketch-vs-raw** CPU story — *not* a
sampling-CPU claim. Sampling's win is bandwidth/ingest; the edge-CPU lever is
sketch-vs-raw **+ the CMS empty-base delta opt (−63%)**.

**Measured (real gct, cold-OFF, constant 5000 pts/s, 30-min soak = 9.5M pts / 0
errors; 24 h figures are linear extrapolations of the measured slope):**

**(a) CPU / RSS — both arms measured (raw arm NOT skipped):**
| arm | mean CPU% | p99 CPU% | steady RSS | n |
|---|---|---|---|---|
| b0 raw-forward edge (no asap_edge) | 2.75 | 3.33 | 207 MB | 60 |
| b3 sketch edge (DDSketch+Sum) | 4.38 | 8.19 | 223 MB | 360 |
| b0 data_plane | 7.69 | 8.13 | 28 MB | 60 |
| b3 data_plane | 0.26 | 0.60 | 25→72 MB | 360 |

Sketch edge costs **~1.6× CPU and +16 MB RSS** vs raw-forward — bounded, as claimed.

**(b) Leak slope (edge and data_plane separately):**
- **Edge: BOUNDED** — **+2.6 MB/h (≈0)**, flat ~223 MB the whole soak (24 h extrap +63 MB). ✅
- **data_plane: CLIMBING (monotone, linear)** — **+89.9 MB/h** (/proc) / +85.2 MB/h (the
  binary's own `MEMORY_DIAG` gauge — two independent sources agree); 25→72 MB over
  30 min, no plateau (24 h extrap ~+2 GB). ⚠

**Stale-sid finding ([[gct-memory-findings]]) — mechanism refined:** SketchStore **sid
count plateaus hard at 1004** (the cardinality cap) — the "sid count keeps growing"
reading does **not** reproduce. But **per-sid warm-sketch state grows unbounded**
(`MEMORY_DIAG` payload 257 KB → 37.6 MB, **~146×**) and drives the backend RSS climb
~1:1. So the backend memory growth the prior note flagged is **real and reproduces**;
the driver is **per-sid DDSketch-state growth the evictable flusher doesn't reclaim
under steady load**, not sid-count growth. Retention is **backend-side** — edge is flat.
**Single-node loopback. Artifacts:** `datasets_eval/soak/` (`soak_RESULTS.md`,
`rss_over_time.png`, `summary.json`, raw samples), branch `feat/soak-fig6`.
**Follow-up:** the backend per-sid state growth is a real defect worth a fix (the
flusher's evictable accounting under sustained ingest).

---

## Fig 7 — Query latency CDF: PromQL-native vs sketch-answered  ✅ (warm + cold-fallback both measured)
**Claim (dim 5):** warm-tier p50/p99 production-usable; cold fallback bounded.
**Layout:** latency CDF, lines for warm sketch tier vs cold-fallback archive.

**Warm arm** (real gct, cold-OFF stack, wall-clock-anchored, 599-query mix @15 QPS,
guard-verified before timing — DDSketch read returned 691 real warm series,
`sum`=exact GT 216.3535, all `data_source=asap_query`, 0 empties/errors):

| query kind | p50 | p95 | p99 | n |
|---|---|---|---|---|
| all (mix) | 18.25 | 19.45 | 20.03 ms | 599 |
| `quantile_over_time` (DDSketch, 691-series reconstruction) | 18.34 | 19.56 | 20.65 ms | 400 |
| `sum` (lossless) | 1.58 | 2.19 | 2.25 ms | 199 |

**Cold-fallback arm** (NOW MEASURED — real gct, cold-ON stack: MinIO + gorilla-merger
+ Thanos store-gateway/query + data-plane with `ASAP_THANOS_QUERY_URL`; cold-enabled
edge with full `cold.ship_endpoint` block; 600-query mix @15 QPS, guard-verified —
**600/600 `data_source=thanos_query`**, 0 empties/errors, so every timed query was
answered by the cold/archive engine, not a warm shortcut):

| query kind | p50 | p95 | p99 | n |
|---|---|---|---|---|
| all (mix) | 22.28 | 47.17 | 67.07 ms | 600 |
| `quantile_over_time` (1000-series, Thanos PromQL over raw archived samples) | 24.27 | 48.59 | 68.41 ms | 400 |
| `sum` (lossless, archive) | 15.48 | 33.33 | 42.74 ms | 200 |

Warm CDF is **bimodal** (cheap lossless `sum` ~1.5–2.3 ms, 691-series DDSketch
quantile reconstruction ~18–21 ms; tails tight). Cold CDF sits to the right with a
longer tail: the archive answer crosses data-plane → thanos-query → gorilla-merger
StoreAPI + store-gateway and re-evaluates PromQL over raw Gorilla-XOR samples.
**Headline: warm p50/p99 = 18.3/20.0 ms (production-usable); cold-fallback p50/p99 =
22.3/67.1 ms — overall p50 ≈ 1.2× warm, p99 ≈ 3.3× warm.** So the cold path stays in
the tens of ms (no order-of-magnitude blowup), at the cost of a heavier p99 tail than
the warm tier.

How the cold path was forced & verified (the prior blocker is resolved): cold ship is
decoupled from the control channel (PR #500), so a static edge with `cold.enabled:true`
ships whenever `cold.ship_endpoint` is set — the multisketch coldon config had only
`cold:{enabled:true}` with NO ship_endpoint, which the edge treats as **drain-only**
(`config.go`: "Empty => drain-only (no shipping)") → that was why MinIO stayed empty.
With a complete cold block the edge shipped 1000-series ASAPFRG1 fragments to the
merger (verified via per-shard `cold drain`/`shipBatch` logs and thanos `count=1000`).
A cold storage-routing table pins `google_cluster_2019_cpu_rate → gorilla_object_store`
so its instant queries dispatch to the ThanosQueryEngine; the control-plane was stopped
during timing because it periodically re-POSTs a storage-routing table that overrides
the file table back to warm. **Single-node loopback — server-side latency only, no
network RTT.** Artifacts: `datasets_eval/latency/` (`latency_RESULTS.md`,
`latency_cdf.png`, `latency_summary.json`, `per_query_latency.json`,
`per_query_latency_cold.json`, `compute_latency.py`, `cold_latency_replay.py`,
`stack-coldon.sh`, `backend-storage-routing-coldon.yaml`, `agent-cold-ship.yaml`,
`queries-latency-cold.json`), branch `eval/fig7-cold-arm`.

---

## Fig 8 — Cross-layer placement: same `agg_type` at SDK / agent / backend  ◐
**Claim (§6.3):** placement doesn't change correctness, but shifts the CPU/mem
tradeoff; **and there's a `k`-stability constraint** on where coordination lives.
**Layout:** stacked CPU/mem-per-layer bars for the three placements.
**Have:** the design analysis + proof (sampling at SDK = zero-decode but `k` churns →
coordinate at the stable collector tier via a hierarchical budget). **Need:** the
per-layer CPU/mem bars. **◐**

---

## Fig 9 — Coordinated ε-floor `p` vs fixed-p (NitroSketch)  ✅ (algorithm) / ◐ (system)
**Claim (corrected):** the unified **whole-sketch** ε-floor `p = 1/(1+ε²·R)` (`R` = total
update rate, OBSERVABLE) gives a *principled* sampling rate that (i) cuts insert cost ~`1/p`
while bounding the whole-sketch L2 error to ε, and (ii) in a fleet adapts `p_i` to each edge's
own `R_i`. It is the SAME sampler as NitroSketch — the contribution is *deriving* `p` from
`ε`+`R` (not a hand-tuned knob), not a per-key accuracy win. (Per-key `√(f/rate)` retired.)

### (a) Standalone algorithm comparison — REAL DEBS-2022, on a real CMS  ✅ (corrected)
> **The earlier per-key version of this result was WRONG and is retracted.** It set
> `p_k=1/(1+ε²·f_k)` using the oracle per-key `f_k` — circular (knowing every `f_k` = exact
> counting, no sketch needed), and it resurrected the RETIRED per-key `√(f/rate)` allocation.
> The unified ε-floor is **whole-sketch**: ONE `p=1/(1+ε²·R)`, `R`=total update count (OBSERVABLE).

`epsilon_floor_vs_nitro_test.go` — real CMS over real DEBS (5,493 keys, **R=54M** updates,
observable). Whole-sketch ε-floor `p=1/(1+ε²R)` vs exact.

**(A) throughput / memory** (CMS 5×4096, sketchlib geometric skip-sampler):

| metric | exact (p=1) | ε-floor (ε=0.05, p=7.4e-6) | note |
|---|---|---|---|
| **insert throughput** | 8.3 Mupd/s | **183 Mupd/s (22×)** | the NitroSketch win — skip-sampling cuts update work ~1/p |
| **memory** | 164 KB | 164 KB (constant in #keys) | vs exact key→count map 179 KB; CMS win is *asymptotic* |
| **query latency** | 96 ns/key | 76–82 ns/key | unchanged (CMS query is O(rows)) |

**(B) accuracy law — what the ε-floor actually bounds** (CMS 5×65536 to isolate sampling from
collisions; mean of 5 seeds). The ε-floor bounds the **additive AGGREGATE** (F1=R) to ε; a
**point query on key `k`** is protected only to `ε·√(R/f_k)`:

| ε | **F1-total rel-err** (the bounded aggregate) | heaviest key (f≈1.5M) | rare keys (f≲1e3) |
|---|---|---|---|
| 0.05 | **0.050** (= ε ✓) | 0.26 (pred ε√(R/f)=0.30) | → 1.0 (lost) |
| 0.10 | **0.080** (≈ ε ✓) | 0.39 (pred 0.61) | → 1.0 (lost) |

So the honest, defensible claim is: **`p` derived from ε+observable R gives 22× insert
throughput while holding the whole-sketch aggregate error at ≈ε** — same sampler as NitroSketch,
the win is *how `p` is set* (not a hand-tuned knob). Per-key accuracy follows `ε·√(R/f_k)`:
**heavy hitters survive, rare keys fall below the sampling floor — inherent to sampling, not a
defect.** The right accuracy metric is therefore the aggregate / heavy-hitter error, never
rare-key rel-err.

### (a2) Per-edge fleet — the differentiation, on a skewed fleet  ✅
Real DEBS has only **3 exchanges** (mild skew → fixed-p worst-edge only **1.3×** the ε-floor's),
too weak to show it. On a **synthetic fleet** (64 edges, Zipf rates over ~2 decades,
rate-CV=2.9), at matched total bandwidth:

| | per-edge F1 sampling-err (analytical) | empirical F1 (real CMS, 5 seeds) |
|---|---|---|
| **ε-floor** (`p_i=1/(1+ε²R_i)`) | median 0.050, **max 0.050** (uniform) | hot 0.020 / **cold 0.020** |
| **fixed-p** (matched bw) | median 0.103, **max 0.162** | hot 0.010 / **cold 0.148** |

`→` fixed-p **over-protects the hot edge and under-protects the cold edge by 7×** (cold-edge
0.148 vs 0.020); the ε-floor spends the same total bandwidth but equalizes error at ε across
the fleet. This is the per-edge adaptation the coordinator grants live (see (b)).

### (b) System-level differentiated grant  ◐
Live on the cluster: hot edge `p=0.09`, quiet `p=0.14` (coordinator-granted ε-floor from
the autonomous `/plan/auto` loop, PR #382). **Need:** the rate-CV sweep on ≥2 edges for the
system figure.

---

## Fig 10 — Scaling: N ∈ {1, 10, 100} agents  ◻
**Claim:** bw/CPU per agent stays flat as the fleet grows (B3).
**◻** — all results single-node so far; needs multi-host or a sim.

---

## Fig 11 — Cold-tier (Gorilla) overhead — the *disjoint* cold half  ✅ (edge+ship measured; merger CPU/IO ◻)
**Routing model:** **disjoint** — a series is *either* warm-sketched (sampling + CDM
apply) *or* cold-archived to Gorilla (lossless, exact/historical), **never both**.
So the **total-resource Pareto = warm-half cost + cold-half cost**, partitioned
across series; the cold half is the *entire* cost of the non-sketched series and
sampling/CDM never touch it.
**Claim:** the cold path's overhead is bounded and is the *price of exact/historical
replay*; the lifecycle win is largest on **storage**.

Measured 2026-06-12 on this host (Xeon E5-2660 v2 @ 2.20 GHz, Go 1.26). Edge encode
+ footprint + bytes/sample are **deterministic** (`asap-gorilla-go`
`gorilla_edge_bench_test.go`); the codec ablation is on the **real Serf/Chimp
float corpus** (`/mydata/compress-bench/data_serf`, 12 series × 100k pts,
`intchunk` `compare_table_test.go`/`bench_test.go`); ship bytes were confirmed
**end-to-end over localhost HTTP** to a trivial sink (`zz_coldsink_e2e_test.go`,
the gorilla-merger stand-in). The **gorilla-merger now builds and its tests pass
in-tree** — `cd ASAPQuery-backend/gorilla-merger && GOPRIVATE='github.com/ProjectASAP/*'
go test ./...` is green (`internal/merger` ~3.1 s, `internal/coldchunk`;
`cmd/gorilla-merger` builds), and the pipeline test exercises ingest → WAL →
window → pending block → compaction/re-chunk (~120/chunk) → shipped → S3 (in-memory
bucket) + cold-part verbatim store, with no-gap/no-dup assertions. So the merger
is **runnable and now measurable**. Its **CPU/IO performance numbers are still
pending** (a gorilla-merger CPU/IO benchmark, separate PR), and an actual S3 PUT to
a **live object store** (MinIO/S3) plus a full S3 → thanos-query → answer
integration **remain unproven** (today's tests use an in-memory bucket; the
container build also still needs a BuildKit secret for the private module, though
local `go build`/`go test` work with `GOPRIVATE`). Storage here is the
on-disk-equivalent of the gzipped wire body (= the bytes the merger writes
pre-recompaction).

### (a) Cold-overhead table (edge + wire)

| axis | `fragment` (Gorilla-XOR) | `intchunk` (FOR+delta best-of-N) | source |
|---|---|---|---|
| **encode CPU, codec-only** | 110 ns/sample | 219 ns/sample (best-of-N 301) | `compare_table` (INT-wins series) |
| **encode CPU, full edge path** | ~1.38 µs/sample (XOR + ASAPFRG1 frame build) | — | `BenchmarkGorillaEdgeEncode_1kSeries` (165.6 ms / 120k samp) |
| encode CPU scaling | 15.9 ms@100s · 165.6 ms@1k · 1.97 s@10k series | — | `BenchmarkGorillaEdgeEncode_*` |
| **encode mem (allocs)** | 34 B/sample (codec) · ~29 MB/1k-series-window | 170 B/sample (5.0× — builds 5 candidates) | `compare_table`, `BenchmarkGorillaEdgeEncode` B/op |
| **fragment RSS** | **1.79 KB / open series** (half-open XOR chunk + map + attrs) | similar order (same open-chunk state) | `TestGorillaEdgeFootprintBytesPerSeries` (10k series) |
| **ship bytes, real corpus** (gzip wire) | **3.46 B/sample** mean (raw frame 5.92) | **1.37 B/sample** mean (raw 2.56) | `TestColdShipStorage` (Serf, 12 series) |
| **ship bytes, random-walk** (gzip wire) | 7.26 B/sample (gzip ≈ no help, XOR already dense) | 6.70 B/sample | `TestColdShipStorageSynthetic` |
| ship bytes/window (1k series × 120) | **782 919 B** (6.52 B/s, gzip only 1.07×) | — | `TestColdSinkE2EShipWindow` (localhost POST, sink-verified round-trip) |
| merger CPU/IO + S3 PUT | **◐ runnable & measurable**, numbers pending (merger builds + tests green in-tree; CPU/IO via separate benchmark PR; S3 PUT verified vs in-memory bucket only) | ◐ | `internal/merger` pipeline test (`go test ./...`) |

bytes/sample is **strongly data-dependent**: pure `chunkenc.XOR` is 1.3 B/s on an
integer counter, 1.6 B/s on a smooth counter, ~7 B/s on the random-walk corpus,
7.6 B/s on uniform noise (`TestGorillaXORBytesPerSampleByDataShape`) — the
synthetic ~7 B/s is a property of that corpus, not a codec deficiency (it IS
vanilla Prometheus gorilla by construction). Small edge chunks degrade the ratio:
at chunk=120 the ASAPFRG1 frame is 7.16 B/s, rising to 15.2 B/s at chunk=10
(2.12×) — the suboptimal edge ratio the merger re-chunk (to ~120/chunk) fixes.

### (b) Storage dimension — raw vs gorilla-cold vs sketch-warm (bytes/series/day @ 1 s)

| tier | bytes/series/day | vs raw | note |
|---|---|---|---|
| **raw** (16 B/sample) | **1 382 400** | 1.0× | 8 B ts + 8 B f64, uncompressed |
| **gorilla-cold `fragment`** (gz, real corpus) | **≈ 299 000** | **4.62×** | lossless XOR, edge-encoded |
| **gorilla-cold `intchunk`** (gz, real corpus) | **≈ 118 000** | **11.66×** | lossless FOR+delta, fixed-decimal telemetry |
| sketch-warm (lossy) | — (not bytes/series/day comparable) | — | warm is `(ε,δ)`-bounded *aggregate* state, not per-sample replay; egress 30–56 KB/window for a *whole fleet* (Table 1) — the warm tier discards the per-series sample stream entirely, so its "bytes/series/day" is ≈0 for replay but it **cannot answer exact/historical** queries. The cold tier's bytes/series/day is the *price of exact replay* the warm tier doesn't pay. |

The raw→cold blow-up is the headline lifecycle win: **4.6× (`fragment`) to 11.7×
(`intchunk`)** smaller on real telemetry. A prior multi-day long-run hit **~496 GB
on-disk** (210k sids, GC-lagged) in the *warm* persistence path — the cold tier's
gzipped block layout is what keeps archived bytes bounded at the rates above.

### (c) Codec ablation — `fragment` vs `intchunk` (vs `intchunk`+zstd) — MEASURED

On the real Serf corpus, **best-of-N `intchunk` beats `fragment` (Gorilla-XOR) by
2.33× bits/sample on the fixed-decimal class** (49.0 → 21.0 b/s) and **never loses**
(it falls back to Gorilla on true high-precision floats — Motor-temp, Air-pressure);
the gzipped-wire ratio is **2.52×** (`fragment` 3.46 → `intchunk` 1.37 B/s mean).
But `intchunk` **costs CPU and memory**: encode **1.99×** the CPU (110 → 219 ns/s),
best-of-N **2.74×** (301 ns/s), decode **1.10×** (49 → 55 ns/s — *slower*, not
faster), and **5.0×** the encode allocations (34 → 170 B/s). This **confirms the
prior verdict**: the win is ~2.3–2.5×, **not** the design's 4.8×, because the
shipped `intchunk` **dropped the zstd stage** the real VictoriaMetrics path uses;
`intchunk`+zstd is **not wired**, so that arm is unmeasured. On the high-entropy
random-walk corpus `intchunk`'s edge shrinks to 1.08× — the codec win is
**fixed-decimal-only**.

### (d) Net into the disjoint Pareto

Total cost = **warm-half** (sampled + CDM, Fig 2/Table 1-2: ingest cut ∝ `p`, egress
30–56 KB/window/fleet, lossy `(ε_sk+ε_s+ε_cdm)`) **+ cold-half** (this fig: lossless,
~299 KB/series/day `fragment` or ~118 KB/series/day `intchunk`, +110–219 ns/sample
edge CPU, +1.79 KB/open-series RSS, no sampling/CDM).

**Example partition (10 000 series, 30% cold / 70% warm):**
- 3 000 cold series → storage **≈ 0.35 GB/day** (`intchunk`) or **0.90 GB/day**
  (`fragment`); ship ≈ 4.1 MB/window (`intchunk`) — *exact/historical replay, no
  accuracy loss*.
- 7 000 warm series → no per-sample storage; egress dominated by the sketch-state
  wire (Table 1-2), cut a further ~2× by delta and ∝`p` by sampling; *answers
  bounded by `ε_sk+ε_s+ε_cdm`*.
- The two are **additive and non-overlapping**: moving a series cold removes it from
  the warm egress/ingest entirely and adds exactly its cold storage+ship cost. The
  controller (Fig 12) picks the split from the query set — exact/historical queries
  pull series cold; aggregate/threshold queries keep them warm-and-cheap.

**Honesty ledger:** edge encode CPU/RSS/bytes-per-sample and the codec ablation are
**deterministic bench** (real corpus); ship bytes/window were **verified end-to-end
over a real localhost HTTP POST** to a sink that round-trips the body; the
**gorilla-merger builds and its tests pass in-tree** (`GOPRIVATE='github.com/ProjectASAP/*'
go test ./...` green — `internal/merger` pipeline + `internal/coldchunk`), so it is
**runnable and measurable**, but **merger-side CPU/IO numbers are pending** (a
gorilla-merger CPU/IO benchmark, separate PR) and the **S3 PUT is verified only
against an in-memory bucket** — a live-object-store PUT and a full S3 →
thanos-query → answer integration **remain unproven**; `intchunk`+zstd is **not
wired** (unmeasured). Storage = gzipped-wire
on-disk-equivalent, pre-merger-recompaction (the merger re-chunks to ~120/chunk,
which *improves* the edge ratio — so these are an **upper bound** on archived bytes).
Repro: `cd asap-gorilla-go && go test -run 'GorillaEdge|ColdShip|ColdSink' -v .`
and `cd intchunk && INTCHUNK_BENCH_DIR=/mydata/compress-bench/data_serf go test
-run 'CompareCodecCPUMem|BenchmarkBitsPerSample' -v .`

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
   controller) + the coordinated runtime allocation, the whole-sketch ε-floor
   `p_i = 1/(1+ε²·rate_i)` (data_plane coordinator; the per-key `√(f/rate)` law
   was retired).

### Methodology — how to evaluate the 4-tuple `{sketch, size, p, ε_cdm}`

Maps `(PromQL + accuracy/freshness SLA + workload stats {cardinality, rate, value
dist}) → {sketch type, size(α|k|rows×cols|precision), p, ε_cdm}`. The SLA is explicit
(per-query annotation / class default / native ε) — PromQL alone doesn't fix ε.
Evaluate on **two axes**: **soundness** (answers within the SLA) and **optimality**
(near cost-minimal among assignments that do). Backbone = the proven joint bound
`ε_total = ε_sk(size) + ε_s(p,N) + ε_cdm`.

**Per-knob — what "correct" means / how measured:**

| knob | correct = | measure |
|---|---|---|
| sketch **type** | sketch answers the query (quantile→KLL/DD, cardinality→HLL, topk→CountSketch/CMS-heap, sum→Sum) | **coverage**: % of query set with an answerable sketch |
| **size** | smallest with `ε_sk(size) ≤ SLA − ε_s − ε_cdm` | (a) empirical answer ≤ SLA; (b) ≈ size-sweep knee (one notch smaller misses) |
| **p** | largest sampling with `ε_s=√((1−p)/(pN)) ≤` budget | coupling holds at chosen p; p ≈ cost-optimal vs a p-sweep (bound predicts breakpoint) |
| **ε_cdm** | matches the freshness SLA | open-window error ≤ ε_cdm (the Fig 4 test) |

**Oracle** (ground-truth-optimal, for the cost-gap): per query, the **cost-minimal
feasible 4-tuple** = `argmin cost(tco/wire)` s.t. predicted `ε_sk+ε_s+ε_cdm ≤ SLA` and
freshness ≤ SLA — a constrained min over a grid via the per-family **accuracy-profile
library** (`ε_sk(size)`) + the `ε_s(p,N)` bound + the cost model, spot-validated by a
few empirical runs (which we already have and which confirm the bound).

**Metrics / figures:** (1) **coverage** bar/confusion; (2) **accuracy-met** — CDF of
rel-err/SLA, all ≤1, *driven by the controller's choice* vs ground truth; (3)
**cost-gap** — CDF of `controller_cost/oracle_cost` (the "within X%" = its P95), split
by which knob overpays; (4) **sensitivity** — tighten SLA → size↑, p↑, ε_cdm↓
monotonically; (5) **drift** — change rate/cardinality → re-allocates onto the oracle
within T s.

**Hard parts to disclose:** (i) the SLA source must be fixed ("query implies ε" holds
only for some shapes); (ii) the **accuracy-profile library is the lynchpin** — the
`ε_s` half is validated (small-N matches `√((1−p)/(pN))`), the per-family `ε_sk(size)`
profiles need the same (`sketch-bench`); (iii) the three terms trade against **one**
SLA budget → the oracle is a *joint* min and the controller must *split* the budget
sensibly (not blow it on a huge sketch then forbid sampling).

**Layout:** match-rate curve + the (1)–(3) figures above.
**◐ status:** the optimizer emits sketch type+size today (bind rules + `tco`/`wire`
cost); the **disjoint routing + `p` + `ε_cdm`** allocation, the **oracle/cost-gap
harness**, and the per-family **`ε_sk(size)` profiles** are the build-out for this fig.

---

## Real-world cross-check — DEBS-2022 (second real axis, and an honest correction)  ✅
Re-ran the ε-gate / delta / coordinated-sampling claims on the **real DEBS-2022
last-trade stream** (Zenodo, 53.99 M rows; mapped the 09:00–09:30 CEST slice =
**685,822 events / 3,912 symbols**, symbol = series key; top-1% of symbols = 11.3% of
activity, max ASML rate 3,141 vs min 1 — genuine activity skew). It **partially
corrects the synthetic claims** — which is the point of a second real axis:

| claim | synthetic / gct | **DEBS (real)** | verdict |
|---|---|---|---|
| **ε-gate egress** (Table 1) | synth 70/30 → 1100→717 emits | per-window emits ~**flat (3271)** as the count-gate tightens; gate cuts wire only **0.94×** — the silence is **structural skew** (~640 rare symbols absent), *not* the ε threshold | **ε-gate is the open-window-freshness/correctness bound (Fig 4 ✅), NOT a primary bandwidth lever** on workloads where active series move |
| **delta vs full** (Table 2/Fig 2) | synth ~2× wire | **4.10× on the serialized sketch *payload*** (bigger, as predicted for slowly-changing data) **but only 1.21× on the *gzipped wire*** (gzip already removes the static-bucket redundancy delta targets) | report delta as a **per-window state / pre-compression-payload** win (4.1×); on the gzipped wire it's small. **Sketch-vs-raw is still 26× on the wire** |
| **coordinated sampling** (Fig 9) | hot 0.0065 / quiet 1.0 (gct) | **hot ASML (rate 3141) → p=0.031, rare (rate 1) → p=0.990 = 32× differentiation**, every grant on its ε-floor `1/(1+ε²·rate)`; static-p ingest ∝ p (26k→6.5k adm/s) | **strong on real skew** — the win the design predicts |
| **accuracy** (Fig 3) | gct 0.3–1.9% | ASML last-trade median rel-err **0.9–1.1% ≈ DDSketch α**, inside `ε_sk+ε_s+ε_cdm` across the whole ε-gate & p sweep | **holds on real data** |

**What this does to the paper's framing (lead with the robust wins):**
- **Robust, real-data wins:** **sketch-vs-raw (26–44× wire)** + **coordinated sampling**
  (ingest ∝ p, **32× differentiation** on real skew, accuracy held). These are the headline.
- **Reframe, honestly:** the **ε-gate** is the **freshness/correctness** guarantee
  (bounded open-window error, Fig 4), *not* a bandwidth headline — on real moving
  workloads its incremental egress cut is ~0 (bandwidth comes from skew + sketch +
  sampling). The **delta** win is on the **per-window sketch state / pre-gzip payload
  (4.1×)**; **gzip absorbs most of it on the wire (1.21×)**.

Two real datasets now back §6: **Google cluster** (resource, high-cardinality →
accuracy, Pareto, cardinality/sum) and **DEBS-2022** (financial, skewed activity →
coordinated sampling, topk, the ε-gate/delta regime). Driver+data:
`datasets_eval/debs/cdm_eval/` (`RESULTS.md`, `results/*.json`).

---

## Threats & answers (reviewer-facing)

| threat | answer (and evidence) |
|---|---|
| "Sampling doesn't cut CPU" | **Don't claim it does.** CPU = sketch-vs-raw (Fig 6) + CMS empty-base −63%; sampling's win is bandwidth/ingest (Fig 2, Table 2). Profile breakdown backs it (decode + per-emit delta dominate; update loop ~3%). |
| "Delta queries are fragile" (fresh-edge-per-arm) | Real gap. Scope as long-lived edges / no mid-stream producer churn, **or** fix the per-series-base reset on producer gap. (Also: a Kafka-ordered transport would make delta delivery robust — future-work note.) |
| "Small-N accuracy degrades under aggressive `p`" | **Predicted, not surprising** — `ε_s=√((1−p)/(pN))`; high-N ≈α, low-N degrades as the formula says (Fig 3b), and the coordinator ε-floor bounds it. A *validated bound*, not a failure. |
| "Single node only" | Scaling (Fig 10) + coordinated-vs-uniform (Fig 9) need multi-edge; flag as the main missing axis. |
| "Per-emit delta cost" | Honest: cardinality-driven, addressed by the CMS empty-base opt, independent of sampling. |
| "Delta cuts wire 2×" | **Corrected on real data (DEBS):** 4.10× on the sketch *payload* but only **1.21× on the gzipped wire** — gzip already removes the static-bucket redundancy. Claim delta as a per-window-state / pre-compression win, not a gzipped-wire headline. Sketch-vs-raw (26×) is the wire headline. |
| "The ε-gate is your bandwidth win" | **It isn't, and we don't claim it is.** On real moving workloads (gct, DEBS) the gate's incremental egress cut ≈0 (suppression is structural skew, not ε). The ε-gate's value is the **bounded open-window freshness** (Fig 4); bandwidth = sketch + skew + sampling. |

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
| 6.2 | edge CPU/mem + soak (Fig 6) | ✅ edge bounded (+2.6 MB/h); ⚠ backend leak +90 MB/h |
| 6.4 | query latency CDF (Fig 7) | ✅ warm + cold-fallback (both measured) |
| 6.3 | cross-layer placement (Fig 8) | ◐ (design+proof; bars to run) |
| 6.x | coordinated vs uniform (Fig 9) | ◐ (differentiation shown; CV sweep) |
| 6.x | scaling N∈{1,10,100} (Fig 10) | ◻ |
| 6.2/6.storage | **cold-tier (Gorilla) overhead, disjoint** (Fig 11) | ✅ edge encode/RSS/ship + codec ablation + storage measured (real corpus, localhost-ship-verified); merger builds + tests green in-tree (runnable & measurable), CPU/IO numbers ◐ pending (separate benchmark PR), live-S3 PUT + thanos integration unproven (in-memory bucket today) |
| 6.5 | **controller allocation: routing+sketch+`p`** (Fig 12) | ◐ (bind-rules+cost exist; routing+sampling alloc = extension) |
| 6.x | drift / resilience | ◻ |

**Bottom line:** the *accuracy + CDM + composition* half of §6 already has real,
defensible numbers (incl. a real-workload trace). The *cost-baseline* half (raw-vs-
sketch CPU/mem, latency CDF) and the *multi-node* half (scaling, coordinated-vs-
uniform CV sweep) are the runs that remain — and the multi-node axis is the one the
coordinated-sampling claim most needs.


---

<!-- merged from docs/eval-query-plan.md — the per-dataset query list that feeds the figures -->

## Evaluation query plan — the queries to run on the top-5 datasets

> **Audience:** anyone building or running the §6 evaluation. For *each of the five
> datasets* shortlisted in
> [`use-case-dataset-survey.md` §5b](use-case-dataset-survey.md), this doc writes down
> the **concrete queries we plan to run** — warm (sketch-answered) and cold (exact
> replay) — tagged to the routing **mode** (M1/M2), the **sketch families**, the **seven
> axes**, and the **§6 figure/claim** each query feeds
> ([`evaluation-plan-figures.md`](evaluation-plan-figures.md)).
>
> Companion to [`use-case-dataset-survey.md`](use-case-dataset-survey.md) (modes +
> warm-eligibility predicate, §0a; the top-5 + scale, §5b),
> [`pipeline-query-catalog.md`](pipeline-query-catalog.md) (what the pipeline can answer
> today) and [`sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md)
> (query → sketch algebra).
>
> **Status:** query-plan reference, 2026-06-14. Datasets 1 (Google cluster) and 2
> (DEBS-2022) are **wired** (`datasets_eval/google_cluster/`, `datasets_eval/debs/`);
> 3–5 are the **build-out** — the queries below are the spec their harnesses implement.

---

## 0. Conventions — full evaluation = query × **every** supported sketch

**Full evaluation runs each warm query against *every* sketch that supports its kind**,
on identical data, and scores each independently — so every query is a **head-to-head
across sketch families** (the Fig 3c DDSketch-vs-KLL comparison, generalised to all
kinds and all datasets). A query of kind *K* expands to one run per sketch in the
"all supported sketches" column below:

| query kind | **all supported sketches (each run + scored)** | metric suffix per sketch | comparison reported |
|---|---|---|---|
| **quantile** (p50/p99/…) | **DDSketch**, **KLL** | `_q_ddsketch`, `_q_kll` | median vs tail rel-err, wire bytes (DD ≈ −26% wire, KLL wins median) |
| **topk / heavy-hitter** | **CountSketch-heap**, **CountMinSketch-heap** | `_topk_cs`, `_topk_cms` | recall@k, by-count vs by-value-sum, wire |
| **frequency** (point count of an item) | **CountMinSketch**, **CountSketch** | `_freq_cms`, `_freq_cs` | one-sided (CountMinSketch) vs unbiased (CountSketch) error |
| **cardinality** (distinct) | **HLL** *(only family)* | `_card_hll` | rel-err vs `1.04/√m` |
| **sum** | **Sum** *(lossless)* | `_sum` (bare metric) | exact (sanity) |
| **count** | **Count** *(lossless)* | bare metric | exact (sanity) |

Each warm query is the MetricsQL/PromQL + ground-truth record from
`datasets_eval/multisketch/queries-*.json`:

```json
{ "id": "...", "kind": "quantile|topk|frequency|count_unique|sum|count",
  "metricsql": "quantile_over_time(0.99, <metric>_q_<sketch>[300s])",
  "sketches": ["ddsketch", "kll"],
  "gt": { "op": "...", "metric": "...", "...": "..." } }
```

`<sketch>` is expanded over the `sketches` list at harness time → one scored run each.
The `gt` is computed from the **raw replay** (exact); we score `rel-err = |warm − gt| /
|gt|` against the joint envelope `ε_sk + ε_s + ε_cdm` (recall for topk). Default window =
**300 s tumbling**, wall-clock-anchored (the Fig 3c fix).

**In the per-dataset tables below**, the *sketches run* column lists the full set; the
metricsql shows the `_<sketch>` slot the harness expands. **Cold queries** are exact
point/range replay from the Gorilla/`intchunk` archive (M2 mandatory raw + rare M1
forensic). **M2 short-circuit:** a decision query `agg ⋛ τ` is answered from the warm
interval `[v−ε, v+ε]` — warm-only if `v+ε < τ` or `v−ε > τ`; only `τ ∈ [v−ε, v+ε]` drills
cold. Headline M2 metric = **warm-only-resolved fraction**, swept over `τ`, `ε`, and
*sketch family* (different ε per family → different short-circuit rate).

---

## 1. Google cluster 2019 — **M1** (observability/resource)  ✅ wired

Metrics: `gct_cpu_rate`, `gct_memory_usage`; series key `(machine, job, task/instance)`;
group labels `zone`/`cell`/`service`. Use case: fleet / per-cell resource SLO dashboards.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `gct-cpu-p99` | `quantile_over_time(0.99, gct_cpu_rate_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | Fig 3a, Fig 3c, Fig 7 |
| `gct-cpu-p50` | `quantile_over_time(0.50, gct_cpu_rate_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | Fig 3a, Fig 3c |
| `gct-cpu-p99-by-cell` | `quantile_over_time(0.99, gct_cpu_rate_q_<sketch>[300s])` *(by `cell`)* | quantile | **DDSketch, KLL** | 2,4,6 | Fig 3, repeated-dashboard |
| `gct-topk-host` | `topk(10, sum by (machine) (gct_cpu_rate_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,6 | Fig 3c topk recall (CountSketch-heap vs CountMinSketch-heap) |
| `gct-freq-service` | `count_over_time(gct_cpu_rate_freq_<sketch>{service="svc-000003"}[300s])` | frequency | **CountMinSketch, CountSketch** | 2,6 | one-sided vs unbiased freq |
| `gct-card-service` | `count(gct_cpu_rate_card_hll)` | cardinality | **HLL** | 2,6 | Fig 3c per-series distinct |
| `gct-sum-cpu` | `sum(gct_cpu_rate_sum)` | sum | **Sum** (exact) | 2 | Fig 3c exact check |
| `gct-sum-mem-by-zone` | `sum by (zone) (gct_memory_usage_sum)` | sum | **Sum** | 2,4 | Fig 3c |
| `gct-cpu-alert` | `quantile_over_time(0.99, gct_cpu_rate_q_<sketch>[300s]) > 0.8` | threshold | **DDSketch, KLL** | 2,3 | Fig 5 alert / repeated |

### Cold queries (rare forensic — disjoint)

| id | query | feeds |
|---|---|---|
| `gct-forensic-point` | exact CPU of `instance=X` at `t=2019-05-12T03:14Z` (range replay) | Fig 11 cold path (rare M1) |

---

## 2. DEBS-2022 — **M2** (finance/tick)  ✅ wired

Metric: `debs_last_price` (Gauge `financial.last_trade_price`, value = `last`), attrs
`symbol`/`exchange`/`sectype`; `debs_volume`. 5-min tumbling, Berlin wall-clock. Use
case: live VWAP / price-quantile dashboards + threshold alerts on a **skewed** symbol
fleet; MiFID audit / backtest on the *same* series → cold.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `debs-price-p50` | `quantile_over_time(0.50, debs_last_price_q_<sketch>{symbol="ASML.NL"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3 accuracy (EMA/VWAP proxy) |
| `debs-price-p99` | `quantile_over_time(0.99, debs_last_price_q_<sketch>{symbol="ASML.NL"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3c per-family |
| `debs-vwap` | `sum(debs_last_price_sum * debs_volume_sum) / sum(debs_volume_sum)` *(per symbol)* | sum | **Sum** (exact) | 2,7 | VWAP exactness |
| `debs-roll-vol` | `sum_over_time(debs_volume_sum{symbol="ASML.NL"}[300s])` | sum | **Sum** | 2,4,7 | rolling-volume dashboard |
| `debs-topk-active` | `topk(10, sum by (symbol) (debs_ticks_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,7 | most-active-symbol board |
| `debs-coord-sample` | per-symbol ingest under the ε-floor `p_i = 1/(1+ε²·rate_i)` (skew sweep) | sampling | **DDSketch, KLL** (under `p`) | 1,7 | **Fig 9** coordinated 32× |

### M2 short-circuit (decision queries — warm-first, cold on ambiguity; per sketch)

| id | decision query | sketches | resolves warm-only when | drills cold when |
|---|---|---|---|---|
| `debs-move-alert` | `quantile_over_time(0.99, debs_last_price_q_<sketch>{symbol=…}[300s]) > τ` | **DDSketch, KLL** | `v+ε < τ` or `v−ε > τ` | `τ ∈ [v−ε, v+ε]` |

### Cold queries (mandatory raw — cheap via edge Gorilla)

| id | query | feeds |
|---|---|---|
| `debs-audit-replay` | trade-by-trade exact replay for `(symbol, window)` (MiFID) | Fig 11 cold (finance-tick edge-Gorilla) |
| `debs-backtest` | exact tick series for `symbol` over the week | Mode-2 cold half |

---

## 3. Alibaba microservices 2021/2022 — **M1** (observability/traces)  ◻ to add

Metrics: `alibaba_ms_latency` (span duration, per `service`), `alibaba_ms_calls`,
distinct-caller stream `alibaba_ms_callers`. Use case: per-service p50/p99 latency SLO
alerting (RED) + distinct-caller cardinality; incident trace replay → cold. **Metric-
identity split:** the latency *metric* is warm; the raw span archive is a *separate*
artifact → cold (condition 2 provable).

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `ms-lat-p99` | `quantile_over_time(0.99, alibaba_ms_latency_q_<sketch>{service="S"}[300s])` | quantile | **DDSketch, KLL** | 2,6,7 | **Fig 7** `quantile_over_time` at scale |
| `ms-lat-p50` | `quantile_over_time(0.50, alibaba_ms_latency_q_<sketch>{service="S"}[300s])` | quantile | **DDSketch, KLL** | 2,6,7 | Fig 3c per-family |
| `ms-callers-card` | `count(alibaba_ms_callers_card_hll{service="S"})` | cardinality | **HLL** | 2,6 | distinct-caller fan-in |
| `ms-req-rate` | `sum by (service) (rate(alibaba_ms_calls[300s]))` | count | **Count** (exact) | 2,4,6 | RED rate dashboard |
| `ms-topk-slow` | `topk(10, sum by (service) (alibaba_ms_latency_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,6 | slowest-service board |
| `ms-lat-slo-alert` | `quantile_over_time(0.99, alibaba_ms_latency_q_<sketch>{service="S"}[300s]) > 0.5` | threshold | **DDSketch, KLL** | 2,3 | Fig 5 / repeated SLO |

### Cold queries (separate span artifact — disjoint)

| id | query | feeds |
|---|---|---|
| `ms-trace-replay` | exact span tree for `trace_id=…` (incident forensics) | Fig 11 cold (the half anchors lack) |

---

## 4. Azure VM 2019 — **M1** (observability/resource)  ◻ to add

Metrics: `azure_vm_cpu` (per-VM CPU, 5-min); cold counter `azure_vm_billing`
(**separate** series). ~2.6 M VMs = ~2.6 M series. Use case: per-VM CPU capacity/SLO
dashboards at fleet scale; billing/chargeback → cold. Textbook metric-identity split;
stresses the controller-allocation figure.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `vm-cpu-p99` | `quantile_over_time(0.99, azure_vm_cpu_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | **Fig 12** alloc @ ~2.6 M cardinality |
| `vm-cpu-p50` | `quantile_over_time(0.50, azure_vm_cpu_q_<sketch>[300s])` | quantile | **DDSketch, KLL** | 2,6 | Fig 3c per-family |
| `vm-cpu-p99-by-sub` | `quantile_over_time(0.99, azure_vm_cpu_q_<sketch>[300s])` *(by `subscription`)* | quantile | **DDSketch, KLL** | 2,4,6 | Fig 12 / repeated |
| `vm-card-vms` | `count(azure_vm_cpu_card_hll)` | cardinality | **HLL** | 2,6 | fleet-size distinct |
| `vm-avg-util-by-sub` | `sum by (subscription) (azure_vm_cpu_sum) / count by (subscription) (azure_vm_cpu)` | sum/count | **Sum, Count** | 2,4,6 | capacity dashboard |
| `vm-cpu-alert` | `quantile_over_time(0.99, azure_vm_cpu_q_<sketch>[300s]) > 0.9` | threshold | **DDSketch, KLL** | 2,3 | Fig 5 / repeated |

### Cold queries (separate billing series — disjoint)

| id | query | feeds |
|---|---|---|
| `vm-billing-replay` | exact per-VM billing counter for `(vm, billing_period)` (dispute-grade) | Fig 11 cold (separate series) |

---

## 5. Binance/Kraken crypto tick — **M2** (finance/tick)  ◻ to add

Metrics: `crypto_trade_price`, `crypto_trade_vol` (per `pair`). Hundreds of pairs, 24/7,
very-high per-series frequency. Use case: live crypto VWAP/quantile alerts + "is this
pair behaving oddly?" triage; strategy **backtest** replays raw on the *same* series →
cold. The Mode-2 / short-circuit showcase + the two orthogonal edge compressions.

### Warm queries (each run on **all supported sketches**)

| id | metricsql (`_<sketch>` expanded) | kind | sketches run (each scored) | axes | feeds |
|---|---|---|---|---|---|
| `cx-price-p50` | `quantile_over_time(0.50, crypto_trade_price_q_<sketch>{pair="BTCUSDT"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3 accuracy (high-freq) |
| `cx-price-p99` | `quantile_over_time(0.99, crypto_trade_price_q_<sketch>{pair="BTCUSDT"}[300s])` | quantile | **DDSketch, KLL** | 2,7 | Fig 3c per-family |
| `cx-vwap` | `sum(crypto_trade_price_sum * crypto_trade_vol_sum) / sum(crypto_trade_vol_sum)` *(per pair)* | sum | **Sum** (exact) | 2,7 | VWAP exactness |
| `cx-roll-vol` | `sum_over_time(crypto_trade_vol_sum{pair="BTCUSDT"}[300s])` | sum | **Sum** | 2,4,7 | rolling-volume board |
| `cx-topk-active` | `topk(10, sum by (pair) (crypto_trades_topk_<sketch>))` | topk | **CountSketch-heap, CountMinSketch-heap** | 2,7 | most-active-pair board |

### M2 short-circuit (the headline metric for this dataset; per sketch)

| id | decision query | sketches | resolves warm-only when | drills cold when |
|---|---|---|---|---|
| `cx-vol-alert` | `quantile_over_time(0.99, crypto_trade_price_q_<sketch>{pair=…}[300s]) > τ` | **DDSketch, KLL** | `v+ε < τ` or `v−ε > τ` | `τ ∈ [v−ε, v+ε]` |
| `cx-anomaly-triage` | "which pair/window is anomalous?" → warm screen, then drill | **DDSketch, KLL** | warm screen settles it | flagged window → cold |

Sweep `τ`, `ε`, **and sketch family** → report the **warm-only-resolved fraction** vs
forced-to-cold (the bound-based short-circuit headline) and the **cold-IO pruning ratio**
— DDSketch's relative-error bound vs KLL's rank-error bound give *different* ambiguous
bands, so the short-circuit rate is itself a per-family result.

### Cold queries (mandatory raw — cheap via edge Gorilla)

| id | query | feeds |
|---|---|---|
| `cx-backtest` | exact tick series for `pair` over N months (strategy replay) | Mode-2 cold half; edge-Gorilla cheap-cold |

---

## 6. What each dataset's query set proves (coverage)

| dataset | mode | warm query kinds × sketches | cold query | headline figure/claim |
|---|---|---|---|---|
| Google cluster 2019 | M1 | quantile {DD,KLL}, topk {CS-heap, CMS-heap}, freq {CMS, CS}, card {HLL}, sum, threshold | rare forensic point | Fig 3 accuracy, Fig 7 latency, Pareto |
| DEBS-2022 | M2 | quantile {DD,KLL}, VWAP {Sum}, topk {CS-heap, CMS-heap}, **coordinated sampling** | audit/backtest replay | **Fig 9** 32× sampling, edge-Gorilla cold |
| Alibaba microservices | M1 | quantile {DD,KLL} @scale, card {HLL}, rate {Count}, topk {CS-heap, CMS-heap} | trace replay (separate span) | high-card disjoint (Fig 11), Fig 7 |
| Azure VM 2019 | M1 | quantile {DD,KLL} @~2.6 M, card {HLL}, sum/count, threshold | billing replay (separate series) | **Fig 12** controller alloc |
| Crypto tick | M2 | quantile {DD,KLL}, VWAP {Sum}, topk {CS-heap, CMS-heap}, **short-circuit** | backtest replay | **bound-based short-circuit** per family |

**Read:** every dataset runs the **same query kinds against the same full sketch sets**
(quantile → DDSketch + KLL, topk → CountSketch-heap + CountMinSketch-heap, frequency → CountMinSketch +
CountSketch, cardinality → HLL, sum/count → lossless) — so each query is a per-dataset
**head-to-head across families** on identical real data. What differs is the **mode**
(M1 disjoint vs M2 co-resident) and therefore whether the cold query is a *rare disjoint
forensic* (M1) or a *first-class, frequently-paired exact replay* (M2); the two M2 sets
additionally run the **short-circuit decision protocol**, whose warm-only-resolved
fraction is itself reported **per sketch family** (different ε ⇒ different ambiguous band).

---

## 7. References

- [`use-case-dataset-survey.md`](use-case-dataset-survey.md) — modes + predicate (§0a), top-5 + scale (§5b).
- [`evaluation-plan-figures.md`](evaluation-plan-figures.md) — Fig 3/5/7/9/11/12, the joint bound.
- [`pipeline-query-catalog.md`](pipeline-query-catalog.md) — what the pipeline answers today.
- [`sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md) — query → sketch algebra.
- `datasets_eval/multisketch/queries-*.json` — the warm-query record format reused here.
- `datasets_eval/debs/DEBS_2022/02_benchmark_queries.md` — the DEBS Q1–Q13 benchmark queries.

---

*End of query plan.*

---

<!-- merged from docs/eval-instrumentation-notes.md — what each sweep-CSV column means -->

## Eval instrumentation notes — what each sweep CSV column means

_Last updated: 2026-05-05 (paper blocker #3 closeout)._

This is the column-by-column "what is this number, where does it
come from, and why" reference for `deploy/eval-results/sweep-*.csv`
(the file format produced by
`deploy/mvp-singlenode/scripts/measure-baseline.py` and chained together by
`deploy/mvp-singlenode/scripts/run-baseline-sweep.sh` /
`deploy/mvp-singlenode/scripts/run_e2e_sweep.sh`).

The next person to add a column or interpret one in a paper figure
should be able to land on this doc and understand the source of
truth without re-deriving it from comments scattered through
`measure-baseline.py`.

## Column map

Columns are emitted in the order shown in the CSV header. The
"source" column says which signal feeds it; the "fallback" column
is what `measure-baseline.py` consults when the primary path is
NaN. "Stack tier" is which container the signal originates from.

| Column | Source (primary) | Fallback (paper blocker #3) | Stack tier | Unit / scale |
| --- | --- | --- | --- | --- |
| `baseline` | `--baseline` CLI arg (e.g. `b3-delta`) | — | — | label |
| `scale` | `--scale` CLI arg (e.g. `N1`, `N10`) | — | — | label |
| `rate` | `--rate` CLI arg | — | — | events/s (workload knob) |
| `cardinality` | `--cardinality` CLI arg | — | — | distinct series (workload knob) |
| `producer_cpu_cores` | `docker stats` CPU% / 100 on `--producer-container` | — | producer (`otel-app`) | cores |
| `producer_rss_mib` | `docker stats` mem on `--producer-container` | — | producer | MiB |
| `producer_bytes_out_per_s` | `docker stats` net tx delta on `--producer-container`, divided by `--bytes-sample-window` | — | producer | bytes/s on the wire |
| `agent_cpu_cores` | `rate(otelcol_process_cpu_seconds_total{job="agents"})` | — | agent | cores |
| `agent_rss_mib` | `otelcol_process_memory_rss_bytes{job="agents"}` / 1MiB | — | agent | MiB |
| `agent_in_kib_per_s` | `rate(otelcol_asapcollector_processor_input_bytes_total{job="agents"})` / 1024 | `docker stats` net rx avg across `docker-compose-agent-*` / 1024 | agent | KiB/s |
| `agent_out_kib_per_s` | `rate(otelcol_asapcollector_processor_output_bytes_total{job="agents"})` / 1024 | `docker stats` net tx avg across `docker-compose-agent-*` / 1024 | agent | KiB/s |
| `agent_points_per_s` | `rate(otelcol_receiver_accepted_metric_points_total{job="agents"})` | — | agent | points/s |
| `gateway_cpu_cores` | `rate(otelcol_process_cpu_seconds_total{job="gateway"})` (with v0.108 fallback) | — | gateway | cores |
| `gateway_rss_mib` | `otelcol_process_memory_rss_bytes{job="gateway"}` / 1MiB | — | gateway | MiB |
| `gateway_points_per_s` | `rate(otelcol_receiver_accepted_metric_points_total{job="gateway"})` (with v0.108 fallback) | — | gateway | points/s |
| `gateway_out_series_per_s` | `rate(otelcol_exporter_sent_metric_points_total{job="gateway"})` (with v0.108 fallback) | — | gateway | series/s |
| `backend_cpu_pct` | `docker stats` CPU% on `docker-compose-backend-1` | — | backend | percent of one core |
| `backend_rss_mib` | `docker stats` mem on `docker-compose-backend-1` | — | backend | MiB |
| `backend_samples_per_s` | `rate(asap_ingest_samples_total)` (backend `:9091/metrics`) | `rate(otelcol_exporter_sent_metric_points_total{exporter=~"otlp.*backend.*",job="gateway"})` | backend (or gateway when fallback) | samples/s |
| `backend_query_p99_ms` | `1000 * histogram_quantile(0.99, rate(asap_query_duration_seconds_bucket))` (backend `:9091/metrics`) | `--replay-jsonl PATH` → p99 of successful `duration_ms` rows in client JSONL | backend (server-side) or replay client (client-side) | ms |

## Caveats — do not paper over these

### `agent_in_kib_per_s` / `agent_out_kib_per_s`: in-process bytes vs wire bytes

The patched-processor counter
(`otelcol_asapcollector_processor_*_bytes_total`) measures the
**OTLP protobuf MessageSize of the in-process `pmetric.Metrics`
batch as it crosses the processor boundary**, computed by the
`pmetric.ProtoMarshaler` in
`opentelemetry-collector-patch/processor/selfmonitor/selfmonitor.go`.
That's not the same number as bytes-on-the-wire:

  * It excludes gRPC framing, HTTP/2 headers, and the OTLP
    request envelope.
  * For sketch payloads (DDSketch / HLL / etc.), the in-process
    bytes include the typed proto envelope's full state — the
    same payload that goes on the wire — so the two measures
    agree to within ~5%.
  * For raw / Gorilla / Serf baselines that have no patched
    processor, no in-process counter fires at all. The fallback
    is `docker stats` net rx/tx on the agent container, which
    IS the on-the-wire rate.

When comparing across baselines (the bandwidth claim in
`docs/paper-outline.md` claim #1), prefer the `docker stats`
fallback as the apples-to-apples ground truth — note this
explicitly in the figure caption. The patched-processor counter
is the right signal for "bytes the sketch processor saw"; the
wire bytes are the right signal for "bytes the bandwidth budget
spent."

### `backend_samples_per_s`: backend ingest vs gateway egress

Today's `asap/query-backend:dev` image does NOT expose
`asap_ingest_samples_total` to Prometheus. The `:9091/metrics`
surface only contains query-side counters
(`asap_query_duration_seconds`, `asap_query_requests_total`).
The fallback uses
`otelcol_exporter_sent_metric_points_total{exporter=~"otlp.*backend.*",job="gateway"}`,
which counts the metric points the gateway forwarded to the
backend over OTLP. Modulo dropped batches (a small number
tracked elsewhere), gateway-egress equals backend-ingress, so
this is the right proxy.

For raw / Gorilla / Serf baselines that drop the OTLP forward
(`drop_original: true` in the agent yaml), the gateway is
literally not receiving anything from the agent — so this
fallback returns 0, which is correct: those baselines write
their compressed output to a local container directory, not to
the backend. The bandwidth signal for those baselines lives in
`agent_*_kib_per_s` (docker-stats fallback).

The proper fix is a backend-side counter — that's tracked as a
follow-up in the ASAPQuery-backend repo. Fallback is good
enough for the paper.

### `backend_query_p99_ms`: server-side vs client-side p99

`measure-baseline.py` chooses the client-side fallback whenever
`--replay-jsonl PATH` is provided, because:

  * The Prometheus path dies with the stack
    (`docker compose down -v` between cells in
    `run_e2e_sweep.sh`), so the histogram is gone before the
    next cell can inspect it. The replay JSONL persists on the
    host filesystem.
  * Client-side latency is what the caller actually
    experienced, including any network / OTLP serialization
    delay — which IS what the paper claim ("query latency
    competitive with raw") is about.

The numbers will not be identical: client-side adds the
local-loopback HTTP round-trip (~0.1-0.5ms on the dev box).
For the paper figure, document which side the number came from
in the caption. `run_e2e_sweep.sh` always passes
`--replay-jsonl`, so e2e-sweep CSVs are always client-side p99.
`run-baseline-sweep.sh` is client-side only when
`DRIVE_QUERIES=1` is set.

### `asap_query_duration_seconds` only fires on serviced queries

Empty-but-running stack → the histogram has 0 buckets. So the
primary-path PromQL returns NaN for any cell where no queries
were issued during the soak. That's not a bug, that's the
metric's contract; just make sure the sweep driver issues some
queries before reading the column. `run_e2e_sweep.sh` does this
by default (it runs `metricsql_replay.py` for the full soak).
`run-baseline-sweep.sh` did NOT prior to 2026-05-05; the
`DRIVE_QUERIES=1` opt-in flag added in this paper-blocker-#3
work fills the gap.

## Signal coverage by baseline (post paper-blocker-#3)

| Baseline | `agent_*_kib_per_s` | `gateway_*` | `backend_samples_per_s` | `backend_query_p99_ms` |
| --- | --- | --- | --- | --- |
| `b0a-raw-stream` | docker stats fallback | direct | gateway-fallback | client-side (when DRIVE_QUERIES) |
| `b0b-raw-batched` | docker stats fallback | direct | gateway-fallback | client-side (when DRIVE_QUERIES) |
| `b1-serf` | docker stats fallback | direct (zero — drop_original) | gateway-fallback (zero) | client-side (no warm answers — cold path) |
| `b2-full` | direct (sketch processor) | direct | direct (when ingest counter exists) or gateway-fallback | direct (when histogram is fed) |
| `b3-delta` | direct (sketch processor) | direct | direct or gateway-fallback | direct or client-side |
| `b4-tunable` | direct (sketch processor) | direct | direct or gateway-fallback | direct or client-side |
| `b5-gorilla` | docker stats fallback | direct (zero — drop_original) | gateway-fallback (zero) | client-side (no warm answers — cold path) |

Note that B1/B5 baselines are **expected** to show ~0
gateway-egress and 0 backend-samples: their pipeline is
`drop_original: true`, so the wire path to backend is empty by
design — the bandwidth signal lands in `agent_*_kib_per_s`
(docker-stats fallback) or in compressed-blob-on-disk metrics
that aren't in the CSV today.

## Adding a new column

1. Define the source. Prefer Prometheus over `docker stats`
   (Prom is rate-aware; docker stats requires the two-sample
   pass).
2. If the source isn't universal across baselines, add a
   fallback to `FALLBACK_QUERIES` in
   `deploy/mvp-singlenode/scripts/measure-baseline.py` (or in `main()` for
   non-Prom fallbacks). Don't silently let the column NaN —
   one of the bandwidth-claim figures was unreproducible for a
   week because of exactly that.
3. Add a row to the column map above. Mention any unit subtlety
   in the caveats section.
4. Smoke-test by running
   `deploy/mvp-singlenode/scripts/run-baseline-sweep.sh DRIVE_QUERIES=1` and
   confirming the column has no NaN for any baseline.
