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

### (c) Per-family accuracy — all 6 families, real gct, 1000 series  ✅/◑
Each family ingested the same real `cpu_rate` rows and was queried against exact
ground truth in a historical standalone experiment whose artifacts were removed.
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
This historical single-node figure experiment is outside the issue-46 MVP
acceptance harness; its committed generated artifacts have been removed.
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
network RTT.** The historical standalone latency artifact bundle was removed;
the issue-46 harness is now the source of latency evidence.

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
   controller) + the coordinated runtime allocation `p_i ∝ √(f_i/rate_i)` (data_plane
   coordinator, ε-floored).

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
