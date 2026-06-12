# Multi-sketch-family evaluation on the real ASAP stack

**Date:** 2026-06-12
**Branch:** `feat/multisketch-eval`
**Dataset:** Google 2019 cluster trace (`instance_usage`), fetched + mapped via
`datasets_eval/google_cluster/{fetcher,otlp_mapper}.py`. 200k OTLP points
(100k `*_cpu_rate` + 100k `*_memory_usage`), `--cardinality-cap 1000` →
1000 hosts / 1000 services / 4 zones. Family-specific metric aliases derived
from the same `cpu_rate` rows (`make_aliases.py`) so all six families ingest
identical real values → 800k points on the wire for the all-families compound.
**Stack:** real fused `asap_edge` agent + `data_plane` + `control_plane` +
gorilla/MinIO/Thanos cold tier, all collapsed onto node0 (`stack.sh`), fresh
per arm. Both `asap/data-plane:dev` and `asap/asap-otel:dev` **rebuilt from
current source** (`/mydata/ASAPQuery-backend` @ main 845c56b,
`/mydata/ASAPCollector`) — the pre-built images were ~5 days stale and missing
the sketch-query fixes (see §"Honest status").

---

## Headline result

| Layer | Status |
|---|---|
| Agent ingest + 6-family fused aggregation + flush + ship | ✅ works, real numbers |
| Backend ingest + sketch-state storage (4669 sids, 22 MB payload) | ✅ works |
| Backend **warm SUM / exact-agg** query | ✅ **lossless**, matches GT exactly |
| Backend **warm SKETCH** query (quantile/topk/cardinality/frequency) | ❌ returns empty (`No result`) — read-path defect in this build |
| Exact offline ground truth for ALL families | ✅ computed (real trace) |
| Edge CPU / RSS / wire for compound + per-family arms | ✅ measured live |

The end-to-end **warm sketch read path does not resolve** for OTLP-fed
fused-edge sketches in the current build: every sketch family ingests, stores
state (verified: 4669 sketch sids, per-family aggregations registered with the
right types — `DatasketchesKLL`, `DDSketch`, `HLL`, `CountSketchWithHeap`,
`CountMinSketch`), but `quantile_over_time` / `topk` / `count` / `count_over_time`
all return `No result` while `sum(...)` resolves exactly. Root-caused below;
this reproduces (independently) the MEMORY note "asapedge-validation-rootcauses:
5 bugs break 4/6 query families." Rebuilding both images from source (which
carries the merged `fix/pwr-delta-query`, `fix/sketch-sample-p-rescale`,
`dfc358d decode DDSketch delta as bucket-delta` commits) did NOT fix it — the
remaining miss is in the per-series sketch `query_range(sid,t0,t1)` returning no
samples for the warm tier.

---

## A) Per-family accuracy vs ground truth

Ground truth is exact, stdlib-only (`e2e/gt_eval.py`), over the same replayed
rows. **GT is real**; the **warm** column is what the live backend returned.

| Family | Query | True (GT) | Warm result | In-envelope? | Wire |
|---|---|---|---|---|---|
| **Sum (exact)** | `sum(cpu_rate)` | **1883.9586305618286** | **1883.9586305618286** | ✅ lossless (Δ=0) | part of 25.7 MB compound |
| **Sum (exact, by zone)** | `sum by (zone)(memory)` | z0 311.835 / z1 318.641 / z2 318.072 / z3 314.757 | **identical** per zone | ✅ lossless (Δ=0) | — |
| DDSketch | `quantile_over_time(0.99, …)` | 0.0494384765625 | ❌ empty | n/a (read-path) | — |
| DDSketch | `quantile_over_time(0.50, …)` | 0.019287109375 | ❌ empty | n/a | — |
| KLL | `quantile_over_time(0.99, …)` | 0.0494384765625 | ❌ empty | n/a | — |
| KLL | `quantile_over_time(0.50, …)` | 0.019287109375 | ❌ empty | n/a | — |
| CountSketch (+heap) | `topk(10, sum by host)` | 10 hosts, 4.38–5.06 CPU | ❌ empty | n/a | — |
| HLL | `count(distinct service)` | 1000 | ❌ empty | n/a | — |
| CountMinSketch | `count_over_time(svc=…)` | 244 | ❌ empty | n/a | — |

**Sum is the only family that answers warm, and it is lossless** — exact to the
last float digit, confirming the first-class Sum envelope claim (#4) on real
data. This was verified repeatedly across fresh stacks and query times.

### Theoretical envelopes (for the families that would be scored if the read
### path resolved) — recorded for completeness:
- DDSketch α=0.01 → |q̂−q|/q ≤ 0.01
- KLL k=200 → rank error ≈ 1/k = 0.005
- CMS rows=5 cols=2048 → over-estimate ≤ ε·N, ε≈e/2048≈1.3e-3
- CountSketch L2 → |f̂−f| ≤ ε·‖f‖₂
- HLL → ±1.04/√(2^p)

### Quantile head-to-head (DDSketch vs KLL)
Both ingest the same `cpu_rate` values; GT p99=0.0494, p50=0.0193. **Accuracy
head-to-head could not be measured** (both return empty warm). What WAS
observed at config/registration level: DDSketch registers as `DDSketch`
(per-series, delta-capable), KLL as `DatasketchesKLL` (per-series, **no delta
variant** — the edge correctly omits `delta_transmission` for KLL; the
processor rejects it). So on wire mechanics they differ as designed: DDSketch
ships bucket-deltas after window 1, KLL always ships full state.

### Top-k head-to-head (CountSketch+heap vs plain CountSketch)
The intended "CMS-with-heap" head-to-head is **not expressible in the fused
`asap_edge`**: `emit_heap` is a CountSketch-only feature (the processor rejects
`emit_heap` on `family: countminsketch`). So the realizable head-to-head is
**CountSketch+heap (FrequencyTopk)** vs **plain CountSketch matrix (no heap)**:

| variant | warm `topk()` answerable? | wire (full data, incl. sum baseline) | edge RSS |
|---|---|---|---|
| CountSketch **+heap** | would be (capability `FrequencyTopk`) | **57.9 MB** | **74.8 MiB** |
| CountSketch **no-heap** | **NO** — capability miss → archive failover | 67.5 MB | 105.4 MiB |

**The heap-bearing variant is ~14% smaller wire and ~29% smaller edge RSS AND
is the only one that can serve `topk()` warm.** Plain (matrix-only) CountSketch
logs `SketchStore has no sid satisfying capability FrequencyTopk(Any) … failing
over to archive`. This backs the direction of the #371 recall-aware-topk
finding: the bounded heap is the compact, query-answering representation; the
raw count matrix is both bigger and cannot answer top-k warm. (Wire totals
include the two common Sum metrics, so the per-family heap saving is a lower
bound.)

---

## B) Compound — multiple families together

### B.1 All six families in ONE deployment (the realistic "one collector,
### many sketch types" cost)
Single fused `asap_edge` running **sum + ddsketch + kll + hll + countsketch
(+heap) + countminsketch** simultaneously, 800k OTLP points:

| metric | value |
|---|---|
| Edge CPU (steady, post-flush) | ~0.10% of one core |
| Edge RSS | **178.9 MiB** |
| Agent → backend total wire | **25.69 MB** (delta-transmitted) |
| Envelopes on the wire | 5559 (from 800k raw points → **143× point-count reduction**) |
| send-failed points | 0 |
| Data-plane RSS | 63.8 MiB |
| Backend sketch sids | 4669 |
| Backend sketch payload | 22.3 MB |

All six families' state lands in the backend (sids + per-family aggregations
registered). The Sum query answers in-envelope (lossless); the sketch queries
hit the read-path defect. So the **compound cost is real and cheap** (one
collector, 6 families, <180 MiB RSS, 26 MB wire), but only the Sum family is
warm-queryable end-to-end in this build.

### B.2 Multiple sketches on ONE metric (quantile AND topk on the same series)
**Not supported by the routing model** — this is a real finding, not a
limitation of the harness. The fused `asap_edge` assigns **exactly one warm
family per metric NAME** (`config.go`: one `MetricFamily` per metric). To run
DDSketch AND CountSketch on the same underlying series we had to emit the row
under **two metric-name aliases** (`*_q_ddsketch` and `*_topk_cs`) from the
same `cpu_rate` values. With that aliasing, BOTH families coexist in one
collector (the compound above) and both register state — but they are distinct
metric names, not "two sketches on one metric." Co-locating two families on a
single metric name is not expressible; the routing model is one-family-per-metric.

---

## C) Sampling × family

Sampling was driven by the real `asap_edge` per-metric `sample_p` knob (thins
sketch UPDATES at the edge; the backend rescales count-based families by 1/p —
`fix(precompute): rescale sampled CMS + HLL by 1/sample_p`). Measured the wire
and resource impact of `sample_p=0.25` on the sampling-aware families.

| Family | sampling wired? | wire @ p=1.0 | wire @ p=0.25 | verdict |
|---|---|---|---|---|
| CountMinSketch | yes (1/p rescale) | (in 25.7 MB compound) | **unchanged** | **neutral on wire**, benefits edge CPU |
| CountSketch | yes (frequency) | — | unchanged | neutral on wire, benefits edge CPU |
| HLL | **`sample_p` accepted but max/register-based** | — | unchanged | neutral→harmful (registers are max-merge; thinning loses distinct values, no wire win) |
| DDSketch | thins input, quantile preserved | — | unchanged | neutral on wire; reduces accuracy if over-thinned |
| KLL | no delta + cheap | — | unchanged | not beneficial (KLL is already cheap, full-state) |
| Sum | must see all points | n/a | n/a | **harmful** (sampling breaks exactness) |

**Key empirical finding:** for the fixed-width sketches (CMS / CountSketch / HLL),
`sample_p=0.25` produces **byte-identical wire** (25.69 MB vs 25.69 MB) —
because the sketch STATE size is set by its matrix/register dimensions, NOT by
the number of updates folded in. Sampling's payoff is therefore **edge CPU**
(fewer hash+update ops per point) and update-variance, **not bandwidth**. Edge
RSS dipped slightly (166 vs 179 MiB compound). This matches the design's
applicability table: sampling is a CPU lever for count-based sketches, neutral
on wire; it is **harmful for Sum** (exactness) and **not beneficial for KLL**
(already full-state/cheap) or HLL bandwidth (max-merged, fixed register array).

The per-family **accuracy under 1/p rescale** could not be confirmed because
the sketch read path returns empty (the rescale fix is in the backend but the
query never reaches it for these sids).

---

## Honest status — what would NOT run, and why

1. **Warm sketch READ path is defective in this build** (the dominant blocker).
   Every sketch family ingests + stores state (verified 4669 sids, correct
   per-family aggregation types registered, send-failed=0), but
   `quantile_over_time` / `topk` / `count` / `count_over_time` return
   `No result` at every query time and range, while `sum(...)` resolves
   exactly. Isolated single-family repro (3000 points → 132 sids, DDSketch
   query empty at all 600s of swept query times and all `[Ns]` ranges). The
   data-plane logs `backend-storage-routing: … shape=Quantile backend=SketchStore`
   then the SketchStore's `query_range(sid,t0,t1)` returns empty — the
   per-series sketch sids carry state but no `query_range`-visible window
   samples. **Rebuilding both images from current source did not fix it.** This
   is a backend query-engine defect, not a harness/config issue.

2. **Version skew in the pre-built images** (found + worked around). The
   shipped `control_plane` emits an `enable_series_id` otlp-receiver key that
   NO collector build (even freshly rebuilt) accepts → the supervised agent
   crash-loops applying the pushed config (`'otlp' has invalid keys:
   enable_series_id`). We therefore drive the agent with a **static fused
   `asap_edge` config** (exporting to `data-plane:14317`) instead of the
   OpAMP-pushed config; the control plane still POSTs the matching backend
   streaming-config, which is what the query path consumes.

3. **One-family-per-metric routing** (B.2) — co-locating two families on one
   metric name is not expressible; worked around with metric aliases.

4. **CMS-with-heap top-k** is not expressible in the fused edge (`emit_heap`
   is CountSketch-only). The realizable top-k head-to-head is
   CountSketch+heap vs plain-CountSketch (above).

5. **`query_range` always forwards to the archive (Thanos)**, not the warm
   sketch store — only **instant** queries hit the warm tier. (Found while
   debugging; not a blocker for instant-query families.)

6. **Window alignment is operationally fragile**: the backend answers per
   tumbling 60s window (the controller clamps the streaming-config window to
   60s regardless of query range), so the full replay must land in one 60s
   wall-clock window for warm == GT-over-all-rows. We align replay to a 60s
   boundary (`run_eval.py --align-window`) and verify via `sum == GT`. Large
   single-window flushes (800k points) initially RST_STREAM'd the backend's
   gRPC ingest; a downstream `batch` processor (≤1500 pts/msg) fixed it
   (send-failed → 0).

7. **gct top-k skew is low** — the true top-10 hosts cluster at 4.38–5.06 CPU
   (near-ties), which is the HARD case for top-k recall. A DEBS-skewed arm was
   prepared (4.6 GB CSV present) but not mapped into the alias pipeline given
   the read-path blocker made the topk number unobtainable anyway.

---

## Reproduce

```bash
cd /mydata/ASAPCollector
# (images already rebuilt from source on this host)
python3 datasets_eval/multisketch/make_aliases.py \
    --in /tmp/gct-otlp.jsonl --out /tmp/gct-aliased.jsonl
datasets_eval/multisketch/stack.sh up \
    datasets_eval/multisketch/workloads/all-families.yaml \
    datasets_eval/multisketch/agent-allfamilies.yaml
python3 datasets_eval/multisketch/run_eval.py \
    --jsonl /tmp/gct-aliased.jsonl \
    --queries datasets_eval/multisketch/queries-allfamilies.json \
    --out datasets_eval/multisketch/results/compound-allfamilies.json \
    --arm compound-allfamilies --flush-wait 260
datasets_eval/multisketch/stack.sh down   # cleanup
```

## Files
- `stack.sh` — collapsed single-host stack bring-up/down (fresh per arm).
- `make_aliases.py` — family-aliased + optionally row-sampled JSONL from gct.
- `make_perfamily.py` — per-family isolated workload/agent/queries generator.
- `run_eval.py` — replay → align → seal-wait → query → GT-compare → CPU/RSS/wire.
- `workloads/*.yaml` — controller workloads (all-families + per-family).
- `agent-*.yaml` — static fused `asap_edge` configs (all-families, per-family,
  no-delta, sampling p=0.25).
- `queries-allfamilies.json`, `queries-*.json` — query + structured GT specs.
- `results/` — `gt-*.json` (exact ground truth), `compound-allfamilies.json`
  (live warm vs GT), `resource-measurements.json` (CPU/RSS/wire).
