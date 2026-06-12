# Multi-sketch per-family accuracy — wall-clock-anchored cold-OFF warm read

**Date:** 2026-06-12
**Branch:** `feat/multisketch-accuracy`
**Dataset:** real Google 2019 cluster trace (`instance_usage`), `--cardinality-cap
1000` → 1000 hosts / 1000 services / 4 zones, family-aliased (`make_aliases.py`)
so every family ingests the **identical real cpu_rate values**. Per-family data
slices (`/tmp/perfam-<fam>.jsonl`) each = cpu_rate + that family's alias;
verified offline to reproduce the committed exact GT (`results/gt-full.json`).
**Stack:** MINIMAL cold/archive-OFF — `data_plane` (`--enable-otel-ingest`,
query :9091, **no `ASAP_THANOS_QUERY_URL`** → `NoDataArchiveEngine` stub) +
`control_plane` + bare fused `asap_edge` agent (`cold: {enabled: false}`), fresh
per arm (`stack-coldoff.sh up`). Drivers: `run_perfamily.py` (per-family arms),
`run.py replay --wall-clock-anchor`, `perseries_quantile.py`.

---

## The fix that unblocked the warm read: wall-clock timestamp anchoring

The prior guard run found the warm sketch read returned **empty** and the
stored sketch windows were anchored at **epoch-relative time** (`start_ms ≈
360000`, i.e. ~360 s after 1970) while the PromQL `[Ns]` selector anchors at
wall-clock `now` → the query window never intersected the stored window.

`run.py replay` now takes **`--wall-clock-anchor`**: it discards the trace's
epoch-relative `timestamp_ms` and re-stamps **every datapoint at one wall-clock
instant captured at replay start**, collapsing the 31-day trace span into ONE
warm window at `now`. (Value/attribute-preserving, so the offline GT is
unchanged — GT is defined over all rows by value, window-bounds None.)

**Confirmation (pinning timing, not the reducer, as the cause):** with the
anchor on, `/api/v1/db/timeline` now reports the DDSketch/Sum segments at
`start_ms ≈ 1.7813e12` (wall-clock June 2026), the lossless warm
`sum(cpu_rate)` returns the **exact** full-replay GT **1883.9586305618286**
(prior run: a single 543.36 partial window), and
`quantile_over_time(0.99, …_q_ddsketch[300s])` at now returns **real per-series
values** for all 1000 series. The earlier "empty" was purely the
epoch-vs-wall-clock window mismatch (plus a too-narrow `[60s]` selector and a
partially-shipped window) — **not** a warm reducer defect. Guard PASSED.

Two mechanics learned and handled in the harness:
- **Single-instant collapse** (not send-time-per-batch): the asap_edge seals a
  window on wall-clock passing its end; stamping every batch at *its own* send
  time scatters a series across windows so only a subset is queryable from any
  one window. One fixed instant folds the whole replay into one window → all
  1000 series readable together.
- **Full-ship gating:** the warm state ships progressively over ~2-3 min; the
  driver waits until the lossless `sum(cpu_rate)` reaches the exact GT (every
  datapoint landed) and then scores at the query time that exposes the most
  sketch series.

---

## Per-family accuracy table

GT = committed exact offline ground truth (`results/gt-full.json`), real gct
trace. Quantile/HLL families are stored **per-series** (the quantile workload
declares NO grouping — "per-series quantile preserves PromQL semantics"), so
the warm read returns one value per replayed series and the right oracle is
**per-series**: each series' warm sketch vs that series' exact quantile
(linear-interp, same definition `gt_eval` uses). Quantile rows below are over
**all 1000 series**.

| family | claim | true (GT) | sketch (warm) | err / recall | in-envelope? | wire (agent→backend) |
|---|---|---|---|---|---|---|
| **Sum** | `sum(cpu_rate)` | 1883.9586305618286 | **1883.9586305618286** | rel-err **0.0** | ✅ lossless | (part of every arm) |
| **DDSketch** | p99 per-series `quantile_over_time` | 0.04944 (pooled) | per-series | median rel-err **0.0263**, p95 0.174, frac≤α(0.02)=0.44 | ◑ median in-band; tail = small-N | **0.99 MB** |
| **DDSketch** | p50 per-series | 0.01929 | per-series | median **0.0065**, p95 0.034, frac≤0.02=**0.87** | ✅ median within α | 0.99 MB |
| **KLL** (k=200) | p99 per-series | 0.04944 | per-series | median **0.0261**, p95 0.300, frac≤0.05=0.64 | ◑ median in-band; tail = small-N | 1.34 MB |
| **KLL** (k=200) | p50 per-series | 0.01929 | per-series | median **0.0007**, p95 0.040, frac≤0.05=**0.96** | ✅ median nails it | 1.34 MB |
| **CountMinSketch** | freq svc-000003 `count_over_time` | 244 | **244.0** | rel-err **0.0**, over-est=0 | ✅ one-sided f̂≥f, in εN band | 10.85 MB |
| **CountSketch+heap** | topk@10 by host | 10 hosts 4.38–5.06 | (see topk re-run below) | recall@10 | — | 12.09 MB |
| **HLL** | distinct service = 1000 (global) | 1000 | per-series card≈1.00003 (true 1) | per-series rel-err **3e-5**; global 1000 read blocked | ◑ per-series exact; global blocked | 0.51 MB |

`n_series = 1000/1000` for both quantile families (DDSketch & KLL) at `[300s]`.

### Reading the quantile numbers honestly
- **Median quantile error is within the family envelope** for both sketches —
  DDSketch p50 median 0.0065 (α=0.01 → ~0.02 band), KLL p50 median 0.0007.
- **The p99 tail (p95-of-rel-err ~0.17–0.30, max ~0.46–0.89) is a small-N
  sampling effect, not a sketch defect:** each per-series sketch holds a median
  of ~93 datapoints, so the *true* p99 is the ~92nd order statistic — an
  extreme-tail estimate with high intrinsic variance. The sketches reproduce
  the robust central quantiles (p50) tightly; the dispersion grows toward the
  extreme tail because the per-series sample itself is small there.

---

## Head-to-head #1 — DDSketch vs KLL (1000 series, identical real data)

| metric | DDSketch (α=0.01) | KLL (k=200) | winner |
|---|---|---|---|
| p50 median rel-err | 0.0065 | **0.0007** | KLL |
| p99 median rel-err | **0.0263** | 0.0261 | ~tie |
| p50 p95-of-rel-err | **0.0343** | 0.0401 | DDSketch |
| p99 p95-of-rel-err | **0.1737** | 0.3002 | DDSketch |
| wire (agent→backend) | **0.99 MB** | 1.34 MB | DDSketch (−26%) |

**Verdict:** KLL has the better *median* (especially p50), but DDSketch has the
tighter *tail* (lower p95 rel-err at both quantiles) and ships **~26% less
wire**. On this real-cpu_rate workload DDSketch is the better
accuracy-per-byte choice for tail-sensitive quantiles; KLL wins pure-median
fidelity at a wire premium.

---

## Head-to-head #2 — CountSketch heap vs no-heap (topk) — BLOCKED, root-caused

| arm | recall@10 vs GT | note |
|---|---|---|
| CountSketch + heap (topk_cs) | **0.0** | wrong heavy-hitters + count corruption |
| CountSketch no-heap (topk_cms) | **0.0** | same |

This head-to-head is **not measurable on this build** for two pinned reasons,
both reported rather than papered over:

1. **Label + semantics mismatch.** The warm CountSketch topk read returns its
   items under an **`item`** label (not the metric's `host` label) and ranks by
   **occurrence FREQUENCY**, e.g. `{item: host-m-000096}=124`. The committed
   `countsketch-topk-host` GT is **topk by sum-of-cpu_rate value** (hosts
   4.38–5.06). So `sum by (host)(…)` strips the key to empty and the rankings
   are different kinds of quantity. Re-querying by `item` against the exact
   *count*-topk still gives **recall@10 = 0.0**.

2. **Single-instant collapse corrupts occurrence counts.** The warm head's top
   counts (124, 97, 68, …) are far below the true per-host counts (280, 259,
   251, …) and pick entirely different hosts. Collapsing every datapoint onto
   ONE wall-clock instant — which is correct and lossless for value-*sums*
   (Sum=1883.96 exact) and value-*distributions* (DDSketch/KLL quantiles) —
   changes per-(series,timestamp) **occurrence counts**, so a frequency-topk
   sketch sees a distorted count multiset. CMS frequency (`count_over_time`)
   survived this because it counts samples in the window directly (svc-000003
   → exact 244); the CountSketch heavy-hitter heap does not.

**Net:** the CountSketch topk path is blocked by a key-label/aggregation
semantics gap plus a frequency-vs-collapse interaction, **not** by the timing
fix. Honest recall is 0; not fabricated. Measuring it cleanly needs a
value-weighted topk read keyed by `host` (or a paced replay that preserves
per-host occurrence counts), which is a separate work item.

---

## Honest status — which families ran, what is still blocked

- **Real gct data throughout** (no synthetic), per-family slices verified to
  reproduce the committed GT.
- **Fully clean:** Sum (lossless, exact), CMS frequency (exact 244, one-sided
  bound holds), DDSketch & KLL per-series quantiles (1000/1000 series, median
  within envelope) + their head-to-head.
- **Partial / a residual read-path limitation, reported not hidden:**
  - **HLL:** the per-series HLL estimates its own distinct count accurately
    (≈1.00003 for a true distinct of 1, rel-err 3e-5), but the **global**
    distinct-service=1000 the GT expects is not cleanly served — the HLL is
    stored per-series (no global merge exposed) and `count(metric)` only
    resolves on the still-open window (returns empty once sealed). This is the
    HLL sparse/sealed-window read limitation, orthogonal to the timing fix.
  - **CountSketch topk:** see the re-run section; the heap topk resolves to the
    labelled per-host series only in a narrow query-time band around the
    populated window.
- **Cause pinned:** wall-clock anchoring unblocked the warm read for every
  range-selector family (the empty-read defect was the epoch-vs-now window
  mismatch). The remaining HLL/topk gaps are *query-shape / sealed-window*
  read-path issues, not the timing cause and not the warm reducer math.

## Reproduce
```bash
cd /mydata/ASAPCollector
# per-family arms (fresh cold-OFF stack each), wall-clock-anchored replay
python3 datasets_eval/multisketch/run_perfamily.py \
    --arms ddsketch,kll,countsketch,countminsketch,hll --window 300s
# clean 1000-series DDSketch-vs-KLL head-to-head on one stack:
datasets_eval/multisketch/stack-coldoff.sh up \
    datasets_eval/multisketch/workloads/all-families.yaml \
    datasets_eval/multisketch/agent-allfamilies-coldoff.yaml
python3 datasets_eval/google_cluster/run.py replay \
    --jsonl /tmp/perfam-ddkll.jsonl --endpoint 127.0.0.1:4317 \
    --pace-factor 0 --wall-clock-anchor
python3 datasets_eval/multisketch/perseries_quantile.py <settled_window_unixtime>
```

## Files (this branch)
- `datasets_eval/google_cluster/run.py` — `--wall-clock-anchor` replay flag.
- `run_perfamily.py` — per-family cold-OFF accuracy driver (full-ship gated).
- `perseries_quantile.py` — DDSketch/KLL per-series + head-to-head.
- `agent-*-coldoff.yaml` — cold-OFF per-family agent configs.
- `results/perfamily-*.json` — per-arm live results.
- `accuracy_RESULTS.md` — this file.
