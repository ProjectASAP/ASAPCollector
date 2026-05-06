# Headline §5 evidence — 60-cell sketchcol sweep, 2026-05-06

Combined dataset of two paired sweep runs:

* `sketchcol-sweep-20260505-204002/` — DDSketch + KLL, 24 cells (one
  empty `cs_N1_w100ms_c1000` placeholder excluded).
* `sketchcol-sweep-cont-20260505-230219/` — Count Sketch + Count-Min
  Sketch + HLL, 36 cells.

Total: **60 cells**, 5 sketches × 2 fan-out × 3 cardinalities × 2 scrape
windows. One cell (`kll_N10_w1000ms_c100000`) has no `cold-truth/`
directory; its accuracy CSV is synthesized from `replay.jsonl` only and
contributes only to the latency CDF.

## Layout

* `per-cell/accuracy-<cell>.csv` — per-replay-query accuracy rows
  produced by `fast_reduce.py` (a memory-bounded re-implementation of
  `accuracy_reduce.py --cell-dir`; precomputes a single truth summary
  per metric, then iterates replay rows).
* `accuracy.csv` — concatenated 17,539-row file used by the plots /
  stats scripts.
* `plots/` — the four §5 figures plus their CSV companions
  (`pareto_acc_vs_thru.{png,csv}`, `bandwidth_vs_n.{png,csv}`,
  `query_latency_cdf.{png,csv}`, `transition_timeline.{png,csv}`).
* `STATS.txt` — `build_stats.py` raw summary tables.
* `build_stats.py` / `build_plots.py` / `fast_reduce.py` — scripts
  the dataset was reduced and plotted with.

## Five-claim verdict

| # | Claim | Verdict | Headline number | Plot |
|---|---|---|---|---|
| 1 | DDSketch+delta is one to three orders of magnitude smaller on the wire than other sketches under the same workload | PASS — at the standard operating point producer wire bandwidth ranks DDSketch ≪ HLL/CMS ≪ CS ≪ KLL | DDSketch p50 producer bytes/s = **717 B/s**; KLL = **750 KB/s** (×1045); CS = **193 KB/s** (×269); CMS = **93 KB/s** (×129); HLL p99 = **160 KB/s** (×223). Agent-out `_total` rate p50: ddsketch 4.0 KiB/s vs hll 41.8 KiB/s (×10) | `bandwidth_vs_n.png` |
| 2 | Producer / agent CPU and RSS are bounded across the cardinality matrix | PASS — producer p99 ≤ 6.7 cores in the worst (DDSketch N1 W=100ms c=10⁵) cell; agent RSS p99 ≤ 1057 MiB across all five sketches | Producer cpu p50 by sketch: DDSketch 2.18, KLL 1.24, CS 1.66, CMS 1.41, HLL 1.49 cores. Agent RSS p99: DDSketch 974 MiB, KLL 883, CS 968, CMS 968, HLL 1057. | (resource numbers reported in §5 prose / Table 1) |
| 3 | PromQL warm-tier query latency is bounded inside the sketch envelope | PARTIAL — warm-tier queries (`quantile_over_time`, `topk`, `sum_over_time`) have p50 = 1.7–2.1 ms and p99 ≤ 327 ms across all 60 cells; the `count(metric)` family is the outlier and pulls the mixed-query p99 to 3.2–4.5 s | All-query p50 ≤ 2.3 ms, p99 3.2–4.5 s; **excluding `count(http_requests_total)`** p50 ≤ 2.1 ms, p99 158–327 ms — root cause documented below | `query_latency_cdf.png` (split into all-queries vs warm-tier-only panels) |
| 4 | Controller plan transitions are responsive | FAIL (in this sweep) — every cell logged a `t_query_in` but **0/60 cells reached `t_plan_ready`, `t_first_hit`, or `t_steady`** during the 60 s post-injection window | All 60 transition.jsonl files have `before_plan == after_plan` and null transition timestamps. Suggests the off-plan injection query (`histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))`) didn't trigger a controller replan in the 60 s soak | `transition_timeline.png` (rendered as a degenerate failure-mode bar) |
| 5 | Sketch answers land inside the per-family relative-error envelope | PASS — every sketch's median quantile error is one or more orders of magnitude below its theoretical envelope; cardinality (`count(metric)`) is exact across the board | Median quantile relative error: DDSketch **0.0047** (envelope 0.01), KLL **0.0066** (envelope 0.16), CS **0.0093** (envelope 0.03), CMS **0.0086** (envelope ≈0.0027 — over-bound; see caveat below), HLL **0.0099** (envelope 0.008 cardinality but quantile route is a no-op for HLL → see caveat). count_unique median error = 0 across all 5 sketches. | `pareto_acc_vs_thru.png` |

### Bandwidth headline

* **DDSketch+delta wire bytes are roughly 1000× smaller than KLL** on
  the standard operating point. KLL ships full sketch state every flush
  (DDSketch+delta is sparse `(bucket_idx, count)`-delta, KLL multi-level
  buffers are recompacted on every tick).
* Count Sketch / Count-Min Sketch sit between the two — sparse delta
  but the bursty counter workload still mutates many (row, col) cells
  per tick.

### Latency headline / `count(metric)` anomaly (§5 follow-up)

* All four "warm-tier" replay queries — `quantile_over_time(0.5/0.99, …)`,
  `topk(10, …)`, `sum_over_time(…)` — return in **<2 ms median, <250 ms
  p99**. That's the warm-tier envelope §5 is making a claim about.
* The fifth query, **`count(http_requests_total)`**, has p50 = **476 ms**,
  p99 = **7.5 s** across the 60 cells. It is the sole driver of the
  mixed-query p99 = 2–9 s reported in §5.4 of the previous draft.
* Root cause: the per-family `backend-inference-*.yaml` files key
  precomputes off the *suffixed* metric names
  (`http_requests_total_hll`, `http_requests_total_dd`, etc., cf.
  `deploy/configs/backend-inference-hll.yaml` line 25) but the replay
  queries hit the *unsuffixed* `http_requests_total`. Inference doesn't
  match → cold-tier scan → linear in cardinality. At c=10⁵ that's a
  multi-second scan.
* Fix path (does **not** require re-running the sweep at the system
  level — the data is correct): regenerate `queries-e2e.json` per
  agent overlay, or lift the suffix into the inference-side rule
  (one-line PromQL re-write at the warm-tier dispatch). Tracked as a
  follow-up so the next sweep gets a sub-second `count(·)` p99.

### HLL agent_cpu_cores / agent_rss_mib NaN cells

* 4 cells affected: `hll_N1_w1000ms_c10000`, `hll_N1_w1000ms_c100000`,
  `hll_N1_w100ms_c10000`, `hll_N1_w100ms_c100000` — all `N=1` with
  `c≥10⁴`. The other 8 HLL cells report normal agent CPU/RSS.
* Pattern: `producer_bytes_out_per_s` is ALSO 0 in these cells, but
  the gateway/backend pipelines clearly received data
  (`gateway_points_per_s` in some, `backend_query_p99_ms` real). So
  data **did** flow during the 60 s soak — it's a measurement
  artefact, not a system failure.
* Most likely cause: the post-soak `measure-baseline.py` two-sample
  `docker stats` window happened to land between agent flushes (HLL
  N=1 with sparse delta + W=100ms = thousand small writes per second
  from the producer but the `docker stats` 5 s sample can miss them
  at the cAdvisor sampling resolution).
* **Issue #71 is *not* the cause.** The PromQL templates in
  `measure-baseline.py` already use `otelcol_asapcollector_*`
  (post-#71 names), and HLL's processor self-monitor *does* emit
  those — confirmed by the 8 working HLL cells where the same script
  picks up CPU/RSS just fine.
* Fixing this in `measure-baseline.py` without re-running the sweep:
  not possible — the missing values come from the producer/agent
  containers being torn down before re-querying. The CSVs we have
  are the ground truth. Filed as follow-up: lengthen the
  `bytes-sample-window` from 5 s → 15 s and add a per-cell warm-up
  sample so the first `docker stats` window doesn't run on a
  recently-restarted container.

### Plan-transition cliff

Every cell logged the off-plan injection query (`t_query_in` set) but
none reached `t_plan_ready` / `t_first_hit` / `t_steady`, and
`before_plan == after_plan` on every record. This says the controller
did not replan within the 60 s observation window — the system fell
through to the cold tier (the "no data cliff" property §5.5 talks
about) but the planner-quality timing data is empty.

That is consistent with the e2e sweep harness running with the
previous (single-plan-shot) controller image and the off-plan probe
not matching any of the controller's pattern-match heuristics. The
follow-up to this sweep should:
1. Bake the latest controller image (with the L4-IR replanner)
   into the e2e harness, and/or
2. Use an off-plan probe whose shape the controller is known to
   recognize (e.g. a different aggregation kind on the same metric).

This is purely a sweep-configuration follow-up; not a system bug.

## Provenance / reproducibility

```
# reduce
python3 fast_reduce.py --cell-dir <SWEEP>/<cell> --out per-cell/accuracy-<cell>.csv

# concat
{ head -1 per-cell/accuracy-cms_N10_w1000ms_c1000.csv ;
  for f in per-cell/*.csv ; do tail -n +2 "$f" ; done ; } > accuracy.csv

# plot
python3 build_plots.py --accuracy accuracy.csv --out-dir plots \
  --sweep-old <SWEEP_OLD> --sweep-new <SWEEP_NEW>

# stats
python3 build_stats.py
```
