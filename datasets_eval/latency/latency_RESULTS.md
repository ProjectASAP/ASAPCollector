# Backend query latency (Fig 7) — warm sketch tier, §6 eval

**Date:** 2026-06-12
**Branch:** `feat/query-latency-cdf`
**Headline:** warm-tier sketch-answered PromQL is **production-usable**: across a
599-query mix at 15 QPS, overall **p50 = 18.3 ms, p99 = 20.0 ms** on the warm
SketchStore. The exact lossless `sum` is **p50 1.6 ms / p99 2.3 ms**; the
691-series DDSketch `quantile_over_time` read is **p50 18.3 ms / p99 20.6 ms**.
**Cold-fallback arm: NOT measured — blocked by archive/ship fragility (see
below). Warm-only is reported honestly.**

---

## Setup (the working query path — cold/archive OFF, wall-clock-anchored replay)

Same proven path as the `feat/multisketch-accuracy` accuracy eval:

- **Stack:** MINIMAL cold/archive-OFF single host — `data_plane`
  (`--enable-otel-ingest`, OTLP :14317, query :9091, **no
  `ASAP_THANOS_QUERY_URL`** → `NoDataArchiveEngine` stub) + `control_plane` +
  bare fused `asap_edge` agent (`cold: {enabled: false}`, `window_duration:
  60s`). Brought up with `datasets_eval/multisketch/stack-coldoff.sh up
  workloads/ddsketch.yaml agent-ddsketch-coldoff.yaml`.
- **Workload:** real Google 2019 cluster trace slice `/tmp/dd-only.jsonl`
  (36 000 OTLP gauge points; the same cardinality-capped 1000-series
  `cpu_rate` the accuracy run used) carrying:
  - `google_cluster_2019_cpu_rate` → **Sum** family (`tier: both`, lossless),
  - `google_cluster_2019_cpu_rate_q_ddsketch` → **DDSketch** family
    (`tier: warm`, per-series),
  - (`memory_usage` Sum-by-zone also present, not queried).
- **Replay:** `datasets_eval/google_cluster/run.py replay --pace-factor 0
  --wall-clock-anchor` — re-stamps every datapoint at one wall-clock instant
  captured at replay start, collapsing the 31-day trace span into ONE warm
  window at `now` so a `[Ns]` PromQL selector at `now` intersects it (this is
  the timing fix that unblocks the warm read; epoch-relative stamps never
  intersected the wall-clock query window).
- **Window gating:** after replay we poll until the lossless warm
  `sum(cpu_rate)` reaches the exact offline GT (**216.35351276397705**) — that
  proves every datapoint of the window has sealed + shipped — before timing
  anything.

### GUARD (ran before any timing — PASSED)

At the sealed window (and equally at live `now`, which is what the replay
client queries):

| query | status | result | data_source |
|---|---|---|---|
| `quantile_over_time(0.99, …_q_ddsketch[300s])` | success | **691 real series** (e.g. 0.0358) | `asap_query` (warm) |
| `quantile_over_time(0.50, …_q_ddsketch[300s])` | success | 691 real series (e.g. 0.0282) | `asap_query` (warm) |
| `sum(google_cluster_2019_cpu_rate)` | success | **exact 216.35351276397705** | `asap_query` (warm) |
| `count_over_time(…_q_ddsketch[300s])` | success | **0 series** | `thanos_query` (empty archive) |

Both required claims (`quantile_over_time(0.99,…[300s])` and `sum(…)`) returned
**real warm values**, so timing is meaningful. `count_over_time` does **not**
resolve on the warm path (it falls through to the empty archive engine →
`thanos_query`, 0 series), so it was **excluded** from the timed mix per the
task ("`count_over_time` if it resolves").

---

## Measurement

`deploy/mvp-singlenode/scripts/metricsql_replay.py` fired the warm query mix at
a **fixed 15 QPS** against `:9091/api/v1/query` (instant queries at live `now`,
which the range selectors look back 300 s to cover), `--no-plan-poll`, capturing
per-query wall-clock latency.

- **Query mix:** `quantile_over_time(0.99,…[300s])`,
  `quantile_over_time(0.50,…[300s])`, `sum(cpu_rate)`
  (`deploy/mvp-singlenode/scripts/queries-latency-warm.json`).
- **QPS:** 15 (aggregate across the 3 queries).
- **Query count:** **599** (40 s steady window).
- **Window:** 2026-06-12T20:41:36Z → 20:42:16Z, inside one fully-shipped warm window.
- **Realness:** **599/599 success, 599/599 non-empty, 599/599
  `data_source=asap_query`** (0 empties, 0 errors, 0 archive fallthroughs).

---

## Latency table (warm sketch tier)

| query kind | p50 (ms) | p95 (ms) | p99 (ms) | n | note |
|---|---|---|---|---|---|
| **all (mix)** | **18.25** | **19.45** | **20.03** | 599 | mean 12.96, min 1.51, max 23.78 |
| `quantile_over_time` (DDSketch, 691-series read) | 18.34 | 19.56 | **20.65** | 400 | p50 & p99 both quantiles |
| `sum` (Sum, lossless, 1-series) | 1.58 | 2.19 | **2.25** | 199 | exact identity answer |

The CDF (`latency_cdf.png`) is **bimodal**: the cheap lossless `sum` cluster at
~1.5–2.3 ms (≈1/3 of the mix) and the 691-series DDSketch quantile reconstruction
cluster at ~18–21 ms. Both tails are tight (p99 within ~1 ms of p50 for each
kind) — no long latency tail on the warm path.

---

## Cold-fallback arm — attempted, BLOCKED (warm-only reported)

I brought up the **full cold stack** (`stack.sh`: MinIO + Thanos
store/query/compact + gorilla-merger + data-plane with `ASAP_THANOS_QUERY_URL`
and `ASAP_GORILLA_S3_*`), with a **cold-enabled** agent
(`agent-ddsketch-coldon.yaml`: `cold: {enabled: true}`, sketch `tier: both`),
re-replayed the same trace, and waited for the warm window to seal.

**Result: no data ever reached the cold tier**, so there was nothing to query
on a cold path:

- data-plane registered `ThanosQueryEngine` (archive `data_source_id=thanos_query`) OK;
- the gorilla-merger ingest frontend (`:10908`) logged **zero** ingest /
  append / received-samples lines; its shipper found nothing to ship;
- **MinIO `asap/` held 0 objects** after the run (both `asap-gorilla` and
  `asap-gorilla-tsdb` empty);
- the bare static-config agent emitted **no** cold/ship/gorilla/s3 log lines —
  the `cold: {enabled: true}` block does not drive a gorilla ship in this
  static fused-agent path (cold endpoint wiring rides the control channel,
  which is `{enabled: false}` here);
- even querying **old timestamps** (1 h ago) still returned `data_source=asap_query`
  (warm), confirming there was no archived data to fall through to.

This is the archive/ship/routing fragility the task anticipated. Rather than
fabricate a cold arm, I report **warm-only**. (Building the cold arm would need
the supervised agent + live control channel to actually drive the gorilla ship,
plus aging the warm window out — out of scope for a clean, real measurement
here.)

---

## Honesty / caveats

- **Single-node loopback:** data-plane, agent, and replay client all on
  `127.0.0.1`. **No network RTT** is included — these are server-side query
  latencies only; a remote client adds its own RTT on top.
- **QPS / count / window:** 15 QPS, 599 queries, one 40 s steady window inside
  a single fully-shipped warm window. Modest QPS — this measures per-query
  serving latency, not a saturation/throughput study.
- **Warm-only:** the cold-fallback arm did not produce archived data (above);
  no cold numbers are claimed, so the "cold ≤ 2× warm" check was not evaluated.
- **Query realness verified:** every timed query returned a real warm value
  (`data_source=asap_query`); the reducer hard-fails if any timed query were
  empty/errored, so these latencies are not "latency of No result".

---

## Reproduce

```bash
# 1. cold-OFF stack
bash datasets_eval/multisketch/stack-coldoff.sh up \
  datasets_eval/multisketch/workloads/ddsketch.yaml \
  datasets_eval/multisketch/agent-ddsketch-coldoff.yaml
# 2. wall-clock-anchored replay
python3 datasets_eval/google_cluster/run.py replay \
  --jsonl /tmp/dd-only.jsonl --endpoint 127.0.0.1:4317 --pace-factor 0 --wall-clock-anchor
# 3. wait for warm sum(cpu_rate) == 216.35351276397705 (window sealed+shipped), GUARD
# 4. timed replay @ 15 QPS
python3 deploy/mvp-singlenode/scripts/metricsql_replay.py --target http://127.0.0.1:9091 \
  --queries deploy/mvp-singlenode/scripts/queries-latency-warm.json \
  --qps 15 --duration 40 --no-plan-poll --out datasets_eval/latency/replay-warm.jsonl
# 5. reduce -> table + CDF
python3 datasets_eval/latency/compute_latency.py --warm datasets_eval/latency/replay-warm.jsonl \
  --out-json datasets_eval/latency/latency_summary.json --out-png datasets_eval/latency/latency_cdf.png
# 6. teardown
bash datasets_eval/multisketch/stack-coldoff.sh down
```

## Deliverables (this dir)

- `latency_RESULTS.md` — this file
- `latency_cdf.png` — Fig 7 CDF (warm, overall + per-kind)
- `latency_summary.json` — p50/p95/p99 (overall + per-kind), data_source counts
- `per_query_latency.json` — slim per-query latency log (599 records: ts, query,
  kind, duration_ms, status, data_source, n_result_series). The raw
  `replay-warm.jsonl` (42 MB, verbatim 691-series result vectors per query) is
  the reducer's input but is intentionally **not committed** — regenerate via
  step 4 of Reproduce.
- `compute_latency.py` — reducer (also renders the CDF)
- `../../deploy/mvp-singlenode/scripts/queries-latency-warm.json` — the timed mix
