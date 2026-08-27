# Backend query latency (Fig 7) — warm sketch tier, §6 eval

**Date:** 2026-06-12
**Branch:** `feat/query-latency-cdf`
**Headline:** warm-tier sketch-answered PromQL is **production-usable**: across a
599-query mix at 15 QPS, overall **p50 = 18.3 ms, p99 = 20.0 ms** on the warm
SketchStore. The exact lossless `sum` is **p50 1.6 ms / p99 2.3 ms**; the
691-series DDSketch `quantile_over_time` read is **p50 18.3 ms / p99 20.6 ms**.
**Cold-fallback arm: NOW MEASURED** (cold-ON stack, end-to-end through the
gorilla cold tier): across a 600-query mix @15 QPS with **600/600
`data_source=thanos_query`**, the cold/archive-answered PromQL is **p50 22.3 ms
/ p95 47.2 ms / p99 67.1 ms** — overall **p50 ≈ 1.2× warm, p99 ≈ 3.3× warm**.
Both arms are reported below; the warm numbers are unchanged.

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

`deploy/mvp-multinode/scripts/metricsql_replay.py` fired the warm query mix at
a **fixed 15 QPS** against `:9091/api/v1/query` (instant queries at live `now`,
which the range selectors look back 300 s to cover), `--no-plan-poll`, capturing
per-query wall-clock latency.

- **Query mix:** `quantile_over_time(0.99,…[300s])`,
  `quantile_over_time(0.50,…[300s])`, `sum(cpu_rate)`
  (`deploy/mvp-multinode/queries-latency-warm.json`).
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

## Cold-fallback arm (MEASURED) — cold/archive-answered PromQL

This is the arm that was previously blocked. It is now measured end-to-end
through the gorilla cold tier.

### Setup (cold-ON stack, `datasets_eval/latency/stack-coldon.sh`)

Full cold stack on one host: **MinIO** (:9000) + **gorilla-merger**
(:10908 HTTP ingest / :10907 Thanos StoreAPI) + **thanos-store-gateway**
(reads the merger's shipped `asap-gorilla-tsdb` blocks) + **thanos-query**
(:10903, federates the merger StoreAPI + store-gateway) + **data-plane** with
`ASAP_THANOS_QUERY_URL=http://thanos-query:10903` (so it registers the real
**`ThanosQueryEngine`**, `data_source_id=thanos_query`, NOT the
`NoDataArchiveEngine` stub) + a **cold-enabled edge** with a *complete* cold
block (`agent-cold-ship.yaml`: `cold.enabled:true`,
`cold.ship_endpoint: http://gorilla-merger:10908/ingest/gorilla`,
`block_duration:60s`, `external_labels.cluster:asap-mvp`).

- **Routing:** a cold storage-routing table
  (`backend-storage-routing-coldon.yaml`) pins
  `google_cluster_2019_cpu_rate → gorilla_object_store` for every query shape,
  so its instant queries dispatch through the `EngineRouter` to
  `thanos_query` (the cold/archive engine) rather than the warm
  `SketchStore`.
- **Workload:** same `/tmp/dd-only.jsonl` slice (lossless raw
  `google_cluster_2019_cpu_rate` Sum family, `tier: both` — the series that
  ships cold), wall-clock-anchored replay (one cold window at a fixed instant).
- **Control-plane stopped during timing:** it periodically re-POSTs a
  storage-routing table to `/api/v1/storage_routing` (~every 60 s) that
  overrides the file-loaded cold table back to warm; with it stopped, the
  data-plane keeps the file cold table and the cold archive answers
  independently (cold ship is decoupled from the control channel — PR #500).

### What was the original blocker (and the fix)

The original cold arm failed because the multisketch `agent-ddsketch-coldon.yaml`
set only `cold: {enabled: true}` with **no `ship_endpoint`** — and the
asapedge processor treats an empty ship endpoint as **drain-only, no
shipping** (`config.go`: "Empty => drain-only (no shipping)"). So the cold
encoder accumulated samples but never POSTed them → MinIO/merger stayed empty.
With a complete cold block the edge ships per-shard ASAPFRG1 fragment batches
to the merger. Verified live (per-shard agent logs):
`cold drain {active_series:245, fragments:245}` → `cold shipBatch
{fragments:245, shipper_noop:false}` with **no** ship-failure/spool lines, and
the merger then served `count(...)=1000` over its StoreAPI.

### GUARD (ran before any timing — PASSED, at the pinned cold anchor)

| query | status | result | data_source |
|---|---|---|---|
| `sum(google_cluster_2019_cpu_rate)` | success | **22.13** (exact archive sum) | **`thanos_query`** |
| `quantile_over_time(0.99, …_cpu_rate[300s])` | success | **1000 series** | **`thanos_query`** |
| `quantile_over_time(0.50, …_cpu_rate[300s])` | success | 1000 series | **`thanos_query`** |
| `count(google_cluster_2019_cpu_rate)` | success | 1000 | **`thanos_query`** |

Every required timed query returns a **real archive value** served by the
**cold engine** (`thanos_query`), so the cold timing is meaningful and is the
cold tier (not a warm shortcut). The `quantile_over_time` is computed by
thanos-query **over the raw archived Gorilla-XOR samples** (not over a
DDSketch — the cold tier stores lossless raw samples).

### Measurement

`cold_latency_replay.py` fired the cold mix at a **fixed 15 QPS** against
`:9091/api/v1/query`, **pinning the PromQL eval timestamp** (`time=<anchor>`)
to the instant the cold workload was anchored at (the cold window sits at a
fixed past instant because the ship takes ~one window to land; the warm arm
queried live `now` because its in-memory warm window sat at `now`). The pin
changes only WHICH timestamp the backend evaluates at — the per-query
server-side latency it measures is identical in kind to the warm arm.

- **Query mix:** `quantile_over_time(0.99,…_cpu_rate[300s])`,
  `quantile_over_time(0.50,…_cpu_rate[300s])`, `sum(cpu_rate)`
  (`queries-latency-cold.json`).
- **QPS / count / duration:** 15 QPS, **600 queries**, 40 s window.
- **Realness:** **600/600 success, 600/600 non-empty, 600/600
  `data_source=thanos_query`** (0 empties, 0 errors, 0 warm shortcuts).

### Latency table (cold-fallback archive tier)

| query kind | p50 (ms) | p95 (ms) | p99 (ms) | n | note |
|---|---|---|---|---|---|
| **all (mix)** | **22.28** | **47.17** | **67.07** | 600 | mean 25.57, min 9.12, max 85.14 |
| `quantile_over_time` (1000-series, Thanos PromQL over raw samples) | 24.27 | 48.59 | **68.41** | 400 | |
| `sum` (lossless, archive) | 15.48 | 33.33 | **42.74** | 200 | exact archive sum |

**Warm vs cold:** overall **p50 18.25 → 22.28 ms (≈1.2×)**, **p99 20.03 →
67.07 ms (≈3.3×)**. The cold path stays in the tens of ms (no order-of-magnitude
blowup) but has a heavier p99 tail: each cold answer crosses data-plane →
thanos-query → gorilla-merger StoreAPI + store-gateway and re-evaluates PromQL
over the raw archived samples, vs the warm tier's in-memory sketch read.

---

## Honesty / caveats

- **Single-node loopback:** data-plane, agent, merger, thanos, MinIO, and the
  replay client all on `127.0.0.1`. **No network RTT** is included — these are
  server-side query latencies only; a remote client adds its own RTT on top.
  (For the cold arm the inter-service hops data-plane→thanos→merger are still
  loopback, so a real multi-host deploy would add per-hop RTT to the cold tail.)
- **QPS / count / window:** 15 QPS; 599 (warm) / 600 (cold) queries; one 40 s
  steady window inside a single fully-shipped window. Modest QPS — this measures
  per-query serving latency, not a saturation/throughput study.
- **Cold eval-time pin:** cold queries are evaluated at the fixed cold-window
  anchor (the data sits at one instant after a wall-clock-anchored replay), so
  the cold latencies are the backend's time to *serve a cold/archive query*,
  not a study of cold-window freshness/aging.
- **Query realness + tier verified:** the reducer hard-fails if any timed query
  was empty/errored OR served by the wrong tier — warm must be
  `data_source=asap_query`, cold must be `data_source=thanos_query` — so these
  latencies are neither "latency of No result" nor a warm answer mislabeled as
  cold.

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
python3 deploy/mvp-multinode/scripts/metricsql_replay.py --target http://127.0.0.1:9091 \
  --queries deploy/mvp-multinode/queries-latency-warm.json \
  --qps 15 --duration 40 --no-plan-poll --out datasets_eval/latency/replay-warm.jsonl
# 5. reduce -> table + CDF
python3 datasets_eval/latency/compute_latency.py --warm datasets_eval/latency/replay-warm.jsonl \
  --out-json datasets_eval/latency/latency_summary.json --out-png datasets_eval/latency/latency_cdf.png
# 6. teardown
bash datasets_eval/multisketch/stack-coldoff.sh down
```

### Cold-fallback arm

```bash
# build the dev images if absent (buildx --load; data-plane/control-plane/
# gorilla-merger from /mydata/ASAPQuery-backend, asap-otel via build_asap_otel.sh)
# then bring up the COLD-ON stack (uses sudo docker; --user 0 on the merger):
bash datasets_eval/latency/stack-coldon.sh up \
  datasets_eval/multisketch/workloads/ddsketch.yaml \
  datasets_eval/latency/agent-cold-ship.yaml \
  datasets_eval/latency/backend-storage-routing-coldon.yaml
# 1. wall-clock-anchored replay; capture the printed `now=<ms>` anchor
python3 datasets_eval/google_cluster/run.py replay \
  --jsonl /tmp/dd-only.jsonl --endpoint 127.0.0.1:4317 --pace-factor 0 --wall-clock-anchor
# 2. wait until the cold ship lands: poll thanos-query / data-plane until
#    count(google_cluster_2019_cpu_rate)@<anchor_s> returns 1000 via thanos_query
# 3. stop the control-plane so it can't override the cold routing table back to warm:
sudo docker rm -f asap-control-plane
# 4. GUARD then timed cold replay @ 15 QPS, pinned to the anchor:
python3 datasets_eval/latency/cold_latency_replay.py --target http://127.0.0.1:9091 \
  --queries datasets_eval/latency/queries-latency-cold.json \
  --at-time <anchor_s> --qps 15 --duration 40 \
  --out datasets_eval/latency/replay-cold.jsonl
# 5. reduce BOTH arms -> tables + combined CDF (hard-fails if any cold query
#    was not data_source=thanos_query, or any warm query not asap_query):
python3 datasets_eval/latency/compute_latency.py \
  --warm datasets_eval/latency/per_query_latency.json \
  --cold datasets_eval/latency/per_query_latency_cold.json \
  --out-json datasets_eval/latency/latency_summary.json \
  --out-png datasets_eval/latency/latency_cdf.png
# 6. teardown
bash datasets_eval/latency/stack-coldon.sh down
```

## Deliverables (this dir)

- `latency_RESULTS.md` — this file
- `latency_cdf.png` — Fig 7 CDF (warm + cold-fallback, overall + per-kind)
- `latency_summary.json` — p50/p95/p99 (overall + per-kind), data_source counts,
  for BOTH the `warm` and `cold` arms
- `per_query_latency.json` — slim warm per-query log (599 records)
- `per_query_latency_cold.json` — slim cold per-query log (600 records: ts,
  query, kind, duration_ms, status, http_code, data_source, n_result_series)
- `compute_latency.py` — reducer (warm + cold; renders the combined CDF; guards
  each arm's `data_source`)
- `cold_latency_replay.py` — cold-arm replay client (eval-time-pinned)
- `stack-coldon.sh` — cold-ON single-host stack (MinIO + merger + thanos +
  cold-routed data-plane + cold-enabled edge)
- `backend-storage-routing-coldon.yaml` — cold routing table
  (`cpu_rate → gorilla_object_store`)
- `agent-cold-ship.yaml` — cold-enabled edge with a complete `cold.ship_endpoint`
  block (the missing piece that unblocked the arm)
- `queries-latency-cold.json` — the timed cold mix
- The raw `replay-warm.jsonl` / `replay-cold.jsonl` (verbatim result vectors) are
  the reducer's optional input but intentionally **not committed** — regenerate
  via Reproduce.
- `compute_latency.py` — reducer (also renders the CDF)
- `../../deploy/mvp-multinode/queries-latency-warm.json` — the timed mix
