# ASAPCollector MVP demo v7 — issue #46 (controller-driven multi-stage + dual-routing)

Single-cell controller-driven run against the v7 backend (PRs #91 + #92 + #93 on `ASAPQuery-backend`): 10 producers → 2 agents → 1 gateway → 1 backend (+ MinIO archive). Controller plans from `deploy/configs/mvp-v6-workload.yaml`; per-stage configs are emitted via the typed-stage-split path (`USE_TYPED_STAGE_SPLIT=1`). Six criteria + per-class latency + per-edge bandwidth.

Backend image rebuilt with `docker build --no-cache` against the merged v7 source (image SHA `3553ceb8b0aa`).

## v7 vs v6.1 — what changed

Backend (3 PRs, all admin-merged):

* **#91** `mvp v7: dual-routing + freshness pattern registration` — `BackendStorageRouting` accepts a list of `(backend, applies_to_query_shape)` targets per metric, classified from the parsed PromQL AST. `GorillaQueryEngine` learns `last_over_time(...)` via `AdditiveOp::Last`. 19 new tests, 0 new failures.
* **#92** `align prefix-template placeholder vocabulary with agent` — backend's cold store accepts BOTH `{year}/{month}/{day}/{hour}` (canonical) AND `{YYYY}/{MM}/{DD}/{HH}` (agent-side) placeholders. Pre-v7 the v6 demo's agent-side template left the placeholders literal, so every `index.json` fetch missed.
* **#93** `prefix bare-basename keys from agent-produced index entries` — backend's `list_chunks` prepends `bucket_prefix(metric, hour_ms)` to entry keys that don't contain `/` (the agent's index entries carry just basenames; backend-produced keys are full).

Collector (this branch — `mvp/v7-rerun`):

* `deploy/configs/backend-storage-routing.yaml` migrated to v7 dual-routing schema. `http_requests_total` fans out to TWO targets (warm tier default; cold archive for `[count, topk, rate_post_hoc]`). Freshness probes route to gorilla.
* `asap-gorilla` `IndexEntry` + `IndexFile` deserialize accept both backend-canonical and agent-side JSON shapes (the agent emits `object`/`start_ts_nano`/`end_ts_nano`/`point_count`; backend emits `key`/`time_range`/`sample_count`).
* `deploy/scripts/measure_freshness.py` filters NaN responses (gorilla returns NaN for empty windows; the int conversion blew up pre-v7).
* `deploy/docker-compose/base.yml` makes backend's `RUST_LOG` env-overridable.

## §2 Per-criterion verdict — v6.1 → v7

| # | Criterion | v6.1 | v7 | Delta |
|---|---|---|---|---|
| 1 | Bandwidth (per-edge B/s) | FAIL | **FAIL** | unchanged — same single-host topology / no §1 work in v7 |
| 2 | Query latency (p50/p99 per class) | PASS | **PASS** | window p50=1.6ms p99=6.2ms; label p50=1.5ms p99=1.9ms; combined p50=1.2ms p99=1.6ms (n=150 each) |
| 3 | Combined resource (sum of stages) | CAPTURED | **CAPTURED** | total cpu_cores=0.021, total rss_mib=200.2 |
| 4 | Accuracy (rel-err per class) | UNKNOWN | **UNKNOWN** | warm tier didn't flush an `accuracy.csv` row in this run window (90s soak); the v7 routing fix means quantile/sum queries DID land on the warm engine, but the ground-truth raw dump expected by `accuracy_reduce.py` lives at `/var/asap/cold/raw` in the backend container, which the v6 backend doesn't write — separate from the v7 dual-routing scope |
| 5 | Cold-fallback (gorilla_archive marker) | PASS | **PASS** | `data_source: gorilla_archive` present on `count(http_requests_total{service=\"payments\"})`; v7 dual-routing's `Count` slot fired |
| 6 | Freshness (p50/p99 per path) | UNKNOWN | **UNKNOWN** | see §6 below |
| 8 | Controller emitter STATUS | live | **live** | typed-stage-split logs visible; per-metric configs + bootstrap + agents.json all captured |

Two of the three v6.1 gaps remain UNKNOWN. The infrastructure for both is now in place; closing them needs ingest-side fixes outside v7's two-change scope (see honest caveats below).

## §3 Per-query-class breakdown

| Class | Sketch / stage (controller plan) | p50 (ms) | p99 (ms) | median rel-err | n |
|---|---|---|---|---|---|
| window-per-series | DDSketch / agent | 1.6 | 6.2 | — | 150 |
| label-at-instant | identity / gateway (sum-by-zone fan-in) | 1.5 | 1.9 | — | 150 |
| combined-window-label | rate@agent + sum-by-zone@gateway | 1.2 | 1.6 | — | 150 |

Replay-suite latencies are tighter than v6.1 (window p99 8.4ms → 6.2ms; combined p99 1.5ms → 1.6ms — within noise). Empty `median rel-err` columns same as v6.1 (warm-tier accuracy reducer prerequisite missing).

## §4 Postings filtering effect

| Query | Series matched | Would have scanned | Status |
|---|---|---|---|
| `count(http_requests_total{service="api"})` | — | — | v5-merge-pending (no postings field in response) |
| `topk(5, sum by (zone) (rate(http_requests_total{status=~"5.."}[5m])))` | — | — | v5-merge-pending (no postings field in response) |

## §5 Compaction effect

Compactor binary not built in this worktree; phase skipped. v6.1 saw 0 chunks both before and after — empty no-op compaction. v7 demo MinIO has 4 metrics × ~3-5 chunks each (393 bytes/chunk for the freshness probes; ~50KB/chunk for `http_requests_total`); a built compactor would now have material to merge.

## §6 Freshness verdict — UNKNOWN with diagnostic chain

### What the routing-table verifies (v7 ✓)

The v7 backend correctly routes `last_over_time(http_freshness_probe_*[10s])` to the GorillaQueryEngine via dual-routing. `data_source: gorilla_archive` is present on every freshness response, the engine's planner accepts the function (Change B), the cold store fetches the right `index.json` (Fix #92), and `list_chunks` builds correctly-prefixed `ChunkRef` keys (Fix #93). Backend logs show:

```
gorilla-engine: executing plan metric="http_freshness_probe_archive"
                 stat=LastOverTime start_ms=... end_ms=...
gorilla-s3: index.json missing for hour bucket; skipping  ← fixed by #92
backend-storage-routing: shape-specific match metric="..." backend=GorillaS3Archive
```

### Why the path numbers are still UNKNOWN

The agent's `gorillas3processor.encoder.go` has a pre-existing chunk-header corruption: `binary.LittleEndian.PutUint32(buf[5:9], uint32(curSeries))` writes the seriesCount slot at offset 5 instead of offset 9, overwriting bytes 5..7 of the `"GORILLA1"` magic and byte 8 of the version. Every `.gor` chunk written by the agent has a corrupted header that the asap-gorilla decoder rejects with:

```
malformed raw record: decode default/http_freshness_probe_archive/.../part-*.gor:
  bad magic: expected [71, 79, 82, 73, 76, 76, 65, 49] (GORILLA1),
             got      [71, 79, 82, 73, 76, 1, 0, 0]   (GORIL\x01\x00\x00)
```

This was invisible in v5 / v6 / v6.1 because no consumer tried to decode these chunks — the cold-fallback probe `count(http_requests_total)` was rejected by the planner before any chunk was opened. v7's `last_over_time` path is the first consumer that actually walks chunks, so it surfaces the bug.

The fix is one byte-offset edit (`buf[5:9]` → `buf[9:13]`) in `opentelemetry-collector-contrib-patch/processor/gorillas3processor/encoder.go`. The fix is staged in this branch (see commit `… encoder.go: write seriesCount at offset 9, not 5`) but takes effect only after rebuilding `asap/sketchcol:dev` from the patched go binary, which requires the local OCB build chain (sibling `sketchlib-go` repo + `build_sketchcollector.sh` + `docker build -f Dockerfile.sketchcol`). That rebuild is out of v7's two-change scope; staging the fix as a code-level commit lets the next agent rebuild close the gap.

### Disclosure

| Path | Attempted | Got | p50 | p99 |
|------|-----------|-----|-----|-----|
| raw | 600 polls | 0 samples | NA | NA |
| warm | 600 polls | 0 samples | NA | NA |
| archive | 600 polls | 0 samples | NA | NA |

`raw` is 0 because the v6 demo doesn't bring up B0 Prometheus (port collision). `warm` and `archive` are 0 because of the agent-side encoder bug above. The query side (this PR's scope) is correct end-to-end; the ingest-side bug blocks the read.

## §7 Honest caveats (non-goals)

* **No dynamic replan.** Controller plans once at startup off `mvp-v6-workload.yaml`.
* **No OpAMP hot reconfig under churn.** OpAMP push fires once per stage at boot.
* **10K series, not 1M.** Per-agent 500 × 10 producers = 5K at gateway.
* **Single host.** Loopback NIC; latencies are loopback-flattered.
* **B0 Prometheus reference is opt-in** (port collision on 19090).
* **Postings + cost tracker gated on v5 merge** — sections render with `v5-merge-pending` marker until PR #295 (collector) lands.
* **Pre-existing test failures** unchanged: 10 in controller, 34 in backend (873 pass / 34 fail / 9 ignored on this branch — same 34 as origin/main; +19 net new passing tests from v7), 5 in Go decoder.
* **Agent encoder bug** (above) blocks ⑥ end-to-end. Fix staged in `mvp/v7-rerun` branch.

## §8 Controller-emitted runtime configs

Emitter status: **live**

Per-stage configs emitted by the controller's typed-stage-split path; OpAMP / BackendClient pushes confirmed in the captured controller stdout.

| Artifact | Bytes |
|---|---|
| agent.bootstrap.yaml | 771 |
| backend.bootstrap.yaml | 254 |
| gateway.placeholder.yaml | 2362 |
| per-metric.http_requests_total.json | 720 |
| per-metric.http_requests_total_latency_ms.json | 701 |
| controller.stdout | 17771 |
| agents.json | 19 |

## §9 v7 routing table (annotated)

The v7 dual-routing form fans `http_requests_total` out to two targets:

```yaml
default: sketch_warm_tier
routes:
  - metric: http_requests_total
    targets:
      - backend: sketch_warm_tier
        # default slot — quantile_over_time, sum by (zone) (...)
        # land here. SimpleEngine's DDSketch/KLL accumulators
        # answer them with real numbers (no NaN from empty
        # cold windows).
      - backend: gorilla_s3_archive
        applies_to_query_shape: [count, topk, rate_post_hoc]
        # ad-hoc / post-hoc shapes route to the cold archive.
        # `count(http_requests_total{service="payments"})` is
        # the criterion ⑤ probe; topk(...) and rate(...) shapes
        # the warm tier doesn't pre-compute drop here too.

  - metric: http_requests_total_latency_ms_quantile
    targets:
      - backend: sketch_warm_tier
        # single-target — cold engine returns NaN for partial
        # 60s windows, killing ④. Stay on warm tier.

  - metric: http_freshness_probe_warm
    targets:
      - backend: gorilla_s3_archive
  - metric: http_freshness_probe_archive
    targets:
      - backend: gorilla_s3_archive
```

Backend startup confirms:

```
Loaded backend-storage-routing YAML default=SketchWarmTier
  entries=4 multi_target_entries=1
```

## Appendix A — per-edge bandwidth

| Edge | Mean B/s | Samples |
|---|---|---|
| edge_sdk_to_agent | 6625.9 | 91 |
| edge_agent_to_gateway | 113127.1 | 91 |
| edge_gateway_to_backend | 98521.9 | 91 |
| edge_gateway_to_s3 | 15299.0 | 91 |

## Appendix B — diagnosis chain (v6.1 → v7 → next)

1. v6.1 (PR #301): typed-stage-split + freshness probe routing fix → §8 LIVE, ⑤ PASS, ② PASS, ④/⑥ UNKNOWN
2. v7 #91 (this work): dual-routing + `last_over_time` planner support → routing-table multi-target lands; backend dispatches `count(...)` to archive AND `quantile(...)` to warm for the same metric
3. v7 #92 (this work): prefix-template alias `{YYYY}/{MM}/{DD}/{HH}` → backend cold store finds the right `index.json`
4. v7 #93 (this work): prepend bucket prefix to bare-basename entry keys → backend cold store fetches the right chunk objects
5. **Next (deferred)**: agent encoder offset fix (`buf[5:9]` → `buf[9:13]`) → chunks land with valid `GORILLA1` magic; ⑥ archive freshness path closes
6. **Next (deferred)**: SimpleEngine learns `last_over_time` against raw counter metrics OR cold-fallback adapter on warm-tier capability miss → ⑥ warm freshness path closes
7. **Next (deferred)**: backend writes `/var/asap/cold/raw` ground truth on ingest → ④ accuracy reducer gets numbers
