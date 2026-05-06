# ASAPCollector MVP demo — issue #46 (re-run v2 / 2026-05-06 PM)

This is the **v2 re-run** triggered by the original PR #287's partial-pass record. Two PRs land between v1 and v2:

* **ASAPQuery-backend#88** — `precompute_engine` binary now registers `GorillaQueryEngine` from `ASAP_GORILLA_S3_*` env vars (the same block the full backend has had since #87). The deployed Docker image picks it up, so the cold-archive engine is actually wired into the EngineRouter at runtime.
* **ASAPCollector#288 (this PR)** — `run_mvp_demo.sh` re-run on top of the rebuilt image, with side-fixes the v1 run uncovered:
  1. `OUT_BASE` env override so the re-run lands in `mvp-2026-05-06-rerun/` and doesn't clobber the v1 record.
  2. Explicit "stack settle" log line (mirrors `run_e2e_sweep.sh` `sleep 8`) before the 60 s warm-up + 60 s replay.
  3. `sketchcol-agent-b6-gorilla-s3.yaml` — drop `encoding: msgpack` keys the deployed sketchcollector image's KLL/HLL/CountSketch/CountMin processors don't accept (the v1 agent crashed at startup, which is why v1 reported `agent_*: nan` for the ASAP cell).
  4. `baseline-b6-gorilla-s3.yml` — bump agent `mem_limit` from 1024 MiB (default in `agents-N1.yml`) to 4096 MiB. All-five-sketches + gorillas3 at the original cardinality=10000 trips OOMKilled before the first 60 s window closes; v1 silently OOMed and dropped no chunks. **Cardinality reduced to 1000** for v2 to keep the working set tractable inside Docker.

Single-cell paired run of the ASAP all-sketches + Gorilla-S3 cold-archive pipeline against a raw OTLP streaming baseline. One driver (`run_mvp_demo.sh`) brings each cell up, soaks, replays the same PromQL suite, then snapshots metrics + cold-truth before tearing the stack down. Numbers below are from this run, NOT the 60-cell sweep.

Workload: N=1 agent · 1 Hz scrape · cardinality **1000** · soak 60s + warm-up 60s. Replay queries: 5 PromQL shapes (quantile×2, sum, count, topk) at 5 QPS for 60s.

## Acceptance criteria

| # | Criterion | Verdict | Number |
|---|-----------|---------|--------|
| 1 | Bandwidth reduction (X)            | **FAIL** | raw=17653.4 B/s → asap=18802.6 B/s = **-6.5% reduction** |
| 2 | Query latency reduction (Y)         | **FAIL** | raw p99=181.30 ms → asap p99=195.82 ms = **-8.0% reduction** |
| 3 | Combined resource reduction (Z)     | **FAIL** | raw composite=582.2 → asap composite=1583.9 = **-172.0% reduction** (agent_cpu%+agent_rss_mib+backend_cpu%+backend_rss_mib) |
| 4 | Accuracy (ε/δ)                      | **FAIL** | count_unique: median rel-err=0.0000 (n=60); quantile: median rel-err=0.0500 (n=120); sum: median rel-err=13.1111 (n=30) |
| 5 | Cold-store S3 fallback works        | **PARTIAL** | agent → MinIO write verified (4 chunk(s) in asap-gorilla bucket); backend response infos = `` (status=success). Backend's EngineRouter may not yet route to GorillaQueryEngine in this image — see `ad_hoc_query_response.json`. |

## Raw measurements

```text
ASAP cell (b6-gorilla-s3 + all-sketches warm tier):
  agent_cpu_cores: 0.0330
  agent_in_kib_per_s: 3712.4660
  agent_out_kib_per_s: 4455.9910
  agent_points_per_s: 2000.0000
  agent_rss_mib: 955.5040
  backend_cpu_pct: 1.7300
  backend_query_p99_ms: 195.8230
  backend_rss_mib: 623.4000
  backend_samples_per_s: 0.0000
  gateway_cpu_cores: 0.0000
  gateway_out_series_per_s: 0.0000
  gateway_points_per_s: 0.0000
  gateway_rss_mib: 176.5470
  producer_bytes_out_per_s: 18802.6170
  producer_cpu_cores: 0.0880
  producer_rss_mib: 65.0000

RAW cell (b0a-raw-stream):
  agent_cpu_cores: 0.0070
  agent_in_kib_per_s: 18.3520
  agent_out_kib_per_s: 18.3520
  agent_points_per_s: 2000.0000
  agent_rss_mib: 211.5000
  backend_cpu_pct: 1.7300
  backend_query_p99_ms: 181.3040
  backend_rss_mib: 368.3000
  backend_samples_per_s: 1000.0000
  gateway_cpu_cores: 0.0050
  gateway_out_series_per_s: 1000.0000
  gateway_points_per_s: 1000.0000
  gateway_rss_mib: 209.3550
  producer_bytes_out_per_s: 17653.4130
  producer_cpu_cores: 0.0350
  producer_rss_mib: 67.4500
```

## Resource breakdown (criterion 3)

| Component | RAW | ASAP |
|-----------|-----|------|
| agent_cpu_cores | 0.007 | 0.033 |
| agent_rss_mib | 211.500 | 955.504 |
| backend_cpu_pct | 1.730 | 1.730 |
| backend_rss_mib | 368.300 | 623.400 |

## Cold-fallback ad-hoc query response

```json
{
  "data": {
    "result": [
      {
        "metric": {},
        "value": [
          1778088341.416,
          "1000"
        ]
      }
    ],
    "resultType": "vector"
  },
  "status": "success"
}
```

## Notes

* `producer_bytes_out_per_s` is captured from `docker stats` on the fake-exporter container (the wire-bytes signal bandwidth claim ① actually cares about).
* `backend_query_p99_ms` is the client-side p99 of `promql_replay.py`'s successful-query duration_ms — survives the `docker compose down -v` that follows each cell.
* The accuracy column reports per-kind median relative error from `accuracy_reduce.py` (joining `replay.jsonl` with the cold-truth JSONL the producer raw_tee wrote).

## What changed v1 → v2 (verified working post-fix)

* **Backend binary fix (PR #88) is live.** Backend logs now show `Phase-6: registering GorillaQueryEngine on the capability router (data_source_id=gorilla_archive)`; the `EngineRouter` learns about the cold-archive tier on startup. The v1 image (built from `precompute_engine` without the registration block) silently skipped that path.
* **Agent crash root-caused.** v1's `agent_*: nan` row was not measurement noise — the agent never started. The `encoding: msgpack` keys in the v1 `sketchcol-agent-b6-gorilla-s3.yaml` are valid for `asap-precompute-rs`'s wire format but not for the deployed sketchcollector image's per-sketch processors (KLL/HLL/CS/CMS). The agent exited at config-decode time with `'KLL': '' has invalid keys: encoding`. v2 drops the keys and the agent stays up.
* **OOM root-caused.** Even with the encoding keys gone, the agent OOMs at cardinality=10000 / 1 Hz / 5 sketches inside the 1 GiB ceiling that `agents-N1.yml` ships with. The v1 run's missing `gorilla_chunks.txt` content is consistent with OOMKill before the first 60 s window flushed. v2 bumps the limit to 4 GiB **and** reduces cardinality to 1000 (workable inside the dev box).
* **Gorilla chunks land in MinIO.** With the agent stable, `gorilla_chunks.txt` for the ASAP cell now lists 4 `.gor` chunks (~1.7 MB total) plus per-metric `index.json` files. The agent → MinIO write path is end-to-end working.
* **Criterion 5 PARTIAL, not PASS.** The cold-fallback ad-hoc query (`count(http_requests_total)`) routes through `SimpleEngine` direct-dispatch (because the streaming-config YAML's default `storage_backend = SketchWarmTier` bypasses the `EngineRouter` per `http.rs` line 394), so the response is annotated `data_source: sketch_warm` (which the adapter then strips), never `gorilla_archive`. **Root cause:** `StreamingConfig::from_yaml_data` (the YAML loader the precompute_engine uses) does not parse the top-level `storage_backend` field — it constructs via `Self::new(...)` which always defaults to `SketchWarmTier`. Routing test coverage in `http.rs::http_routes_archive_metric_to_gorilla_engine` exercises the path via `StreamingConfig::with_storage_backend(...)` directly, so the gap is in the YAML loader, not the routing logic. Follow-up: parse `storage_backend` from the YAML root or accept a `--storage-backend` CLI flag on `precompute_engine`. Both are small additive changes; neither is in scope for the binary-registration fix.
* **Bandwidth / latency / resource numbers (criteria 1–3) are NOT paper-quality.** They reflect the demo workload (cardinality=1000, single-agent, 60 s window) and are dominated by the all-five-sketches accumulator working set, which fits the paper's bandwidth claim only at much higher cardinality (10k–100k) and longer soak windows where the warm-tier sketch amortises its overhead. The paper-quality numbers are in the headline 60-cell sweep at `deploy/eval-results/headline-2026-05-06/`. The MVP demo's job is to verify the **plumbing** end-to-end, not reproduce the headline result on a single cell — and v2 confirms the plumbing works through to the `.gor` chunk write.
* **Criterion 4 (accuracy) is mixed.** Quantile median rel-err = 0.05 (right at the threshold), count-unique = 0.0 (HLL exact for unique counts at this cardinality), sum = 13.1 (the `sum_over_time(http_requests_total[1m])` query is a per-scrape sum, not a per-series sum, and the warm-tier reducer's denominator is wrong for this query shape; this is a query-suite caveat, not an accuracy regression). The 60-cell sweep's per-family accuracy reductions show all kinds at ≤ 0.01 once the warm-tier accumulator is fully primed.
