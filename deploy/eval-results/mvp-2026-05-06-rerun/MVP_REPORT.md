# ASAPCollector MVP demo — issue #46

Single-cell paired run of the ASAP all-sketches + Gorilla-S3 cold-archive pipeline against a raw OTLP streaming baseline. One driver (`run_mvp_demo.sh`) brings each cell up, soaks, replays the same PromQL suite, then snapshots metrics + cold-truth before tearing the stack down. Numbers below are from this run, NOT the 60-cell sweep.

Workload: N=1 agent · 1 Hz scrape · cardinality 10000 · soak 60s + warm-up 60s. Replay queries: 5 PromQL shapes (quantile×2, sum, count, topk) at 5 QPS for 60s.

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
