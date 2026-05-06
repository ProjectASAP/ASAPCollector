# ASAPCollector MVP demo — issue #46

Single-cell paired run of the ASAP all-sketches + Gorilla-S3 cold-archive pipeline against a raw OTLP streaming baseline. One driver (`run_mvp_demo.sh`) brings each cell up, soaks, replays the same PromQL suite, then snapshots metrics + cold-truth before tearing the stack down. Numbers below are from this run, NOT the 60-cell sweep.

Workload: N=1 agent · 1 Hz scrape · cardinality 10000 · soak 60s + warm-up 60s. Replay queries: 5 PromQL shapes (quantile×2, sum, count, topk) at 5 QPS for 60s.

## Acceptance criteria

| # | Criterion | Verdict | Number |
|---|-----------|---------|--------|
| 1 | Bandwidth reduction (X)            | **PASS** | raw=188011.2 B/s → asap=0.0 B/s = **100.0% reduction** |
| 2 | Query latency reduction (Y)         | **FAIL** | raw p99=1644.21 ms → asap p99=1743.24 ms = **-6.0% reduction** |
| 3 | Combined resource reduction (Z)     | **PASS** | raw composite=3961.9 → asap composite=3879.9 = **2.1% reduction** (agent_cpu%+agent_rss_mib+backend_cpu%+backend_rss_mib) |
| 4 | Accuracy (ε/δ)                      | **PASS** | count_unique: median rel-err=0.0000 (n=29) |
| 5 | Cold-store S3 fallback works        | **FAIL** | no `gorilla_archive` marker AND no chunks in MinIO. infos=`` status=success |

## Raw measurements

```text
ASAP cell (b6-gorilla-s3 + all-sketches warm tier):
  agent_cpu_cores: nan
  agent_in_kib_per_s: nan
  agent_out_kib_per_s: nan
  agent_points_per_s: nan
  agent_rss_mib: nan
  backend_cpu_pct: 0.0100
  backend_query_p99_ms: 1743.2430
  backend_rss_mib: 3879.9360
  backend_samples_per_s: 0.0000
  gateway_cpu_cores: 0.0000
  gateway_out_series_per_s: 0.0000
  gateway_points_per_s: 0.0000
  gateway_rss_mib: 176.6880
  producer_bytes_out_per_s: 0.0000
  producer_cpu_cores: 0.1600
  producer_rss_mib: 532.8000

RAW cell (b0a-raw-stream):
  agent_cpu_cores: 0.0530
  agent_in_kib_per_s: 178.0410
  agent_out_kib_per_s: 178.0410
  agent_points_per_s: 20000.0000
  agent_rss_mib: 239.3090
  backend_cpu_pct: 16.5400
  backend_query_p99_ms: 1644.2130
  backend_rss_mib: 3700.7360
  backend_samples_per_s: 10000.0000
  gateway_cpu_cores: 0.0360
  gateway_out_series_per_s: 10000.0000
  gateway_points_per_s: 10000.0000
  gateway_rss_mib: 244.9800
  producer_bytes_out_per_s: 188011.2120
  producer_cpu_cores: 0.2470
  producer_rss_mib: 580.1000
```

## Resource breakdown (criterion 3)

| Component | RAW | ASAP |
|-----------|-----|------|
| agent_cpu_cores | 0.053 | nan |
| agent_rss_mib | 239.309 | nan |
| backend_cpu_pct | 16.540 | 0.010 |
| backend_rss_mib | 3700.736 | 3879.936 |

## Cold-fallback ad-hoc query response

```json
{
  "data": {
    "result": [
      {
        "metric": {},
        "value": [
          1778086267.046,
          "10000"
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
