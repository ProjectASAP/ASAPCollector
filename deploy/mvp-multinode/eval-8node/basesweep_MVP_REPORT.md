# MVP multi-node sweep report

Run: `basesweep-174814`  |  Soak: `60s`  |  Workload: PER_AGENT_CARDINALITY=200, FREQ_HZ=50, SDK_AGG=raw-buffer, SDK_WINDOW=1s, N_PRODUCERS_PER_NODE=2

## §1 Per-arm NIC bandwidth (cluster-wide, /sys/class/net/enp130s0f0)

| arm | node | role | rx_MB/s | tx_MB/s | rx_total_MB | tx_total_MB |
|---|---|---|---|---|---|---|
| asap | node1 | (unused since #400) | 0.44 | 0.11 | 29.2 | 7.5 |
| asap | node2 | backend | 0.21 | 0.26 | 13.7 | 16.9 |
| asap | node3 | producer+agent-b | 0.00 | 0.28 | 0.3 | 18.6 |
| asap | node4 |  | 0.00 | 0.25 | 0.3 | 16.4 |
| b0 | node1 | (unused since #400) | 25.47 | 0.02 | 1674.9 | 1.6 |
| b0 | node2 | backend | 0.00 | 0.00 | 0.1 | 0.0 |
| b0 | node3 | producer+agent-b | 0.01 | 11.74 | 0.7 | 768.6 |
| b0 | node4 |  | 0.01 | 13.77 | 0.8 | 899.7 |
| b1 | node1 | (unused since #400) | 1.07 | 0.01 | 70.4 | 0.5 |
| b1 | node2 | backend | 0.00 | 0.00 | 0.1 | 0.0 |
| b1 | node3 | producer+agent-b | 0.00 | 0.51 | 0.2 | 33.7 |
| b1 | node4 |  | 0.00 | 0.56 | 0.2 | 36.7 |

## §2 Per-container resource usage

| arm | host | container | cpu_mean_% | cpu_max_% | mem_mean_MiB | mem_max_MiB | n |
|---|---|---|---|---|---|---|---|
| asap | node3 | asap-agent-a | 122.6 | 273.9 | 1396.4 | 1776.6 | 9 |
| asap | node4 | asap-agent-b | 115.8 | 343.9 | 1622.0 | 1862.7 | 9 |
| asap | node2 | asap-control-plane | 0.0 | 0.0 | 2.1 | 2.2 | 9 |
| asap | node2 | asap-data-plane | 29.1 | 37.5 | 85.9 | 92.0 | 9 |
| asap | node1 | asap-gorilla-merger | 26.6 | 92.7 | 201.7 | 248.0 | 9 |
| asap | node1 | asap-minio | 0.3 | 2.0 | 83.9 | 84.3 | 9 |
| asap | node3 | asap-producer-a-1 | 93.4 | 102.9 | 2338.0 | 2672.6 | 9 |
| asap | node3 | asap-producer-a-2 | 93.7 | 101.5 | 1897.5 | 2632.7 | 9 |
| asap | node4 | asap-producer-b-1 | 79.2 | 105.5 | 1617.9 | 1795.1 | 9 |
| asap | node4 | asap-producer-b-2 | 68.9 | 103.6 | 1608.0 | 1779.7 | 9 |
| asap | node1 | asap-prometheus | 0.4 | 3.0 | 24.1 | 24.5 | 9 |
| asap | node1 | asap-thanos-compact | 0.0 | 0.0 | 11.3 | 11.7 | 9 |
| asap | node1 | asap-thanos-query | 14.7 | 30.2 | 21.4 | 26.4 | 9 |
| asap | node1 | asap-thanos-store-gateway | 0.0 | 0.2 | 11.3 | 11.9 | 9 |
| b0 | node3 | asap-agent-a | 33.4 | 132.8 | 1008.7 | 1515.5 | 9 |
| b0 | node4 | asap-agent-b | 42.0 | 64.7 | 221.2 | 288.0 | 9 |
| b0 | node3 | asap-producer-a-1 | 87.1 | 101.2 | 1334.2 | 1904.6 | 9 |
| b0 | node3 | asap-producer-a-2 | 92.4 | 102.2 | 1307.7 | 1548.3 | 9 |
| b0 | node4 | asap-producer-b-1 | 85.2 | 92.0 | 263.5 | 297.1 | 9 |
| b0 | node4 | asap-producer-b-2 | 86.8 | 100.0 | 277.1 | 293.1 | 9 |
| b0 | node1 | asap-victoriametrics | 223.5 | 383.3 | 2621.0 | 4186.1 | 10 |
| b1 | node3 | asap-agent-a | 41.7 | 135.7 | 1250.5 | 1629.2 | 9 |
| b1 | node4 | asap-agent-b | 57.9 | 197.2 | 1629.4 | 1931.3 | 9 |
| b1 | node3 | asap-producer-a-1 | 93.6 | 102.5 | 1612.2 | 2154.5 | 9 |
| b1 | node3 | asap-producer-a-2 | 95.8 | 100.9 | 1656.5 | 2157.6 | 9 |
| b1 | node4 | asap-producer-b-1 | 100.1 | 101.5 | 1855.4 | 2380.8 | 9 |
| b1 | node4 | asap-producer-b-2 | 97.2 | 102.7 | 1689.0 | 1834.0 | 9 |
| b1 | node1 | asap-victoriametrics | 152.9 | 387.1 | 4050.8 | 6044.7 | 10 |

## §3 Ingest rate observed at query backend

| arm | ingest count | event rate (/s) |
|---|---|---|
| b0 | ? | ? |
| b1 | ? | ? |
| asap | 800 | ? |

## §4 Ingest validation (series count, producer count, per-series rate)

| arm | series_expected | series_observed | series_OK | producers_expected | producers_observed | producers_OK | per-series rate (events/s) | rate_OK |
|---|---|---|---|---|---|---|---|---|
| b0 | — | NOT CAPTURED | — | — | — | — | — | — |
| b1 | — | NOT CAPTURED | — | — | — | — | — | — |
| asap | 800 | 800 | PASS | 4 | 4 | PASS | observed=0, expected=50 | FAIL |

## §5 PromQL replay latency per query class (p50/p99 ms)

| arm | query_id | n | p50_ms | p90_ms | p99_ms |
|---|---|---|---|---|---|
| b0 | (no parseable lines) | — | — | — | — |
| b1 | (no parseable lines) | — | — | — | — |
| asap | (no parseable lines) | — | — | — | — |

## §6.5 Data freshness — sample-generation-to-backend-write delay (ms)

| arm | tier | n | p50_ms | p90_ms | p99_ms | min_ms | max_ms |
|---|---|---|---|---|---|---|---|
| b0 | (no successful polls) | — | — | — | — | — | — |
| b1 | (no successful polls) | — | — | — | — | — | — |
| asap | (no successful polls) | — | — | — | — | — | — |

## §6 Cross-arm query-value accuracy (B0 = ground truth)

(Computed from the per-arm replay.jsonl response values for the same query.)

| query_id | b0_value | b1_value | asap_value | b1_rel_err_% | asap_rel_err_% |
|---|---|---|---|---|---|
