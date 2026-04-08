# Exathlon benchmark report

## Accuracy (sketch vs ground truth)
| query | file | metric | value | threshold | pass |
| --- | --- | --- | --- | --- | --- |
| Q1 | app1_1_0_10000_17 | frac_q50_lt_1pct | 0.9700996677740864 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q95_lt_1pct | 0.94875 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q99_lt_1pct | 0.9637912673056444 | 0.85 | True |

## Throughput (replay)
query,file,replay_mode,speed_factor,total_events,elapsed_s,events_per_sec,export_count
Q2,app1/1_0_10000_17,max,100,5953960,48.736052,122167.467579,1191


## Latency (send_times.csv, this run)
| stat | ms |
| --- | --- |
| p50_delta_event | 36.274688 |
| p95_delta_event | 64.930792 |
| p99_delta_event | 86.229385 |
| p50_send_lag | 1769004.454903 |
| p99_send_lag | 3504660.797558 |
| p50_send_lag_drift | -1770721.838616 |
| p99_send_lag_drift | -35065.495961 |

_`send_lag` uses min-observed wall-event offset as baseline (non-negative, extra delay)._

## Export diagnostics (export_diagnostics.csv, this run)
| stat | value |
| --- | --- |
| queue_wait_p50_ms | 1148.642648 |
| queue_wait_p95_ms | 1358.685068 |
| queue_wait_p99_ms | 1436.351566 |
| event_span_p50_ms | 3000.000000 |
| event_span_p95_ms | 3000.000000 |
| event_regressions_total | 0 |
| event_regressions_max_export | 0 |

## Latency history (latency.csv)
query,file,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms
Q2,app1/1_0_10000_17,throughput,36.274688,64.930792,86.229385,1769004.454903,3504660.797558


## Notes
- Accuracy rows are only available for queries with ground truth (Q1, Q3–Q9).
- Q2, Q10, Q11, Q12 are throughput/latency-only (NOP collector path).
- Threshold per (entity, metric_base) = file-local p95 of non-sentinel values.