# Exathlon benchmark report

## Accuracy (sketch vs ground truth)
| query | file | metric | value | threshold | pass |
| --- | --- | --- | --- | --- | --- |
| Q1 | app1_1_0_10000_17 | frac_q50_lt_1pct | 0.9700996677740864 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q95_lt_1pct | 0.94875 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q99_lt_1pct | 0.9637912673056444 | 0.85 | True |

## Throughput (replay)
query,file,replay_mode,speed_factor,total_events,elapsed_s,events_per_sec,export_count
Q1,app1/1_0_10000_17,paced,100,995666,608.294476,1636.815784,40
Q1,app1/1_0_10000_17,paced,100,995666,608.185405,1637.109329,40


## Latency (send_times.csv, this run)
| stat | ms |
| --- | --- |
| p50_delta_event | 202.358130 |
| p95_delta_event | 30856.244562 |
| p99_delta_event | 30857.720807 |
| p50_send_lag | 6638.797878 |
| p99_send_lag | 14938.689532 |
| p50_send_lag_drift | -8263.883152 |
| p99_send_lag_drift | 36.008502 |

_`send_lag` uses min-observed wall-event offset as baseline (non-negative, extra delay)._

## Export diagnostics (export_diagnostics.csv, this run)
| stat | value |
| --- | --- |
| queue_wait_p50_ms | 345.546492 |
| queue_wait_p95_ms | 393.256833 |
| queue_wait_p99_ms | 517.055683 |
| event_span_p50_ms | 15000.000000 |
| event_span_p95_ms | 16000.000000 |
| event_regressions_total | 0 |
| event_regressions_max_export | 0 |

## Latency history (latency.csv)
query,file,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms
Q1,app1/1_0_10000_17,sketch-telemetry,199.088399,30858.792234,30888.344414,6636.07948,14925.709668
Q1,app1/1_0_10000_17,sketch-telemetry,199.088399,30858.792234,30888.344414,6636.07948,14925.709668
Q1,app1/1_0_10000_17,sketch-telemetry,202.35813,30856.244562,30857.720807,6638.797878,14938.689532


## Notes
- Accuracy rows are only available for queries with ground truth (Q1, Q3–Q9).
- Q2, Q10, Q11, Q12 are throughput/latency-only (NOP collector path).
- Threshold per (entity, metric_base) = file-local p95 of non-sentinel values.