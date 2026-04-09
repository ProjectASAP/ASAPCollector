# Exathlon benchmark report

## Accuracy (sketch vs ground truth)
| query | file | metric | value | threshold | pass |
| --- | --- | --- | --- | --- | --- |
| Q1 | app1_1_0_10000_17 | frac_q50_lt_1pct | 0.9700996677740864 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q95_lt_1pct | 0.94875 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q99_lt_1pct | 0.9637912673056444 | 0.85 | True |
| Q3 | app1_1_0_10000_17 | topk_overlap | 0.4 | 0.8 | False |
| Q3 | app1_1_0_10000_17 | rank_correlation | 1.0 | 0.7 | True |
| Q4 | app1_1_0_10000_17 | frac_min_lt_2pct | 0.9689655172413794 | 0.9 | True |
| Q4 | app1_1_0_10000_17 | frac_max_lt_2pct | 0.998407643312102 | 0.9 | True |
| Q4 | app1_1_0_10000_17 | frac_minmax_both_lt_2pct | 0.9688581314878892 | 0.9 | True |

## Q3 Top-K Comparison
### Q3 / app1_1_0_10000_17

- Exact match: False
- Ground truth Top-K:
`aggregation=count;entity=3;metric_base=CodeGenerator_generatedMethodSize;`
`aggregation=value;entity=3;metric_base=jvm_pools_PS-Eden-Space_committed;`
`aggregation=unknown;entity=node6;metric_base=CPU032_Sys%;`
`aggregation=value;entity=3;metric_base=jvm_pools_PS-Eden-Space_usage;`
`aggregation=value;entity=3;metric_base=jvm_pools_PS-Eden-Space_used;`
`aggregation=count;entity=4;metric_base=CodeGenerator_generatedMethodSize;`
`aggregation=value;entity=2;metric_base=jvm_pools_PS-Eden-Space_usage;`
`aggregation=value;entity=2;metric_base=jvm_pools_PS-Eden-Space_used;`
`aggregation=unknown;entity=node6;metric_base=CPU009_Sys%;`
`aggregation=unknown;entity=node6;metric_base=CPU002_Sys%;`
- Sketch Top-K:
`aggregation=count;entity=3;metric_base=CodeGenerator_generatedMethodSize;`
`aggregation=value;entity=3;metric_base=jvm_pools_PS-Eden-Space_committed;`
`aggregation=unknown;entity=node5;metric_base=CPU015_User%;`
`aggregation=unknown;entity=node5;metric_base=CPU027_Steal%;`
`aggregation=value;entity=2;metric_base=jvm_pools_Metaspace_usage;`
`aggregation=value;entity=2;metric_base=jvm_pools_PS-Eden-Space_usage;`
`aggregation=value;entity=2;metric_base=jvm_pools_PS-Eden-Space_used;`
`aggregation=value;entity=driver;metric_base=jvm_pools_PS-Old-Gen_init;`
`aggregation=unknown;entity=node6;metric_base=CPU032_Wait%;`
`aggregation=unknown;entity=node6;metric_base=CPU025_Steal%;`

## Throughput (replay)
query,file,replay_mode,speed_factor,total_events,elapsed_s,events_per_sec,export_count
Q2,app1/1_0_10000_17,max,100,5953960,48.736052,122167.467579,1191
Q3,app1/1_0_10000_17,paced,100,27058,620.814441,43.584682,6
Q4,app1/1_0_10000_17,paced,100,995666,608.985995,1634.957138,40
Q4,app1/1_0_10000_17,paced,100,995666,610.151657,1631.833641,40
Q4,app1/1_0_10000_17,paced,100,5953960,3591.329325,1657.870794,239
Q4,app1/1_0_10000_17,paced,100,995666,608.326515,1636.729578,40


## Latency (send_times.csv, this run)
| stat | ms |
| --- | --- |
| p50_delta_event | 196.784296 |
| p95_delta_event | 30861.778510 |
| p99_delta_event | 30901.624967 |
| p50_send_lag | 6615.748457 |
| p99_send_lag | 14997.669065 |
| p50_send_lag_drift | -8309.784260 |
| p99_send_lag_drift | 72.136348 |

_`send_lag` uses min-observed wall-event offset as baseline (non-negative, extra delay)._

## Export diagnostics (export_diagnostics.csv, this run)
| stat | value |
| --- | --- |
| queue_wait_p50_ms | 352.111097 |
| queue_wait_p95_ms | 432.122458 |
| queue_wait_p99_ms | 538.973150 |
| event_span_p50_ms | 15000.000000 |
| event_span_p95_ms | 16000.000000 |
| event_regressions_total | 0 |
| event_regressions_max_export | 0 |

## Latency history (latency.csv)
query,file,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms
Q2,app1/1_0_10000_17,throughput,36.274688,64.930792,86.229385,1769004.454903,3504660.797558
Q3,app1/1_0_10000_17,sketch-telemetry,72999.964267,188200.281649,192040.317519,0.069016,0.422412
Q4,app1/1_0_10000_17,sketch-telemetry,223.990087,30865.177578,30880.07419,6647.613104,14944.06005
Q4,app1/1_0_10000_17,sketch-telemetry,236.24348,30837.432604,30879.342863,6651.450376,14911.803857
Q4,app1/1_0_10000_17,sketch-telemetry,918.342888,30887.892815,30920.051457,356.347615,16217.763925
Q4,app1/1_0_10000_17,sketch-telemetry,196.784296,30861.77851,30901.624967,6615.748457,14997.669065


## Notes
- Accuracy rows are only available for queries with ground truth (Q1, Q3–Q9).
- Q2, Q10, Q11, Q12 are throughput/latency-only (NOP collector path).
- Threshold per (entity, metric_base) = file-local p95 of non-sentinel values.