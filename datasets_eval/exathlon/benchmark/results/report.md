# Exathlon benchmark report

## Accuracy (sketch vs ground truth)
| query | file | metric | value | threshold | pass |
| --- | --- | --- | --- | --- | --- |
| Q1 | app1_1_0_10000_17 | frac_q50_lt_1pct | 0.9700996677740864 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q95_lt_1pct | 0.94875 | 0.9 | True |
| Q1 | app1_1_0_10000_17 | frac_q99_lt_1pct | 0.9637912673056444 | 0.85 | True |
| Q3 | app1_1_0_10000_17 | topk_overlap | 0.4 | 0.8 | False |
| Q3 | app1_1_0_10000_17 | rank_correlation | 1.0 | 0.7 | True |

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
Q3,app1/1_0_10000_17,paced,100,995666,610.380757,1631.22115,40
Q3,app10/10_0_100000_10,paced,100,1089539,664.513993,1639.602794,43
Q3,app1/1_0_10000_17,paced,100,995666,611.252294,1628.895318,40
Q3,app1/1_0_10000_17,paced,100,995666,611.938066,1627.069887,40
Q3,app1/1_0_10000_17,paced,100,27058,618.025041,43.781398,1
Q3,app1/1_0_10000_17,paced,100,27058,619.433478,43.68185,1
Q3,app1/1_0_10000_17,paced,100,27058,618.90243,43.719331,6
Q3,app1/1_0_10000_17,paced,100,27058,619.656615,43.66612,28
Q3,app1/1_0_10000_17,paced,100,27058,618.948964,43.716044,28
Q3,app1/1_0_10000_17,paced,100,27058,621.17483,43.559395,6
Q3,app1/1_0_10000_17,paced,100,27058,620.560412,43.602524,6
Q3,app1/1_0_10000_17,paced,100,27058,620.814441,43.584682,6


## Latency (send_times.csv, this run)
| stat | ms |
| --- | --- |
| p50_delta_event | 72999.964267 |
| p95_delta_event | 188200.281649 |
| p99_delta_event | 192040.317519 |
| p50_send_lag | 0.069016 |
| p99_send_lag | 0.422412 |
| p50_send_lag_drift | 0.068572 |
| p99_send_lag_drift | 0.421968 |

_`send_lag` uses min-observed wall-event offset as baseline (non-negative, extra delay)._

## Export diagnostics (export_diagnostics.csv, this run)
| stat | value |
| --- | --- |
| queue_wait_p50_ms | 5.331934 |
| queue_wait_p95_ms | 5.605884 |
| queue_wait_p99_ms | 5.635152 |
| event_span_p50_ms | 68500.000000 |
| event_span_p95_ms | 187000.000000 |
| event_regressions_total | 0 |
| event_regressions_max_export | 0 |

## Latency history (latency.csv)
query,file,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms
Q2,app1/1_0_10000_17,throughput,36.274688,64.930792,86.229385,1769004.454903,3504660.797558
Q3,app1/1_0_10000_17,sketch-telemetry,316.225507,30787.069049,30845.512318,6707.179942,14900.011961
Q3,app10/10_0_100000_10,sketch-telemetry,10298.510447,27864.983257,28436.378402,707.646357,14464.439319
Q3,app1/1_0_10000_17,sketch-telemetry,349.132221,30724.088214,30865.076916,6671.169057,14923.078885
Q3,app1/1_0_10000_17,sketch-telemetry,,,,,
Q3,app1/1_0_10000_17,sketch-telemetry,,,,,
Q3,app1/1_0_10000_17,sketch-telemetry,72999.887241,188199.928874,192039.890001,0.118,0.298885
Q3,app1/1_0_10000_17,sketch-telemetry,16000.129378,42099.892578,42999.976892,0.098528,0.238501
Q3,app1/1_0_10000_17,sketch-telemetry,16000.141378,42100.003957,43000.012312,0.13677,0.427144
Q3,app1/1_0_10000_17,sketch-telemetry,72999.743755,188199.956489,192039.909204,0.08405,0.253078
Q3,app1/1_0_10000_17,sketch-telemetry,72999.91791,188200.161129,192040.191092,0.071894,0.32006
Q3,app1/1_0_10000_17,sketch-telemetry,72999.964267,188200.281649,192040.317519,0.069016,0.422412


## Notes
- Accuracy rows are only available for queries with ground truth (Q1, Q3–Q9).
- Q2, Q10, Q11, Q12 are throughput/latency-only (NOP collector path).
- Threshold per (entity, metric_base) = file-local p95 of non-sentinel values.