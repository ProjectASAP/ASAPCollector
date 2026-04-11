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
| Q5 | app1_1_0_10000_17 | frac_iqr_lt_10pct | 0.3927335640138408 | 0.85 | False |
| Q5 | app1_1_0_10000_17 | anomaly_precision | 0.3238471927455141 | 0.8 | False |
| Q5 | app1_1_0_10000_17 | anomaly_recall | 0.9366629464285714 | 0.8 | True |
| Q5 | app1_1_0_10000_17 | anomaly_f1 | 0.4812903225806451 | 0.8 | False |
| Q6 | app1_1_0_10000_17 | hll_rel_err | 0.0027143338178891 | 0.05 | True |
| Q7 | app1_1_0_10000_17 | entity_topk_overlap | 1.0 | 0.8 | True |
| Q8 | app1_1_0_10000_17 | frac_drift_p95_lt_20pct | 0.4511970534069981 | 0.8 | False |

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
Q4,app1/1_0_10000_17,paced,100,995666,608.326515,1636.729578,40
Q5,app1/1_0_10000_17,paced,100,1493466,909.698153,1641.715985,60
Q6,app1/1_0_10000_17,paced,100,995666,612.781967,1624.829146,40
Q7,app1/1_0_10000_17,max,100,257986,30.363709,8496.524716,11
Q8,app1/1_0_10000_17,paced,100,995666,610.075852,1632.036405,40
Q8,app1/1_0_10000_17,paced,100,995666,610.393333,1631.187541,40
Q8,app1/1_0_10000_17,paced,100,5953960,3591.388456,1657.843498,239
Q8,app1/1_0_10000_17,paced,100,995666,608.437357,1636.431406,40
Q8,app1/1_0_10000_17,scaled,100,995666,22.84134,43590.525584,40


## Latency (send_times.csv, this run)
| stat | ms |
| --- | --- |
| p50_delta_event | 279.375697 |
| p95_delta_event | 693.409756 |
| p99_delta_event | 750.858771 |
| p50_send_lag | 283498.974521 |
| p99_send_lag | 565503.234445 |
| p50_send_lag_drift | -287771.186508 |
| p99_send_lag_drift | -5766.926583 |

_`send_lag` uses min-observed wall-event offset as baseline (non-negative, extra delay)._

## Export diagnostics (export_diagnostics.csv, this run)
| stat | value |
| --- | --- |
| queue_wait_p50_ms | 3358.824355 |
| queue_wait_p95_ms | 6136.135740 |
| queue_wait_p99_ms | 6567.499435 |
| event_span_p50_ms | 15000.000000 |
| event_span_p95_ms | 16000.000000 |
| event_regressions_total | 0 |
| event_regressions_max_export | 0 |

## Latency history (latency.csv)
query,file,replay_mode,p50_inter_arrival_ms,p95_inter_arrival_ms,p99_inter_arrival_ms,p50_send_lag_ms,p99_send_lag_ms
Q2,app1/1_0_10000_17,throughput,36.274688,64.930792,86.229385,1769004.454903,3504660.797558
Q3,app1/1_0_10000_17,sketch-telemetry,72999.964267,188200.281649,192040.317519,0.069016,0.422412
Q4,app1/1_0_10000_17,sketch-telemetry,196.784296,30861.77851,30901.624967,6615.748457,14997.669065
Q5,app1/1_0_10000_17,sketch-telemetry,185.786311,30795.165543,30851.375795,6656.860166,15313.055376
Q6,app1/1_0_10000_17,sketch-telemetry,213.696195,30853.571179,30878.754613,6634.171686,14965.655349
Q7,app1/1_0_10000_17,sketch-telemetry,305.520033,3106.740982,3130.444911,1430781.317423,3213206.131658
Q8,app1/1_0_10000_17,sketch-telemetry,240.837003,30878.884644,30893.098271,6663.955786,14938.167076
Q8,app1/1_0_10000_17,sketch-telemetry,265.739742,30846.236372,30896.289566,6680.997215,14915.703231
Q8,app1/1_0_10000_17,sketch-telemetry,924.99507,30877.11003,30902.762755,403.436516,16249.468056
Q8,app1/1_0_10000_17,sketch-telemetry,213.881741,30874.621386,30890.060429,6649.369196,14921.57358
Q8,app1/1_0_10000_17,sketch-telemetry,279.375697,693.409756,750.858771,283498.974521,565503.234445


## Notes
- Accuracy rows are only available for queries with ground truth (Q1, Q3–Q9).
- Q2, Q10, Q11, Q12 are throughput/latency-only (NOP collector path).
- Threshold per (entity, metric_base) = file-local p95 of non-sentinel values.