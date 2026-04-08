# Q2 Report

Date: 2026-04-08

Query: `Q2`

File: `app1/1_0_10000_17`

## Scope

This report summarizes the Q2 results produced today for the Exathlon benchmark.

Q2 in this repository currently has two distinct result types:

1. Exact offline Q2 values derived from Q1 ground truth:
   - `p99 / p50`
   - `p95 / p50`
2. Throughput and latency measurements for the current benchmark harness path:
   - NOP collector path
   - throughput-only
   - no sketch-vs-ground-truth accuracy comparison is implemented for Q2 yet

## Run Configuration

Benchmark command used:

```bash
python3 datasets_eval/exathlon/benchmark/run.py test \
  --query Q2 \
  --file app1/1_0_10000_17 \
  --mode throughput \
  --batch-size 5000 \
  --clear-results
```

Replay mode:
- `max`

Batch size:
- `5000`

Collector path:
- `NOP`

## Throughput Result

Source:
- `datasets_eval/exathlon/benchmark/results/throughput.csv`

| Field | Value |
| --- | --- |
| Query | `Q2` |
| File | `app1/1_0_10000_17` |
| Replay mode | `max` |
| Speed factor | `100` |
| Total events | `5,953,960` |
| Elapsed time | `48.736052 s` |
| Events / sec | `122,167.467579` |
| Export count | `1,191` |

Interpretation:
- The Q2 NOP path processed about `5.95M` events in `48.7s`.
- Sustained replay throughput was about `122.2k events/s`.
- Export count is consistent with batch size `5000` because `5,953,960 / 5000 ≈ 1190.8`, which rounds up to `1,191` exports.

## Latency Result

Source:
- `datasets_eval/exathlon/benchmark/results/latency.csv`
- `datasets_eval/exathlon/benchmark/results/report.md`

| Metric | Value |
| --- | --- |
| p50 inter-arrival | `36.274688 ms` |
| p95 inter-arrival | `64.930792 ms` |
| p99 inter-arrival | `86.229385 ms` |
| p50 send lag | `1,769,004.454903 ms` |
| p99 send lag | `3,504,660.797558 ms` |

Notes:
- `send_lag` in this harness is computed relative to the minimum observed wall-clock minus event-time offset during the run.
- For throughput mode, send lag is useful mainly as a relative queueing indicator, not as an end-user latency SLA measurement.

## Export Diagnostics

Source:
- `datasets_eval/exathlon/benchmark/results/report.md`

| Metric | Value |
| --- | --- |
| queue_wait_p50_ms | `1148.642648` |
| queue_wait_p95_ms | `1358.685068` |
| queue_wait_p99_ms | `1436.351566` |
| event_span_p50_ms | `3000.000000` |
| event_span_p95_ms | `3000.000000` |
| event_regressions_total | `0` |
| event_regressions_max_export | `0` |

Interpretation:
- No event-time regressions were observed.
- Queue wait is stable and indicates buffering under max-speed replay.

## Exact Offline Q2 Result

Source:
- `datasets_eval/exathlon/benchmark/results/ground_truth/Q2/app1_1_0_10000_17.csv`

### Dataset Coverage

- Rows: `133,474`
- Entities: `10`
- Metric bases: `458`
- Window labels present: `1min`, `5min`, `15min`, `30min`, `1hour`

### Overall Q2 Distribution

- `tail_ratio_p99_p50` median: `1.025956`
- `tail_ratio_p99_p50` p95: `42.157979`
- `tail_ratio_p99_p50` max: `1.2813374917994795e12`
- `tail_ratio_p95_p50` median: `1.023789`
- `tail_ratio_p95_p50` p95: `23.296755`
- `tail_ratio_p95_p50` max: `3.050803551933989e11`

Interpretation:
- Most series are stable most of the time: the median tail ratio is only slightly above `1.0`.
- The distribution is extremely heavy-tailed: a non-trivial minority of windows show very large amplification.
- The max values are dominated by extreme outliers and should not be treated as typical behavior.

### Per-Window Summary for `p99/p50`

| Window | N | Median | Max | Fraction `> 2` |
| --- | ---: | ---: | ---: | ---: |
| `1min` | `36,548` | `1.016576` | `1.2813374917994795e12` | `0.233118` |
| `5min` | `7,813` | `1.070188` | `833,588.612562` | `0.251248` |
| `15min` | `3,050` | `1.173307` | `833,588.612562` | `0.288852` |
| `30min` | `1,843` | `1.155255` | `833,588.612562` | `0.288660` |
| `1hour` | `1,247` | `1.234568` | `833,588.612562` | `0.304731` |

Interpretation:
- As window size increases, the median Q2 ratio increases modestly.
- The fraction of spiky rows where `p99/p50 > 2` rises from about `23.3%` at `1min` to about `30.5%` at `1hour`.
- Longer windows accumulate more heterogeneous behavior, so tail amplification becomes more visible.

### Entities with Highest Median `p99/p50`

By window, the most extreme entity is consistently `entity=5`:

- `1min`: median `54.316459`
- `5min`: median `54.316459`
- `15min`: median `54.316459`
- `30min`: median `54.316459`
- `1hour`: median `54.316459`

Other notable entities:
- `driver` is elevated across larger windows, for example `1hour` median `1.837963`
- `node6` is consistently above baseline, for example `5min` median `1.265823` and `1hour` median `1.57358`
- `entity=4` is notable at `1min` with median `20.984878`, but not across all windows

### Most Extreme 5-Minute Metric Series

Top 5 by median `p99/p50` in `5min` windows:

1. `entity=5`, `CodeGenerator_compilationTime`: `833,588.612562`
2. `driver`, `LiveListenerBus_queue_executorManagement_listenerProcessingTime`: `9,270.923077`
3. `node8`, `CPU008_Wait%`: `3,070.123947`
4. `node5`, `CPU024_Idle%`: `2,937.56`
5. `node5`, `CPU023_Wait%`: `2,937.56`

This indicates the strongest tail amplification is concentrated in:
- Spark code generation metrics
- driver-side listener queue latency
- several CPU wait/idle-related host metrics

### Important Caveat

The largest ratios come from very small or unstable denominators:

- Q2 leaves the ratio blank when `p50 == 0`
- but if `p50` is nonzero and still very small, the ratio can explode

Examples from the top outliers:
- `node6 / NETPACKET_em3-write/s / 1min`: `p50=1.0`, `p99≈1.281e12`
- `node6 / NETPACKET_em1-read/s / 1min`: `p50=1.0`, `p99≈1.281e12`

## Conclusion

Today’s Q2 benchmark produced two usable outputs:

1. Exact offline Q2 values for `app1/1_0_10000_17`
2. Throughput and latency results for the current NOP-based Q2 harness path

The current repository does not yet produce a third result type for Q2:
- sketch-vs-ground-truth accuracy comparison

Therefore, the correct interpretation of today’s results is:
- Q2 analytical result: available
- Q2 throughput result: available
- Q2 accuracy benchmark result: not implemented in the harness yet
