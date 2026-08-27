# Summary aggregation and transmission

## TL;DR

ASAPCollector converts selected metric streams into mergeable summaries and
transmits raw observations, full summaries, or supported summary deltas as
directed by the control plane. The design covers three aggregation shapes,
fresh open-window results, and equivalent full/delta query semantics within a
declared accuracy bound.

**Status:** active

**MVP relationship:** in-scope.

## Scope

This document owns the design of:

- summary families and their merge semantics;
- aggregation across time and label groups;
- open and completed time windows;
- raw, full-summary, and delta-summary transmission; and
- the relationship between accuracy, freshness, and communication cost.

It does not define query planning, collector configuration delivery, wire
schemas, serialization layouts, or implementation phases.

## Aggregation model

ASAP supports three logical aggregation shapes.

### Per-series aggregation over a time window

Each series contributes to its own summary during a selected window. Typical
results include sums, counts, quantiles, distinct counts, and frequency
estimates over time.

**PromQL example:**

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
```

This returns one p95 result for each input label set, such as
`{service, instance}`. No cross-series merge is required.

### Aggregation across label groups at a timestamp

Series are grouped by selected labels and their compatible summaries are
merged. The grouping dimensions preserved by collection determine which
queries can be answered without raw data.

**PromQL example:**

```promql
count by (region) (
  count by (region, user_id) (active_user)
)
```

Evaluated as an instant query, this counts distinct `user_id` label sets per
`region`. An HLL-backed realization may answer it approximately.

### Aggregation across labels and a time window

Summaries are merged across both selected series and window state. This shape
requires compatible grouping, aligned windows, and merge semantics that remain
valid across every contributing collector.

**PromQL example:**

```promql
sum by (service) (
  increase(http_requests_total[5m])
)
```

The backend merges compatible five-minute summaries across collectors before
returning one result per `service`.

The collection plan declares the aggregation shape. A query whose grouping or
window cannot be reconstructed from the collected state is unsupported.

## Summary families

| Family | Result | Accuracy model | Merge model | Delta mode |
| --- | --- | --- | --- | --- |
| Sum/Count | exact scalar aggregate | predeclared absolute/relative tolerance | addition | additive delta |
| DDSketch | quantiles | relative value error | compatible-state merge | supported |
| KLL | quantiles | rank error | compatible-state merge | full state for MVP |
| HLL | distinct cardinality | cardinality error | register-wise maximum | changed registers |
| Count-Min | non-negative frequency | additive overestimate bound | cell-wise addition | sparse cell delta |
| Count-Sketch | signed frequency | probabilistic point-error bound | cell-wise addition | sparse cell delta |

Every family is parameterized. Two states may be merged only when their
family, parameters, grouping, window identity, and plan semantics are
compatible.

### Query examples by summary family

| Family | PromQL example |
| --- | --- |
| Sum/Count | `sum by (service) (increase(http_requests_total[1m]))` |
| DDSketch | `quantile_over_time(0.99, request_duration_seconds[5m])` |
| KLL | `quantile_over_time(0.5, payload_size_bytes[5m])` |
| HLL | `count by (tenant) (count by (tenant, user_id) (active_user))` |
| Count-Min | `topk(10, sum by (route) (rate(http_requests_total[5m])))` |
| Count-Sketch | `topk(10, abs(sum by (event_type) (rate(events_total[5m]) - rate(events_total[5m] offset 1h))))` |

## Window model

The collector assigns observations to timestamp-defined windows. Window
identity is part of summary identity; states from different windows are never
merged accidentally.

### Open window

The current window accepts updates and may be queried before it closes. Its
answer represents all updates visible at query time. Freshness is therefore
measured from an observation's source timestamp to the first query result that
contains it.

**PromQL example:**

```promql
quantile_over_time(0.95, request_duration_seconds[1m])
```

A latency observation timestamped `12:00:20` belongs to the open
`12:00–12:01` window. Evaluating this query at `12:00:23` may return the
partial p95 that includes it; the measured freshness lag is three seconds.

### Completed window

A completed window is immutable. Late observations are handled by an explicit
lateness policy; they are never silently inserted into an unrelated active
window.

**Example:** The `12:00–12:01` window closes at `12:01:00`. An observation for
`12:00:58` arriving at `12:01:08` is accepted only if the declared allowed
lateness covers eight seconds; otherwise it is rejected and counted.

### Cross-collector alignment

Grouped queries may combine summaries from multiple collectors. Those
collectors must agree on window boundaries, timestamp interpretation, summary
parameters, and plan identity. Misaligned state is rejected or kept separate.

**PromQL example:**

```promql
quantile by (region) (0.99, request_duration_seconds)
```

Collector A labels a summary as window `12:00–12:05`, while Collector B labels
its state `12:01–12:06`. The backend must not merge them to answer this query
for one logical five-minute window.

## Transmission modes

### Raw/pass-through

The collector forwards selected observations without summarizing them. This is
used when exact values are required or no supported summary can answer the
declared workload.

**PromQL example:**

```promql
queue_depth{instance="worker-7"} @ 1787841600
```

This exact point query selects raw pass-through because a window summary
cannot reproduce the original observation.

### Full summary

The collector sends a complete summary state for a specific identity and
window. The receiver can reconstruct the represented state without relying on
an earlier payload.

**PromQL example:**

```promql
quantile_over_time(0.5, payload_size_bytes[5m])
```

At the end of the window, one complete KLL state can answer this median query
without relying on an earlier KLL payload.

### Delta summary

The collector sends only the change since an identified base state. A delta is
valid only when the family defines a safe update operation and the receiver has
the required base or ordering context.

**Example:** A Count-Min base contains request frequencies through sequence
`41`. Sequence `42` carries only the cells changed by the next batch. The
backend applies it only to base `41` for the same plan, group, and window.

The ASAPQuery-backend data plane applies the backend portion of the active
plan when ingesting either form. Delta transmission reduces repeated state but
introduces lifecycle requirements: base identity, duplicate handling, loss
recovery, ordering rules, and resynchronization must all be explicit.

**Recovery example:** If sequence `42` is missing and sequence `43` arrives,
the backend marks the state incomplete and requests or waits for a full
summary. It does not return a plausible frequency estimate from the broken
sequence.

## Full and delta equivalence

For the same observations, plan, and logical interval, applying all valid
deltas must produce query semantics equivalent to receiving the corresponding
full state. Equivalence means:

- exact families match under their predeclared absolute and relative numeric
  tolerances;
- approximate families remain within the same configured accuracy SLA;
- no series, labels, or timestamps are lost or invented; and
- duplicate or missing deltas cannot produce an apparently valid result.

For an exact result point, the checked-in acceptance configuration declares
`absolute_tolerance` and `relative_tolerance`. A finite pair passes when:

```text
abs(ASAP - exact) <= max(
  absolute_tolerance,
  relative_tolerance * abs(exact)
)
```

The absolute tolerance governs exact values at or near zero. NaN matches only
NaN; positive and negative infinity match only the same infinity; finite and
non-finite values never match. These rules are fixed before the run.

Periodic or requested full-state synchronization provides a recovery point
when delta continuity cannot be established.

**Equivalence PromQL example:**

```promql
topk(10, sum by (event_type) (rate(events_total[5m])))
```

One collector sends a complete Count-Sketch after 10,000 observations;
another sends a compatible base plus all deltas for the same observations.
Their results for this query must agree within the same configured
Count-Sketch error bound.

## Delta behavior by family

Additive summaries transmit changed scalar or cell values that are added to
the receiver's compatible base. HLL transmits register increases and merges by
maximum. KLL uses full-state transmission in the MVP because a compact,
order-independent delta contract is not claimed.

A family-specific threshold may delay transmission of small changes to reduce
communication. Such a threshold consumes part of the declared error and
freshness budgets; it cannot be selected independently of those SLAs.

**Family examples:** A Sum delta adds `+37`; a Count-Min delta adds changed
cells; an HLL delta raises selected registers; a KLL scenario sends a complete
compatible state. If a threshold holds a DDSketch change for two seconds, that
delay is charged to the freshness SLA.

## Accuracy contract

Before an MVP run, every supported PromQL query defines how its result will be
validated against the exact baseline. Its checked-in acceptance configuration
states:

- what one result value represents;
- whether that value must be exact or may be approximate;
- which summary family and parameters produce it;
- how error is calculated;
- the largest permitted error; and
- the percentage of aligned result points that must remain within that error.

The percentage applies to result points across matching label sets,
timestamps, and repetitions. It does not mean that some supported query
definitions may fail: every supported query must satisfy its own predeclared
SLA.

Before calculating numeric error, ASAP and exact results are aligned by labels
and timestamps. Missing or extra series and points are validation errors; they
are not treated as zero-valued answers or excluded from the denominator.

Sampling error, summary error, delayed-delta error, and merge error must fit
within one declared end-to-end budget. Internal error budgets may be divided
among mechanisms, but the user-facing SLA applies to the final query result.

**PromQL example:**

```promql
quantile_over_time(0.99, request_duration_seconds[5m])
```

| Acceptance field | Predeclared value |
| --- | --- |
| Result meaning | p99 request duration for each input series over five minutes |
| Semantics | approximate |
| Summary | DDSketch with 1% relative accuracy |
| Error calculation | `abs(ASAP - exact) / max(abs(exact), epsilon)` |
| Maximum error | 1% relative error |
| Required passing points | at least 99% of aligned label-and-timestamp points |

For example, if the exact and ASAP responses contain 10,000 aligned result
points, at least 9,900 must have relative error no greater than 1%. Missing or
extra points still fail validation separately and cannot be hidden inside the
allowed 1% of out-of-bound numeric results.

## Freshness contract

Freshness is the time from a source observation timestamp to the first
successful query whose backend progress evidence proves that the corresponding
observation or window contribution has been applied. The MVP reports freshness
separately by aggregation class and transmission mode.

The load generator assigns a run identity and monotonically increasing source
sequence to test observations. For each plan, group, and window, the collector
and backend expose their applied high-watermark. A query response is fresh for
sequence `N` only when its recorded backend watermark is at least `N` and its
run, plan, group, and window identities match. The harness uses this progress
evidence rather than inferring inclusion from the numeric answer.

An answer from an earlier run, earlier plan, or earlier window is stale even if
its numeric value appears plausible. Run identity, plan identity, and window
identity are therefore part of validation.

**PromQL example:**

```promql
sum by (service) (increase(http_requests_total[1m]))
```

An observation timestamped `12:00:20` first appears in a valid result for this
query at `12:00:24.5`, and the response evidence reports the matching run and
plan with a watermark at or beyond that observation's sequence. Its freshness
lag is 4.5 seconds. A cached answer from a previous run does not satisfy this
measurement.

## Cost model

Summary construction trades edge CPU and memory for lower network, backend
ingest, storage, and query costs. Delta transmission further trades state and
recovery complexity for lower repeated network cost.

No transmission mode is universally cheaper. The fair comparison measures the
same observation stream and reports CPU, memory, bytes, storage, and query work
separately before applying fixed cost weights.

The checked-in acceptance configuration defines one cost model used by both
arms:

```text
normalized_cost =
  cpu_weight     * CPU-seconds +
  memory_weight  * GiB-seconds +
  network_weight * GiB-transmitted +
  storage_weight * GiB-hours-stored
```

The model includes collector processing, transmission, backend ingestion,
storage, and query execution. A stage may not be omitted because it regresses.
All weights and per-resource regression guardrails are fixed before the run.

The collector-cost gate passes only when ASAPCollector's normalized cost is
lower than the raw-forwarding collector and every CPU, memory, and network
guardrail passes. The end-to-end gate passes only when ASAPCollector plus the
ASAPQuery-backend has lower normalized cost than the full exact pipeline and
the functional, accuracy, freshness, and latency gates also pass. Unlike units
are never added without these declared weights.

**Example:** Compare a workload that transmits every 100 ms raw observation
with one that emits five-second summary deltas. Measure collector CPU and
memory, transmitted bytes, backend ingest and query CPU, and stored bytes for
both arms before applying the checked-in cost weights.

## Failure behavior

| Failure scenario | Example | Required behavior |
| --- | --- | --- |
| Unsupported query | `request_errors_total / on (service) group_left request_total` cannot be realized from the selected summaries. | Reject it or route it to the configured exact backend and mark the result `exact-fallback`. |
| Incompatible summaries | Two DDSketch states use different accuracy parameters. | Keep them separate and report incompatibility. |
| Missing delta base | Delta sequence `43` arrives without the required base or sequence `42`. | Resynchronize or fail; do not query incomplete state. |
| Stale result | A response carries an earlier run or plan identity. | Fail validation even if its value looks reasonable. |
| Empty result | The exact arm returns three aligned series while ASAP returns none. | Record missing series and fail accuracy validation. |
| Dropped payload | A collector reports one failed summary delivery. | Preserve the counter and surface the incomplete interval. |
| Query error | The backend cannot evaluate a declared supported summary query. | Surface the error and fail the scenario. |

## MVP requirements

One reproducible run must exercise all three aggregation shapes and every
claimed transmission mode. It must prove plan application, compare aligned
results with the exact baseline, measure freshness and latency, and retain the
full and delta observations needed to reproduce each verdict.

**Example MVP PromQL workload:**

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
count by (region) (count by (region, user_id) (active_user))
sum by (service) (increase(http_requests_total[5m]))
```

Replay one labeled request stream into both arms, evaluate these queries,
exercise raw, full, and supported delta modes, then compare aligned accuracy,
freshness, latency, and measured resource costs from that same run.

Claims for additional query operators, arbitrary windows, new summary
families, or archive behavior are outside the MVP until separately declared.
