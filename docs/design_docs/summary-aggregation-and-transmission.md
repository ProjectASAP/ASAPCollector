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

**Example:** For each `{service, instance}` series, answer the p95 of
`request_duration_seconds` over the last completed five-minute window. Each
series contributes to its own quantile summary; no cross-series merge is
required.

### Aggregation across label groups at a timestamp

Series are grouped by selected labels and their compatible summaries are
merged. The grouping dimensions preserved by collection determine which
queries can be answered without raw data.

**Example:** At `12:00:00`, answer the approximate distinct number of active
`user_id` values grouped by `region`. HLL summaries from series with the same
`region` are merged at that timestamp.

### Aggregation across labels and a time window

Summaries are merged across both selected series and window state. This shape
requires compatible grouping, aligned windows, and merge semantics that remain
valid across every contributing collector.

**Example:** Answer the p99 of `request_duration_seconds` over five minutes,
grouped by `service`, across all collectors. The backend merges compatible
per-service quantile summaries from the same five-minute window.

The collection plan declares the aggregation shape. A query whose grouping or
window cannot be reconstructed from the collected state is unsupported.

## Summary families

| Family | Result | Accuracy model | Merge model | Delta mode |
| --- | --- | --- | --- | --- |
| Sum/Count | exact scalar aggregate | numeric tolerance | addition | additive delta |
| DDSketch | quantiles | relative value error | compatible-state merge | supported |
| KLL | quantiles | rank error | compatible-state merge | full state for MVP |
| HLL | distinct cardinality | cardinality error | register-wise maximum | changed registers |
| Count-Min | non-negative frequency | additive overestimate bound | cell-wise addition | sparse cell delta |
| Count-Sketch | signed frequency | probabilistic point-error bound | cell-wise addition | sparse cell delta |

Every family is parameterized. Two states may be merged only when their
family, parameters, grouping, window identity, and plan semantics are
compatible.

### Query examples by summary family

| Family | Example query or scenario |
| --- | --- |
| Sum/Count | Total requests per service during a one-minute window. |
| DDSketch | p99 request latency per region with a declared relative-value error. |
| KLL | Median payload size per endpoint with a declared rank-error bound. |
| HLL | Approximate distinct users per tenant during an hour. |
| Count-Min | Estimated request count for a named URL and candidate heavy hitters. |
| Count-Sketch | Estimated signed change in event frequency between two populations. |

## Window model

The collector assigns observations to timestamp-defined windows. Window
identity is part of summary identity; states from different windows are never
merged accidentally.

### Open window

The current window accepts updates and may be queried before it closes. Its
answer represents all updates visible at query time. Freshness is therefore
measured from an observation's source timestamp to the first query result that
contains it.

**Example:** A latency observation timestamped `12:00:20` belongs to the open
`12:00–12:01` window. A query at `12:00:23` may return the partial p95 that
includes that observation; the measured freshness lag is three seconds.

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

**Example:** Collector A labels a summary as window `12:00–12:05`, while
Collector B labels its state `12:01–12:06`. The backend must not merge them to
answer one five-minute regional p99 query.

## Transmission modes

### Raw/pass-through

The collector forwards selected observations without summarizing them. This is
used when exact values are required or no supported summary can answer the
declared workload.

**Example:** A point query requesting the exact value of
`queue_depth{instance="worker-7"}` at a specific timestamp selects raw
pass-through because a window summary cannot reproduce that observation.

### Full summary

The collector sends a complete summary state for a specific identity and
window. The receiver can reconstruct the represented state without relying on
an earlier payload.

**Example:** At the end of a five-minute window, the collector sends one
complete KLL summary for payload size. The backend can answer the window's
median query without any earlier KLL payload.

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

- exact families match within numeric tolerance;
- approximate families remain within the same configured accuracy SLA;
- no series, labels, or timestamps are lost or invented; and
- duplicate or missing deltas cannot produce an apparently valid result.

Periodic or requested full-state synchronization provides a recovery point
when delta continuity cannot be established.

**Equivalence example:** One collector sends a complete Count-Sketch after
10,000 observations; another sends a compatible base plus all deltas for the
same observations. Their point-frequency queries must agree within the same
configured Count-Sketch error bound.

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

Each supported query declares:

- its exact or approximate semantics;
- the summary family and parameters;
- the comparison metric;
- the acceptable error bound; and
- the required fraction of results within that bound.

Results are aligned by labels and timestamps before comparison with the exact
baseline. Missing and extra series are errors, not zero-valued answers.

Sampling error, summary error, delayed-delta error, and merge error must fit
within one declared end-to-end budget. Internal error budgets may be divided
among mechanisms, but the user-facing SLA applies to the final query result.

**Example:** A regional p99 latency query declares at most 1% relative error
for at least 99% of answers. The result series are aligned with the exact
baseline by `{region}` and timestamp before that SLA is evaluated.

## Freshness contract

Freshness is the time from source observation timestamp to the first successful
query that includes the corresponding observation or window contribution. The
MVP reports freshness separately by aggregation class and transmission mode.

An answer from an earlier run, earlier plan, or earlier window is stale even if
its numeric value appears plausible. Run identity, plan identity, and window
identity are therefore part of validation.

**Example:** An observation timestamped `12:00:20` first appears in a valid
query at `12:00:24.5`, producing a 4.5-second freshness lag. A cached answer
from a previous run does not satisfy this measurement.

## Cost model

Summary construction trades edge CPU and memory for lower network, backend
ingest, storage, and query costs. Delta transmission further trades state and
recovery complexity for lower repeated network cost.

No transmission mode is universally cheaper. The fair comparison measures the
same observation stream and reports CPU, memory, bytes, storage, and query work
separately before applying fixed cost weights.

**Example:** Compare a workload that transmits every 100 ms raw observation
with one that emits five-second summary deltas. Measure collector CPU and
memory, transmitted bytes, backend ingest and query CPU, and stored bytes for
both arms before applying the checked-in cost weights.

## Failure behavior

| Failure scenario | Example | Required behavior |
| --- | --- | --- |
| Unsupported query | Exact sample-level vector matching cannot be realized from the selected summaries. | Reject it or route it to the configured exact path. |
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

**Example MVP scenario:** Replay one labeled request stream into both arms;
evaluate per-instance five-minute p95, per-region distinct users at a
timestamp, and per-service five-minute p99; exercise raw, full, and supported
delta modes; then compare aligned accuracy, freshness, latency, and measured
resource costs from that same run.

Claims for additional query operators, arbitrary windows, new summary
families, or archive behavior are outside the MVP until separately declared.
