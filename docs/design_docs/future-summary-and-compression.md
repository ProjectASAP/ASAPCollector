# Future summary families and compression

## TL;DR

This document records design directions that are intentionally outside the
current MVP: multivariate correlation summaries and compression for raw or
archival telemetry paths. They must not be included in MVP correctness,
performance, or cost claims until promoted through separate acceptance
criteria.

**Status:** dormant

**MVP relationship:** future.

## Scope

The active MVP focuses on summaries required by its declared query workload
and compares them with a full-raw-data exact baseline. This document preserves
two possible extensions without treating them as active system behavior.

## Multivariate correlation summaries

### Motivation

Independent per-metric summaries cannot detect a change in the relationship
among several metrics when each metric remains individually plausible.
Multivariate summaries could support questions such as whether latency, load,
memory, and error rate have developed an unusual joint pattern.

**PromQL input workload:**

```promql
sum by (service) (rate(http_requests_total{service="checkout"}[10m]))
avg by (service) (rate(process_cpu_seconds_total{service="checkout"}[10m]))
avg by (service) (process_resident_memory_bytes{service="checkout"})
quantile by (service) (0.99, request_duration_seconds{service="checkout"})
sum by (service) (rate(http_request_errors_total{service="checkout"}[10m]))
```

The future query asks whether the relationship among these result series has
deviated from its reference behavior. Correlation drift is not standard
PromQL, so this document does not invent a PromQL function for the final
score; it treats the expressions above as the declared input workload.

### Proposed summary

For a selected vector of aligned metrics, each edge contributes mergeable
first- and second-order statistics over a declared window. A central service
combines those summaries and derives a covariance or low-rank representation
used for correlation-change scoring.

**Example scenario:** Each collector produces one aligned summary for the
vector `(request_rate, cpu, memory, p99_latency, error_rate)` grouped by
`service` over a ten-minute window. The backend merges summaries for
`service="checkout"` and returns a correlation-drift score.

The summary identity must include the metric vector, ordering, normalization,
window, grouping, and plan identity. States with different identities are not
mergeable.

### Design constraints

- Metric alignment and missing-value semantics must be explicit.
- Dimensionality determines whether exact covariance state is practical.
- Projection or low-rank approximation introduces a separate accuracy budget.
- Cross-collector window alignment is required.
- The query and alert semantics must be declared before selecting the summary.

| Constraint scenario | Example |
| --- | --- |
| Metric alignment | CPU is observed every 10 seconds but latency every 30 seconds; the plan declares how their vector timestamps align. |
| Missing values | One collector has no error-rate observation for a vector timestamp; the plan declares whether to omit or impute it. |
| High dimensionality | A tenant requests correlation across 2,000 metrics; a projected summary needs its own accuracy bound. |
| Window alignment | Two collectors contribute to the same service but use different ten-minute boundaries; their states cannot be merged. |
| Query semantics | The plan distinguishes returning a drift score from emitting an alert above a threshold. |

### Open questions

- Which multivariate query is important enough to justify collection cost?
- How is the metric vector selected and versioned?
- What accuracy metric is understandable to users?
- How are sparse, delayed, or partially observed vectors handled?
- When does a raw exact fallback remain necessary?

**Exact fallback PromQL example:**

```promql
request_duration_seconds{service="checkout", instance="worker-7"}
```

The query-range API retrieves the exact source series for the incident. A
covariance summary cannot reconstruct those observations, so the investigation
requires a separately configured exact path.

This direction becomes active only after an end-to-end workload, accuracy SLA,
and exact comparison method are checked in.

## Compression for raw and archival paths

### Motivation

Some observations may remain raw because their queries require exact values or
because no supported summary has been selected. Lossless compression may lower
the transmission or storage cost of those paths without changing query
semantics.

**PromQL range-query example:**

```promql
queue_depth{instance="worker-7"}
```

The query-range API evaluates this expression over the incident's one-hour
`start`, `end`, and one-second `step`. The stream remains raw, while lossless
compression may reduce its transmission and archive cost.

### Design boundary

Compression and summarization solve different problems:

- a summary discards detail under a declared semantic and accuracy contract;
- lossless compression preserves the original observations and changes only
  their representation.

The control plane must select them independently. A compressed raw stream is
still part of the exact path and must not be counted as a summary-backed
result.

**Example:** Compressing 3,600 exact queue-depth observations does not turn
them into a summary. After decoding, the exact query must see the same 3,600
values, timestamps, and labels.

### Required properties

- Round-trip decoding preserves values, timestamps, labels, and ordering
  semantics required by the exact consumer.
- Framing permits explicit corruption and truncation detection.
- Recovery boundaries limit the effect of a lost or damaged segment.
- Resource accounting includes compression and decompression CPU and memory.
- Cost comparisons use measured bytes rather than projected compression ratios.

| Property scenario | Example |
| --- | --- |
| Round trip | A sequence containing repeated values, NaN, and timestamp gaps decodes with identical query-visible semantics. |
| Corruption detection | A damaged frame is rejected instead of yielding plausible numeric values. |
| Recovery boundary | Losing one frame does not prevent decoding every later independent frame. |
| Resource accounting | A 40% byte reduction is reported together with compression and decompression CPU. |
| Measured comparison | The experiment uses transmitted byte counters rather than an assumed compression ratio. |

### Open questions

- Which telemetry types and value distributions justify compression?
- Where are compression boundaries placed relative to batching and transport?
- How does random access affect archival layout?
- Which recovery and compatibility guarantees are required?

**Archive PromQL example:**

```promql
queue_depth{instance="worker-7"}
```

If a query-range request evaluates this expression over only the final five
minutes of a one-hour archive, the framing design determines whether it can
decode that range directly or must process the preceding 55 minutes.

Compression is excluded from the MVP unless introduced as a separately scoped
exact-baseline or archive experiment.

## Promotion criteria

A future design moves to active status only when it has:

1. a declared query or storage requirement;
2. an end-to-end semantic contract;
3. predeclared correctness and performance SLAs;
4. a reproducible baseline comparison; and
5. explicit inclusion in the MVP or a separately named experiment.

**Promotion example:** Multivariate correlation becomes active only after a
checked-in workload defines its metric vector, exact comparison, drift-score
accuracy SLA, freshness SLA, and resource-cost baseline in one reproducible
experiment.
