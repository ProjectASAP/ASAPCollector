# Future summary families and compression

## TL;DR

This document records design directions outside the current summary MVP:
multivariate summaries and additional compression for raw, summary, and archive
paths. The current deployment already uses transport gzip, summary full/delta
encodings, and a Gorilla-based raw archive path; those mechanisms are the
baseline, not proof that the future representations below are complete.

**Status:** mixed: existing transport/archive mechanisms plus future designs

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

## Stored-series identity and representation

Compression never changes what SID identifies. Raw, exact-aggregate, and
summary series use distinct materialization kinds even when they originate from
the same metric and labels:

```text
source metric
  +-- raw materialization SID               -> exact archive encoding
  +-- sketch materialization SID            -> full/delta sketch encoding
  +-- exact-aggregation materialization SID -> accumulator encoding
```

For example, DDSketch is one sketch materialization and Sum is one
exact-aggregation materialization. The categories above also cover the other
supported sketch families and exact aggregation operators.

A codec or checkpoint-policy change that remains semantically compatible may
preserve SID and declare a new representation version per frame. A lossy change
or different accuracy contract creates a different materialization kind and
therefore a different SID. See [stored-series identity](stored-series-identity.md).

## Placement and framing

The plan must declare compression placement because each boundary has different
trade-offs:

| Boundary | Candidate representation | Required property |
| --- | --- | --- |
| In-memory accumulator | Family-native state | Fast updates and bounded memory |
| Collector → Backend | Full/delta plus transport compression | Recoverable frames and observable wire bytes |
| Collector → raw archive | Lossless raw blocks | Exact value/timestamp/label recovery |
| Backend durable summary tier | Immutable summary parts | Random access by SID and time |

Frames need materialization/SID evidence, logical window, producer, encoding
version, uncompressed length, checksum, and—where applicable—base checkpoint
and sequence. Transport gzip is outside this logical frame: decompressing the
transport must still leave one independently validated ASAP payload.

## Checkpoint and delta policy

Delta transmission trades bandwidth for dependency length. A future policy
must bound that dependency:

- emit a full checkpoint at a configured time or delta-count interval;
- retain the base until every dependent delta is acknowledged or expired;
- never advance acknowledgement across a missing sequence;
- fall back to a full frame after Backend reports unknown SID/base; and
- measure reconstruction CPU and bytes saved for the same workload.

The checkpoint interval is a physical-plan decision constrained by freshness,
failure recovery, and bandwidth. It does not change summary mathematics.

## Raw archive compression

Raw compression must preserve the original Prometheus-visible series. A block
should group one raw stored-series SID over a bounded time interval and encode:

```text
header: SID/materialization identity, labels or dictionary reference,
        min/max timestamp, sample count, codec version, checksum
body:   timestamp stream + value stream
```

Timestamp delta-of-delta and Gorilla-XOR values are candidates for regular
numeric series. Sparse or irregular data may need another codec. The encoder
must choose by measured size and CPU, not by assuming one codec is universally
better. Every block has an independent restart point so a range query need not
decode the entire archive prefix.

## Summary payload compression

Summary families expose different redundancy:

| Family | Candidate optimization | Compatibility condition |
| --- | --- | --- |
| DDSketch | Sparse changed bins, full checkpoints | Same mapping/accuracy |
| KLL | Family-native compact serialization | Same `k` and format version |
| HLL | Changed registers or sparse/dense switch | Same precision/hash contract |
| Count-Min/Count-Sketch | Sparse changed cells, optional heap frame | Same dimensions/hash contract |
| Exact scalar | Varint/delta where lossless | Same operator/window semantics |

General gzip or zstd may wrap payload blocks, but family-aware encoding must be
evaluated separately so benchmark results explain where savings came from.

## Failure and recovery

| Failure | Required Collector behavior |
| --- | --- |
| Backend rejects SID | Evict assignment and resend identity evidence |
| Backend lacks delta base | Send a full checkpoint; do not continue an unverifiable chain |
| Corrupt local/archive frame | Reject the frame and preserve later independent recovery boundaries |
| Plan changes codec/family | Drain old materialization; start the new compatible SID/version explicitly |
| Export retry duplicates a frame | Producer identity/sequence makes application idempotent |
| Resource pressure | Apply declared backpressure/drop policy and expose counters; do not alter accuracy silently |

## Cost evidence

For every representation, report at least:

- source samples and logical uncompressed bytes;
- family payload bytes before transport compression;
- bytes on wire and bytes in archive;
- encoding/decoding CPU and allocation;
- checkpoint frequency and recovery bytes;
- p50/p95/p99 export and query latency; and
- loss/corruption recovery outcome.

Dictionary savings, summary reduction, delta reduction, and transport
compression must be reported as separate stages. This prevents the SID label
dictionary from being credited to sketch compression.

## Rollout sequence

1. Freeze a cross-language frame/version contract and golden vectors.
2. Add decoder support before enabling new writers.
3. Shadow-encode and compare decoded semantics and measured cost.
4. Enable one family/workload with a full-frame kill switch.
5. Exercise missing-base, unknown-SID, corruption, and rollback paths.
6. Expand only after end-to-end accuracy, freshness, and cost gates pass.

Old readers and stored frames remain supported through the declared rollback
window. A deployment must not require rewriting all archived raw data merely to
roll back a Collector release.

## Promotion criteria

A future design moves to active status only when it has:

1. a declared query or storage requirement;
2. an end-to-end semantic contract;
3. predeclared correctness and performance SLAs;
4. a reproducible baseline comparison; and
5. explicit inclusion in the MVP or a separately named experiment.

It must additionally define wire/format versioning, SID/materialization
compatibility, checkpoint/recovery behavior, and a rollback path.

**Promotion example:** Multivariate correlation becomes active only after a
checked-in workload defines its metric vector, exact comparison, drift-score
accuracy SLA, freshness SLA, and resource-cost baseline in one reproducible
experiment.
