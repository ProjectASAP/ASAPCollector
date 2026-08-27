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

### Proposed summary

For a selected vector of aligned metrics, each edge contributes mergeable
first- and second-order statistics over a declared window. A central service
combines those summaries and derives a covariance or low-rank representation
used for correlation-change scoring.

The summary identity must include the metric vector, ordering, normalization,
window, grouping, and plan identity. States with different identities are not
mergeable.

### Design constraints

- Metric alignment and missing-value semantics must be explicit.
- Dimensionality determines whether exact covariance state is practical.
- Projection or low-rank approximation introduces a separate accuracy budget.
- Cross-collector window alignment is required.
- The query and alert semantics must be declared before selecting the summary.

### Open questions

- Which multivariate query is important enough to justify collection cost?
- How is the metric vector selected and versioned?
- What accuracy metric is understandable to users?
- How are sparse, delayed, or partially observed vectors handled?
- When does a raw exact fallback remain necessary?

This direction becomes active only after an end-to-end workload, accuracy SLA,
and exact comparison method are checked in.

## Compression for raw and archival paths

### Motivation

Some observations may remain raw because their queries require exact values or
because no supported summary has been selected. Lossless compression may lower
the transmission or storage cost of those paths without changing query
semantics.

### Design boundary

Compression and summarization solve different problems:

- a summary discards detail under a declared semantic and accuracy contract;
- lossless compression preserves the original observations and changes only
  their representation.

The control plane must select them independently. A compressed raw stream is
still part of the exact path and must not be counted as a summary-backed
result.

### Required properties

- Round-trip decoding preserves values, timestamps, labels, and ordering
  semantics required by the exact consumer.
- Framing permits explicit corruption and truncation detection.
- Recovery boundaries limit the effect of a lost or damaged segment.
- Resource accounting includes compression and decompression CPU and memory.
- Cost comparisons use measured bytes rather than projected compression ratios.

### Open questions

- Which telemetry types and value distributions justify compression?
- Where are compression boundaries placed relative to batching and transport?
- How does random access affect archival layout?
- Which recovery and compatibility guarantees are required?

Compression is excluded from the MVP unless introduced as a separately scoped
exact-baseline or archive experiment.

## Promotion criteria

A future design moves to active status only when it has:

1. a declared query or storage requirement;
2. an end-to-end semantic contract;
3. predeclared correctness and performance SLAs;
4. a reproducible baseline comparison; and
5. explicit inclusion in the MVP or a separately named experiment.
