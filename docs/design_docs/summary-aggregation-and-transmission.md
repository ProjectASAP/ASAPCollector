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

### Aggregation across label groups at a timestamp

Series are grouped by selected labels and their compatible summaries are
merged. The grouping dimensions preserved by collection determine which
queries can be answered without raw data.

### Aggregation across labels and a time window

Summaries are merged across both selected series and window state. This shape
requires compatible grouping, aligned windows, and merge semantics that remain
valid across every contributing collector.

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

## Window model

The collector assigns observations to timestamp-defined windows. Window
identity is part of summary identity; states from different windows are never
merged accidentally.

### Open window

The current window accepts updates and may be queried before it closes. Its
answer represents all updates visible at query time. Freshness is therefore
measured from an observation's source timestamp to the first query result that
contains it.

### Completed window

A completed window is immutable. Late observations are handled by an explicit
lateness policy; they are never silently inserted into an unrelated active
window.

### Cross-collector alignment

Grouped queries may combine summaries from multiple collectors. Those
collectors must agree on window boundaries, timestamp interpretation, summary
parameters, and plan identity. Misaligned state is rejected or kept separate.

## Transmission modes

### Raw/pass-through

The collector forwards selected observations without summarizing them. This is
used when exact values are required or no supported summary can answer the
declared workload.

### Full summary

The collector sends a complete summary state for a specific identity and
window. The receiver can reconstruct the represented state without relying on
an earlier payload.

### Delta summary

The collector sends only the change since an identified base state. A delta is
valid only when the family defines a safe update operation and the receiver has
the required base or ordering context.

The ASAPQuery-backend data plane applies the backend portion of the active
plan when ingesting either form. Delta transmission reduces repeated state but introduces lifecycle
requirements: base identity, duplicate handling, loss recovery, ordering
rules, and resynchronization must all be explicit.

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

## Delta behavior by family

Additive summaries transmit changed scalar or cell values that are added to
the receiver's compatible base. HLL transmits register increases and merges by
maximum. KLL uses full-state transmission in the MVP because a compact,
order-independent delta contract is not claimed.

A family-specific threshold may delay transmission of small changes to reduce
communication. Such a threshold consumes part of the declared error and
freshness budgets; it cannot be selected independently of those SLAs.

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

## Freshness contract

Freshness is the time from source observation timestamp to the first successful
query that includes the corresponding observation or window contribution. The
MVP reports freshness separately by aggregation class and transmission mode.

An answer from an earlier run, earlier plan, or earlier window is stale even if
its numeric value appears plausible. Run identity, plan identity, and window
identity are therefore part of validation.

## Cost model

Summary construction trades edge CPU and memory for lower network, backend
ingest, storage, and query costs. Delta transmission further trades state and
recovery complexity for lower repeated network cost.

No transmission mode is universally cheaper. The fair comparison measures the
same observation stream and reports CPU, memory, bytes, storage, and query work
separately before applying fixed cost weights.

## Failure behavior

- Unsupported queries are rejected or routed to the configured exact path.
- Incompatible summaries are not merged.
- Missing bases or broken delta sequences trigger resynchronization or failure.
- Empty, stale, or incomparable results fail validation.
- Dropped observations, payloads, and query errors remain visible.

## MVP requirements

One reproducible run must exercise all three aggregation shapes and every
claimed transmission mode. It must prove plan application, compare aligned
results with the exact baseline, measure freshness and latency, and retain the
full and delta observations needed to reproduce each verdict.

Claims for additional query operators, arbitrary windows, new summary
families, or archive behavior are outside the MVP until separately declared.
