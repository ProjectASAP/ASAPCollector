# ASAPCollector and ASAPQuery system overview

## TL;DR

ASAP is a summary-based metrics pipeline. A collector turns selected metric
streams into bounded summaries, transmits those summaries to ASAPQuery, and
the query service answers supported aggregate queries without scanning every
raw sample. A control plane selects the summary, its parameters, and the
transmission mode from the expected query workload. Metrics that cannot be
represented accurately by a supported summary remain outside this design.

**Status:** active

**MVP relationship:** in-scope. This document defines the system boundary and
the claims that the MVP end-to-end harness must validate.

## Scope

This document describes the logical data path and its design goals:

1. metric samples enter an edge collector;
2. the control plane assigns a collection plan;
3. the collector maintains the selected sketches or exact aggregates;
4. the collector transmits full state or supported deltas;
5. ASAPQuery reconstructs mergeable state and evaluates supported queries.

It does not specify wire schemas, control-plane algorithms, storage layouts,
or individual sketch implementations. Those concerns belong in the focused
documents listed in the design-doc index.

## Design goals

- **Functional correctness:** the collection plan is applied, transmitted
  state is ingestible, and supported queries return correctly aligned series.
- **Bounded accuracy:** approximate answers meet a declared error bound; exact
  aggregates retain exact semantics.
- **Freshness:** updates remain queryable while the current time window is
  open; freshness is measured from source timestamp to query visibility.
- **Performance:** supported summary-based queries avoid repeated scans of the
  raw stream.
- **Cost reduction:** edge and end-to-end resource use is lower than the
  declared full-raw-data baseline.

These are claims to measure, not assumptions. The MVP harness must use the
same generated samples, timestamps, labels, duration, and query workload for
the ASAP and exact arms.

## Logical architecture

```text
query workload --> ASAPPlanner --> ASAPQuery-backend control plane
                                      |                  |
                              collector plan      backend plan
                                      |                  |
source samples --> ASAPCollector --> summary state --> ASAPQuery data plane
                         |                                |
                         +-- exact/pass-through           +--> summary-based query

exact baseline: the same source samples --> Prometheus/VictoriaMetrics
                                             |
                                             +--> exact query result
```

The exact baseline is the ground truth for the MVP comparison. Both arms use
the same logical query interval and results are aligned by labels and
timestamps before accuracy is assessed.

## Collection and transmission model

The control plane may select one of three MVP transmission modes:

- **raw/pass-through:** preserve the selected stream for an exact path;
- **full sketch:** transmit a complete sketch state;
- **delta sketch:** transmit an incremental update for a sketch family that
  supports delta encoding.

The collector and the ASAPQuery-backend data plane must expose evidence that
they applied compatible portions of the issued plan. A control-plane response
alone is not evidence of execution. Full and delta transmission must have
equivalent query semantics within the configured accuracy bound.

Here, compatible means that both portions agree on the plan identity and
version, metric selection, retained labels, summary family and parameters,
window and grouping rules, transmission mode, and any delta base and sequence
rules. A mismatch fails plan application or ingestion.

## Query model

The MVP claims only cover query classes that have an explicit summary
realization:

- aggregation for each series over a selected time window;
- aggregation across label groups at a timestamp; and
- aggregation across both a time window and label groups.

A query is **supported** only when the checked-in MVP query catalog contains:

- its PromQL expression or normalized query pattern;
- the result semantics and aggregation shape;
- the selected summary family and parameters;
- the required collector and backend plan behavior;
- its accuracy, freshness, and latency SLAs; and
- whether exact fallback is permitted.

Absence from that catalog means unsupported, even if the backend can parse the
PromQL expression. Unsupported queries must be rejected explicitly or routed
to the configured exact fallback. They must not receive a plausible but
incorrect summary result.

The ASAPQuery-backend control plane selects an allowed fallback. The data plane
sends the query to the configured Prometheus or VictoriaMetrics exact backend
and marks the response provenance as `exact-fallback`. Such a response is
validated for functional correctness but is excluded from claims about
summary-query accuracy and acceleration. Missing or ambiguous provenance is a
failure.

## Summary families

The system may use the following logical summary families:

| Family | Intended result | Merge rule |
| --- | --- | --- |
| DDSketch | value quantiles | mergeable |
| KLL | value quantiles | mergeable full state |
| HLL | distinct cardinality | register-wise maximum |
| Count-Min | non-negative frequency estimates | additive |
| Count-Sketch | signed frequency estimates | additive |
| Sum/Count | exact scalar aggregates | additive |

The selected family and parameters are part of the collection plan. A family
is not a claim that every query operator is supported.

## Freshness and windows

ASAPQuery maintains mergeable state for the active time window and retains
completed windows according to its configured storage policy. The active
window can be queried before it closes. The MVP therefore measures freshness
as end-to-end visibility lag, rather than treating a window-close event as the
first valid result.

Window boundaries and timestamps must be consistent across collectors that
contribute to one grouped query. Clock alignment and delayed samples are
correctness conditions for the deployment and must be declared in the test
configuration.

## Boundaries and non-goals

This overview does not claim support for arbitrary PromQL, exact recovery of
raw samples from sketches, or lower cost at an undeclared workload scale.
Unsupported operators, archive/cold-store behavior, and future runtimes are
separate scopes. The MVP report must identify any such path as unsupported,
not silently include it in the results.

## Related design scopes

- [`physical-planning.md`](../control-plane/physical-planning.md): compilation
  of Planner output into separate Collector and Backend physical plans.
- [`collector-transmission-protocol.md`](../asapcollector/collector-transmission-protocol.md): summary families, aggregation
  shapes, windows, accuracy, freshness, and transmission semantics.
- [`future-summary-families-and-raw-archive.md`](future-summary-families-and-raw-archive.md):
  deferred summary families and raw archival compression.
- [`future-storage-and-compression.md`](../asapquery-backend/future-storage-and-compression.md):
  backend tiering, compaction, checkpoints, and storage codecs.

Each related document owns its detailed design; this document only states the
system-level relationship.
