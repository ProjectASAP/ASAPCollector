# Plan-aware query execution

> Status: proposed
>
> MVP relation: required for every summary-backed query and exact fallback.

Developer guides:
[BackendPlan runtime](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/query-engine/backend-plan-runtime.md),
[OTLP summary ingestion](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/ingest-engine/ingest-engine.md), and
[query routing/readout](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/query-engine/query-engine.md).

## TL;DR

The data plane accepts only state compatible with its active BackendPlan. At
query time it matches the PromQL request to a planned readout, checks coverage
and freshness, reads the matching materialization, and returns a
Prometheus-compatible result. A query that cannot be served safely is rejected
or sent to the exact fallback selected by the plan.

## Data flow

```text
ASAPCollector -- OTLP summary state --> ingest validation --> SummaryStore
                                                        |
PromQL request --> protocol adapter --> planned routing + readiness
                                                        |
                              summary readout <----------+
                                      |
                         Prometheus-compatible response

Unsupported/planned-exact query --> exact fallback backend
```

The data plane never infers a summary family from a metric name or query text.
It uses the materialization and readout declared by BackendPlan.

## Ingestion contract

Before accepting a payload, the data plane validates:

- plan and plan-version identity;
- materialization, producer, and tenant identity;
- summary family, parameters, grouping, window, and representation;
- full/delta sequence and checkpoint requirements; and
- lifecycle and schema compatibility.

Unknown, expired, reordered, or incompatible state is rejected and surfaced.
Receiving bytes is not evidence that the corresponding window is queryable.

## Query routing

Routing has three outcomes:

1. **Summary readout** when BackendPlan contains a compatible route and the
   required windows are fresh and complete.
2. **Exact fallback** when BackendPlan explicitly routes the query shape to an
   exact backend.
3. **Explicit failure** when neither route is valid.

A store miss, stale window, or incompatible payload must not be converted into
an empty or plausible approximate result.

## Inputs and ownership

The query engine has four inputs; only one is supplied by the request caller.

| Input | Producer | What it contributes |
| --- | --- | --- |
| Prometheus-compatible instant/range request | HTTP/API adapter | Query expression, evaluation time/range, step, tenant/auth context |
| Active BackendPlan view | ASAPQuery physical compiler through plan publication | Canonical routes, materialization descriptors, readouts, guarantees, storage tier and fallback policy |
| SID/materialization and window indexes | Summary store/ingest engine | Concrete materialized series, labels, lifecycle, coverage, frame lineage, and payload locations |
| Exact/archive engine | Configured backend adapter | Explicit fallback execution when the plan permits it |

ASAPPlanner runs when the workload is planned, not on every incoming query. It
selects the logical summary/readout and guarantee. The physical compiler turns
that selection into BackendPlan routes. At request time the query engine
executes an installed route; it must not ask Planner to search again or choose
a new sketch because stored coverage is missing.

## Execution workflow

```text
PromQL HTTP request
  -> authenticate / tenant scope
  -> parse and canonicalize query shape
  -> active BackendPlan route lookup
       -> exact route ------------------------------> exact/archive engine
       -> summary route
            -> materialization descriptor
            -> compatible SID instance lookup
            -> lifecycle + guarantee + freshness checks
            -> window/coverage index lookup
            -> memory/durable payload read
            -> full/delta reconstruction
            -> shard/pane merge and planned readout
            -> remaining backend operators
  -> Prometheus labels/timestamps/result encoding + provenance/accuracy
```

The steps are:

1. **Establish request context.** The protocol adapter validates tenant,
   authorization, instant/range parameters, step, and timeout.
2. **Canonicalize only for route lookup.** Parsing produces the stable query
   shape/query ID expected by BackendPlan. This is not candidate planning.
3. **Select the active plan view.** Evaluation time must fall within the
   plan/schema timeline. Stale, premature, or conflicting versions fail.
4. **Resolve the planned route.** The route names an exact path or a
   materialization fingerprint, readout, grouping/window composition,
   guarantee, storage preference, and permitted fallback.
5. **Find compatible series.** Using the materialization catalog and SID
   instance index, select only active/retained SIDs whose canonical metric and
   concrete retained labels satisfy the request. Metric-name matching alone is
   insufficient.
6. **Check semantic compatibility.** Verify aggregation kind/parameters,
   capability, requested grouping, result guarantee, producer completeness,
   and state schema against the materialization descriptor.
7. **Check temporal readiness.** Ask the store's coverage/watermark index for
   the required panes. Distinguish missing, stale, gapped delta lineage,
   backfill-in-progress, and complete coverage.
8. **Read and reconstruct.** The store locates mutable/sealed epochs and
   durable parts by `(SID, range)`, retrieves carry-in checkpoints where
   necessary, and reconstructs ordered full/delta state.
9. **Execute the planned algebra.** Merge compatible shards/panes, apply the
   declared sketch or exact-aggregate readout, then execute remaining backend
   operators and label roll-ups represented by the route.
10. **Return or follow explicit fallback.** Encode Prometheus-compatible
    results with plan/materialization provenance, freshness, and accuracy. A
    failed summary route uses exact/archive only when BackendPlan authorizes
    that failure class; otherwise return an explicit error.

## Component interactions

| Component | Interaction with query engine |
| --- | --- |
| ASAPPlanner | Supplies the selected logical plan/guarantee upstream; never serves as a request-time optimizer |
| ASAPQuery control plane | Compiles and publishes immutable route/materialization views and activation timelines |
| Collector | Produces state according to the matching CollectorPlan; has no direct query-time control |
| Ingest/precompute engine | Validates producers and frames, resolves SIDs, registers metadata, updates coverage |
| Summary store engine | Owns semantic instance lookup plus time/coverage/physical indexes and returns typed state/miss reasons |
| Query engine | Owns request classification, route execution, readout/operator evaluation, fallback decision, and response |
| Exact/archive adapter | Executes only the explicit fallback/exact route and returns its errors unchanged |

The storage engine exposes facts and typed lookup outcomes; the query engine
decides how the installed route uses them. Conversely, the query engine cannot
register metadata, repair delta gaps, or mark incomplete windows complete.

## Supported aggregation shapes

The MVP exercises these shapes without defining Planner's query-to-summary
rules here:

- within one series over time:

  ```promql
  quantile_over_time(0.95, request_duration_seconds[5m])
  ```

- across label groups at an evaluation timestamp:

  ```promql
  sum by (region) (http_requests_total)
  ```

- across both a time range and label groups:

  ```promql
  sum by (region) (rate(http_requests_total[5m]))
  ```

Whether a particular expression is exact, summary-backed, or unsupported is
the selected plan's decision. The data plane only executes that decision.

## Readiness and freshness

A readout is ready only when all materializations required by its route:

- belong to the active plan version;
- cover the requested logical interval;
- satisfy watermark and allowed-lateness policy;
- have no unresolved delta gap; and
- meet any declared source-completeness requirement.

For example, a query at `12:05` over `[5m]` cannot reuse complete panes from an
earlier run merely because their labels match. The plan identity and logical
window must also match.

## Result semantics

The response preserves Prometheus labels, timestamps, result type, and error
behavior. Summary error guarantees come from the selected Planner result and
are carried by BackendPlan; the data plane neither tightens nor loosens them.

When several physical shards contribute to one result, they may be merged only
if their materialization contracts match and the chosen summary supports the
declared merge.

## Exact fallback

Fallback is a correctness path, not a silent catch-all. BackendPlan identifies
the backend and query scope eligible for fallback. Transport failures and exact
query errors remain visible to the caller.

Examples that may require exact fallback include an unsupported PromQL
operator, a request outside retained summary coverage, or a query whose exact
accuracy requirement has no compatible maintained state.

## Non-goals

This document does not define PromQL parsing, summary selection, Planner IR,
summary algorithms, state byte encoding, storage-engine implementation, or
protocol-specific server code.
