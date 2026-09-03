# ASAPQuery-backend integration with ASAPPlanner

> Status: proposed
>
> MVP relation: required for query planning and control-plane decisions.

Developer guide: [Planner adapter and physical compiler](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/control-plane/physical-compiler.md).

Canonical stage-ownership and implementation contract:
[`post-asap-physical-compilation.md`](post-asap-physical-compilation.md).

## TL;DR

ASAPPlanner owns abstract primitive search and selection for a query workload,
including sketch algorithms, summary-window frameworks, and lifecycle
candidates. ASAPQuery-backend enumerates concrete runtime implementations for
those candidates, feeds their performance under the workload back into
Planner's cost model, and turns the selected abstract plan into deployable
collector and backend plans.

ASAPQuery-backend does not maintain a second design for parsing PromQL,
building Planner IR, mapping queries to summaries, reasoning about accuracy,
sharing work across queries, or ranking logical candidates. Those designs are
owned by [ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner).

```text
PromQL workload
      |
      v
ASAPPlanner
abstract candidates and selection
      |                              ^
      | candidates                   | implementation cost evidence
      v                              |
ASAPQuery-backend control plane -----+
physical implementation, compilation, and activation
      |
      +-------------------------+
      |                         |
      v                         v
CollectorPlan               BackendPlan
ASAPCollector               ASAPQuery data plane
```

## Ownership boundary

### ASAPPlanner

Planner is the source of truth for:

- query parsing and semantic IR;
- logical exact and summary-based alternatives;
- summary family, parameters, grouping, and readout semantics;
- abstract summary-window frameworks, including tumbling, sliding, and
  exponential-histogram alternatives;
- summary-maintenance lifecycle candidates and legality;
- accuracy constraints and logical result guarantees;
- workload-wide reuse and common subexpressions;
- logical cost comparison and candidate selection; and
- logical rejection and explanation.

See ASAPPlanner's own design documents for those semantics. Their types and
rules must be consumed from the pinned Planner revision, not copied into this
repository.

### ASAPQuery-backend control plane

The control plane owns:

- supplying workload context, runtime statistics, and executor capabilities;
- enumerating feasible concrete runtime implementations and configurations for
  every Planner candidate;
- supplying each implementation's measured or estimated resource behavior
  under the same `DataWorkload` to Planner's cost model;
- invoking the pinned Planner revision and consuming its selected abstract
  workload plan;
- choosing collector/backend placement, concrete panes and sizing,
  transmission mode, and storage routes;
- compiling matching CollectorPlan and BackendPlan artifacts;
- staging, activating, retiring, and rolling back plan versions; and
- exposing planning and activation status to operators.

These decisions are described in [physical planning](physical-planning.md).

### ASAPQuery data plane

The data plane owns:

- installing [BackendPlan](backend-plan.md);
- validating, ingesting, storing, and merging summary state;
- applying the selected readout and remaining backend-side operators;
- enforcing readiness, freshness, and plan identity; and
- executing an explicit exact fallback when the selected plan requires one.

### ASAPCollector

ASAPCollector owns validation and execution of CollectorPlan: observing the
selected metrics, maintaining the requested state, transmitting raw data or
full/delta summaries, and reporting which plan is actually active. Its
interface is documented in the
[ASAPCollector repository](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md).

## Integration contracts

### Workload request

ASAPQuery-backend passes the complete workload to Planner so that sharing and
global selection remain possible. A request includes the query expressions,
evaluation shape, requested accuracy, and relevant schema or cost context.
Protocol-specific scheduling and alert state remain outside Planner.

For example, two dashboard queries that read the same five-minute latency
distribution are planned together. ASAPQuery-backend must not reduce them to
two independent `(metric, sketch)` requests before Planner can identify shared
state.

### Candidate and selected abstract plans

Planner exposes abstract candidates through the pinned API. ASAPQuery-backend
expands each candidate into legal physical implementations and supplies their
cost evidence under one common workload horizon. Planner then returns a
selected post-ASAP workload plan. Shared logical nodes remain shared. Exact
fallback is an explicit part of that plan; absence of a supported summary is
not permission for the backend to invent one.

For example, Planner may select one DDSketch producer shared by:

```promql
quantile_over_time(0.50, request_duration_seconds[5m])
quantile_over_time(0.95, request_duration_seconds[5m])
```

The backend consumes the shared producer and two readouts. It must not select
DDSketch again from the query strings.

Likewise, Planner may choose incremental maintenance with a sliding-window
framework. The backend chooses a concrete pane width, state layout, placement,
and executor implementation for that combination. Incremental maintenance and
window framework are orthogonal: the former describes update behavior, while
the latter describes temporal state organization.

### Physical plans

One physical compile produces CollectorPlan and BackendPlan from the same
selected logical plan. Both artifacts carry the same plan version and
materialization identities. Independent compilation is invalid because it can
make the collector and data plane disagree about family, parameters, grouping,
windows, or state representation.

### Runtime evidence

Delivery acknowledgement is not activation evidence. Query routing changes
only after the backend has installed BackendPlan and each required collector
has reported that it applied the matching CollectorPlan. Emitted state must
also carry identities that the backend can validate.

## Planner version policy

All Planner crates used by one backend build are pinned to one immutable
revision. The revision is recorded in compiled plans and diagnostic output.

An upstream Planner change is adopted only after the backend has deliberately
handled any new IR variant, guarantee, capability, or cost-model requirement.
Unknown variants fail as unsupported; the backend must not pre-copy proposed
Planner types or infer semantics from explain/viewer output.

Open Planner pull requests are compatibility inputs, not backend design. Their
details stay in Planner and in the implementation change that updates the pin,
instead of becoming a second long-lived specification here.

## Failure behavior

Planning or activation fails closed when:

- the selected logical operation is unsupported by its assigned executor;
- collector and backend plans disagree on any materialization contract;
- a required accuracy guarantee is missing or insufficient;
- full/delta state semantics are incompatible;
- a plan is stale, expired, partially applied, or from another run; or
- exact fallback is required but unavailable.

The system must not silently choose another summary, loosen an accuracy
requirement, remove grouping, or serve state from a different plan.

## MVP requirements

The integration is MVP-ready when one end-to-end run demonstrates that:

- the submitted PromQL workload reaches Planner as one workload;
- the selected logical plan is preserved through physical compilation;
- matching collector and backend plans are captured as artifacts;
- ASAPCollector reports semantic application of the collector plan;
- the data plane serves supported summary-backed queries from BackendPlan;
- unsupported queries are rejected or use the selected exact fallback; and
- missing, stale, or incompatible plan evidence makes the run fail.

## Non-goals

This document does not define Planner IR, query-to-summary mappings, accuracy
algebra, summary algorithms, collector configuration fields, state byte
encoding, or query-engine implementation details.
