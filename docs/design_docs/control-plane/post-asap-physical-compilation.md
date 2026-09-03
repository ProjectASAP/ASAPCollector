# From a Post-ASAP DAG to distributed execution plans

> Status: proposed architecture and MVP implementation contract
>
> Scope: ownership across ASAPPlanner, the ASAPQuery-backend control plane,
> ASAPCollector, source SDKs, and the ASAPQuery data plane.

ASAPPlanner does not need to know about Collector processes, backend storage,
OTLP, delta transmission, GOS, or sampling. Its job is to optimize a query or
intent into a deployment-independent Post-ASAP DAG. The ASAPQuery-backend
control plane then compiles that DAG into a distributed physical execution
plan.

```text
Query / Intent
      │
      ▼
ASAPPlanner
Post-ASAP logical/algorithm DAG
      │
      ▼
ASAPQuery-backend Control Plane
Physical plan compilation + deployment optimization
      │
      ├── SDKPlan
      ├── CollectorPlan
      ├── PrecomputePlan
      ├── TransmissionPlan
      ├── BackendPlan
      └── QueryPlan / RoutingPlan
```

These plans are runtime views of one compiled decision. Some views may be
embedded in CollectorPlan or BackendPlan instead of becoming separate wire
APIs, but they must not be compiled independently.

## What ASAPPlanner owns

ASAPPlanner emits a deployment-independent Post-ASAP DAG, for example:

```text
Source(metric)
  → Filter
  → Project
  → GroupBy
  → Window
  → Sketch(CMS)
  → Merge
  → EstimateFrequency
```

The DAG should describe:

- operators and their edges;
- logical sources;
- filter, grouping, and window semantics;
- aggregate and query semantics;
- the selected sketch algorithm and parameters;
- merge and readout semantics;
- required accuracy and confidence;
- freshness and other query SLOs;
- algebraic properties of operators;
- state-compatibility constraints;
- shared producers and common sub-DAGs across the workload;
- the Planner-selected exact fallback; and
- stable node and provenance identities.

For example:

```rust
PostAsapDag {
    nodes: Vec<PostAsapNode>,
    edges: Vec<PostAsapEdge>,
    requirements: QueryRequirements {
        error_bound,
        confidence,
        freshness,
    },
}
```

It should not contain:

```text
collector_endpoint
storage_backend
sampling_probability
GOS threshold
delta encoding
checkpoint interval
producer sequence
OTLP encoding
deployment placement
runtime queue policy
```

Those are deployment-level physical-planning decisions.

Planner must still expose the semantics needed to make those decisions safely:
whether state is mergeable, what its merge operation is, whether updates may
be signed, the sketch's intrinsic error guarantee, and the query's requested
error and freshness. Exposing these properties does not make Planner aware of
sampling or GOS.

Planner-owned intent algebra, semantic rewrites, logical optimizer rules, and
logical sketch reasoning must come from the pinned ASAPPlanner revision.
ASAPQuery-backend must not maintain a second copy of them.

## What the ASAPQuery-backend control plane owns

The control plane receives the Post-ASAP DAG and performs physical plan
compilation using:

```text
Post-ASAP DAG
+ SDK capabilities
+ Collector capabilities
+ backend ingest/store/query capabilities
+ deployment topology
+ traffic, cardinality, and skew estimates
+ CPU, memory, network, and storage costs
+ active-plan compatibility
+ runtime feedback
```

Capabilities here are deployment capabilities, such as:

```text
this SDK supports per-row admission v1
this Collector supports CMS sparse delta v2
this backend supports idempotent delta apply v2
```

They are not a copy of Planner's logical capability algebra.

The control plane decides:

- which DAG nodes execute in the SDK, Collector, backend precompute, storage,
  or query stages;
- whether source sampling is enabled;
- the sampling probability;
- whole-item versus per-row sampling;
- whether GOS is enabled;
- the GOS threshold and adaptive policy;
- full snapshots versus sparse deltas;
- checkpoint and freshness intervals;
- encoding and compression;
- sharding, placement, and storage backend; and
- activation, rollback, and retirement.

Its physical objective may be:

$$
C = w_{\mathrm{cpu}} C_{\mathrm{cpu}}
  + w_{\mathrm{net}} C_{\mathrm{net}}
  + w_{\mathrm{mem}} C_{\mathrm{mem}}
  + w_{\mathrm{store}} C_{\mathrm{store}}.
$$

If it selects sampling and delayed transmission, it must satisfy the accuracy
requirement carried by the DAG. A conservative allocation is:

$$
\epsilon_{\mathrm{sketch}}
+ \epsilon_{\mathrm{sampling}}
+ \epsilon_{\mathrm{transmission}}
\le \epsilon_{\mathrm{query}}.
$$

Sampling/GOS error-budget allocation therefore belongs to the backend control
plane, not ASAPPlanner.

## Physical compilation

One compilation should:

1. Validate the canonical Post-ASAP DAG and preserve shared nodes.
2. Bind logical sources to deployed telemetry sources.
3. Enumerate legal SDK, Collector, backend-precompute, storage, and query
   partitions.
4. Reject placements whose executors lack the required semantics.
5. Choose physical windows, panes, grouping layout, and sharding.
6. Allocate accuracy and freshness budgets across physical mechanisms.
7. Enumerate sampling, GOS, full/delta, checkpoint, encoding, and storage
   alternatives.
8. Choose a feasible alternative using measured resource costs.
9. Assign stable plan, materialization, producer, window, and protocol
   identities.
10. Emit every runtime-plan view from the same in-memory decision.
11. Validate shared fields across those views before publication.

A useful internal result is:

```rust
CompiledDeploymentPlan {
    envelope,
    stage_assignments,
    materializations,
    sdk_plan,
    collector_plans,
    precompute_plan,
    transmission_plan,
    backend_plan,
    query_plan,
}
```

This type has one concrete purpose: it prevents Collector and backend plans
from drifting in family, parameters, grouping, windows, schema, or identity.

## SDKPlan

SDKPlan is needed only when work must happen before OTLP serialization. It may
configure:

- source instrument and attribute routing;
- materialization identity;
- exact pass-through or source sampling;
- whole-item or per-row admission;
- bootstrap sampling probability and grant endpoint;
- stable producer and sampling-stream identity; and
- admission metadata consumed by CollectorPlan.

The SDK executes this plan. It does not choose its own probability or sketch
shape.

## CollectorPlan

CollectorPlan describes collection and local state construction:

- source metric routing and canonical grouping;
- materialization identity;
- sketch algorithm and parameters;
- windows, panes, lateness, and local sharding;
- interpretation of SDK admission metadata;
- full/checkpoint state construction; and
- the associated TransmissionPlan.

ASAPCollector validates and executes this plan. It does not run Planner or a
physical cost model.

## PrecomputePlan

PrecomputePlan describes backend-ingest execution:

- payload validation and materialization resolution;
- full/checkpoint decoding;
- ordered delta application and merge;
- backend-side precompute operators left by partitioning; and
- the atomic visibility boundary for committed state.

It may remain an internal control-plane view embedded in BackendPlan.

## TransmissionPlan

TransmissionPlan is independent of the logical sketch choice. It describes:

- raw, full, or ordered-delta mode;
- GOS or another transmission trigger;
- threshold cap and runtime adjustment policy;
- periodic freshness and checkpoint deadlines;
- encoding and compression;
- maximum in-flight frames and retry policy; and
- sequence, gap recovery, and durable acknowledgement.

For adaptive GOS, the control plane first derives an accuracy-safe cap. Runtime
feedback may choose only:

$$
1 \le T(t) \le T_{\max}.
$$

Even when the operational threshold changes, OctoSketch-style worst-case
evidence is stated using $T_{\max}$.

## BackendPlan

BackendPlan installs:

- the shared plan envelope and materialization identities;
- expected producers and exact state schemas;
- PrecomputePlan and TransmissionPlan validation contracts;
- storage placement and retention;
- query capabilities and materialization routes; and
- activation, expiry, retirement, and fallback behavior.

The data plane does not infer sketch family, parameters, or grouping from query
text or received payloads.

## QueryPlan / RoutingPlan

QueryPlan binds Post-ASAP readouts and remaining operators to installed
materializations. It defines readiness and freshness checks, lookup,
merge/readout execution, remaining backend operators, and the Planner-selected
exact fallback.

The query path does not rerun candidate search, resize a sketch, or substitute
another summary.

## Shared identity and activation

All runtime plans share a compiled-plan envelope containing at least:

```text
plan_id
plan_version
planner_revision
capability_snapshot_id
activation_time
expiry
protocol_version
```

Every full, checkpoint, or delta frame identifies at least:

```text
plan_id
plan_version
materialization_id
producer_id
producer_epoch
window_id
sequence
frame_kind
payload_checksum
```

`producer_epoch` distinguishes sequence spaces before and after a Collector
restart.

Activation is staged:

1. Install BackendPlan and its ingest/deduplication state.
2. Receive backend readiness for that exact plan version.
3. Stage matching SDKPlan and CollectorPlan instances.
4. Receive semantic application evidence from required producers.
5. Activate production and query routing at the declared boundary.
6. Retain the previous plan until its windows and in-flight frames drain.

Configuration delivery or an OpAMP delivery acknowledgement is not plan
activation evidence.

## Durable ordered-delta execution

When delta transmission is selected, Collector maintains the following state
per `(producer, materialization, window, series)`:

```text
active residual
pending frame
in-flight frame
last acknowledged sequence
next sequence
```

The MVP may use `max_in_flight = 1`:

```text
residual crosses threshold
        │
        ▼
form frame(sequence = n)
        │
        ▼
send or retry identical bytes
        │
        ▼
backend atomically commits payload + sequence ledger
        │
        ▼
APPLIED or DUPLICATE acknowledgement
        │
        ▼
Collector retires in-flight state and advances sequence
```

The backend handles sequence state as follows:

| Received sequence | Required behavior |
| --- | --- |
| `sequence == expected` | Atomically apply and return `APPLIED` |
| `sequence < expected` | Do not apply again; return `DUPLICATE` |
| `sequence > expected` | Do not apply; return `GAP(expected)` |
| Recovery is impossible | Return `CHECKPOINT_REQUIRED` |
| Plan/window/schema is incompatible | Return `REJECTED` |

Forming or enqueueing a frame, or completing an ordinary OTLP request, does not
remove it from unacknowledged drift. Only acknowledgement after durable backend
apply advances Collector state.

## What to implement

### Phase A: canonical Planner boundary

In ASAPQuery-backend:

- pin the latest compatible ASAPPlanner revision;
- consume canonical Post-ASAP DAG types directly;
- remove or isolate copied intent algebra, logical optimizer, and logical sketch
  capability code;
- reject unknown DAG variants explicitly; and
- preserve Planner node and provenance identities.

Acceptance: backend code never reselects a summary from query text, and an
unknown node is never silently ignored.

### Phase B: baseline physical compilation

Start with one simple physical alternative:

```text
p = 1
GOS disabled
full transmission
one checkpoint per window
```

Implement DAG partitioning, `CompiledDeploymentPlan`, matching CollectorPlan
and BackendPlan emission, backend-first staged activation, and one complete
summary ingest/store/query path.

Acceptance: captured plans agree exactly on plan, materialization, family,
parameters, grouping, window, and schema identities.

### Phase C: source-sampling alternative

The control plane reads SDK deployment capabilities, allocates a sampling error
budget, selects whole-item or per-row admission and $p$, and emits matching
SDKPlan and CollectorPlan views.

The SDK performs admission at `Record()` and drops an empty mask before OTLP
serialization. For $n$ inputs and $d$ rows, transmitted datapoints should be
close to:

$$
n_{\mathrm{wire}}
= n\left(1-(1-p)^d\right).
$$

Acceptance: source-to-Collector datapoints and bytes decrease, and measured
sampling error remains within its allocated evidence.

### Phase D: GOS/delta alternative

The control plane allocates the transmission error budget, derives
$T_{\max}$, chooses GOS, freshness, and checkpoint policy, and emits compatible
producer/backend TransmissionPlan views.

Collector implements insert-time crossings, sparse cell/bucket deltas,
deadline fallback, and the final residual/checkpoint.

Acceptance: without injected failures, sparse deltas plus the final residual
reconstruct the full-state reference:

$$
x_j = \sum_{\ell=1}^{m}\Delta_j^{(\ell)} + \rho_j.
$$

### Phase E: durable delivery

Collector and backend data plane jointly implement frame identity and sequence,
pending/in-flight/ack state, atomic delta-plus-ledger apply, duplicate
suppression, gap detection, checkpoint recovery, and restart persistence.

Inject failures:

1. before backend apply;
2. after apply but before acknowledgement;
3. during duplicate delivery;
4. with a sequence gap;
5. across Collector restart; and
6. across backend restart.

Acceptance: every committed result equals the failure-free baseline, with no
lost or double-applied frame.

### Phase F: runtime adaptation

The control plane consumes update rate, density, queue length, apply latency,
and resource measurements. It may adjust physical knobs only within the active
compiled policy:

$$
p_{\min} \le p(t) \le 1,
\qquad
1 \le T(t) \le T_{\max}.
$$

A change outside the error, compatibility, or deployment envelope requires a
new compiled plan version.

## Final ownership

- **ASAPPlanner:** generates and optimizes the deployment-independent Post-ASAP
  DAG.
- **ASAPQuery-backend control plane:** partitions stages, performs deployment
  optimization, chooses sampling/GOS/delta/placement, and compiles every runtime
  plan from one decision.
- **SDK:** executes SDKPlan and performs selected admission at the source.
- **ASAPCollector:** executes CollectorPlan and TransmissionPlan, maintains
  local state, and sends identified frames.
- **ASAPQuery data plane:** executes PrecomputePlan, BackendPlan, and QueryPlan,
  including idempotent ingest, storage, merge, readout, and query.
- **Collector and data plane:** execute plans; they do not redo logical or
  physical optimization.

Sampling/GOS savings must be demonstrated with datapoint, byte, CPU, memory,
freshness, and accuracy measurements. Enabling a configuration field is not
evidence of a performance or cost improvement.
