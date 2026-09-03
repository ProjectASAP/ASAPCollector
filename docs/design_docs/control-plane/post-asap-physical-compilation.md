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

These names describe runtime views of one compiled decision, not six public
protocols. `CompiledDeploymentPlan` is the control plane's internal result.
For the MVP, `CollectorPlan` and `BackendPlan` are the two deployed plan
documents. `TransmissionPlan` is a typed section shared by both documents;
`PrecomputePlan` and `QueryPlan` are sections of `BackendPlan`. `SDKPlan` is an
optional build/startup artifact. No view may be compiled independently.

## Implementation status

This document is a target contract, not a claim that the end-to-end protocol
already exists. At the revision that introduced this document:

| Area | Status in ASAPCollector | Remaining MVP work |
| --- | --- | --- |
| Typed CollectorPlan validation and staged Collector activation | Partial | Align the schema with the compiler output and require semantic apply evidence. |
| SDK admission before OTLP serialization | Partial | Treat SDK configuration as a startup artifact and add cross-repository golden fixtures. |
| Insert-time GOS and sparse deltas | Partial | Bind it to a compiled TransmissionPlan and the durable state machine below. |
| Durable frame ACK, retry, recovery, and restart replay | Not implemented end to end | Implement jointly in Collector and ASAPQuery-backend. |
| BackendPlan, ingest ledger, storage, and query routing | Outside this repository | Implement in ASAPQuery-backend from the same compiled decision. |
| Runtime cost-based alternative selection | Not implemented end to end | Add only after the baseline, sampling, and durable-delta paths have measurable evidence. |

“Partial” means useful components and focused tests exist; it does not mean the
cross-repository MVP acceptance criterion is satisfied.

## What ASAPPlanner owns

ASAPPlanner emits a deployment-independent Post-ASAP DAG, for example:

```text
Source(metric)
  → Filter
  → Project
  → GroupBy
  → Window
  → SummaryAgg(SketchQuery { algorithm: CMS, ... })
  → Merge
  → SummaryEstimate(Frequency)
```

The labels are illustrative Post-ASAP operators, not additional wire types.

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
requirement carried by the DAG. The compiler first maps every mechanism to a
bound in the same query-output metric $M_q$. Let $B_m(q)$ be the resulting
absolute error bound for mechanism $m$. A conservative deterministic
composition is:

$$
B_{\mathrm{sketch}}(q)
+ B_{\mathrm{sampling}}(q)
+ B_{\mathrm{transmission}}(q)
\le B_{\mathrm{query}}(q).
$$

For probabilistic bounds, the compiler must also allocate failure probability,
for example

$$
\delta_{\mathrm{sketch}} + \delta_{\mathrm{sampling}}
\le \delta_{\mathrm{query}}.
$$

Sketch and sampling terms may instead compose in quadrature only when the
selected family supplies a proof under its stated independence and tail
assumptions. Raw `epsilon` values with different units or normalizations must
never be added. Deterministic transmission staleness still composes linearly
after the selected readout maps coordinate drift into $M_q$.

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
    transmission_contracts,
    backend_plan,
}
```

This type has one concrete purpose: it prevents Collector and backend plans
from drifting in family, parameters, grouping, windows, schema, or identity.
Its `backend_plan` contains the precompute and query-routing sections; its
transmission contracts are projected into both deployed documents.

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

For the MVP, SDKPlan is a generated build/startup artifact delivered through
the application's normal configuration mechanism. The control plane does not
dynamically activate SDK code. While a plan is active, an already-configured
grant channel may change $p$ only inside the compiled range and for the same
sampling identity. Adding dynamic SDK plan delivery requires a separate
authenticated delivery channel and semantic application acknowledgement.

The SDK executes this artifact. It does not choose its own probability or
sketch shape.

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

It is an internal control-plane view embedded in BackendPlan for the MVP.

## TransmissionPlan

TransmissionPlan is a typed contract embedded in both CollectorPlan and
BackendPlan. It does not change the Planner's logical sketch choice. It
describes:

- raw, full, or ordered-delta mode;
- GOS or another transmission trigger;
- threshold cap and runtime adjustment policy;
- periodic freshness and checkpoint deadlines;
- encoding and compression;
- maximum in-flight frames and retry policy; and
- sequence, gap recovery, and durable acknowledgement.

For adaptive GOS, the control plane first derives a family-specific,
accuracy-safe cap. Runtime feedback may choose only:

$$
0 < T(t) \le T_{\max}.
$$

Integer counter families additionally quantize this to an integer threshold of
at least one. Real-valued families define their own positive minimum. Even when
the operational threshold changes, worst-case evidence is stated using
$T_{\max}$ and the delivery bound below.

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

The MVP checksum is SHA-256 over
`"ASAP-FRAME-V1" || header_length_be32 || canonical_header || payload`, where
`canonical_header` excludes the checksum field. Canonical field ordering and
integer encoding are part of the protocol version.
The shared `plan_id` and `plan_version` semantics are defined in
[`physical-planning.md`](physical-planning.md#shared-plan-envelope).

`producer_epoch` distinguishes sequence spaces, but a process restart alone
must not change it. The Collector resumes a persisted epoch until the backend
has acknowledged the epoch-closing checkpoint.

Activation is staged:

1. Install BackendPlan and its ingest/deduplication state.
2. Receive backend readiness for that exact plan version.
3. Verify the required SDK startup artifact is deployed, then stage matching
   CollectorPlan instances. For sampling, Collector must observe the exact
   plan, materialization, sampling-stream, and admission-policy identities on
   source telemetry before declaring that producer ready.
4. Receive semantic CollectorPlan application evidence from required
   producers, including the plan version and materialization identities.
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
producer epoch
```

The telescoping identity and every drift bound are scoped to exactly this key;
deltas from different producers, series, materializations, windows, or epochs
must never be combined as one sequence.

### Bounded queue policy and continuous drift

The MVP uses `max_in_flight = 1` and `max_pending = 1`. Updates continue while
one frame is in flight. If another threshold crossing occurs, Collector seals
one pending frame and immediately applies backpressure to that series until the
in-flight frame is acknowledged. It does not drop, overwrite, or repeatedly
coalesce threshold crossings into an unbounded pending frame.

For one scalar or sketch coordinate, let $U_{\max}$ bound the absolute effect
of one accepted update, and let a crossing be detected immediately after that
update. A threshold-sealed frame then has magnitude at most
$T + U_{\max}$. With at most $s$ sealed but unacknowledged frames and an active
residual below $T$, the unacknowledged coordinate drift satisfies

$$
|D_j(t)| < (s+1)T + sU_{\max}.
$$

Here $s \le \mathtt{max\_in\_flight}+\mathtt{max\_pending}$. For the MVP,
$s \le 2$, so a conservative bound is

$$
|D_j(t)| < 3T + 2U_{\max}.
$$

Across $k$ producers, the uniform-policy coordinate bound is therefore

$$
\left|\sum_{i=1}^{k}D_{i,j}(t)\right|
< k\left(3T+2U_{\max}\right),
$$

or, for nonuniform policies, the sum of each producer's individual bound.
The physical compiler maps this coordinate bound through the selected
family's readout to obtain $B_{\mathrm{transmission}}(q)$. It must size
$T_{\max}$ using this delivery-aware bound, not the idealized $kT$ bound that
assumes immediate application. If no finite $U_{\max}$ is enforceable, the
compiler must reject thresholded delta mode or use a family-specific bound that
handles weighted updates. Freshness deadlines bound time, but do not by
themselves bound update-magnitude drift.

The state transition is:

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

If a pending frame exists at acknowledgement, it becomes the next in-flight
frame using its already assigned sequence and exact persisted bytes, and
ingestion resumes. A deadline may seal a sub-threshold active residual, subject
to the same one-pending-frame and backpressure rule.

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

### ACK transport

For the MVP, summary frames use the patched OTLP metrics export path. Its
response carries one ASAP `FrameAck` per frame, and the patched exporter returns
those acknowledgements to the ASAP processor's state machine. `FrameAck`
contains the full frame identity, status, and `expected_sequence` for `GAP`.
An unmodified OTLP success response is never translated into `APPLIED`.

### Restart and write-ahead persistence

Every accepted source update must be recoverable. Either the upstream transport
replays it until Collector acknowledges durable intake, or Collector persists
the resulting active residual before acknowledging intake. Before resetting an
active residual or exposing a new sequence for send, Collector atomically
persists a write-ahead transition containing the producer epoch, next and
acknowledged sequences, new active residual, pending frame, and in-flight
frame's exact bytes. On restart it restores that record and retries the same
in-flight bytes in the same epoch. A residual-only checkpoint is not sufficient
because it can orphan a reset-but-unacknowledged contribution.

Collector may create a new producer epoch only after the old epoch has no
unacknowledged frames and the backend has durably acknowledged an
epoch-closing full checkpoint. The backend persists both materialized state and
the sequence ledger atomically, so backend restart preserves duplicate and gap
detection.

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
parameters, grouping, window, and schema identities. Store the canonical
Post-ASAP input, CompiledDeploymentPlan, CollectorPlan, BackendPlan, and
expected validation result as versioned JSON golden fixtures consumed by both
repositories.

### Phase C: source-sampling alternative

The control plane reads SDK deployment capabilities, allocates a sampling error
budget, selects whole-item or per-row admission and $p$, and emits matching
SDKPlan and CollectorPlan views.

The SDK performs admission at `Record()` and drops an empty mask before OTLP
serialization. For $n$ inputs, $d$ independently admitted rows, and sampling
probability $p$, the probability that an input produces a nonempty mask is
$q = 1-(1-p)^d$. Therefore

$$
N_{\mathrm{wire}} \sim \mathrm{Binomial}(n,q),
\qquad
\mathbb{E}[N_{\mathrm{wire}}] = nq.
$$

Acceptance has two layers:

1. With a fixed seed, the SDK's admitted row masks and next-admission cursors
   exactly match a simple per-row reference implementation for every input.
2. Across independent seeds, require

   $$
   |N_{\mathrm{wire}}-nq|
   \le 6\sqrt{nq(1-q)} + 1,
   $$

   and separately report serialized bytes before and after admission. This
   tolerance is an executable distribution sanity check, not an accuracy
   proof. Query-error tests must independently satisfy the allocated
   $(B_{\mathrm{sampling}},\delta_{\mathrm{sampling}})$ evidence.

### Phase D: GOS/delta alternative

The control plane allocates the transmission error budget, derives
$T_{\max}$, chooses GOS, freshness, and checkpoint policy, and emits compatible
producer/backend TransmissionPlan views.

Collector implements insert-time crossings, sparse cell/bucket deltas,
deadline fallback, and the final residual/checkpoint.

Acceptance: without injected failures, acknowledged sparse deltas plus the
final residual reconstruct the full-state reference for each exact
`(producer, materialization, window, series, epoch)` key:

$$
x_j = \sum_{\ell=1}^{m}\Delta_j^{(\ell)} + \rho_j.
$$

### Phase E: durable delivery

Collector and backend data plane jointly implement frame identity and sequence,
the bounded queue policy, pending/in-flight/ACK state, atomic
delta-plus-ledger apply, duplicate suppression, gap detection, checkpoint
recovery, and restart persistence over the ACK transport specified above.

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
0 < T(t) \le T_{\max}.
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
