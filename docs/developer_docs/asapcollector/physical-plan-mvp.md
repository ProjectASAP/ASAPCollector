# Physical-plan MVP contract

ASAPCollector consumes a deployment projection compiled by ASAPQuery-backend.
It does not parse PromQL, inspect post-ASAP IR, choose a sketch candidate, or
derive an independent materialization identity.

The authoritative flow is:

```text
ASAPPlanner abstract candidates and selection
          |                                  ^
          | candidates                       | implementation cost evidence
          v                                  |
ASAPQuery physical implementation -----------+
          |
          v
ASAPQuery PhysicalPlan
  |-- CollectorPlan       -> collector configuration and lifecycle
  |-- PrecomputePlan      -> backend ingest/schema/producer contract
  |-- TransmissionPlan    -> frame, cadence, sampling, delta, and GOS rules
  |-- BackendPlan         -> SID/materialization routing
  `-- QueryPlan           -> serving-time executable DAG
```

For each collector, ASAPQuery publishes one atomic `CollectorPlan` containing
the shared plan envelope, assigned materializations, and the exact subset of
transmission rules whose `producer_id` is that collector. The Collector rejects
the whole plan if any materialization lacks exactly one matching rule, any
physical family or runtime policy is unsupported, or any identity is invalid.

## Window ownership

`abstract_window_framework` is Planner-owned IR. The accompanying
`window_implementation_id`, `pane_secs`, and `state_layout` are the concrete
ASAPQuery-owned realization chosen from complete workload-specific evidence.
Collector validates this pairing and never substitutes another framework.

The MVP runtime supports `tumbling` realized as equal-width anchored panes
with `state_layout: anchored-pane-v1`. Sliding, exponential-histogram, unknown
extension frameworks, missing physical identities, and mismatched pane widths
are rejected atomically until the executor advertises those complete
semantics.

## Activation

`(plan_id, plan_version)` names an immutable generation. A generation is first
validated and staged; staging never changes the active runtime. At
`activation_unix_ms`, it may atomically become active. The previous generation
enters draining so its windows can finish, then becomes retired. Expired or
previously seen generations cannot be reactivated.

`PrecomputeConfigSet.version` is `plan_version`, while every runtime `agg_id` is
the backend-issued materialization fingerprint. The Collector must never hash
query text or locally generate an alternate ID.

## Frame identity

Every emitted summary belongs to one lineage scoped by materialization,
concrete retained-label series, producer, and producer epoch. Sequence and
checkpoint continuity cross window boundaries; each frame still carries its
own window. A full frame carries a checkpoint ID; a delta carries its base
checkpoint ID; delta rules periodically produce a new full checkpoint.

Until modified OTLP has a dedicated message, the identity travels in the
reserved `asap.frame.*` attributes. Window start/end remain the data point's
timestamps. `SummaryFrameIdentity::otlp_attributes` is the sender-side mapping
accepted by ASAPQuery-backend's ingest parser.

HTTP 2xx / gRPC OK is the delivery acknowledgement. The Collector retries a
failed request with the same identity and advances only after transport
success; there is no second application-level ACK or sender WAL contract.

## Runtime policy boundary

Sampling, transmission cadence, delta thresholds, and GOS are immutable fields
of a plan generation. A running Collector does not autonomously mutate them.
Runtime feedback produces a successor `plan_version`, subject to the controller
guardrails and evidence checks, and the Collector applies it through the same
stage/activate state machine.

The current executable matrix is fail-closed:

| Capability | Supported physical family |
| --- | --- |
| Hash-threshold sampling | HLL |
| Geometric-admission sampling | CMS (heap-less) |
| Delta | DDSketch, HLL, CMS variants, CountSketch variants |
| GOS delta gating | CountSketch variants |

The contract fixture in
`asap-precompute-rs/tests/fixtures/collector_plan_v1.json` and the
`collector_plan` integration tests pin this wire shape, atomic validation,
staged activation, exact materialization IDs, runtime-policy projection, and
full/delta frame identity.
