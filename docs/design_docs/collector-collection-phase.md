# Collector collection-phase protocol and configuration

> Status: active MVP contract

## TL;DR

The collection phase turns ordinary telemetry observations into plan-selected
materializations. A `CollectorPlan` identifies the source metric, retained label
keys, aggregation kind and parameters, window policy, output SID, and delivery
policy. The Collector validates and atomically activates the plan, routes matching
observations into isolated per-SID state, closes windows, and emits typed summary
records without changing the identity of unrelated raw series.

## Boundary

ASAPPlanner selects logical materializations. The ASAPQuery control plane compiles
that selection into separate `CollectorPlan` and `BackendPlan` artifacts. The
Collector owns only edge execution: matching, grouping, accumulation, window close,
encoding, and export. It does not reinterpret queries or choose a different summary.

## Physical collection rule

Each rule needs:

| Field | Purpose |
| --- | --- |
| plan/materialization version | Correlates edge behavior with backend installation |
| source metric matcher | Selects input observations |
| retained `group_by_keys` | Defines the output label space |
| `agg_kind` and parameters | Selects raw, sketch, or exact aggregation state |
| window policy | Defines boundaries, lateness, flush, and empty-window behavior |
| SID | Identifies this materialization throughout the data lifecycle |
| transmission policy | Full/delta mode, encoding/version, destination, retry policy |
| lifecycle policy | Activation, retirement, drain, and expiry behavior |

The grouping key is the source metric plus the values of retained label keys. Labels
not retained by the rule cannot be reconstructed later and must not silently appear
in query results.

## Activation lifecycle

```text
receive -> validate -> stage -> acknowledge-ready -> activate atomically
        -> observe -> close windows -> export -> drain/retire
```

A rule is ready only after its algorithm, parameters, SID, and encoding are
supported and its destination is available. A replacement plan must not mix state
created under incompatible policies: either drain the old SID or start a new SID.
Duplicate versions are idempotent; stale versions are rejected.

## Failure behavior

- Unknown aggregation or encoding: reject the rule before activation.
- Resource exhaustion: apply the explicit cardinality/eviction policy and expose a
  counter; never merge unrelated groups.
- Late observation: follow the configured lateness policy and report drops.
- Export uncertainty: retry idempotently using SID and window identity.
- Control-plane loss: continue the last acknowledged non-expired plan and expose
  plan age; do not invent a new configuration.

## Acceptance evidence

Tests must cover plan validation, atomic replacement, per-label grouping, boundary
timestamps, late data, duplicate delivery, shutdown drain, resource limits, and
CollectorPlan/BackendPlan version agreement.

