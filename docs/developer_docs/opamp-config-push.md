# ASAPQuery-to-ASAPCollector collection-plan interface

## TL;DR

ASAPPlanner produces candidate **logical**
[post-ASAP DAGs](https://github.com/ProjectASAP/ASAPPlanner/blob/main/docs/design_docs/post-asap-ir.md).
It chooses summary
semantics, such as `SummaryAgg`, a summary family and parameters, a reduction,
and `SummaryEstimate`; it does not choose where operators run or how state is
transmitted.

The ASAPQuery-backend control plane selects a candidate and compiles it into a
physical plan bundle:

```text
ASAPPlanner candidate post-ASAP DAG
                  |
                  v
ASAPQuery-backend control plane
  candidate selection + stage allocation + runtime policy
             /                         \
            v                           v
CollectorPlan                       BackendPlan
build/transmit materializations     ingest/route/read materializations
            \                           /
             +-- same plan and materialization identities --+
```

The collector portion is sent in an OpAMP protobuf `AgentRemoteConfig`. Its
body is a versioned YAML `CollectorPlan`, not a serialized ASAPPlanner DAG and
not an untyped map of processor options. The collector validates and applies
the whole plan atomically and reports the active plan identity.

This document defines the target interface. The final section distinguishes it
from the narrower interface implemented today.

## Ownership boundary

| Component | Owns | Must not own |
| --- | --- | --- |
| ASAPPlanner | Query parsing and canonicalization; candidate post-ASAP DAGs; summary family, algorithm, parameters, reduction, and readout semantics | Collector/backend placement, runtime resources, transmission mode, OpAMP encoding |
| ASAPQuery-backend control plane | Candidate selection; physical placement; source binding; window materialization; transmission policy; plan versioning; splitting one decision into collector and backend portions | Reimplementing PromQL-to-summary rules already represented by ASAPPlanner |
| ASAPCollector | Validating and executing `CollectorPlan`; building and transmitting the named materializations | Selecting a different summary, changing parameters, or inferring omitted query semantics |
| ASAPQuery-backend data plane | Installing the matching `BackendPlan`; ingesting materializations; applying `SummaryEstimate` and remaining logical operators | Re-planning a different summary at query time |

ASAPPlanner's documented
[scope](https://github.com/ProjectASAP/ASAPPlanner#scope) excludes stage and
physical-resource assignment. It also describes the downstream split as an
open integration question: one post-ASAP plan must become a streaming graph
that constructs summaries and a query plan that reads them. This interface
places that split in the ASAPQuery-backend physical-planning layer.

## From ASAPPlanner DAG to the two runtime plans

The control plane performs the following compilation, without changing the
selected candidate's semantics:

| ASAPPlanner concept | Collector plan | Backend plan |
| --- | --- | --- |
| `SummaryAgg` | A `materialization` producer | A materialization declaration and storage route |
| `SummaryFamilyType` | `summary.family`, `summary.algorithm`, and typed `summary.parameters` | The identical family, algorithm, and parameters |
| `col` | `input.value` or `input.item_label` | Readout input metadata |
| `Reduction::PerEntity` | `reduction.kind: per_entity` | Preserve each source series identity |
| `Reduction::Reduce(GroupKeys)` | `reduction.kind: reduce` plus explicit `by` or `without` labels | Matching group/roll-up shape |
| Time-range semantics around `SummaryAgg` | Concrete streaming `window` and lateness policy | Compatible query-window/read range |
| `SummaryEstimate` | No collector readout; collector emits summary state | Query-time readout, such as quantile, cardinality, point count, or top-k |
| `Logical` subtree | Raw pass-through only when the physical plan explicitly selects it | Exact execution or configured fallback |
| `SummaryMerge` | Shard/stage outputs with the same materialization contract | Merge only states with identical compatibility fields |

A control-plane-assigned reference to a Planner node is useful for traceability
but is not a runtime identity. The physical control plane creates stable,
content-addressed materialization identities after source binding, placement,
windowing, and transmission have been decided.

## Wire encoding

The interface has two layers:

| Layer | Encoding | Contract |
| --- | --- | --- |
| Transport | OpAMP Protocol Buffers | Delivery, targeting, retry, and remote-config hash |
| `CollectorPlan` body | UTF-8 YAML with `content_type: application/yaml` | Versioned ASAP collector execution contract |

The OpAMP `AgentConfigMap` entry is named `asap-collector-plan.yaml`. A receiver
must select this exact entry; it must not silently choose an arbitrary first
file. `AgentRemoteConfig.config_hash` is the hash used by OpAMP delivery. It is
separate from `metadata.content_hash`, which identifies the canonical plan
content across transports and processes.

The plan is YAML because it is an operator-visible configuration artifact and
fits OpAMP's configuration-file model. YAML is only the serialization: fields
below form a closed, versioned schema. Unknown required fields, unknown enum
values, and type mismatches are validation failures. JSON may be supported in
a later schema version, but a sender must identify the media type and a
receiver must never guess it.

## `CollectorPlan` schema

### Envelope

| Field | Type | Required | Definition |
| --- | --- | --- | --- |
| `api_version` | string | yes | Schema version. MVP value: `asap.io/v1alpha1`. |
| `kind` | string | yes | Must be `CollectorPlan`. |
| `metadata.plan_id` | string | yes | Identity shared by this collector plan and its matching backend plan. |
| `metadata.revision` | uint64 | yes | Monotonically increasing revision for this target. |
| `metadata.content_hash` | string | yes | SHA-256 of the canonical plan with this field omitted. |
| `metadata.generated_at` | RFC 3339 timestamp | yes | Time the control plane produced this revision. |
| `metadata.valid_from` | RFC 3339 timestamp | yes | Earliest activation time. |
| `metadata.expires_at` | RFC 3339 timestamp | yes | Time after which the collector must stop using the plan. |
| `metadata.planner_revision` | string | yes | ASAPPlanner commit/version used to create the candidate DAG. |
| `metadata.candidate_id` | string | yes | Stable identifier of the selected candidate post-ASAP plan. |
| `metadata.query_ids` | list of strings | yes | Workload queries whose selected plan requires these materializations. |
| `target.instance_uid` | string | yes | Exact OpAMP agent instance this plan targets. |
| `target.capability_hash` | string | yes | Capability snapshot against which the physical plan was validated. |
| `on_unsupported` | enum | yes | `reject_plan` for MVP. No silent downgrade or substitution is allowed. |
| `materializations` | list | yes | Collector-side producers. An empty list is valid only for an explicit no-op plan. |

`plan_id` groups compatible collector and backend portions. `revision` orders
updates. `content_hash` makes a revision immutable. Reusing the same
`(plan_id, revision)` with different content is an error.

### Materialization identity and input

| Field | Type | Required | Definition |
| --- | --- | --- | --- |
| `materializations[].id` | string | yes | Content-addressed identity shared with the backend `Materialization.fingerprint`. |
| `materializations[].logical_node_ref` | string | yes | Deterministic reference assigned by the control plane to the selected Planner `SummaryAgg` or logical pass-through node. |
| `materializations[].input.metric` | string | yes | Exact input metric name. |
| `materializations[].input.matchers` | list | yes | Canonical label matchers; each has `label`, `op`, and `value`. Empty means all series of the metric. |
| `materializations[].input.value` | enum | yes | `sample_value` for numeric summaries, or `label` with `label_name` for item/set summaries. |

Matcher `op` is one of `eq`, `neq`, `regex`, or `not_regex`. The collector
must apply the same matcher semantics used when ASAPPlanner canonicalized the
query. A metric-name regex or unresolved source is outside the MVP and must be
rejected during physical planning.

### Summary

| Field | Type | Required | Definition |
| --- | --- | --- | --- |
| `summary.family` | enum | yes | `exact_aggregate`, `sketch`, `sample`, `wavelet`, or `stat_model`, matching Planner `SummaryFamilyType`. |
| `summary.algorithm` | enum | yes | Concrete algorithm within the family. |
| `summary.parameters` | tagged object | yes | Parameters belonging to exactly that algorithm. Empty object for parameterless exact accumulators. |
| `summary.accuracy` | tagged object | yes | Original Planner constraint: `exact`, `epsilon`, or `epsilon_delta`. |

When a Planner exact aggregation carries no explicit accuracy field, the
physical control plane normalizes this field to `{kind: exact}`.

The MVP algorithms and parameter objects are:

| Family | Algorithm | Parameters |
| --- | --- | --- |
| `exact_aggregate` | `sum`, `count`, `min_max`, `increase`, `rate` | `{}` |
| `sketch` | `kll` | `k` |
| `sketch` | `ddsketch` | `alpha` |
| `sketch` | `hll` | `precision` |
| `sketch` | `cms` | `width`, `depth` |
| `sketch` | `cms_with_heap` | `width`, `depth`, `heap_size` |
| `sketch` | `count_sketch` | `width`, `depth` |
| `sketch` | `count_sketch_with_heap` | `width`, `depth`, `heap_size` |

ASAPPlanner also defines KMV, Theta, reservoir sampling, Haar wavelets, and
statistical models. They remain valid Planner alternatives but are rejected by
this interface until both ASAPCollector and ASAPQuery-backend advertise and
implement matching wire/state capabilities. The control plane must not map an
unsupported algorithm to a vaguely similar supported algorithm.

`accuracy` records the constraint that justified the selected candidate; it
does not replace the concrete parameters. For example:

```yaml
summary:
  family: sketch
  algorithm: ddsketch
  parameters:
    alpha: 0.01
  accuracy:
    kind: epsilon
    epsilon: 0.01
```

The control plane must validate the algorithm/parameter pair against the
selected Planner `SketchKind`. A DDSketch with KLL's `k` parameter is invalid.

### Reduction and windows

| Field | Type | Required | Definition |
| --- | --- | --- | --- |
| `reduction.kind` | enum | yes | `per_entity` or `reduce`; preserves Planner's distinction. |
| `reduction.by` | list of strings | for `reduce` | Labels retained when `without` is false. An empty list means a real global reduction. |
| `reduction.without` | boolean | for `reduce` | When true, `by` names excluded labels and all other labels are retained. |
| `window.kind` | enum | yes | `tumbling` for the MVP. |
| `window.size` | duration | yes | Logical summary window. |
| `window.slide` | duration | yes | Window start interval; equal to `size` for tumbling windows. |
| `window.allowed_lateness` | duration | yes | Maximum late-arrival update interval. |

`per_entity` is not encoded as `reduce` with an empty `by` list. The former
keeps one result per input series; the latter merges every matching series
into one global group. This distinction comes directly from ASAPPlanner's
`Reduction` type and must survive physical lowering.

Planner time-range semantics describe what the query means. The physical
control plane selects a concrete streaming window representation capable of
answering that range. If the chosen windows cannot compose to the Planner
query's range without violating semantics or accuracy, the candidate cannot
be deployed.

### Placement output and transmission

| Field | Type | Required | Definition |
| --- | --- | --- | --- |
| `placement.stage` | enum | yes | Must be `collector` in this document. |
| `placement.shards` | uint32 | yes | Number of local producer shards. Physical policy, not a Planner field. |
| `transmission.mode` | enum | yes | `raw`, `full`, or `delta`. |
| `transmission.encoding` | string | yes | State encoding understood by both collector and backend. |
| `transmission.schema_version` | uint32 | yes | Version of the emitted materialization payload. |
| `transmission.emit_every` | duration | yes | Full or delta emission cadence within/following a window. |
| `transmission.full_checkpoint_every` | duration | for `delta` | Maximum interval between full checkpoints used to recover delta state. |
| `transmission.sequence_scope` | enum | for `delta` | MVP value `materialization_window_producer`. |
| `output.endpoint_ref` | string | yes | Reference to a preconfigured exporter endpoint; no credentials are embedded in the plan. |

Transmission is a physical decision made by ASAPQuery-backend, not by
ASAPPlanner. `full` and `delta` change representation, not logical query
semantics. Delta is legal only when the advertised algorithm/state encoding
supports it. Each delta payload must carry plan ID, revision, materialization
ID, window identity, producer identity, sequence number, and base/checkpoint
identity so the backend can reject gaps or incompatible state.

`raw` means explicit pass-through selected for a logical subtree that is not
materialized at the collector. It must not be represented as an unknown
summary family or as `drop_original: false` attached to an unrelated summary.

## Complete example

For the PromQL query:

```promql
quantile_over_time(0.95, request_duration_seconds{region="us-east"}[5m])
```

ASAPPlanner may produce a candidate containing a DDSketch `SummaryAgg` over
the sample value, `Reduction::PerEntity`, and a quantile
`SummaryEstimate { q: 0.95 }`. After selecting that candidate and placing the
aggregation at the collector, ASAPQuery-backend may send:

```yaml
api_version: asap.io/v1alpha1
kind: CollectorPlan
metadata:
  plan_id: workload-dashboard-a
  revision: 42
  content_hash: sha256:6d9f...
  generated_at: 2026-08-27T20:00:00Z
  valid_from: 2026-08-27T20:00:05Z
  expires_at: 2026-08-28T20:00:05Z
  planner_revision: 7278505
  candidate_id: candidate-quantile-ddsketch
  query_ids: [dashboard-latency-p95]
target:
  instance_uid: 550e8400-e29b-41d4-a716-446655440000
  capability_hash: sha256:a31c...
on_unsupported: reject_plan
materializations:
  - id: mat:sha256:98f1...
    logical_node_ref: summary-agg-7
    input:
      metric: request_duration_seconds
      matchers:
        - {label: region, op: eq, value: us-east}
      value: sample_value
    summary:
      family: sketch
      algorithm: ddsketch
      parameters: {alpha: 0.01}
      accuracy: {kind: epsilon, epsilon: 0.01}
    reduction:
      kind: per_entity
    window:
      kind: tumbling
      size: 1m
      slide: 1m
      allowed_lateness: 10s
    placement:
      stage: collector
      shards: 4
    transmission:
      mode: delta
      encoding: asap.summary.ddsketch
      schema_version: 1
      emit_every: 10s
      full_checkpoint_every: 1m
      sequence_scope: materialization_window_producer
    output:
      endpoint_ref: asapquery-primary
```

The matching backend plan uses the same `plan_id`, `revision`, and
materialization ID. It records DDSketch with `alpha: 0.01`, the source/filter,
per-entity reduction, compatible windows, storage route, and the quantile
readout. Query time reads that decision; it must not run ASAPPlanner again and
independently choose KLL or different DDSketch parameters.

## Validation and atomic application

Before activation, ASAPCollector validates:

1. schema version, target instance, revision ordering, content hash, lifetime,
   and capability hash;
2. unique materialization IDs and query/node traceability;
3. source matcher and value-input types;
4. summary family/algorithm/parameter/accuracy compatibility;
5. reduction and window invariants;
6. transmission support, including delta checkpoint and sequence rules; and
7. exporter references and resource guardrails.

The plan is all-or-nothing. An invalid materialization rejects the candidate;
the collector keeps the previous unexpired plan. A valid plan is staged and
activated atomically at `valid_from`. Existing windows follow an explicitly
reported transition policy; state from incompatible revisions is never merged.

Re-delivery of the same `(plan_id, revision, content_hash)` is idempotent. An
older revision is rejected. Reusing `(plan_id, revision)` with another hash is
rejected. An expired plan stops producing state unless a separately configured,
bounded last-known-good policy explicitly permits a grace interval.

## Application report

OpAMP `RemoteConfigStatus` reports delivery/application of the config hash, but
the ASAP contract needs a semantic application report as well. The collector
returns a typed OpAMP custom message with:

| Field | Definition |
| --- | --- |
| `plan_id`, `revision`, `content_hash` | Candidate being reported. |
| `remote_config_hash` | OpAMP configuration hash that carried it. |
| `status` | `rejected`, `staged`, `active`, `expired`, or `failed`. |
| `observed_at`, `activated_at` | Status and activation timestamps. |
| `active_materialization_ids` | Exact materializations installed. |
| `effective_capability_hash` | Collector capabilities used during validation. |
| `errors[]` | Machine-readable `code`, field `path`, and human-readable message. |

The custom-message capability is `io.asap.collector.plan.v1`; message type is
`application_report`; its data is protobuf-encoded. `RemoteConfigStatus.APPLIED`
without an `active` application report does not prove semantic activation.
The MVP harness must additionally observe post-activation input, emitted
payloads carrying the same identities, and successful backend ingestion.

## Fail-closed behavior

| Condition | Required result |
| --- | --- |
| Unknown schema version, enum, or required field | Reject the entire plan. |
| Candidate uses a Planner summary unsupported by the collector | Control plane must choose another candidate or exact fallback; collector rejects if still sent. |
| Family, algorithm, and parameters disagree | Reject; never substitute defaults. |
| `per_entity`/`reduce` semantics are ambiguous | Reject. |
| Delta requested for an incompatible family/encoding | Reject. |
| Backend plan lacks the same materialization identity and contract | Do not activate the bundle or reject emitted state. |
| Revision is stale, conflicting, premature, or expired | Preserve the current valid plan and report the reason. |
| Application evidence is missing | MVP verdict is FAIL, not UNKNOWN or PASS. |

## Current implementation gap

The current ASAPCollector OpAMP extension does not yet implement this target
interface. Today it:

- reads an arbitrary `AgentConfigFile.body` as a complete OTel Collector YAML;
- writes that YAML to a configured path and relies on a supervisor restart;
- identifies it only by OpAMP `config_hash`; and
- reports `APPLIED` after a syntactic YAML check and file write.

It does not yet parse a versioned `CollectorPlan`, validate Planner-derived
semantics, apply a plan atomically in-process, or report plan/materialization
identities. The separate JSON `PrecomputeConfigSet` HTTP polling interface is
also not this target contract; the runtime OpAMP control-channel adapter is
currently a stub.

Until the target interface is implemented, tests must not claim that
`config_hash` or `APPLIED` proves a compatible versioned plan was active.

## Non-goals

This interface does not serialize ASAPPlanner's internal DAG, define PromQL
parsing, rank Planner candidates, define backend query execution, or specify
summary-state byte encoding. It defines the physical control-plane boundary
between ASAPQuery-backend and ASAPCollector and the identities that bind it to
the corresponding backend plan.
