# CollectorPlan consumption and OpAMP publication

## Implemented MVP boundary

ASAPQuery owns logical selection and physical compilation. ASAPCollector owns
validation and execution of the resulting per-target `CollectorPlan`; it must
not parse PromQL, rank Planner candidates, change algorithms, or invent missing
parameters.

The implemented consumers are
`asap-precompute-rs::collector_plan::CollectorPlan` and
`asap-precompute-go.DecodeCollectorPlan`. They accept the JSON emitted by
`ASAPQuery-backend::control_plane::physical::compiler`, validate the entire
plan, and project it atomically to a `PrecomputeConfigSet`. The Go
`controlchannel.OpAmpChannel.ReceiveCollectorPlan` queues a validated plan for
one-time `Poll` delivery and reports `APPLIED` only after the runtime calls
`Ack` with that exact plan version. `asapedgeprocessor` registers the
`io.projectasap.collector-plan.v1` custom capability on the configured OpAMP
extension and connects that transport to the typed channel.

```text
latest ASAPPlanner
        |
        v
ASAPQuery PhysicalCompiler
        |-- CollectorPlan[] ----> ASAPCollector validation/runtime projection
        `-- BackendPlan --------> ASAPQuery ingest/query routing
```

The legacy complete-YAML RemoteConfig/restart path is not the typed-plan MVP
path. `config_hash` alone is not evidence that a physical plan was active; the
MVP requires the `plan_status` response emitted after the runtime applies and
acknowledges the exact plan version.

Configure the processor with exactly one control transport. For OpAMP:

```yaml
extensions:
  opamp:
    server:
      ws:
        endpoint: wss://controller.example/v1/opamp

processors:
  asap_edge:
    control_channel:
      opamp_extension: opamp
      collector_id: edge-a
```

The server sends a custom message with capability
`io.projectasap.collector-plan.v1`, type `collector_plan`, and the JSON plan as
its body. The Collector replies on the same capability with type `plan_status`
and body `{"plan_id":42,"status":"applied"}`. A failed whole-plan validation
uses status `failed` and leaves the pending/active valid plan unchanged.

## Wire schema

The MVP body is UTF-8 JSON. Field names below are exact.

```json
{
  "collector_id": "edge-a",
  "envelope": {
    "plan_id": 42,
    "generated_at_unix_ms": 10000,
    "planner_revision": "3afcba68f4e8397fb81e2be988f47120f63f7a39",
    "capability_snapshot_id": "caps-7"
  },
  "materializations": [
    {
      "query_id": "q-latency",
      "metric": "request_duration_seconds",
      "algorithm": "ddsketch",
      "parameters": {"alpha": 0.01},
      "group_by": ["service"],
      "window_secs": 60,
      "evidence_source": null
    }
  ]
}
```

### Envelope

| Field | Meaning |
| --- | --- |
| `collector_id` | Exact Collector instance allowed to apply the plan. |
| `plan_id` | Non-zero, deterministic bundle identity; becomes runtime config version. |
| `generated_at_unix_ms` | Physical compilation time. |
| `planner_revision` | Exact Planner revision used for selection. |
| `capability_snapshot_id` | Runtime capability snapshot used by compilation. |

### Materialization

| Field | Meaning |
| --- | --- |
| `query_id` | Non-empty, unique workload query identity. |
| `metric` | Exact source metric name. |
| `algorithm` | Committed algorithm; never a preference or wildcard. |
| `parameters` | Committed typed parameters for that algorithm. |
| `group_by` | Labels retained as independent runtime subpopulations. |
| `window_secs` | Positive tumbling-window duration. |
| `evidence_source` | Required for heap-bearing TopK materializations; identifies the certified margin source. |

Implemented algorithms and parameter mapping:

| Physical algorithm | Required wire parameters | Runtime sketch |
| --- | --- | --- |
| `ddsketch` | `alpha` | DDSketch (`relative_accuracy`) |
| `kll` | `k` | KLL |
| `hll` | `precision` | HLL |
| `cms` | `width`, `depth` | Count-Min (`columns`, `rows`) |
| `cmswithheap` | `width`, `depth`, `heap_size` | Count-Min TopK |
| `countsketch` | `width`, `depth` | CountSketch |
| `countsketchwithheap` | `width`, `depth`, `heap_size` | CountSketch TopK |

The projection always preserves the committed algorithm and parameters, uses a
tumbling window, transmits full protobuf sketch state, and derives a stable
non-zero runtime aggregation ID using language-neutral FNV-1a over
`big_endian(plan_id) || 0x00 || utf8(query_id)`. Rust and Go tests pin the same
value.

## Validation and activation rules

Validation is all-or-nothing. Before returning any runtime configuration the
consumer verifies:

- target Collector identity;
- non-zero plan ID and non-empty Planner/capability identities;
- non-empty, unique query IDs;
- non-empty metric and positive window;
- supported algorithm and every required positive numeric parameter; and
- `evidence_source` for heap-bearing TopK materializations.

Unknown algorithms, missing parameters, wrong targets, duplicate IDs, or a
TopK decision without evidence reject the complete plan. The current valid plan
must remain active when a new candidate is rejected.

## MVP test evidence

`asap-precompute-rs/tests/collector_plan.rs` proves the executable boundary:

- backend-shaped DDSketch JSON maps to the exact runtime family, alpha,
  grouping, metric, window, and plan version;
- certified CountSketch-with-heap preserves heap size and evidence lineage;
- missing TopK evidence fails closed;
- one invalid materialization rejects the complete plan; and
- a plan cannot be applied to a different Collector.

These tests prove plan consumption. They do not claim the legacy OpAMP YAML
file-write path has already been replaced or that a live backend acknowledged a
matching BackendPlan.

## Deferred contract fields

Activation/expiry, allowed lateness, delta/checkpoint policy, endpoint
selection, matchers, retention/draining, materialization fingerprints shared
with BackendPlan, and server-side publication orchestration remain follow-up
extensions. Until they are added to both producer and consumer, documentation
and tests must not claim those fields are enforced by the MVP wire contract.
