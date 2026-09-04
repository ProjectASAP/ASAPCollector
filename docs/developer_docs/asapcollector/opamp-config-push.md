# CollectorPlan consumption and OpAMP publication

ASAPQuery owns post-ASAP selection and the atomic PhysicalPlan compile.
ASAPCollector consumes only its per-target CollectorPlan; it does not parse
PromQL, search candidates, choose another sketch, or derive a materialization
identifier.

The implemented consumers are the Rust CollectorPlan and Go
DecodeCollectorPlan. Both validate the shared envelope, materializations, and
transmission rules as one document, then project the accepted generation to a
PrecomputeConfigSet. The agg_id is the exact backend-issued materialization
fingerprint and the config-set version is plan_version.

The complete schema and runtime matrix are described in
[Physical-plan MVP contract](physical-plan-mvp.md). The golden wire document is
[collector_plan_v1.json](../../../asap-precompute-rs/tests/fixtures/collector_plan_v1.json).

## OpAMP sequence

The processor registers custom capability
io.projectasap.collector-plan.v1. ASAPQuery sends a custom message of type
collector_plan.

```text
ASAPQuery                 Collector                   backend
    |                         |                          |
    |-- CollectorPlan ------->| validate + stage         |
    |<-- STAGED --------------|                          |
    |                         |                          |
    |       activation_unix_ms boundary                 |
    |                         |-- atomic runtime swap    |
    |-------------------------|------------------------->| activate
    |<-- APPLIED -------------| runtime acknowledgement |
```

STAGED means the exact (plan_id, plan_version) is decoded, validated, and scheduled; it
lets the controller stage every participant before the common activation
timestamp. APPLIED is sent only after every materialization maps to an installed
executor and the runtime completes one all-shard cutover. Validation or runtime
mapping failure sends FAILED and leaves the current active generation unchanged;
the Collector never acknowledges a partially installed generation. The current
MVP exposes one executor slot per metric and therefore rejects multiple distinct
materializations for the same metric until the runtime registry is keyed by
materialization fingerprint.

The exact response shape is:

```json
{
  "plan_id": 42,
  "plan_version": 7,
  "status": "STAGED"
}
```

The status enum uses uppercase wire values. An optional error is included for
FAILED.

Configure exactly one control transport:

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
      plan_state_file: /var/lib/otelcol/asap/edge-a-plan-state.json
```

## Fail-closed activation

The channel rejects wrong targets, expired generations, duplicate or stale
versions globally, unsupported runtime policies, and malformed or
unknown fields. A future generation remains staged until activation_unix_ms;
polling it early returns no update. `plan_version` is globally monotonic across
plan identities, matching the backend activation state machine and preventing
a new `plan_id` from resetting version ordering. The last APPLIED generation is
atomically persisted in `plan_state_file`, so this ordering survives Collector
restart. A missing or malformed existing state file fails startup closed.

The legacy complete-YAML RemoteConfig/restart path and the untyped HTTP
PrecomputeConfigSet poller are compatibility paths, not physical-plan
authority.
