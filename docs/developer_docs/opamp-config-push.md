# OpAMP collection-plan delivery

## Purpose

OpAMP is the delivery channel between the ASAPQuery-backend control plane and
ASAPCollector. It transports the collector portion of a versioned collection
plan and returns application status and evidence.

OpAMP does not analyze queries, choose summaries, or decide aggregation
placement. Query-to-summary planning belongs to
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner), while placement and
transmission-mode decisions belong to the ASAPQuery-backend control plane.

The authoritative behavior and ownership contract is defined in the
[ASAPCollector control-plane design](../design_docs/control-plane-design.md).
This developer document describes only the OpAMP delivery boundary.

## End-to-end ownership

```text
query workload
      |
      v
ASAPPlanner
  query-to-summary planning information
      |
      v
ASAPQuery-backend control plane
  chooses placement and transmission mode
  creates one versioned plan with collector and backend portions
      |                                      |
      | collector portion over OpAMP         | backend portion
      v                                      v
ASAPCollector                         ASAPQuery-backend data plane
  validates and applies                validates and applies
  computes/transmits summaries         ingests summaries and executes queries
      |                                      |
      +-------------- evidence --------------+
```

ASAPCollector never reconstructs ASAPPlanner rules from the received
configuration. It treats the plan as an execution contract.

## OpAMP plan envelope

Every delivered collector plan includes or unambiguously binds:

- target collector identity;
- plan identity and monotonically ordered version;
- plan digest;
- activation and expiry conditions;
- metric selection and retained labels;
- summary family and accuracy parameters;
- aggregation and window rules;
- raw, full-summary, or delta-summary transmission mode; and
- the backend compatibility identity needed for emitted payloads.

Defaults that affect query semantics are resolved before delivery. The
collector must not infer a missing summary parameter, grouping rule,
transmission mode, or expiry policy.

## Delivery and application sequence

1. The ASAPQuery-backend control plane targets a collector and sends its plan
   portion through an OpAMP remote-configuration message.
2. ASAPCollector verifies the target, plan identity, version, digest, lifetime,
   and every requested capability.
3. If any required field or capability is invalid, ASAPCollector rejects the
   entire candidate and preserves the current valid plan.
4. If validation succeeds, ASAPCollector activates the candidate atomically
   for new observations.
5. ASAPCollector reports accepted or rejected status with the plan identity,
   version, digest, and reason.
6. Emitted summary payloads carry the active compatibility identity so the
   ASAPQuery-backend data plane can reject state from a different plan.

An OpAMP transport acknowledgement proves only message delivery. MVP evidence
requires a successful application report plus observations and emitted bytes
attributed to the active plan.

## Version and retry behavior

- Re-delivery of the same plan identity, version, and digest is idempotent.
- Reuse of an identity and version with a different digest is rejected.
- A version older than the active version is stale and rejected.
- A newer invalid version does not replace the active valid version.
- Reconnection does not reset version ordering or make an expired plan valid.
- A plan may continue during disconnection only under its declared
  last-known-good lifetime and the configured maximum disconnect duration.

These rules prevent retries and out-of-order delivery from rolling the
collector back or silently changing query semantics.

## Application mechanism

The contract requires atomic activation but does not require a particular
runtime mechanism. A deployment may apply a plan through safe in-process
reconfiguration or through a supervised collector replacement. Whichever
mechanism is selected must expose the same plan identity, status, transition
time, and failure evidence.

If activation interrupts an open window, the transition must follow the plan's
declared state policy. State produced under incompatible plan versions is not
merged.

## Failure behavior

| Scenario | Required result |
| --- | --- |
| OpAMP message is malformed | Reject it and retain the active valid plan. |
| Requested summary is unsupported | Reject the whole candidate with an explicit capability error. |
| Plan version is stale | Reject it without changing active processing. |
| Application fails after validation | Report failure; do not claim the candidate is active. |
| Control-plane connection is lost | Continue only under the bounded last-known-good policy, then stop or enter the declared safe mode. |
| Backend plan portion is incompatible | Backend rejects emitted state; the run cannot pass MVP validation. |

Failures are tied to the candidate plan identity and remain visible to the
end-to-end harness.

## MVP validation scenario

For each raw, full-summary, and supported delta-summary mode, the harness:

1. records the plan produced by the ASAPQuery-backend control plane;
2. records the OpAMP message identity, version, and digest;
3. waits for ASAPCollector's successful application report;
4. sends observations after the recorded activation time;
5. verifies collector counters and emitted payloads reference that plan;
6. verifies the ASAPQuery-backend data plane applied the compatible backend
   portion and executed the declared summary-based query; and
7. fails on missing, stale, rejected, or previous-run evidence.

A controller response, transport acknowledgement, configuration file, or
process restart by itself is insufficient proof that the plan was applied.

## Non-goals

This document does not define:

- PromQL or SQL parsing;
- query-to-summary mapping or sketch algebra;
- ASAPPlanner rewrite and optimization rules;
- ASAPQuery-backend placement or cost policy;
- summary payload encoding; or
- the runtime-specific implementation of collector reconfiguration.

Those concerns remain owned by their respective planner, control-plane,
data-plane, and summary-design scopes.
