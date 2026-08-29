# Collector runtime and deployment lifecycle

## TL;DR

ASAPCollector packages the edge runtime that receives metrics, applies a
controller-selected plan, maintains summaries, and exports summary or raw OTLP
to ASAPQuery-backend. A deployment is ready only when the process is healthy,
the intended plan is applied, and emitted data is visible at the backend.

**Status:** active

**Audience:** developers and operators reviewing the collector deployment
boundary.

## Problem and scope

Building an `asap-otel` binary does not prove that a planned summary is running.
The runtime crosses four independently observable stages: bootstrap, plan
delivery, metric processing, and export. This document defines that lifecycle
and the evidence expected at each boundary. It does not redefine plan fields,
summary algebra, or backend query execution.

## Inputs, outputs, and end-to-end behavior

```text
bootstrap configuration
        |
        v
asap-otel + OpAMP client <---- collector plan ---- ASAPQuery control plane
        |
        +---- raw or summarized OTLP ----> ASAPQuery data plane
        |
        +---- health, logs, and runtime evidence
```

The bootstrap configuration supplies receivers, stable exporter endpoints, and
the OpAMP connection. The collector plan selects metric streams, grouping,
windows, summary parameters, and transmission mode. The output is ordinary or
extended OTLP plus evidence identifying the applied plan version.

## Lifecycle

1. **Build:** `build_asap_otel.sh` applies the tracked patch overlays and builds
   the pinned OpenTelemetry Collector distribution.
2. **Bootstrap:** the process starts from a local collector configuration. This
   configuration must be sufficient to connect to the controller without
   embedding workload-specific planning decisions.
3. **Plan delivery:** OpAMP delivers a collector-plan document. The runtime
   validates its target, schema, ordering, processor configuration, and exporter
   reference before activation.
4. **Atomic activation:** a valid plan replaces the prior active plan as one
   unit. Invalid or stale plans leave the last valid plan active.
5. **Processing and export:** selected observations update the configured
   summary state. Completed or active-window state is emitted according to the
   plan's raw, full, or delta policy.
6. **Verification:** operators correlate plan identity, collector activity, and
   backend visibility. Process health alone is insufficient.

## Design decisions

- The controller owns workload decisions; bootstrap configuration owns stable
  connectivity. Keeping those concerns separate permits replanning without
  rebuilding the collector.
- Patch overlays are the repository's source of truth for OpenTelemetry changes.
  Generated submodule worktrees are build inputs, not independently maintained
  forks.
- A plan is fail-closed. Partially applying a new processor graph can produce
  plausible data under the wrong accuracy or window contract.
- Export endpoints are referenced by configuration rather than carrying
  credentials in a plan.

## Acceptance evidence

A deployment check passes when all of the following can be tied to one run:

- the expected collector and configuration versions started;
- the target agent reports the intended plan as applied;
- selected metric traffic reaches the configured processing path;
- the backend observes compatible plan identity and summary metadata; and
- a declared query returns the expected provenance, freshness, and accuracy.

The multi-node harness records this evidence in its run directory. See the
[MVP demo runbook](../../user_guide/mvp-demo-runbook.md).

## Failure and recovery

A rejected plan must not replace the last valid plan. An unavailable controller
may delay replanning but must not reinterpret the active plan. An unavailable
backend causes export failures or retry/backpressure according to the collector
pipeline configuration; it must not silently switch summary semantics. Recovery
is complete only after plan and query-path evidence agree again.

## Related documents

- [System overview](../cross-cutting/system-overview.md)
- [Collector control-plane design](../control-plane/control-plane-physical-planning.md)
- [Summary aggregation and transmission](collector-transmission-protocol.md)
- [OpAMP configuration push](../../developer_docs/asapcollector/opamp-config-push.md)
