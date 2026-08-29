# Canonical system design

This is the authoritative design index for the complete lifecycle:

```text
collection -> summary transmission -> backend ingest -> summary storage -> query
                              ^ physical plans and runtime feedback |
                              +-------------------------------------+
```

## ASAPCollector

- [Collection-phase protocol and configuration](asapcollector/collector-collection-phase.md)
- [Full/delta summary transmission](asapcollector/collector-transmission-protocol.md)
- [Runtime and deployment lifecycle](asapcollector/runtime-deployment-lifecycle.md)

## ASAPQuery-backend

- [Ingest and backend precompute engine](asapquery-backend/query-ingest-engine.md)
- [Summary store engine](asapquery-backend/query-summary-store-engine.md)
- [Query engine](asapquery-backend/query-query-engine.md)
- [Backend service runtime](asapquery-backend/service-runtime.md)
- [Future backend storage and compression](asapquery-backend/future-storage-and-compression.md)

## Control plane

- [Planner ownership and integration](control-plane/planner-integration.md)
- [Physical planning](control-plane/physical-planning.md)
- [BackendPlan contract](control-plane/backend-plan.md)
- [Query and data workload inputs](control-plane/workload-inputs.md)
- [Runtime accuracy feedback and replanning](control-plane/runtime-accuracy-feedback.md)

## Cross-cutting contracts

- [System overview](cross-cutting/system-overview.md)
- [Summary series ID](cross-cutting/summary-series-id.md)
- [Future summary families and compression](cross-cutting/future-summary-families-and-raw-archive.md)

ASAPQuery-backend and ASAPPlanner may keep code-owned implementation notes, but
they link here for system behavior, component boundaries, and shared contracts.
