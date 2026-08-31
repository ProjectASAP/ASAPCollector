# Canonical cross-component developer documents

These guides describe how components integrate and how cross-repository changes
are validated. Private backend and Planner implementation details stay next to
their source code.

## ASAPCollector

- [Collection phase](asapcollector/collector-collection-phase.md)
- [Summary transmission protocol](asapcollector/collector-transmission-protocol.md)
- [Build, test, and patch workflow](asapcollector/build-test-and-patch-workflow.md)
- [OpAMP configuration push](asapcollector/opamp-config-push.md)

## ASAPQuery-backend integration

- [Ingest engine](asapquery-backend/query-ingest-engine.md)
- [Summary store engine](asapquery-backend/query-summary-store-engine.md)
- [Query engine](asapquery-backend/query-query-engine.md)

## Control plane

- [Physical compilation and delivery](control-plane/control-plane-physical-planning.md)
- [Workload collection](control-plane/control-plane-workload-inputs.md)
- [Runtime accuracy feedback](control-plane/control-plane-runtime-accuracy-feedback.md)

## Cross-cutting contracts

- [SID dictionary and recovery](cross-cutting/summary-series-id.md)
- [Sketch algebra and query-mapping ownership](cross-cutting/sketch-algebra-query-mapping.md)
