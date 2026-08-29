# Design documents

This directory is the unified architecture index for ASAPCollector,
ASAPQuery-backend, and their control-plane boundary. Each software component
has one primary design document here; backend links point to the code-owned
detail in ASAPQuery-backend.

## Data plane

| Owner | Component | Primary design |
| --- | --- | --- |
| ASAPCollector | Collection phase | [Collection protocol and configuration](collector-collection-phase.md) |
| ASAPCollector | Summary transmission | [Full/delta transmission protocol](collector-transmission-protocol.md) |
| Cross-system | Materialization identity | [Summary series ID](summary-series-id.md) |
| ASAPQuery-backend | Summary store | [Summary store engine](query-summary-store-engine.md) |
| ASAPQuery-backend | Query execution | [Query engine](query-query-engine.md) |
| ASAPQuery-backend | Ingest/precompute | [Ingest engine](query-ingest-engine.md) |

## Control plane

| Component | Primary design |
| --- | --- |
| Planner integration and physical compilation | [Physical planning](control-plane-physical-planning.md) |
| Query/data workload collection | [Planner workload inputs](control-plane-workload-inputs.md) |
| Runtime evidence and replanning | [Accuracy feedback](control-plane-runtime-accuracy-feedback.md) |

Supporting system documents are [system overview](system-overview.md),
[runtime and deployment lifecycle](runtime-deployment-lifecycle.md), and the
[future summary/compression design](future-summary-and-compression.md).
