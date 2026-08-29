# Developer documents

Use this index to find the implementation boundary for each architecture
component. Collector guides are maintained locally; backend component pages
state the cross-repository contract and link to code-owned Query guides.

## Data plane

| Owner | Component | Developer guide |
| --- | --- | --- |
| ASAPCollector | Collection phase | [Collection phase](collector-collection-phase.md) |
| ASAPCollector | Summary transmission | [Transmission protocol](collector-transmission-protocol.md) |
| Cross-system | Summary series ID | [SID dictionary and recovery](summary-series-id.md) |
| ASAPQuery-backend | Summary store | [Summary store engine](query-summary-store-engine.md) |
| ASAPQuery-backend | Query execution | [Query engine](query-query-engine.md) |
| ASAPQuery-backend | Ingest/precompute | [Ingest engine](query-ingest-engine.md) |

## Control plane

| Component | Developer guide |
| --- | --- |
| Physical compilation and delivery | [Physical planning](control-plane-physical-planning.md) |
| Workload collection | [Workload inputs](control-plane-workload-inputs.md) |
| Empirical feedback | [Runtime accuracy feedback](control-plane-runtime-accuracy-feedback.md) |

Supporting guides cover [build/test/patch workflow](build-test-and-patch-workflow.md),
[OpAMP config push](opamp-config-push.md), and the ownership boundary for
[sketch algebra and query mapping](sketch-algebra-query-mapping.md).
