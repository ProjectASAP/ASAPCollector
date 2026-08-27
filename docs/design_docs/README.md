# Design docs

Design docs capture the problem, goals, constraints, proposed behavior, and trade-offs
for the ASAPCollector system. They are written for readers who need to understand or
review a design without first reading the implementation.

Every document starts with a short TL;DR, an explicit lifecycle status, and its
relationship to the MVP. These documents are design specifications: implementation
details, code walkthroughs, and links to current implementation files do not belong
here. A document may reference another design scope when that relationship is needed,
but each file owns the scope named by its title.

## System and deployment

- [System overview](system-overview.md)
- [Edge precompute framework](design-asap-edge-framework.md)
- [Control plane design](control-plane-design.md)
- [Controller optimization problem](controller-optimization-problem.md)
- [Stateful metrics protocol](design-stateful-protocol.md)
- [ASAP and related systems](comparison-asap-vs-databricks-pantheon-hydra.md)

## Aggregation, sketches, and transmission

- [Delta transmission](delta-transmission-design.md)
- [CMS/CountSketch transmission](cms-cs-delta-transmission-optimizations.md)
- [Continuous monitoring](continuous-windows-related-work.md)
- [Aggregation taxonomy](continuous-monitoring-aggregation-taxonomy.md)
- [Tumbling-window cost analysis](continuous-monitoring-tumbling-cost-analysis.md)
- [Distributed coordinated sampling](distributed-nitrosketch-coordinated-sampling.md)
- [GOS unified telemetry](design-gos-unified-edge-telemetry.md)
- [Multivariate anomaly detection](multivariate-anomaly-frequent-directions.md)
- [Serf compression](serf-compression-architecture.md)

## Evidence and research context

- [SDK cost evaluation](sdk-cost-evaluation.md)
- [Sampling and GOS derivations](sampling-cdm-gos-derivations.md)
- [Benchmark: raw vs. sketches](benchmark-delta-vs-raw-baseline-2026-03-28.md)
- [Benchmark: SDK sampling and KLL](benchmark-sdk-sampling-kll-cost-2026-08-04.md)
- [Use-case and dataset survey](use-case-dataset-survey.md)
- [Paper outline](paper-outline.md)
