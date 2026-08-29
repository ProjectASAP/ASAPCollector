# Planner workload inputs

> Status: architectural contract; collection adapters evolve independently

ASAPPlanner consumes three conceptually separate inputs: query workload, data
workload, and planning horizon. Query workload records normalized query shapes,
frequency, time range, grouping, and required accuracy/freshness. Data workload
records metric/label cardinality, arrival rate, value distribution, lateness,
retention, and deployment locality. The horizon determines how recurring costs and
benefits are compared.

Collectors and query frontends publish observations with tenant, interval, source,
schema version, and sampling/aggregation method. The control plane validates and
normalizes them into a versioned planning snapshot. Missing statistics are marked
unknown; they are not converted to zero. ASAPPlanner remains authoritative for
candidate construction and global selection; this contract follows its
[design at `d31a566`](https://github.com/ProjectASAP/ASAPPlanner/blob/d31a5667ea2d2bd5f2823cc01115e74b9895bd0f/docs/design_docs/README.md).

