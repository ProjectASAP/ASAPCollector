# ASAPQuery ingest and precompute engine

> Status: active ingest; backend precompute is an explicit physical choice

The backend ingest engine accepts two inputs: summaries already materialized by a
Collector, and raw samples selected for backend-side precomputation. For the first it
validates the self-describing envelope and SID metadata before storage. For the
second it applies the BackendPlan grouping/window/aggregation rule, creates the
planned SID state, and sends closed windows to the summary store.

Backend precompute is not an accidental decode fallback. The physical plan must say
where a materialization is computed, and Collector and backend must never both count
the same observation into one window unless the aggregation explicitly defines a
merge topology.

See the [ASAPQuery-backend ingest-engine design](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/design_docs/ingest-engine.md).

