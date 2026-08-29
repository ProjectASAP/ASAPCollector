# Developing physical-plan compilation and delivery

The adapter normalizes ASAPPlanner output and the physical compiler produces two
versioned artifacts: `CollectorPlan` for collection/materialization/transmission and
`BackendPlan` for ingest, storage, routing, and fallback. Compilation is deterministic
for the same Planner result and deployment inventory. Validate both artifacts before
publication, stage them, and activate only when their shared plan version is ready.

Test stable compilation, unsupported candidates, capability/accuracy propagation,
separate destination scopes, partial delivery, duplicate/stale versions, rollback,
and query behavior during replacement. Backend implementation details are in the
[physical compiler guide](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/control-plane-physical-compiler.md).

