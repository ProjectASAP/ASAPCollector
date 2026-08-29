# ASAPQuery query engine

> Status: active backend component; system-level view

The query engine executes the physical result of planning. ASAPPlanner owns logical
query-to-summary selection; the ASAPQuery control plane installs the compiled
`BackendPlan`; the query engine classifies an incoming query, resolves the selected
SID and time coverage, reads warm/archive/remote sources, reconstructs the requested
operation, and returns a result with accuracy and freshness metadata.

It may fall back only when the installed plan permits it. Unknown SID, inadequate
coverage, incompatible state, and unavailable sources remain explicit failures or
fallback reasons. The query engine never silently substitutes a differently
parameterized summary.

Its implementation boundary is documented in the
[ASAPQuery-backend query-engine design](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/query-engine/design.md).

