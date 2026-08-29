# Developing the ASAPQuery query engine

Treat the installed BackendPlan as the execution authority. Preserve tenant, query
range, grouping, SID, accuracy/freshness requirements, and fallback reason through
classification, routing, storage reads, merge, and response construction. Add tests
for exact coverage, overlap, gaps, retired SIDs, incompatible parameters, archive
fallback, and plan replacement. See the
[backend query-engine guide](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/query-engine/README.md).

