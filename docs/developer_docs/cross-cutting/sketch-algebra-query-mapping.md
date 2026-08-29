# Query mapping and sketch algebra

PromQL/SQL parsing, query-to-summary mapping, sketch-algebra IR, rewrite
rules, and planning policy are owned by
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner).

ASAPCollector does not define or duplicate those rules. It consumes the
collector portion of the plan issued by the ASAPQuery-backend control plane,
validates its capabilities, applies the plan, and reports execution evidence.
That collector-side contract is described in
[ASAPCollector control-plane design](../../design_docs/control-plane/control-plane-physical-planning.md).

Use ASAPPlanner as the canonical reference for all query-mapping and algebra
behavior.
