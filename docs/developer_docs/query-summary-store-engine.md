# Developing the ASAPQuery summary store engine

This system-level guide fixes the integration contract: register immutable SID
metadata before accepting payloads; append records with explicit window coverage and
payload kind; publish durability before eviction; and preserve typed missing/stale/
gapped/incompatible read outcomes. Validate lifecycle transitions and recovery with
Collector retries and plan replacement. Code-level interfaces live in the
[backend developer guide](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/summary-store-engine.md).

