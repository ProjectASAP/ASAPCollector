# Developing the ASAPQuery ingest engine

Keep Collector-produced summary ingest and backend raw precompute as explicit input
modes. Both resolve immutable SID metadata before state mutation; only the raw mode
applies grouping and aggregation. Verify envelope/schema validation, duplicate and
late windows, raw/precomputed separation, backend window close, and store handoff.
See the [backend ingest guide](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/ingest-engine/README.md).

