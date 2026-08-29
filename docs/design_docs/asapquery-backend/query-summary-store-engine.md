# ASAPQuery summary store engine

> Status: active backend component; system-level view

The summary store persists materialized state keyed by SID and time window. It owns
the SID metadata index, window coverage, payload typing, lifecycle state, durable
parts/manifests, recovery, and read consistency. It must not infer payload type from
bytes or merge records whose SID metadata is incompatible.

```text
ingest record -> validate SID metadata -> append window/epoch -> durable manifest
query request -> resolve SID -> select coverage -> decode/merge -> typed result
```

One metadata descriptor per SID contains canonical `metric_name`, retained
`group_by_keys`, query `capability`, `agg_kind` and compatible parameters, derived
`accuracy` for sketches, `policy_fp`, and first-seen/retired/expiry timestamps.
Raw materializations use archive storage; sketch and exact-aggregation payloads use
typed summary storage. Missing, stale, gapped, and incompatible reads are distinct
outcomes, not empty success.

The code-owned design and persistence details live in the
[ASAPQuery-backend summary-store design](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/summary-store-engine/design.md).

