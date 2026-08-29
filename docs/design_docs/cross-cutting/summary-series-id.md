# Stored-series identity at the Collector boundary

## TL;DR

ASAPCollector produces stored series for ASAPQuery-backend. A stored series may
contain a summary, an exact aggregate, or raw/pass-through samples. Its series
ID (`sid`) therefore identifies the stored representation, not necessarily the
original source time series.

The backend is the SID authority. Collector initially sends identity evidence
with `series_id = 0`, caches the backend's `SeriesAssignment`, and later sends
ID-only points. When the backend reports `unknown_series_ids`, Collector evicts
those cached IDs and sends the full attributes again.

**Status:** active implementation

**Audience:** developers and architects working across ASAPCollector and
ASAPQuery-backend.

## Ownership boundary

Collector owns:

- source resource/scope/metric extraction;
- plan-directed grouping and removal of unretained dimensions;
- deterministic descriptor and attribute keys;
- retaining labels until the backend confirms an assignment;
- caching confirmed SIDs and emitting ID-only points; and
- evicting stale assignments on backend feedback.

ASAPQuery-backend owns canonical stored identity, SID allocation, conflict
detection, durable resolver state, storage registration, and query label
reconstruction. The complete backend model is documented in
[ASAPQuery-backend series identity](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/docs/developer_docs/summary-series-id/design.md).

## What SID names

```text
stored series = canonical metric
              + retained stored labels
              + materialization kind
```

Materialization kind separates DDSketch from KLL, summary from raw, and
different parameters or filters. Full/delta encoding and logical window are
records beneath the stored series and do not create new SIDs.

The backend stores one `SketchInstanceMetadata` descriptor per SID:

| Field | Meaning |
| --- | --- |
| `metric_name` | Canonical source metric used by query matching |
| `group_by_keys` | Label names retained by edge/backend aggregation |
| `capability` | Quantile, cardinality, frequency, top-k, or exact/raw support |
| `agg_kind` | Sketch, exact-aggregation, or raw materialization and compatible parameters |
| `accuracy` | Derived sketch bound; absent for exact-aggregation and raw state |
| `policy_fp` | Content fingerprint of the policy that produced the SID |
| lifecycle timestamps | First seen, retired, and expiry times |

This descriptor is immutable for the lifetime of a SID. A policy change that
alters grouping, capability, aggregation semantics, or accuracy creates a new
SID; an encoding change may retain the SID only when the representation remains
semantically compatible and is versioned on each record.

| Plan result | Stored labels | Stored kind |
| --- | --- | --- |
| DDSketch grouped by `service` | `service` | DDSketch and α |
| Exact Sum grouped by `service` | `service` | exact Sum parameters |
| Raw pass-through | all labels required by exact series semantics | raw |

The current warm Backend store has sketch and exact-aggregate payload variants.
Raw samples use the configured archive/pass-through path, but their ID still
follows the stored-series concept and must not reuse a summary SID.

## Collector key formation

`asap-precompute-go` builds its internal series key from:

```text
aggregation ID | sorted resource labels | grouped point labels
```

When `aggregateBy` is empty, point labels are sorted by key. When it is set,
only listed labels are included in validated plan order; missing labels are
skipped. Resource labels remain in the Collector-local routing key. The emitted
modified-OTLP summary point carries the post-aggregation point attributes used
by Backend stored-series resolution.

Reserved transport fields are stripped before key formation. They describe
delivery, not user-visible series identity.

## Concrete summary example

Input:

```text
resource: service.name="checkout", cloud.region="us-east"
metric:   request_duration_seconds
labels:   method="POST", status="200", pod="checkout-7f9c"
plan:     group by [method,status], DDSketch α=0.01, 60s window
```

Collector routes observations conceptually as:

```text
resource = cloud.region=us-east;service.name=checkout;
group    = method=POST;status=200;
```

`pod` is folded away by the declared grouping. The first summary export is:

```text
metric.name = request_duration_seconds_ddsketch
attributes  = {method="POST", status="200"}
series_id   = 0
window      = [12:00,12:01)
encoding    = PROTO_FULL
config      = DDSketch(relative_accuracy=0.01)
payload     = <bytes>
```

Backend normalizes the family-suffixed metric, attributes, and kind, then might
allocate SID 42. The response contains a `SeriesAssignment` with descriptor
keys, the attribute fingerprint, and `series_id = 42`.

After Collector applies it, the next frame can be:

```text
metric.name = request_duration_seconds_ddsketch
attributes  = {}
series_id   = 42
window      = [12:01,12:02)
```

Labels are cleared only after confirmation. Before confirmation, they remain in
the aggregation/export state so retries and intermediate Collector hops do not
lose recovery evidence.

## Concrete raw example

For raw pass-through:

```text
http_requests_total{
  service="checkout",pod="checkout-7f9c",method="POST",status="200"
}
```

the plan retains every label needed for exact source-series semantics. A raw SID
can identify this stored raw sample series. A summary grouped across pods has no
`pod` label and a different materialization kind, so it receives a different
SID even though both originate from `http_requests_total`.

## Exporter dictionary

The patched OTLP metric exporter keeps a dictionary keyed by:

```text
resource descriptor
+ instrumentation-scope descriptor
+ metric type/name
+ deterministic point-attribute fingerprint
```

Each entry tracks a numeric ID, whether Backend confirmed it, and the last
export generation in which it was seen. Unconfirmed entries keep SID zero and
retain attributes. Confirmed entries use Backend's ID and suppress repeated
attributes. Entries absent for five export generations are swept to bound
memory; seeing the series again restarts attribute-carrying registration.

The dictionary may be disabled with `WithSeriesDictionary(false)` for a naive
raw baseline. This matters in benchmarks: dictionary label savings must not be
misreported as summary compression savings.

## Wire state machine

| Collector sends | Backend outcome | Collector next action |
| --- | --- | --- |
| SID 0 + attributes | Resolve/mint and return assignment | Cache assignment |
| Non-zero SID + no attributes | Accept if registered | Continue ID-only |
| Non-zero SID + attributes | Validate evidence; report conflict | Apply canonical assignment and evict stale ID |
| Unknown non-zero SID | Drop frame and return `unknown_series_ids` | `EvictByID`, then resend attributes |

A global aggregation may have an empty attribute fingerprint; its metric and
materialization metadata still define a valid stored series. An unknown ID-only
point cannot be recovered without evidence and must not be guessed.

## Restart and multi-hop behavior

If Backend recovers its resolver WAL, cached Collector IDs remain valid. If it
restarts without resolver state, it reports cached IDs as unknown and Collector
re-registers. The rejected frame is not silently rebound.

For agent → gateway → Backend deployments, every modified-OTLP hop must preserve
the same resource, scope, metric, type, and attribute-fingerprint contract.
Either the final authority's assignment must flow back to the producer or the
intermediate hop must retain enough attributes to recover. A hop-local numeric
ID is not automatically meaningful to another authority.

## Full/delta relationship

SID identifies the stored series; delta lineage identifies state transitions
within a window. A delta must additionally carry or imply compatible plan,
window, producer, base/checkpoint, and sequence information. Reusing SID 42
does not make a delta safe if any of those fields disagree.

Collector retains a periodic full-checkpoint policy so Backend can recover from
loss or cache divergence. KLL uses full-state transmission in the MVP because
no compact order-independent delta contract is claimed.

## Guarantees and limitations

Guaranteed:

- SID zero means unassigned;
- attributes remain until assignment is confirmed;
- label order does not change Collector descriptor identity;
- stale IDs are evicted on Backend feedback;
- raw and summary materializations do not intentionally share SIDs; and
- SID does not replace plan/window/delta compatibility checks.

Current limitations:

- SID tenant and registry-version namespaces are not explicit on the wire;
- IDs are allocated by a Backend authority and are not portable between
  independent Backends;
- dictionary state is in memory and relearns after Collector restart; and
- the bidirectional assignment fields are an ASAP modification to OTLP, so
  unpatched hops do not provide this optimization/recovery protocol.

## Acceptance tests

Cross-repository tests must prove first-export assignment, ID-only steady state,
label-order stability, raw/summary separation, family/config separation,
unknown-ID eviction, Backend restart with and without resolver persistence,
multi-hop attribute preservation, and query result label reconstruction.
