# Stateful Metrics Collection Protocol

<!-- Design metadata -->

## TL;DR

Stateful metadata transmission model that reduces repeated series identity data.

**Status:** draft

**MVP relationship:** future.

This document is design-level: it defines scope, behavior, constraints, and trade-offs; implementation details are intentionally out of scope.


## Problem

Current telemetry protocols (OTLP, Prometheus exposition, InfluxDB line protocol)
are **stateless**: every sample includes the full metric name and attributes.

For a metric like:
```
container_cpu_usage_seconds_total{namespace="prod",pod="web-abc123",container="app",node="node-42"}
```

The metric name + attributes are ~120 bytes. The actual value + timestamp are ~16 bytes.
At 100K series × 10 sps = **1.2 GB/sec of redundant metadata**.

## Design: UID ↔ TSID Registry

Inspired by TCP connections, we add a **stateful UID registry** on top of the
existing OTLP protocol. The design is simple:

1. **First export**: client sends full attributes. The collector (receiver)
   assigns a `series_id` (UID) and returns it in the `ExportMetricsServiceResponse`.
2. **Subsequent exports**: client sends only the `series_id`, omitting attributes
   entirely. The collector looks up the cached attributes from the registry.

This is **not a new protocol** — it extends OTLP with a `series_id` field on
every data point and a `SeriesAssignment` list in the response. No sequence
numbers, no retransmission, no changes to OTLP's existing error handling.

### Semi-Reliable Semantics

The protocol is semi-reliable in the paper's sense:
- **Reliable for connection state**: the UID ↔ TSID mapping survives across
  exports and is maintained for the connection lifetime (default TTL: 1 hour).
- **Best-effort for samples**: if a sample is lost (OTLP returns a retryable
  or non-retryable error), the existing OTLP error handling applies — we do
  not add any recovery mechanism on top. See [OTLP error spec](https://opentelemetry.io/docs/specs/otlp/)
  and [OTel error handling](https://opentelemetry.io/docs/specs/otel/error-handling/).

### Format-Agnostic

The UID registry works for **any metric type** — raw samples (Gauge, Sum,
Histogram) and sketch payloads (DDSketch, KLL, HLL, CountSketch, CountMinSketch)
alike. The `series_id` field exists on every data point type in the proto
definition. It is not tied to any specific sketch or aggregation.

### Configurable

The feature is designed to be **opt-in and configurable**:
- When `series_id == 0` on a data point, the receiver falls back to the
  standard stateless path (full attributes required).
- When the exporter has a cached `series_id`, it omits attributes and sends
  only the ID, saving ~120 bytes per sample.
- The controller can enable/disable this in the generated collector config.

## Implementation Status

The UID registry is **already implemented** across three layers:

### 1. Proto: `series_id` field on data points

**File**: `opentelemetry-proto/.../metrics.proto`

Every data point type (NumberDataPoint, HistogramDataPoint, DDSketchDataPoint,
KLLSketchDataPoint, etc.) has a `series_id` field:

```protobuf
message NumberDataPoint {
  repeated KeyValue attributes = 7;
  uint64 series_id = 16;  // collector-assigned UID
  // ...
}
```

The `ExportMetricsServiceResponse` includes series assignments:

```protobuf
message ExportMetricsServiceResponse {
  ExportMetricsPartialSuccess partial_success = 1;
  repeated SeriesAssignment series_assignments = 2;
}

message SeriesAssignment {
  string resource_key = 1;
  string scope_key = 2;
  string metric_name = 3;
  string metric_type = 4;
  bytes attributes_fingerprint = 5;
  uint64 series_id = 6;
}
```

### 2. Collector receiver: series cache + rehydration

**File**: `opentelemetry-collector/receiver/otlpreceiver/internal/metrics/series_cache.go`

The OTLP receiver maintains a `seriesCache` that:
- **On first sight** (data point has attributes but no `series_id`): assigns a
  new UID, stores `{uid → (metric_name, type, attributes)}`, returns the
  assignment in the response.
- **On subsequent** (data point has `series_id` but no attributes): looks up
  the cached attributes and "rehydrates" the data point before passing to
  downstream consumers (processors, exporters).
- **TTL eviction**: entries not seen for 1 hour are evicted.

This is transparent to downstream processors — they always see full attributes,
whether the client sent them or the cache filled them in.

### 3. SDK exporter: series dictionary + attribute elision

**File**: `opentelemetry-go/exporters/otlp/.../internal/series/dictionary.go`

The gRPC exporter maintains a `Dictionary` that:
- **Annotate**: before each export, walks all data points and assigns local
  series IDs. On first export, `series_id` stays 0 (full attributes sent).
- **Apply**: when the response comes back with `SeriesAssignment`, marks the
  entry as `registered`.
- **Subsequent exports**: for registered entries, sets `series_id` on the data
  point and clears `Attributes` to empty — the proto serializer then omits
  the attributes entirely.
- **Staleness sweep**: entries not seen for 5 export cycles are evicted.

The `seriesIdentity` helper in the transform layer decides:
```go
func seriesIdentity(attrs []*cpb.KeyValue, seriesID uint64) ([]*cpb.KeyValue, uint64) {
    if seriesID == 0 {
        return attrs, 0    // stateless: send full attributes
    }
    return nil, seriesID   // stateful: send UID only
}
```

## Bandwidth Savings

| Component | Stateless OTLP | With UID Registry | Savings |
|-----------|---------------|-------------------|---------|
| Metric name + attrs | 120 B/sample | 0 B (registered) | 100% |
| UID (series_id) | 0 | 4 B (varint) | — |
| Timestamp | 8 B | 8 B | 0% |
| Value | 8 B | 8 B | 0% |
| **Per-sample total** | **136 B** | **20 B** | **85%** |

At 100K series × 10 sps:
- Stateless: 136 MB/s
- With UID registry: 20 MB/s
- Savings: 116 MB/s (85%)

Note: the first export for each series still sends full attributes. Savings
reach steady state after one export cycle.

## Remaining Work

- [ ] Make the feature configurable via controller-generated collector YAML
      (enable/disable series cache, configurable TTL)
- [ ] Benchmark: measure actual bandwidth reduction at 1K/10K/100K series
      with e2esdkbench, comparing `series_id` enabled vs disabled
- [ ] Integration test: verify the register → elide → rehydrate round-trip
      works end-to-end through the sketch processors
