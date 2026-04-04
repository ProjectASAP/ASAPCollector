# Stateful Metrics Collection Protocol (Paper §4.5)

## Problem

Current telemetry protocols (OTLP, Prometheus exposition, InfluxDB line protocol)
are **stateless**: every sample includes the full metric name and attributes.

For a metric like:
```
container_cpu_usage_seconds_total{namespace="prod",pod="web-abc123",container="app",node="node-42"}
```

The metric name + attributes are ~120 bytes. The actual value + timestamp are ~16 bytes.
At 10 samples/sec, that's **1.2 KB/sec of redundant metadata** per series.

At 100K series × 10 sps = **1.2 GB/sec of redundant metadata**.

## Design: Stateful Collection Protocol

### Phase 1: UID Registry (connection setup)

```
Client                              Agent Collector
  |                                      |
  |--- REGISTER(metric_name, attrs) ---->|
  |<-- REGISTER_ACK(uid=42) ------------|
  |                                      |
  |--- REGISTER(metric_name, attrs) ---->|
  |<-- REGISTER_ACK(uid=43) ------------|
  |                                      |
  |--- DATA(uid=42, ts, val) ---------->|   # 16 bytes, not 136
  |--- DATA(uid=43, ts, val) ---------->|
  |--- DATA(uid=42, ts, val) ---------->|
  |                                      |
```

### Phase 2: Delta-Aware Delivery

After UID registration, the receiver knows the series schema.
The sender can:
1. Omit unchanged attributes (they're registered)
2. Send delta timestamps (8-byte → 2-byte varint for small deltas)
3. Send delta values (for counters: increment only)

### Phase 3: Semi-Reliable Delivery

```
Client                              Agent Collector
  |                                      |
  |--- BATCH(seq=100, [DATA...]) ------>|
  |<-- ACK(seq=100) -------------------|
  |                                      |
  |--- BATCH(seq=101, [DATA...]) ------>|
  |    (timeout, no ACK)                 |
  |--- BATCH(seq=101, [DATA...]) ------>|   # retry
  |<-- ACK(seq=101) -------------------|
  |                                      |
  |--- BATCH(seq=102, [DATA...]) ------>|
  |<-- NACK(seq=102, reason) ----------|   # explicit rejection
  |                                      |
```

### Wire Format

Option A: **OTLP extension** — add a new `RegisterRequest` / `RegisterResponse`
RPC to the existing OTLP gRPC service.

Option B: **Custom gRPC service** — separate service alongside OTLP:
```protobuf
service StatefulMetrics {
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc SendBatch(BatchRequest) returns (BatchResponse);
  rpc StreamData(stream DataPoint) returns (stream Ack);
}

message RegisterRequest {
  string metric_name = 1;
  repeated KeyValue attributes = 2;
}

message RegisterResponse {
  uint32 uid = 1;
}

message DataPoint {
  uint32 uid = 1;
  fixed64 timestamp_ns = 2;
  oneof value {
    double double_value = 3;
    sfixed64 int_value = 4;
  }
}

message BatchRequest {
  uint64 sequence = 1;
  repeated DataPoint points = 2;
}

message BatchResponse {
  uint64 sequence = 1;
  bool accepted = 2;
  string error = 3;
}
```

### Bandwidth Savings Estimate

| Component | Stateless OTLP | Stateful Protocol | Savings |
|-----------|---------------|-------------------|---------|
| Metric name + attrs | 120 B/sample | 0 B (registered) | 100% |
| UID | 0 | 4 B/sample | — |
| Timestamp | 8 B | 2 B (delta varint) | 75% |
| Value | 8 B | 8 B | 0% |
| **Per-sample total** | **136 B** | **14 B** | **90%** |

At 100K series × 10 sps:
- Stateless: 136 MB/s
- Stateful: 14 MB/s
- Savings: 122 MB/s (90%)

### Implementation Plan

#### Go: New receiver

**File: `receiver/statefulmetricsreceiver/`**
1. gRPC server implementing `StatefulMetrics` service
2. UID registry: `map[string]uint32` (metric_name+attrs hash → uid)
3. Batch processor: accumulate DataPoints, convert to pdata.Metrics
4. Pass to next consumer (sketch processor)

#### Go: New SDK exporter

**File: `exporter/statefulmetricsexporter/`**
1. On first observation of a series: send REGISTER, cache UID
2. On subsequent: send DATA with UID only
3. Batch + sequence numbering for semi-reliable delivery
4. Retry on timeout, handle NACK

#### Rust: Controller awareness

Controller config generator should:
1. Detect when stateful protocol is available
2. Generate receiver config for `statefulmetrics` instead of `otlp`
3. Track bandwidth savings in TCO calculation

### Testing

1. Unit: register → send → verify receiver gets full metric
2. Benchmark: stateless OTLP vs stateful at 1K/10K/100K series
3. Reliability: inject packet loss, verify retry semantics
4. Integration: SDK → stateful receiver → sketch processor → backend
