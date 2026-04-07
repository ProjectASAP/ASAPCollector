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

Inspired by TCP connections, we propose a stateful metrics collection protocol
between the data sources (metrics clients) and an agent telemetry pre-computation
engine. The protocol provides **semi-reliable semantics** over existing collection
protocols.

### Semi-Reliable Semantics

Existing protocols already accept that samples may be lost — OTLP defines return
error codes to signal partial or full rejection. Semi-reliable means:

1. We set up **connections** for the metrics labels (metric name + attributes
   are registered once, assigned a UID, and remembered for the connection
   lifetime).
2. Each sample at a different timestamp travels over the underlying protocol.
   If a sample is lost **and the original protocol (e.g. OTLP) has no recovery
   mechanism**, we keep it as lost — we do not add a retransmission layer on
   top of the transport.
3. What *is* reliable is the **connection state**: the UID registry, the
   delta-encoding baseline, and the sequence counter survive transient sample
   loss. If the connection state itself is lost (e.g. agent restart), the
   client re-registers.

In other words, the protocol is *reliable for metadata/state* and
*best-effort for individual samples*, matching the semantics that telemetry
pipelines already expect.

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

The REGISTER phase establishes the "connection" for a metric series. Once a
UID is assigned, all subsequent samples for that series carry only the 4-byte
UID instead of the full metric name + attributes.

### Phase 2: Delta-Aware Delivery

After UID registration, the receiver knows the series schema.
The sender can:
1. Omit unchanged attributes (they're registered)
2. Send delta timestamps (8-byte → 2-byte varint for small deltas)
3. Send delta values (for counters: increment only)

Delta encoding relies on connection state — both sides remember the last
timestamp/value per UID. If a sample is lost, the next delta is computed
against the last *acknowledged* baseline (or the next sample simply carries
an absolute value to resynchronize).

### Phase 3: Loss Handling (semi-reliable)

```
Client                              Agent Collector
  |                                      |
  |--- DATA(uid=42, seq=100, ...) ----->|
  |--- DATA(uid=42, seq=101, ...) ----->|
  |                                      |  (seq=101 lost in transit)
  |--- DATA(uid=42, seq=102, ...) ----->|
  |                                      |
  |   Agent detects gap: seq 100→102     |
  |   No retransmission requested.       |
  |   Agent records gap, continues.      |
  |                                      |
```

Unlike TCP, the protocol does **not** retransmit lost samples. Sequence
numbers are used for:
- **Gap detection**: the agent knows how many samples were lost.
- **Delta resync**: if delta encoding is active, a gap means the delta
  baseline is stale. The next sample after a detected gap carries an
  absolute value (or the agent requests a resync).
- **Observability**: the agent can expose `samples_lost` counters per
  series, feeding the controller's SLA monitoring.

This matches the paper's definition: "if the original protocol has no
recovery mechanism, we keep it as lost."

### Wire Format

**Custom gRPC service** alongside OTLP:
```protobuf
service StatefulMetrics {
  // Phase 1: register a metric series, get a UID.
  rpc Register(RegisterRequest) returns (RegisterResponse);

  // Phase 2+3: stream data points with UIDs and sequence numbers.
  rpc StreamData(stream DataBatch) returns (stream StreamAck);
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
  uint64 sequence = 2;          // per-UID monotonic counter
  oneof timestamp {
    fixed64 absolute_ns = 3;    // first sample or after resync
    uint32  delta_ms = 4;       // delta from previous (varint)
  }
  oneof value {
    double double_value = 5;
    sfixed64 int_value = 6;
    double delta_value = 7;     // delta from previous (counters)
  }
}

message DataBatch {
  repeated DataPoint points = 1;
}

message StreamAck {
  // Per-UID acknowledgement for delta baseline advancement.
  // Empty when no resync is needed.
  repeated UidAck acks = 1;
}

message UidAck {
  uint32 uid = 1;
  uint64 last_sequence = 2;     // highest contiguous seq received
  bool   resync = 3;            // true = send absolute next time
}
```

### Bandwidth Savings Estimate

| Component | Stateless OTLP | Stateful Protocol | Savings |
|-----------|---------------|-------------------|---------|
| Metric name + attrs | 120 B/sample | 0 B (registered) | 100% |
| UID | 0 | 4 B/sample | — |
| Timestamp | 8 B | 2 B (delta varint) | 75% |
| Value | 8 B | 8 B | 0% |
| Sequence | 0 | 2 B (varint) | — |
| **Per-sample total** | **136 B** | **16 B** | **88%** |

At 100K series × 10 sps:
- Stateless: 136 MB/s
- Stateful: 16 MB/s
- Savings: 120 MB/s (88%)

### Relationship to Existing OTLP Error Handling

OTLP already defines error semantics via gRPC status codes
([OTLP spec](https://opentelemetry.io/docs/specs/otlp/),
[OTel error handling](https://opentelemetry.io/docs/specs/otel/error-handling/)):

- **Retryable**: `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`
  (with RetryInfo), HTTP 429/502/503/504 — client should retry with
  exponential backoff.
- **Non-retryable**: `INVALID_ARGUMENT`, `PERMISSION_DENIED`,
  `UNIMPLEMENTED`, HTTP 400 — client must not retry.
- **Partial success**: server may accept some data points and reject others;
  client must not retry the accepted portion.
- **Scope**: OTLP only guarantees reliability "between one pair of
  client/server nodes" — no end-to-end guarantees across multi-hop
  pipelines.

The OTel SDK itself "MUST NOT throw unhandled exceptions" — when an
exporter fails, the data is silently dropped and a counter is incremented.

The stateful protocol **does not replace** this mechanism. It operates
*within* the transport that OTLP (or any other protocol) provides:
- If the transport retries and succeeds, no samples are lost.
- If the transport gives up (e.g. after max retries), the sample is lost,
  and the sequence gap records it.
- The stateful protocol never adds its own retransmission layer — it
  inherits whatever reliability the underlying transport provides.

This is what "semi-reliable" means: reliable for connection state (UIDs,
delta baselines), best-effort for individual samples — matching the
semantics that OTLP and the OTel SDK already provide.

### Implementation Plan

#### Go: New receiver

**File: `receiver/statefulmetricsreceiver/`**
1. gRPC server implementing `StatefulMetrics` service
2. UID registry: `map[string]uint32` (metric_name+attrs hash → uid)
3. Sequence tracking per UID: detect gaps, trigger delta resync
4. Convert DataPoints to `pdata.Metrics` for downstream consumers
5. Expose `stateful_receiver_samples_lost` counter per UID

#### Go: New SDK exporter

**File: `exporter/statefulmetricsexporter/`**
1. On first observation of a series: send REGISTER, cache UID
2. On subsequent: send DATA with UID + sequence number
3. Delta timestamp/value encoding when enabled
4. Handle `UidAck.resync` — switch to absolute encoding for next sample
5. On connection loss: re-register all active UIDs

#### Rust: Controller awareness

Controller config generator should:
1. Detect when stateful protocol is available
2. Generate receiver config for `statefulmetrics` instead of `otlp`
3. Track bandwidth savings in TCO calculation
4. Monitor `samples_lost` for SLA violation detection

### Testing

1. Unit: register → send → verify receiver gets full metric
2. Unit: simulate sequence gap → verify resync triggers absolute encoding
3. Benchmark: stateless OTLP vs stateful at 1K/10K/100K series
4. Integration: SDK → stateful receiver → sketch processor → backend
5. Loss simulation: drop random batches, verify gap counters and resync
