# DataCollector — Implementation Progress

_Last updated: 2026-03-14 (SDK pre-aggregation update)_

---

## Overview

The DataCollector project extends the OpenTelemetry Collector and SDK with
approximate/probabilistic sketch-based metric aggregation processors.  Each
sketch type provides a compact data structure for answering statistical queries
over streaming metric data without storing every raw data point.

---

## Sketch Processors — Status Matrix

| Processor | Sketch Algorithm | Query | Batch | Window | SDK delivery | Bench script | Proto definition |
|-----------|-----------------|-------|-------|--------|--------------|--------------|------------------|
| `ddsketchprocessor` | DDSketch (DataDog) | Quantiles | ✅ | ✅ | ✅ **pre-aggregated** (SDK histogram) | ✅ | ✅ |
| `kllprocessor` | KLL (Karnin–Lang–Liberty) | Quantiles | ✅ | ✅ | ✅ **pre-aggregated** (SDK histogram) | ✅ | ✅ |
| `countsketchprocessor` | CountSketch | Frequency / heavy hitters | ✅ | ✅ | ✅ **pre-aggregated** (SDK histogram) | ✅ | ✅ |
| `countminsketchprocessor` | Count-Min Sketch | Frequency estimation | ✅ | ✅ | ✅ **pre-aggregated** (SDK histogram) | ✅ | ✅ |
| `hllprocessor` | HyperLogLog | **Cardinality** | ✅ | ✅ | ✅ **pre-aggregated** (SDK histogram) | ✅ | ✅ (proto only) |

---

## Completed (2026-03-14, update 2) — SDK pre-aggregation for all sketch types

All sketch processors now use the **SDK pre-aggregation path** (`sdkSketch` delivery):
the SDK builds the sketch before export and ships it as a typed metric data point
(`KLLSketch`, `CountSketch`, `CountMinSketch`, `HLLSketch`).
The collector-side processor deserializes the incoming sketch bytes and merges them
into its own per-series or per-window sketch, then emits the aggregated result.

Previously only DDSketch used this path; the others used `sdkGauge` (raw `LastValue`).

### `fakemetricload/main.go` — delivery mode change

| Sketch | Before | After |
|--------|--------|-------|
| `ddsketch` | `sdkSketch` + `AggregationDDSketch` | unchanged |
| `kll` | `sdkGauge` + `AggregationKLLSketch` | **`sdkSketch`** + `AggregationKLLSketch` |
| `countsketch` | `sdkGauge` + `AggregationCountSketch` | **`sdkSketch`** + `AggregationCountSketch` |
| `countminsketch` | `sdkGauge` + `AggregationCountMinSketch` | **`sdkSketch`** + `AggregationCountMinSketch` |
| `hll` | `sdkGauge` + `AggregationLastValue` | **`sdkSketch`** + `AggregationHLLSketch` |

### Processor changes

**`kllprocessor/processor.go`**
- Handles `MetricTypeKLLSketch` in both batch and window modes.
- Deserializes incoming sketch bytes with `kll.DeserializeKLLSketchFromBytes` and
  merges into the per-series `*kll.KLLSketch` via `Merge`. Quantile output unchanged.
- Added `replace go.opentelemetry.io/collector/pdata` in `go.mod`.

**`countsketchprocessor/processor.go`**
- Handles `MetricTypeCountSketch` in `processMetrics`.
- Each incoming CountSketch dp counts as one observation (value = dp count) for the
  row (metric-name) and col (host-name) frequency sketches.
- Added `replace go.opentelemetry.io/collector/pdata` in `go.mod`.

**`countminsketchprocessor/processor.go`**
- Handles `MetricTypeCountMinSketch` in `ingestMetric`.
- Added `deserializeCMS` (decodes the gob snapshot format shared by SDK and processor)
  and `mergeWindowSketch` (merges incoming CMS into the per-aggregation-key window CMS).
- Added `replace go.opentelemetry.io/collector/pdata` in `go.mod`.

**`hllprocessor/processor.go`**
- Handles `MetricTypeHLLSketch` in both batch and window modes (added alongside the
  existing `MetricTypeGauge` path which remains for backward compatibility).
- Deserializes with `hll.DeserializeHyperLogLogFromBytes` via a `mergeSketchBytes`
  helper and merges into the per-series `*hll.HyperLogLog`.
- Added `replace go.opentelemetry.io/collector/pdata` in `go.mod`.

### Serialization compatibility

| Sketch | SDK serialization | Processor deserialization |
|--------|-------------------|--------------------------|
| KLL | `kll.SerializeToBytes()` (sketchlib-go canonical) | `kll.DeserializeKLLSketchFromBytes` |
| CountSketch | `cs.SerializeToBytes()` (sketchlib-go canonical) | `cs.DeserializeCountSketchFromBytes` |
| CountMinSketch | custom gob of `countMinSketchSnapshot{Rows,Cols,Count,Sum,Sum2,L1,L2}` | `deserializeCMS` (same snapshot struct) |
| HLL | `hll.SerializeToBytes()` (sketchlib-go canonical) | `hll.DeserializeHyperLogLogFromBytes` |

---

## Completed in this session (2026-03-14)

### 1. `hllprocessor` — new collector-side processor

**Location:** `opentelemetry-collector-contrib-patch/processor/hllprocessor/`

| File | Description |
|------|-------------|
| `processor.go` | Core processor: batch + window modes, cardinality output, `transmit_sketch` attribute embedding |
| `config.go` | Config struct with `mode`, `window_duration`, `transmit_sketch`, `drop_original`, `metric_suffix` |
| `factory.go` | OTel component factory; component type `"HLL"` |
| `go.mod` | Module definition mirroring `countminsketchprocessor` deps |
| `processor_test.go` | Unit tests: batch cardinality output, transmit_sketch, custom suffix, window flush, window merge, race-free concurrent access |

**Behaviour:**
- Consumes `Gauge` metrics (float64 data points).
- Each distinct float64 value is inserted into a per-series `HyperLogLog` sketch
  from `github.com/ProjectASAP/sketchlib-go/sketches/HLL` (precision=14, ~16 KiB
  per sketch, ~0.8% relative error).
- **Batch mode** — aggregates all data points in a single `ConsumeMetrics` call;
  emits one `Gauge` data point per series with the cardinality estimate, then
  passes the augmented batch downstream.
- **Window mode** — accumulates across batches until a tumbling window expires
  (`window_duration`), then flushes the cardinality estimates.
- **`transmit_sketch=true`** — embeds the serialized HLL registers
  (`hll.sketch_payload`), precision (`hll.precision`), and cardinality estimate
  (`hll.cardinality`) as Gauge data point attributes for downstream consumers
  that want to merge sketches before reading cardinality.
- Output metric name: `<input>_hll_cardinality` (configurable via `metric_suffix`).

### 2. `cmd/hllcol/` — collector command configs

**Location:** `opentelemetry-collector-contrib-patch/cmd/hllcol/`

| File | Description |
|------|-------------|
| `build-config.yaml` | OCB builder config (minimal — mirrors `cmd/kll/`) |
| `config.yaml` | Batch mode — OTLP → HLL → Prometheus |
| `config-window.yaml` | Window mode (10 s) — OTLP → HLL → Prometheus |
| `config-bench.yaml` | Benchmark mode — OTLP → HLL+batch → nop; telemetry on :8888 |

### 3. `cmd/bench.sh` — benchmark script extended

Added four new processor variants:

| Variant | Description |
|---------|-------------|
| `hllcol-batch` | pdata load generator → HLL batch processor |
| `hllcol-window` | pdata load generator → HLL window processor |
| `hllcol-sdk-batch` | `fakemetricload --sketch-type=hll` → HLL batch processor |
| `hllcol-sdk-window` | `fakemetricload --sketch-type=hll` → HLL window processor |

Also added an `[HLL CHECK]` correctness assertion that verifies `*_hll_cardinality`
metrics appear on the Prometheus endpoint after each benchmark run.

### 4. `opentelemetry-app/cmd/fakemetricload/main.go` — HLL support

Added `hll` as a valid `--sketch-type` value.  Uses `sdkGauge` (LastValue)
delivery: the SDK emits raw float64 gauge data points; the collector-side
`hllprocessor` accumulates them into the cardinality sketch.

---

## Pre-existing unstaged changes (not from this session)

These were already present in the working tree at the start and remain uncommitted:

| Location | Change |
|----------|--------|
| `opentelemetry-app/go.mod` + `go.sum` | Upgrade `go.opentelemetry.io/otel` → v1.41.0; add `sketchlib-go`, prometheus, grafana/regexp, zeebo/xxh3 deps |
| `opentelemetry-proto-patch/.../metrics.proto` | Proto definitions for `KLLSketch`, `CountSketch`, `CountMinSketch`, `HLLSketch` (fields 14–17) |
| `opentelemetry-collector-contrib-patch/cmd/bench.sh` | SDK benchmark variants for DDSketch/KLL/CountSketch/CountMinSketch (pre-existing) |
| Submodules (collector, collector-contrib, otel-go, proto, telegraf) | Dirty — contain in-progress local modifications |

---

## Known gaps / future work

### HLL pdata types (collector-internal layer)

The `opentelemetry-proto-patch` proto file defines `HLLSketch` at field 17, but
the pdata layer (`opentelemetry-collector-patch/pdata/`) has not been updated.
This means:

- There is **no `MetricTypeHLLSketch`** constant in `pmetric.MetricType`.
- The collector cannot natively route or inspect HLLSketch-typed metric
  payloads from the wire.
- The current `hllprocessor` works around this by outputting plain `Gauge`
  metrics (same approach as `kllprocessor`, `countsketchprocessor`, and
  `countminsketchprocessor`).

To add full pdata support (analogous to `MetricTypeDDSketch`), the following
generated files in `opentelemetry-collector-patch/pdata/` would need to be
created/updated:

| File | Action |
|------|--------|
| `pdata/internal/generated_enum_hllsketchencoding.go` | New — HLLSketchEncoding enum |
| `pdata/internal/generated_proto_hllsketch.go` | New — HLLSketch internal struct + proto marshal/unmarshal |
| `pdata/internal/generated_proto_hllsketchdatapoint.go` | New — HLLSketchDataPoint internal struct |
| `pdata/internal/generated_proto_metric.go` | Update — add `Metric_HLLSketch` variant + pool + marshal/unmarshal at field 17 |
| `pdata/pmetric/metric_type.go` | Update — add `MetricTypeHLLSketch` |
| `pdata/pmetric/generated_metric.go` | Update — add `HLLSketch()` / `SetEmptyHLLSketch()` methods |
| `pdata/pmetric/generated_hllsketch.go` | New — public HLLSketch wrapper |
| `pdata/pmetric/generated_hllsketchdatapoint.go` | New — public HLLSketchDataPoint |
| `pdata/pmetric/generated_hllsketchdatapointslice.go` | New — slice wrapper |
| `pdata/pmetric/hllsketch_encoding.go` | New — HLLSketchEncoding public enum |

### SDK-level HLL aggregation

There is no `AggregationHLLSketch` type in
`opentelemetry-go-patch/sdk/metric/aggregation.go`.  Adding one would enable
the SDK to pre-aggregate recorded values into an HLL sketch before export,
analogous to `AggregationDDSketch`.  This would allow a single compressed
cardinality sketch per series per export window instead of one gauge value.

### Integration tests

`hllprocessor` has unit tests but no integration test (cf. `kllprocessor/integration_test.go`).
An integration test would spin up a full collector binary and verify end-to-end
cardinality output via the Prometheus scrape endpoint.

---

## Architecture summary

```
Load generator (fakemetricload / otel_collector_benchmark)
        │
        │  OTLP gRPC (Gauge data points, float64 values)
        ▼
OpenTelemetry Collector (custom build via OCB)
  ├── receiver/otlpreceiver
  ├── processor/hllprocessor   ← new
  │       Insert float64 → HyperLogLog per series
  │       Flush: emit Gauge{value=cardinality_estimate}
  ├── processor/batchprocessor
  └── exporter/prometheusexporter | nopexporter
        │
        │  Prometheus scrape
        ▼
Prometheus / Grafana
  Metric: <name>_hll_cardinality{host="...", metric="..."} <estimate>
```
