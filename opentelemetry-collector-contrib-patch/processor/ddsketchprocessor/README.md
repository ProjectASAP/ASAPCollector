# DDSketch Processor

The DDSketch processor (`ddsketchprocessor`) merges OTLP DDSketch metrics.
For every DDSketch time series (identified by metric name and attribute set)
it can append a sibling metric named `<original>_ddsketch` (the suffix is
configurable) that contains the union of all DDSketch payloads observed for
that series. Originally the processor only aggregated pre-built DDSketch
metrics; it has been extended to also build sketches from scalar Gauge inputs
and optionally emit quantile summaries.

In addition to this original behavior, the processor now supports:

- Build DDSketches from standard Gauge inputs.
- Operate in two configurable modes that control **when** DDSketch output is
  emitted: per batch or per tumbling window.

## Modes

The `mode` field controls output timing, not input type. Both modes accept
either DDSketch or Gauge metrics:

| Mode     | Input             | Output timing                         |
| -------- | ----------------- | ------------------------------------- |
| `batch`  | Gauge OR DDSketch | Immediately after each incoming batch |
| `window` | Gauge OR DDSketch | Once per tumbling window boundary     |

In real deployments this means, for example, that instead of exporting tens of
thousands of raw values per second, `window` mode can export a small fixed set
of quantile gauges (p50, p90, p99, …) per series every `window_duration`.

## Configuration

### Common fields

| Field               | Description                                                                 | Default          |
| ------------------- | --------------------------------------------------------------------------- | ---------------- |
| `mode`              | Output timing: `"batch"` (per-batch flush) or `"window"` (tumbling window).| `"batch"`        |
| `relative_accuracy` | DDSketch relative accuracy parameter.                                      | `0.01`           |
| `quantiles`         | Quantiles to emit when `transmit_sketch` is `false`.                       | `[0.5, 0.9, 0.99]` |
| `transmit_sketch`   | If `true`, emit DDSketch payload metrics; if `false`, emit quantile gauges.| `true`           |

When `transmit_sketch: false`, at least one quantile must be configured and each
quantile must be in the \[0,1] range.

### Window-specific fields

| Field             | Description                                            | Default |
| ----------------- | ------------------------------------------------------ | ------- |
| `window_duration` | Length of the tumbling aggregation window (e.g. `60s`).| `60s`   |

`window_duration` is only used when `mode: window`. In `mode: batch` it is
ignored.

### Minimal example (batch mode, DDSketch output)

```yaml
processors:
  ddsketch:
    mode: batch
    transmit_sketch: true
```

### Example: batch mode with quantile gauges

```yaml
processors:
  ddsketch:
    mode: batch
    transmit_sketch: false
    relative_accuracy: 0.01
    quantiles: [0.5, 0.9, 0.99]
```

In this mode the processor behaves like the original implementation: for each
batch it merges compatible DDSketch payloads (or builds them from gauges) and
emits sibling metrics such as `<name>_quantile` containing the configured
quantiles.

### Example: window mode (tumbling window aggregation)

```yaml
processors:
  ddsketch:
    mode: window
    window_duration: 60s
    relative_accuracy: 0.01
    transmit_sketch: false
    quantiles: [0.5, 0.9, 0.99]
```

In this mode:

- Input: DDSketch or Gauge metrics (e.g. from `otel_collector_benchmark` or any
  standard OTel SDK).
- The processor groups points by resource, instrumentation scope, metric name,
  and attribute set, accumulating values into DDSketches over each
  `window_duration`.
- At each window boundary it flushes the aggregated sketches as either:
  - DDSketch metrics, if `transmit_sketch: true`, or
  - quantile gauge metrics, if `transmit_sketch: false`.

## Input types

The `mode` field controls **output timing** (when to flush), not the input
format. The processor auto-detects what the SDK is sending:

| SDK input type | OTLP metric type          | Typical client                          |
| -------------- | ------------------------- | --------------------------------------- |
| **sketch**     | `MetricTypeDDSketch`      | Patched OTel SDK with DDSketch support  |
| **raw**        | `MetricTypeGauge` (int or double) | Any standard OTel SDK           |

These two dimensions are **orthogonal**: a patched SDK can send DDSketch
payloads regardless of whether the collector uses batch or window mode, and a
standard SDK sending plain Gauges works in both modes equally well.

## Testing

### Batch mode (`mode: batch`)

Works with either input type:

```bash
# Terminal 1: start the collector in batch mode
./cmd/ddsketchcol/dist/ddsketchcol --config cmd/ddsketchcol/config.yaml

# Terminal 2a: standard SDK (Gauge inputs)
cd otel_collector_benchmark && go run main.go --endpoint=localhost:4317 --type=gauge

# Terminal 2b: patched SDK (DDSketch inputs)
cd opentelemetry-app
go run ./cmd/fakemetricload --enable-ddsketch=true --rate-per-series=25000 --endpoint=localhost:4317
```

### Window mode (`mode: window`)

Works with either input type. Example using the benchmark load generator
(Gauge inputs):

```bash
# Terminal 1: start the collector in window mode
./cmd/ddsketchcol/ddsketchcol --config cmd/ddsketchcol/config-window.yaml

# Terminal 2: run the benchmark (window mode target)
cd opentelemetry-collector-contrib-patch/cmd
./bench.sh ddsketchcol-window
```

The benchmark reports throughput (MPS), CPU, memory, and latency. Because the
processor turns many raw samples into a single sketch/quantile output per
series per window, the reported "data loss rate" will be close to 100%—this is
expected and matches the behavior of other sketch-based processors in this
repository.
