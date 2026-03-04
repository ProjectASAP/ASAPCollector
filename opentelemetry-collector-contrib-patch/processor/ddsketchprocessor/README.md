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
| `quantiles`         | Quantiles to emit when `emit_ddsketch` is `false`.                         | `[0.5, 0.9, 0.99]` |
| `metric_suffix`     | Suffix for generated metrics (applied to either DDSketch or quantile outputs). | `_ddsketch`  |
| `emit_ddsketch`     | If `true`, emit DDSketch payload metrics; if `false`, emit quantile gauges.| `true`           |

When `emit_ddsketch: false`, at least one quantile must be configured and each
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
    emit_ddsketch: true
    metric_suffix: "_merged"
```

### Example: batch mode with quantile gauges

```yaml
processors:
  ddsketch:
    mode: batch
    emit_ddsketch: false
    relative_accuracy: 0.01
    quantiles: [0.5, 0.9, 0.99]
    metric_suffix: "_quantile"
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
    emit_ddsketch: false
    quantiles: [0.5, 0.9, 0.99]
    metric_suffix: "_quantile"
```

In this mode:

- Input: DDSketch or Gauge metrics (e.g. from `otel_collector_benchmark` or any
  standard OTel SDK).
- The processor groups points by resource, instrumentation scope, metric name,
  and attribute set, accumulating values into DDSketches over each
  `window_duration`.
- At each window boundary it flushes the aggregated sketches as either:
  - DDSketch metrics, if `emit_ddsketch: true`, or
  - quantile gauge metrics, if `emit_ddsketch: false`.

## Testing

### Batch mode (`mode: batch`)

You can use either:

- A patched SDK that emits DDSketch metrics (e.g. `opentelemetry-app`), or
- Any client that emits Gauge metrics (the processor will build DDSketches).

Example using the dedicated collector binary:

```bash
# Terminal 1: start the collector in batch mode
./cmd/ddsketchcol/dist/ddsketchcol --config cmd/ddsketchcol/config.yaml

# Terminal 2: run a client that sends metrics
# (DDSketch or Gauge inputs are both supported)
```

### Window mode (`mode: window`) with `otel_collector_benchmark`

The `otel_collector_benchmark` load generator sends standard OTLP Gauge
metrics, which can be used to exercise the windowing behavior:

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

# DDSketch Processor

The DDSketch processor (`ddsketchprocessor`) can operate in two modes, controlled
by the `mode` configuration field:

- `mode: sketch`: the input metrics are **pre-built DDSketch payloads** from a
  patched OTel SDK (`MetricTypeDDSketch`). The processor merges sketches with
  identical metric name and attribute set and appends a sibling metric
  `<original>_ddsketch` (suffix configurable).
- `mode: raw`: the input metrics are **plain gauge samples** from any standard
  OTel SDK (`MetricTypeGauge`). The processor aggregates these samples into
  DDSketches over a configurable **tumbling time window** and exports either:
  - merged DDSketch payloads, or
  - quantile gauges derived from the DDSketch.

This naming is input-centric:

| Mode    | Input type                            | Typical client                           |
| ------- | ------------------------------------- | ---------------------------------------- |
| sketch  | DDSketch metrics (`MetricTypeDDSketch`) | Patched OTel SDK with DDSketch support   |
| raw     | Gauge metrics (`MetricTypeGauge`)       | Any standard OTel SDK or load generator |

## Configuration

### Common fields

| Field               | Type        | Description                                                                 | Default        |
| ------------------- | ----------- | --------------------------------------------------------------------------- | -------------- |
| `mode`              | `string`    | Input type: `"sketch"` or `"raw"`.                                         | `"sketch"`     |
| `metric_suffix`     | `string`    | Suffix appended to the original metric name for generated outputs.         | `"_ddsketch"`  |
| `emit_ddsketch`     | `bool`      | If `true`, emit DDSketch payload metrics. If `false`, emit quantile gauges.| `true`         |
| `relative_accuracy` | `float64`   | DDSketch relative accuracy parameter.                                      | `0.01`         |
| `quantiles`         | `[]float64` | Quantiles to emit when `emit_ddsketch` is `false`.                         | `[0.5,0.9,0.99]` |

> When `emit_ddsketch: false`, at least one quantile must be configured and
> each quantile must be in the \[0,1] range.

### Raw mode specific fields

| Field             | Type          | Description                                            | Default |
| ----------------- | ------------- | ------------------------------------------------------ | ------- |
| `window_duration` | `time.Duration` | Length of the tumbling aggregation window (e.g. `60s`). | `60s`   |

`window_duration` is only used when `mode: raw`. In `mode: sketch` it is
ignored.

## Example configurations

### SDK sketch mode (patched client sends DDSketch)

```yaml
processors:
  ddsketch:
    mode: sketch
    emit_ddsketch: false
    quantiles: [0.5, 0.9, 0.99]
    metric_suffix: "_quantile"
```

In this mode:

- Input: DDSketch metrics from the patched SDK.
- Output: for each input series, the processor appends a new gauge metric
  `<name>_quantile` containing the configured quantiles evaluated from the
  merged sketch in the current batch.

### Raw mode (collector builds DDSketch from gauges)

```yaml
processors:
  ddsketch:
    mode: raw
    window_duration: 60s
    relative_accuracy: 0.01
    emit_ddsketch: false
    quantiles: [0.5, 0.9, 0.99]
    metric_suffix: "_quantile"
```

In this mode:

- Input: plain gauge metrics (e.g. from `otel_collector_benchmark` or any
  standard OTel SDK).
- The processor groups points by resource, instrumentation scope, metric name,
  and attribute set, accumulating values into DDSketches over a 60‑second
  tumbling window.
- At each window boundary, it flushes the aggregated sketches as either:
  - DDSketch metrics, if `emit_ddsketch: true`, or
  - quantile gauge metrics, if `emit_ddsketch: false`.

## Testing

### Raw mode (`mode: raw`)

Use the `otel_collector_benchmark` load generator, which sends standard OTLP
Gauge metrics:

```bash
# Terminal 1: start the collector in raw mode
./cmd/ddsketchcol/dist/ddsketchcol --config cmd/ddsketchcol/config-raw.yaml

# Terminal 2: run the benchmark
cd opentelemetry-collector-contrib-patch/cmd
./bench.sh ddsketchcol
```

The benchmark reports throughput (MPS), CPU, memory, and latency. Because the
processor turns many raw samples into a single sketch/quantile output per
series per window, the reported "data loss rate" will be close to 100%—this
is expected and matches the behavior of other sketch-based processors.

### Sketch mode (`mode: sketch`)

Use the patched `opentelemetry-app` client, which emits DDSketch metrics:

```bash
# Terminal 1: start the collector in sketch mode
./cmd/ddsketchcol/dist/ddsketchcol --config cmd/ddsketchcol/config.yaml

# Terminal 2: run the client
cd opentelemetry-app
go run ./cmd/fakemetricload \
  --enable-ddsketch=true \
  --rate-per-series=25000 \
  --endpoint=localhost:4317
```

In this setup, the SDK does the sketch aggregation and the processor merges
compatible sketches and/or emits quantile summaries, depending on
`emit_ddsketch` and `quantiles`.

