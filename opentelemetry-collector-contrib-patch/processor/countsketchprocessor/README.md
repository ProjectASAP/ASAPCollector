# CountSketch Processor

The CountSketch processor (`countsketchprocessor`) aggregates metrics into CountSketch data structures for estimation of heavy hitters or frequency tracking. It processes incoming metric data points (Gauge, Sum, Histogram) and updates internal row (Metric Name) and column (Host Name) sketches.

The processor maintains these sketches in memory and flushes/resets them periodically based on a configured window duration.

## Configuration

| Field | Description | Default |
| --- | --- | --- |
| `mode` | Flush mode: `batch` (per-batch) or `window` (tumbling window). | `batch` |
| `window_duration` | The duration for the sketch window before reset. Required in window mode. Minimum is 1s. | `5s` |
| `epsilon` | The acceptable error rate (0 < epsilon < 1). Lower values require more memory. | `0.01` |
| `delta` | The probability of failure (0 < delta < 1). Lower values require more CPU (more hash functions). | `0.99` |
| `transmit_sketch` | Reserved shared sketch toggle. CountSketch still emits metric-form summaries in the current implementation. | `false` |
| `drop_original` | Drop original metrics after processing. | `false` |
| `aggregate_by` | Label keys for cross-series aggregation. Empty = per-series (default). | `[]` |
| `label_matchers` | Exact-match filters on data points (`key`/`value` pairs). Empty = include all. | `[]` |

Example:

```yaml
processors:
  countsketch:
    epsilon: 0.001
    delta: 0.99
    window_duration: "1m"
    transmit_sketch: false
