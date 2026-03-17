# CountSketch Processor

The CountSketch processor (`countsketchprocessor`) aggregates metrics into CountSketch data structures for estimation of heavy hitters or frequency tracking. It processes incoming metric data points (Gauge, Sum, Histogram) and updates internal row (Metric Name) and column (Host Name) sketches.

The processor maintains these sketches in memory and flushes/resets them periodically based on a configured window size.

## Configuration

| Field | Description | Default |
| --- | --- | --- |
| `epsilon` | The acceptable error rate (0 < epsilon < 1). Lower values require more memory. | `0.01` |
| `delta` | The probability of failure (0 < delta < 1). Lower values require more CPU (more hash functions). | `0.99` |
| `window_size` | The duration for the sketch window before reset. Minimum is 1s. | `5s` |
| `transmit_sketch` | Reserved shared sketch toggle. CountSketch still emits metric-form summaries in the current implementation. | `false` |

Example:

```yaml
processors:
  countsketch:
    epsilon: 0.001
    delta: 0.99
    window_size: "1m"
    transmit_sketch: false
