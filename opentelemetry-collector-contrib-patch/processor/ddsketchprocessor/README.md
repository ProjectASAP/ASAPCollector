# DDSketch Processor

The DDSketch processor (`ddsketchprocessor`) builds DDSketch summaries for
numeric metrics. For every numeric time series (identified by metric name and
attribute set) it appends a sibling metric named `<original>_ddsketch` (the
suffix is configurable) that contains quantile estimates generated with the
configured relative accuracy.

## Configuration

| Field | Description | Default |
| --- | --- | --- |
| `relative_accuracy` | Target DDSketch relative accuracy (0 < value < 1). | `0.01` |
| `quantiles` | Quantiles to export from the sketch. | `[0.5, 0.9, 0.99]` |
| `metric_suffix` | Suffix for generated metrics. | `_ddsketch` |

Example:

```yaml
processors:
  ddsketch:
    relative_accuracy: 0.02
    quantiles: [0.5, 0.95, 0.99]
```
