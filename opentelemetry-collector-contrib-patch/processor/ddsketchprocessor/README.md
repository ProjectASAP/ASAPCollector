# DDSketch Processor

The DDSketch processor (`ddsketchprocessor`) merges OTLP DDSketch metrics.
For every DDSketch time series (identified by metric name and attribute set)
it appends a sibling metric named `<original>_ddsketch` (the suffix is
configurable) that contains the union of all DDSketch payloads observed for
that series within the batch. The processor no longer converts scalar inputs
into quantile summaries; it now expects the input metric to already be encoded
as a DDSketch and simply aggregates compatible sketches.

## Configuration

| Field | Description | Default |
| --- | --- | --- |
| `relative_accuracy` | _Deprecated. No longer used._ | `0.01` |
| `quantiles` | _Deprecated. No longer used._ | `[0.5, 0.9, 0.99]` |
| `metric_suffix` | Suffix for generated metrics. | `_ddsketch` |

Example:

```yaml
processors:
  ddsketch:
    metric_suffix: "_merged"
```
