# Sketch Metrics Processor

This processor maintains Count-Min sketches over incoming metric datapoints (inspired by the Telegraf `countmin` aggregator). Each batch updates per-metric sketches keyed by tag dimensions, then emits a synthetic metric containing:

- Serialized Count-Min sketch payload (`countmin` attribute, bytes)
- Rows/columns/count metadata
- Approximate top-k heavy hitters (`topk` attribute, JSON)

Use `tag_keys` to explicitly choose which tags participate in the sketch. If omitted, all tags except those in `group_by` are used. Set `drop_original: true` to forward only the sketch metric.

See `config.go` for configuration details.
