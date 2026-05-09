# asap-gorilla-go

Canonical Go implementation of the ASAP `GORILLA1` block encoder.

Runtime edge collectors and processors adapt their local metric/window
buffers into this package instead of carrying private copies of the
timestamp delta-of-delta encoder, value XOR encoder, and block assembly
logic.

The Rust sibling is [`asap-gorilla-rust`](../asap-gorilla-rust/). The two
implementations share the same wire contract:

```text
[8] magic "GORILLA1"
[1] version
[4] little-endian series_count
[...] encoded series bodies
```

The public entry points are:

- `SortAndEncode` for timestamp/value bit-stream encoding.
- `EncodeSeriesBody` for one per-series body.
- `BuildObjects` for generic multi-series `GORILLA1` objects.
- `BuildMetricChunks` for metric-grouped chunks used by `gorillas3processor`.
