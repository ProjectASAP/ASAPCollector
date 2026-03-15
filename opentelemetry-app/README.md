## OpenTelemetry App Helpers

- `cmd/fakemetricload`: Synthetic load generator for sketch-based metric processors.
  Supports all sketch types; all use SDK **pre-aggregation** (the SDK builds the sketch
  before export so the collector receives a typed sketch data point rather than raw gauges).

### Supported sketch types (`--sketch-type`)

| Value | SDK aggregation | Collector processor | Delivery |
|-------|----------------|---------------------|---------|
| `ddsketch` | `AggregationDDSketch` | `ddsketchprocessor` | `Float64Histogram` → DDSketch data point |
| `kll` | `AggregationKLLSketch` | `kllprocessor` | `Float64Histogram` → KLLSketch data point |
| `countsketch` | `AggregationCountSketch` | `countsketchprocessor` | `Float64Histogram` → CountSketch data point |
| `countminsketch` | `AggregationCountMinSketch` | `countminsketchprocessor` | `Float64Histogram` → CountMinSketch data point |
| `hll` | `AggregationHLLSketch` | `hllprocessor` | `Float64Histogram` → HLLSketch data point |

All sketch types follow the same pre-aggregation path: the SDK accumulates recorded
values into a sketch per series per export window, then exports one compact sketch
data point. The collector processor merges incoming sketches and emits query results
(quantiles, frequency estimates, or cardinality estimates).

### Key flags

| Flag | Default | Description |
|------|---------|-------------|
| `--sketch-type` | `ddsketch` | Sketch algorithm (see table above) |
| `--endpoint` | `localhost:4317` | OTLP gRPC endpoint |
| `--workers` | `4` | Parallel goroutines |
| `--hosts` | `10` | Hosts per worker |
| `--metrics` | `10` | Metrics per host |
| `--interval` | `10s` | SDK export interval |
| `--duration` | `60s` | Run duration (0 = forever) |
| `--samples-per-interval` | `1` | Values recorded per series per window |

### Running via bench.sh

The benchmark script (`opentelemetry-collector-contrib-patch/cmd/bench.sh`) runs
end-to-end benchmarks for all sketch types using `fakemetricload` as the SDK load
generator. See the bench script for the full list of targets.
