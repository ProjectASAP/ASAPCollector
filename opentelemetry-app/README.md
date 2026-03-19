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

---

## SDK Pipeline Configuration Files

YAML configuration files define the full SDK→collector pipeline in one place,
replacing scattered CLI flags. A per-sketch-type file is provided plus a raw-gauge
baseline.

| File | Sketch type | `transmit_sketch` |
|------|------------|-------------------|
| `config.yaml` | DDSketch (base template) | `true` |
| `config-ddsketch.yaml` | DDSketch | `true` |
| `config-kll.yaml` | KLL | `true` |
| `config-hll.yaml` | HLL | `true` |
| `config-countminsketch.yaml` | CountMinSketch | `true` |
| `config-countsketch.yaml` | CountSketch | `true` |
| `config-baseline.yaml` | none (raw gauge) | `false` |

### `transmit_sketch`

Controls which side of the pipeline performs sketch aggregation:

- **`transmit_sketch: true`** — SDK attaches an `Aggregation` view to the instrument
  and pre-aggregates all recorded values into a sketch before each export tick. The
  collector receives a single compact sketch data point per series per window.
- **`transmit_sketch: false`** — SDK emits raw `Float64Gauge` (LastValue) observations.
  The collector-side processor (e.g. `kllprocessor`, `hllprocessor`) performs the
  aggregation. Used for the `baseline` type and for collector-driven sketch modes.

### Config schema

```yaml
exporter:
  endpoint: localhost:4317   # OTLP gRPC endpoint
  insecure: true
  max_send_msg_size_mb: 64   # raise for large HLL/CountSketch payloads

reader:
  interval: 1s               # SDK export interval

sketch:
  type: ddsketch             # ddsketch | kll | hll | countsketch | countminsketch | baseline
  transmit_sketch: true      # true = SDK aggregates; false = collector aggregates
  ddsketch:
    relative_accuracy: 0.01
  kll:
    k: 256
  hll: {}
  countsketch:
    rows: 5
    cols: 2000
    epsilon: 0.01
    delta: 0.99
  countminsketch:
    rows: 5
    cols: 2000

instruments:
  - name: benchmark.latency
    unit: ms
    instrument_kind: histogram   # histogram | gauge
    group_by:                    # attribute keys that form the sketch dimension
      - host.name
      - service.name
    series_per_sketch: 1         # 1=per-series, 0=all-in-one, N=group N series

# Mirrors ControllerConfig from asap-planner-rs — can be fed directly to asap-planner-rs
asap_query:
  metrics:
    - metric: benchmark.latency
      labels: [host.name, service.name]
  query_groups:
    - id: 1
      queries: ["quantile_over_time(0.99, benchmark_latency[5m])"]
      repetition_delay: 5000
      controller_options:
        accuracy_sla: 0.01
        latency_sla: 100.0
  sketch_parameters:           # optional overrides consumed by asap-planner-rs
    DatasketchesKLL:
      K: 256
    CountMinSketch:
      depth: 5
      width: 2000

load:
  workers: 10
  series: 1000
  samples_per_sec_per_series: 50.0
  duration: 60s
  distribution:
    type: zipf
    s: 1.1
    v: 1.0
    max: 500
    mean: 250.0
```
