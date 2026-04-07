# End-to-End Test Guide: Controller + OTel Collector + SDK

This guide walks through a full integration test of the control plane against
the custom sketch collector (`ddsketchcol`) and the SDK load generator
(`e2esdkbench`). The test verifies that:

1. The controller correctly plans a sketch type for a workload.
2. The controller makes a delta transmission decision based on the submitted
   `WorkloadCharacteristics` (fill rate, flush rate, memory budget).
3. The generated collector YAML contains `delta_transmission: true` when the
   controller decided to enable delta encoding.
4. The collector fetches the generated config and starts a valid pipeline.
5. The SDK sends OTLP metrics that the collector processes with the planned sketch.
6. The processed metrics are visible on the Prometheus endpoint.

---

## Prerequisites

| Tool | How to get it |
|------|--------------|
| Rust toolchain (`cargo`) | `curl https://sh.rustup.rs -sSf \| sh` |
| Go toolchain (`go 1.22+`) | [go.dev/dl](https://go.dev/dl) |
| `ddsketchcol` binary | `./build_ddsketchcol.sh` from repo root |
| `curl`, `jq` | system packages |

All commands below are run from the **repo root** (`/mydata/DataCollector/`)
unless stated otherwise.

---

## Quick run (automated script)

```bash
# Default: ddsketch, 500 series, 30 s, Zipf distribution
./tests/otel_controller_e2e_test.sh

# Force delta by using a small workload (low fill rate) + frequency sketch
./tests/otel_controller_e2e_test.sh --sketch=countminsketch --series=10 --rate=10

# Block delta with a memory budget smaller than the snapshot requirement
./tests/otel_controller_e2e_test.sh --sketch=countminsketch --series=1000 \
    --memory-budget-mb=50

# Uniform distribution → higher fill rate → delta less likely
./tests/otel_controller_e2e_test.sh --sketch=countminsketch --distribution=uniform \
    --series=10 --rate=10 --duration=60s

# Skip rebuild if binaries are already fresh
./tests/otel_controller_e2e_test.sh --skip-build
```

Results are written to `/tmp/e2e_test_<timestamp>/`.

The script now prints a delta decision summary at the end:

```
==> Delta decision summary:
    mode            = use_delta
    fill_rate       = 0.156
    raw_bw          = 50000 B/s
    sketch_full_bw  = 2000 B/s
    sketch_delta_bw = 125 B/s
```

---

## Manual step-by-step

### Step 1 — Build the binaries

Build the Rust controller:

```bash
cd controller
cargo build --release
# binary: controller/target/release/controller
```

Build the OTel collector with sketch processors (only needed once, or after
patches change):

```bash
cd ..
./build_ddsketchcol.sh
# binary: opentelemetry-collector-contrib-patch/cmd/ddsketchcol/ddsketchcol
```

---

### Step 2 — Start the controller

The controller exposes:
- **`:8080`** — REST API for plan submission, retrieval, and config serving.
- **`:4320`** — OpAMP WebSocket server (agents connect here).

```bash
CONTROLLER_ADDR="0.0.0.0:8080" \
CONTROLLER_OPAMP_ADDR="0.0.0.0:4320" \
CONTROLLER_OPAMP_ENDPOINT="ws://localhost:4320/v1/opamp" \
./controller/target/release/controller
```

Verify it is running:

```bash
curl -s http://localhost:8080/api/v1/agents
# expected: []
```

---

### Step 3 — Submit a collection plan with WorkloadCharacteristics

Tell the controller about your workload. The `workload` field is optional; if
omitted, the controller uses conservative defaults (1 000 series, 100 Hz, Zipf).
Providing it gives the controller enough information to decide whether delta
transmission is worth enabling.

```bash
curl -s -X POST http://localhost:8080/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name":   "latency",
    "aggregations":  ["quantile"],
    "time_window":   "5m",
    "accuracy_sla":  0.01,
    "repeat_every":  "1m",
    "latency_sla":   "10m",
    "workload": {
      "series_count":               500,
      "samples_per_sec_per_series": 50,
      "bytes_per_raw_sample":       100,
      "data_distribution":          "zipf"
    }
  }' | jq .
```

Expected response (delta fields are new):

```json
{
  "metric":          "latency",
  "sketch_type":     "ddsketch",
  "mode":            "window",
  "aggregate_by":    [],
  "valid_until":     "...",
  "agents_notified": 0,
  "precompute_jobs": 0,
  "delta_decision": {
    "mode":                               "use_delta",
    "threshold":                          1.0,
    "estimated_compression_ratio":        6.8,
    "estimated_delta_bytes_per_sec":      8823,
    "delta_cpu_overhead_micros_per_sample": 0.024,
    "delta_memory_overhead_bytes":        4096000
  },
  "transmission_costs": {
    "raw_bytes_per_sec":                   2500000,
    "sketch_full_bytes_per_sec":           60000,
    "sketch_delta_bytes_per_sec":          8823,
    "delta_cpu_overhead_micros_per_sample": 0.024,
    "delta_memory_overhead_bytes":         4096000,
    "estimated_fill_rate":                 0.147,
    "flush_rate_hz":                       0.003
  }
}
```

`delta_decision.mode` will be one of:
- `"use_delta"` — delta enabled; check `delta_transmission: true` in the YAML.
- `"use_full_sketch"` — delta skipped; see `reason` for why.
- `"use_raw"` — workload too small for sketch overhead to be justified.

> **Note on `agents_notified: 0`** — the OpAMP server pushes configs over
> WebSocket using the opamp-go binary protobuf format. The collector fetches
> its config directly via the HTTP config provider in the next step, so this
> count being 0 is expected during testing.

#### Scenarios to exercise each decision branch

| Goal | Key parameters |
|---|---|
| **Force `use_delta`** | Frequency sketch, small series + rate, Zipf (low fill rate) |
| **Force `use_full_sketch` (FillRateTooHigh)** | Uniform distribution, long window, many series |
| **Force `use_full_sketch` (MemoryBudgetExceeded)** | Add `"memory_budget_bytes": 1048576` (1 MB) |
| **Force `use_full_sketch` (SketchTypeUnsupported)** | `"sketch_type": "kll"` |
| **Force `use_raw`** | `"series_count": 1, "samples_per_sec_per_series": 0.5` |

```bash
# use_delta: CountMinSketch, 10 series, 10 Hz, Zipf, 10s window
curl -s -X POST http://localhost:8080/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "errors", "aggregations": ["frequency"],
    "time_window": "10s", "accuracy_sla": 0.01, "repeat_every": "10s",
    "workload": { "series_count": 10, "samples_per_sec_per_series": 10,
                  "bytes_per_raw_sample": 100, "data_distribution": "zipf" }
  }' | jq '.delta_decision'

# use_full_sketch (MemoryBudgetExceeded): same workload, 1 MB cap
curl -s -X POST http://localhost:8080/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "errors", "aggregations": ["frequency"],
    "time_window": "10s", "accuracy_sla": 0.01, "repeat_every": "10s",
    "workload": { "series_count": 10, "samples_per_sec_per_series": 10,
                  "bytes_per_raw_sample": 100, "data_distribution": "zipf",
                  "memory_budget_bytes": 1048576 }
  }' | jq '.delta_decision'

# use_full_sketch (SketchTypeUnsupported): KLL has no delta implementation
curl -s -X POST http://localhost:8080/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "latency", "aggregations": ["quantile"],
    "time_window": "5m", "accuracy_sla": 0.01, "sketch_type": "kll",
    "workload": { "series_count": 10, "samples_per_sec_per_series": 10,
                  "bytes_per_raw_sample": 100, "data_distribution": "zipf" }
  }' | jq '.delta_decision'
```

---

### Step 4 — Inspect the generated collector YAML

The controller exposes the full OTel collector config for any planned metric:

```bash
curl -s http://localhost:8080/api/v1/config/latency
```

Expected output when delta is enabled (abbreviated):

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"
      http:
        endpoint: "0.0.0.0:4318"
processors:
  ddsketch:
    mode: window
    window_duration: 5m
    transmit_sketch: true
    drop_original: true
    relative_accuracy: 0.01
    delta_transmission: true   # present only when use_delta was decided
    delta_threshold: 1.0
exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"
service:
  pipelines:
    metrics:
      receivers:  [otlp]
      processors: [ddsketch]
      exporters:  [prometheus]
```

When `delta_decision.mode` is `use_full_sketch` or `use_raw`, the
`delta_transmission` key will be absent from the YAML.

Verify programmatically:

```bash
# Should print "true" when delta is enabled, empty otherwise
curl -s http://localhost:8080/api/v1/config/latency \
  | grep "delta_transmission" | awk '{print $2}'
```

---

### Step 5 — Start the collector with the HTTP config provider

Pass the controller's config URL using the OTel HTTP config provider syntax.
The collector fetches the YAML at startup and every 30 s by default.

```bash
COLLECTOR=./opentelemetry-collector-contrib-patch/cmd/ddsketchcol/ddsketchcol

$COLLECTOR --config="http://localhost:8080/api/v1/config/latency"
```

Verify the collector is running and its internal metrics are exposed:

```bash
# Prometheus endpoint for pipeline metrics (e.g. sketch size, error rate)
curl -s http://localhost:8889/metrics | head -20

# Collector self-telemetry (process CPU, memory, accepted data points)
curl -s http://localhost:8888/metrics | grep -E "process_cpu|accepted_metric"
```

---

### Step 6 — Run the SDK load generator

`e2esdkbench` sends OTLP metrics from a synthetic workload to the collector.

```bash
cd opentelemetry-app
go run ./cmd/e2esdkbench \
  --sketch-type=ddsketch \
  --endpoint=localhost:4317 \
  --series=500 \
  --samples-per-sec-per-series=50 \
  --duration=30s \
  --output-dir=/tmp/e2e_bench
```

You should see live progress in the terminal, then a summary like:

```
sketch_type: ddsketch
total_samples: 750000
bandwidth_bytes: 1234567
cpu_user_ms: 890
heap_alloc_bytes: 4567890
```

The JSON summary is also written to `/tmp/e2e_bench/ddsketch_<rate>mps_summary.json`.

---

### Step 7 — Verify processed metrics

While the collector is still running (or just after the bench finishes and the
collector flushes), check that processed sketch metrics appeared:

```bash
# Check accepted data points
curl -s http://localhost:8888/metrics \
  | grep otelcol_processor_accepted_metric_points

# Check sketch output size
curl -s http://localhost:8889/metrics \
  | grep -E "latency|otelcol_sketch"
```

Check the plan is still valid in the store:

```bash
curl -s http://localhost:8080/api/v1/plan/latency | jq .
```

---

---

## Quantitative delta comparison with bench_delta.sh

The automated test verifies that the controller makes a decision and the YAML is
correct, but it does not measure the actual wire bandwidth reduction.
`otel_collector_benchmark/bench_delta.sh` runs a collector with and without delta
enabled and reports the real bandwidth, CPU, and memory numbers.

```bash
cd otel_collector_benchmark

# Compare full vs. delta for all supported sketch types (60 s each run)
./bench_delta.sh --sketch all --mode all --duration 60s

# Focus on CountMinSketch in window mode only
./bench_delta.sh --sketch countminsketch --mode window --duration 60s

# Vary the sample rate to see fill rate effects
./bench_delta.sh --sketch countminsketch --mode window --rates "5000 20000 50000"
```

Results land in `benchmark_results/delta/` as CSV timeseries and JSON summaries.
The key columns to compare are `sdk_bw_bytes_per_sec` (bytes the SDK sends to the
collector) vs. `collector_out_bw_bytes_per_sec` (bytes leaving the collector) in
full vs. delta mode.

Cross-check the actual compression ratio against the controller's estimate:

```bash
# 1. Get the controller's estimate
curl -s http://localhost:8080/api/v1/plan/errors | jq '.delta_decision.estimated_compression_ratio'

# 2. Run the real benchmark
./bench_delta.sh --sketch countminsketch --mode window --duration 60s

# 3. Read actual ratio from the JSON summary
cat benchmark_results/delta/countminsketch_window_*_summary.json \
  | jq '.compression_ratio'
```

---

## Rollback test (optional)

To verify rollback works end-to-end:

```bash
# Submit a second plan (changes parameters)
curl -s -X POST http://localhost:8080/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "latency",
    "aggregations": ["quantile"],
    "time_window": "10m",
    "accuracy_sla": 0.05
  }' | jq .

# Roll back to the previous plan
curl -s -X POST http://localhost:8080/api/v1/plan/latency/rollback | jq .

# The config endpoint now reflects the previous plan
curl -s http://localhost:8080/api/v1/config/latency | grep "mode:"
```

The collector will pick up the rolled-back config on its next HTTP config
provider poll (default 30 s), or restart the collector to see it immediately.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `curl: (7) Failed to connect to localhost:8080` | Controller not started | Check Step 2, look at controller logs |
| `404` on `/api/v1/config/latency` | No plan submitted yet | Run Step 3 first |
| Collector exits immediately | Config YAML invalid or port 4317 in use | Check collector log, verify no other collector running |
| `otelcol_sketch_size_bytes` not in Prometheus output | Collector telemetry metric naming varies | Check `:8888/metrics` (collector self-telemetry) vs `:8889/metrics` (pipeline output) |
| `agents_notified: 0` in plan response | OpAMP protobuf mismatch | Expected — use HTTP config provider (Step 5) |
| e2esdkbench errors sending metrics | Collector OTLP port not ready | Wait a few seconds after starting collector, then retry |
| `delta_decision` missing from plan response | Old binary without the feature | Rebuild controller: `cargo build --release` |
| `delta_decision.mode` is always `use_full_sketch` | Default workload is conservative (1 000 × 100 Hz saturates sketch) | Provide explicit `workload` with small `series_count` and `samples_per_sec_per_series` |
| Delta decided but YAML has no `delta_transmission` | `--skip-build` used with stale binary | Rebuild or omit `--skip-build` |
| Estimated compression ratio differs from bench_delta result | Fill rate formula is analytic (upper bound for HLL); Zipf constant calibrated at s=1.1 | Provide `distinct_keys_per_window` directly for higher accuracy |
