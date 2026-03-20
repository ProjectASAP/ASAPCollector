# End-to-End Test Guide: Controller + OTel Collector + SDK

This guide walks through a full integration test of the control plane against
the custom sketch collector (`ddsketchcol`) and the SDK load generator
(`e2esdkbench`). The test verifies that:

1. The controller correctly plans a sketch type for a workload.
2. The collector fetches the generated config and starts a valid pipeline.
3. The SDK sends OTLP metrics that the collector processes with the planned sketch.
4. The processed metrics are visible on the Prometheus endpoint.

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
# Default: ddsketch, 500 series, 30 s
./controller/scripts/e2e_test.sh

# Different sketch type
./controller/scripts/e2e_test.sh --sketch=kll --duration=60s

# Skip rebuild if binaries are already fresh
./controller/scripts/e2e_test.sh --skip-build
```

Results are written to `/tmp/e2e_test_<timestamp>/`.

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

### Step 3 — Submit a collection plan

Tell the controller about your workload. It will choose the right sketch type
(e.g. DDSketch for quantile queries) and store the plan.

```bash
curl -s -X POST http://localhost:8080/api/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name":   "latency",
    "aggregations":  ["quantile"],
    "time_window":   "5m",
    "accuracy_sla":  0.02,
    "repeat_every":  "1m",
    "latency_sla":   "10m"
  }' | jq .
```

Expected response:

```json
{
  "metric":          "latency",
  "sketch_type":     "ddsketch",
  "mode":            "batch",
  "aggregate_by":    [],
  "valid_until":     "...",
  "agents_notified": 0,
  "precompute_jobs": 0
}
```

> **Note on `agents_notified: 0`** — the OpAMP server pushes configs over
> WebSocket using the opamp-go binary protobuf format. The collector fetches
> its config directly via the HTTP config provider in the next step, so this
> count being 0 is expected during testing.

---

### Step 4 — Inspect the generated collector YAML

The controller exposes the full OTel collector config for any planned metric:

```bash
curl -s http://localhost:8080/api/v1/config/latency
```

Expected output (abbreviated):

```yaml
extensions:
  opamp:
    server:
      ws:
        endpoint: ws://localhost:4320/v1/opamp
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"
      http:
        endpoint: "0.0.0.0:4318"
processors:
  ddsketch:
    mode: batch
    transmit_sketch: true
    drop_original: true
    relative_accuracy: 0.02
exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"
service:
  extensions: [opamp]
  pipelines:
    metrics:
      receivers:  [otlp]
      processors: [ddsketch]
      exporters:  [prometheus]
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
