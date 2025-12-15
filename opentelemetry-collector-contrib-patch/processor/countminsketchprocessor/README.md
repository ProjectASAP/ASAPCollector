# Count-Min Sketch Processor (OTel Collector)

A custom OpenTelemetry Collector processor implementing the Count-Min Sketch algorithm for probabilistic data tracking. This processor provides constant-space memory usage for aggregating high-cardinality metrics.

## Quick Start

> **CAUTION:** You do **not** need to start the binary or `telemetrygen` manually if you are using the provided benchmark script (`countmin_bench.sh`). The script handles the startup and cleanup of both the collector and the load generator automatically.

### 1. Build (Prerequisite)

Run this once to compile the collector binary:

```bash
builder --config ./cmd/countminsketchcol/builder-config.yaml
````

-----

### 2\. Run Benchmark (Recommended)

The easiest way to evaluate performance is using the automated script.

```bash
chmod +x countmin_bench.sh
./countmin_bench.sh
```

-----

### 3\. Manual Run (Alternative)

If you prefer to run components individually for debugging:

**Terminal 1: Start Collector**

```bash
./cmd/countminsketchcol/countminsketchcol --config ./cmd/countminsketchcol/config-bench.yaml
```

**Terminal 2: Monitor Real-time Items**

```bash
watch -n 1 "curl -s http://localhost:8888/metrics | grep 'otelcol_processor_incoming_items_total' | grep 'countmin'"
```

**Terminal 3: Generate Load**

```bash
telemetrygen metrics \
  --otlp-insecure \
  --otlp-endpoint "localhost:4317" \
  --rate 10000 --duration 60s --workers 10
```

-----

### Benchmark Results

**Duration:** 30s per scenario | **Query URL:** `http://localhost:8888/metrics`

| Target Rate (MPS) | Actual Throughput (MPS) | Avg CPU Usage | Peak RAM Usage | Latency Avg (ms) | Latency P95 (ms) | Latency P99 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **10,000** | 12,127 | 232.97% | 268.50 MB | 0.91 | 1.70 | 2.23 |
| **20,000** | 12,392 | 228.80% | 272.50 MB | 0.83 | 1.47 | 1.89 |
| **30,000** | 12,318 | 226.96% | 273.89 MB | 0.84 | 1.44 | 2.60 |
| **40,000** | 12,200 | 225.62% | 277.46 MB | 0.87 | 1.67 | 2.20 |
| **50,000** | 12,262 | 225.99% | 276.53 MB | 0.82 | 1.57 | 1.82 |

