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
cd processor/countminsketchprocessor/
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

### Benchmark Results (API Version)

**Duration:** 30s per scenario | **Query URL:** `http://localhost:8888/metrics`

Siap. Berikut **tabel README yang sudah di-update** menggunakan **data benchmark terbaru** yang kamu berikan, dengan konteks **Binary: `countminsketchcol`**, **Duration 60s**, **Window 10s**.

---

### Benchmark Results (API Version – `countminsketchcol`)

**Duration:** 60s per scenario | **Window:** 10s | **Query URL:** `http://localhost:8888/metrics`

| Target Rate (MPS) | Actual Throughput (MPS) | Avg CPU Usage | Peak RAM Usage | Latency Avg (ms) | Latency P95 (ms) | Latency P99 (ms) |
| :---------------- | :---------------------- | :------------ | :------------- | :--------------- | :--------------- | :--------------- |
| **10,000**        | 30,409                  | 416.16%       | 207.37 MB      | 0.79             | 1.24             | -                |
| **20,000**        | 29,902                  | 419.71%       | 208.26 MB      | 0.80             | 1.20             | -                |
| **30,000**        | 28,705                  | 422.54%       | 207.08 MB      | 0.82             | 1.28             | -                |
| **40,000**        | 29,198                  | 417.92%       | 206.39 MB      | 0.82             | 1.29             | -                |
| **50,000**        | 28,331                  | 420.28%       | 206.31 MB      | 0.83             | 1.36             | -                |




