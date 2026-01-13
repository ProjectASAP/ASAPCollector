# OTLP Metric Load Generator

A high-performance, multi-threaded load generator for OpenTelemetry Collectors. It sends OTLP metrics over gRPC, allowing for configurable stress testing of receivers and processors.

## Benchmarking

This repository includes benchmarking infrastructure for testing OpenTelemetry Collector processors with realistic Zipf-distributed metric loads.

### System Information

Benchmarks were run on the following system:
- **OS**: Linux 5.15.0-138-generic (Ubuntu 20.04)
- **CPU**: AMD Ryzen Threadripper PRO 5955WX 16-Cores
- **CPU Cores**: 32 (16 cores × 2 threads)
- **Memory**: 440 GB total, 424 GB available
- **Architecture**: x86_64

## Benchmark Results

**Common Test Configuration:**
- Duration: 60 seconds per scenario
- Load rates: 10,000, 20,000, 30,000, 40,000, 50,000 MPS (Metrics Per Second)
- Workers: 10
- Hosts per worker: 10
- Metrics per host: 10
- Distribution: Zipf (s=1.1, v=1.0, max=500, mean=250)

### NOP Processor Benchmark

The NOP (No-Operation) processor serves as a baseline for measuring collector overhead without any processing logic.

**Results Summary:**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Data Loss | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|-----------|-------------|-------------|-------------|
| 10,000 MPS  | 9,986 MPS         | 99.86%       | 5.13%   | 39.12 MB    | 0%        | 1.70 ms     | 2.52 ms     | 3.12 ms     |
| 20,000 MPS  | 19,988 MPS        | 99.94%       | 9.12%   | 37.92 MB    | 0%        | 1.72 ms     | 2.30 ms     | 2.98 ms     |
| 30,000 MPS  | 29,990 MPS        | 99.97%       | 12.97%  | 38.42 MB    | 0%        | 1.64 ms     | 2.27 ms     | 2.81 ms     |
| 40,000 MPS  | 40,000 MPS        | 100.00%      | 16.67%  | 38.33 MB    | 0%        | 1.64 ms     | 2.10 ms     | 2.69 ms     |
| 50,000 MPS  | 49,990 MPS        | 99.98%       | 20.59%  | 38.26 MB    | 0%        | 1.63 ms     | 2.11 ms     | 2.56 ms     |

**Key Observations:**
- **Throughput Scaling**: >99.8% accuracy across all scenarios (using microsecond-precision intervals)
- **CPU Usage**: Linear scaling (~0.4% per 10k MPS), reaching 20.59% at 50k MPS
- **Memory**: Stable 36-39 MB across all loads
- **Zero Data Loss**: 0% data loss across all scenarios
- **Latency**: Consistently < 3ms (P99 < 2.9ms)

### CountSketch Processor Benchmark

The CountSketch processor aggregates metrics into Count-Min Sketch data structures for frequency estimation and heavy hitter detection.

**Processor Configuration:**
- epsilon: 0.01, delta: 0.99, window_size: 5s

**Results Summary:**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Data Loss | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|-----------|-------------|-------------|-------------|
| 10,000 MPS  | 10,000 MPS        | 100.00%      | 13.11%  | 33.91 MB    | -100%*    | 1.93 ms     | 2.79 ms     | 2.97 ms     |
| 20,000 MPS  | 19,996 MPS        | 99.98%       | 23.51%  | 34.23 MB    | -100%*    | 1.88 ms     | 2.72 ms     | 2.96 ms     |
| 30,000 MPS  | 29,998 MPS        | 99.99%       | 33.18%  | 34.17 MB    | -100%*    | 1.77 ms     | 2.66 ms     | 2.90 ms     |
| 40,000 MPS  | 39,998 MPS        | 99.99%       | 43.24%  | 33.92 MB    | -100%*    | 1.81 ms     | 2.54 ms     | 2.77 ms     |
| 50,000 MPS  | 49,993 MPS        | 99.99%       | 54.24%  | 35.24 MB    | -100%*    | 1.76 ms     | 2.54 ms     | 2.77 ms     |

*Note: The negative "data loss" rate is expected behavior - the processor forwards original metrics while also processing them, resulting in metrics sent being double metrics received.

**Key Observations:**
- **Throughput Scaling**: >99.9% accuracy, matching NOP performance
- **CPU Usage**: 13-54% (2.5x higher than NOP due to sketch computation)
- **Memory**: 33-35 MB (similar to NOP)
- **Latency**: < 3ms despite additional processing

**Running Benchmarks:**
```bash
# NOP Processor
cd opentelemetry-collector-contrib-patch/cmd/nopcol
./bench.sh

# CountSketch Processor
cd opentelemetry-collector-contrib-patch/cmd/countsketchcol
./bench.sh
```

**Generated Files:**
Results are saved to `otel_collector_benchmark/benchmark_results/{processor}/`:
- `resource_*.csv`: CPU and memory usage over time (sampled every second)
- `memory_*.csv`: Memory usage from Prometheus metrics
- `latency_*.csv`: Query latency measurements for telemetry endpoint
- `loadgen_*.log`: Load generator output logs

## Features
* **Native OTLP/gRPC:** Sends standard `pdata` metrics directly to port 4317.
* **High Concurrency:** Spawns multiple worker threads to saturate network/CPU.
* **Realistic Payloads:** Simulates multiple hosts per worker with proper Resource Attributes.
* **Mixed Data Types:** Can alternate between Gauges (random fluctuation) and Sums (monotonic counters) to test different processor logic paths.

## Prerequisites
* Go 1.20 or higher

## Setup

1.  Initialize the module:
    ```bash
    go mod init loadgen
    ```

2.  Install dependencies:
    ```bash
    go get go.opentelemetry.io/collector/pdata/pmetric
    go get go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp
    go get google.golang.org/grpc
    go mod tidy
    ```

## Usage

Run the tool using `go run main.go` with the following flags:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--endpoint` | `localhost:4317` | The target OTLP gRPC address. |
| `--workers` | `1` | Number of concurrent worker threads. |
| `--hosts` | `1` | Number of unique hosts simulated **per worker**. |
| `--metrics` | `10` | Number of unique metrics generated per host. |
| `--interval` | `1s` | How often each worker flushes a batch. |
| `--duration` | `0s` | How long to run (e.g., `60s`, `5m`). `0s` runs forever. |
| `--type` | `mix` | Type of metrics to send: `mix`, `gauge`, or `sum`. |

### Assumptions
* **Insecure Connection:** The tool uses `insecure.NewCredentials()`. It assumes the target Collector has TLS disabled or is accepting insecure gRPC.
* **Fire-and-Forget:** It uses a short 5-second timeout per export. If the Collector is backpressured or down, the generator will log errors but continue running.

## Examples

**1. Basic Connectivity Test**
Send a small batch of mixed metrics to verify the receiver is working.
```bash
go run main.go --workers 1 --hosts 1 --metrics 10 --duration 10s
```
**2. High-Cardinality Stress Test Simulate 1,000 unique hosts (cardinality stress) sending data simultaneously for 1 minute.**
```bash
go run main.go --workers 10 --hosts 100 --metrics 20 --interval 1s --duration 1m
```

**3. Type-Specific Testing Test logic that only handles monotonic counters (Sums).**
```bash
go run main.go --type sum --metrics 50
```