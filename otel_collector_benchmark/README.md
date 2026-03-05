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
| 50,000 MPS  | 49,983 MPS        | 99.97%       | 20.82%  | 37.85 MB    | 0%        | 1.63 ms     | 2.33 ms     | 2.86 ms     |

**Key Observations:**
- **Throughput Scaling**: >99.8% accuracy across all scenarios (using microsecond-precision intervals)
- **CPU Usage**: Linear scaling (~0.4% per 10k MPS), reaching 20.82% at 50k MPS
- **Memory**: Stable 37-39 MB across all loads
- **Zero Data Loss**: 0% data loss across all scenarios
- **Latency**: Consistently < 3ms (P99 < 2.9ms)

### CountSketch Processor Benchmark

The CountSketch processor aggregates metrics into Count Sketch data structures for frequency estimation and heavy hitter detection. It now supports two modes:

- **`batch`**: per-batch aggregation and flush; the processor keeps (or drops) original metrics based on `drop_original` and emits CountSketch summary metadata per batch.
- **`window`**: tumbling-window aggregation over a configurable `window_size`; the processor emits one summary for each window and drops raw metrics to achieve storage reduction.

After each CountSketch benchmark scenario, the central `bench.sh` script also runs a **basic CountSketch correctness check** by scraping the Prometheus exporter on port `8889` and verifying that both `countsketch_row` and `countsketch_col` metadata metrics are present; if either metric is missing, the scenario is flagged as a failure.

**Processor Configuration (batch):** mode: `batch`, epsilon: 0.01, delta: 0.99, drop_original: false  
**Processor Configuration (window):** mode: `window`, window_size: 5s, epsilon: 0.01, delta: 0.99, drop_original: true

**Results Summary (batch mode):**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Output/Input Ratio | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|--------------------|-------------|-------------|-------------|
| 10,000 MPS  | 9,983 MPS         | 99.83%       | 44.00%  | 37.44 MB    | 1.02x (expansion)  | 1.72 ms     | 2.42 ms     | 2.77 ms     |
| 20,000 MPS  | 20,000 MPS        | 100.00%      | 94.00%  | 36.27 MB    | 1.01x (expansion)  | 1.73 ms     | 2.59 ms     | 2.84 ms     |
| 30,000 MPS  | 30,000 MPS        | 100.00%      | 140.00% | 38.00 MB    | 1.01x (expansion)  | 1.73 ms     | 2.62 ms     | 3.00 ms     |
| 40,000 MPS  | 40,000 MPS        | 100.00%      | 185.00% | 36.89 MB    | 1.01x (expansion)  | 1.76 ms     | 2.61 ms     | 3.00 ms     |
| 50,000 MPS  | 50,000 MPS        | 100.00%      | 221.00% | 36.86 MB    | 1.01x (expansion)  | 1.73 ms     | 2.48 ms     | 2.90 ms     |

**Results Summary (window mode, 5s window):**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Data Loss* | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|------------|-------------|-------------|-------------|
| 10,000 MPS  | 9,983 MPS         | 99.83%       | 13.00%  | 35.55 MB    | 99.99%*    | 1.91 ms     | 2.81 ms     | 2.94 ms     |
| 20,000 MPS  | 20,000 MPS        | 100.00%      | 24.00%  | 35.91 MB    | 99.99%*    | 1.88 ms     | 2.65 ms     | 3.08 ms     |
| 30,000 MPS  | 30,000 MPS        | 100.00%      | 34.00%  | 35.68 MB    | 99.99%*    | 1.92 ms     | 2.76 ms     | 3.10 ms     |
| 40,000 MPS  | 39,991 MPS        | 99.98%       | 44.00%  | 35.16 MB    | 99.99%*    | 1.90 ms     | 2.65 ms     | 2.79 ms     |
| 50,000 MPS  | 49,986 MPS        | 99.97%       | 55.00%  | 34.80 MB    | 99.99%*    | 1.79 ms     | 2.59 ms     | 3.02 ms     |

\* **Data Loss Note**: In window mode, the vast majority of raw metrics are intentionally dropped; each window produces a small number of CountSketch summary metrics.

**Key Observations:**
- **Batch mode**: Near-1:1 output/input ratio (1.01–1.02x); suitable when you need both raw metrics and periodic sketch summaries.
- **Window mode**: ~99.99% storage reduction; CPU remains moderate (≤55% at 50k MPS) with sub-3ms P99 latency.

### CountMinSketch Processor Benchmark

The CountMinSketch processor aggregates metrics into Count-Min Sketch data structures with window-based aggregation. With `drop_original: true`, it drops original metrics and emits only sketch summaries for storage reduction.

**Processor Configuration:**
- rows: 5, columns: 1000, seed: 1, window_interval: 10s, drop_original: true

**Results Summary:**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Data Loss* | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|------------|-------------|-------------|-------------|
| 10,000 MPS  | ~10,000 MPS       | ~100.00%     | 6.59%   | 202.45 MB   | 99.99%*    | 1.70 ms     | 2.10 ms     | 2.83 ms     |
| 20,000 MPS  | ~20,000 MPS       | ~100.00%     | 11.85%  | 202.27 MB   | 99.99%*    | 1.67 ms     | 2.12 ms     | 2.60 ms     |
| 30,000 MPS  | ~30,000 MPS       | ~100.00%     | 16.63%  | 201.85 MB   | 99.99%*    | 1.63 ms     | 2.05 ms     | 2.32 ms     |
| 40,000 MPS  | ~40,000 MPS       | ~100.00%     | 20.88%  | 202.91 MB   | 99.99%*    | 1.57 ms     | 1.98 ms     | 2.25 ms     |
| 50,000 MPS  | 49,985 MPS        | 99.97%       | 25.26%  | 204.3 MB    | 99.99%*    | 1.55 ms     | 1.95 ms     | 2.53 ms     |

\* **Data Loss Note**: The 99.99% "data loss" is expected and intentional. With `drop_original: true`, original metrics are dropped and only aggregated sketch summaries are emitted. This achieves the storage reduction goal.

**Key Observations:**
- **Throughput Scaling**: >99.9% accuracy, matching NOP performance
- **CPU Usage**: 6.4-25.3% (1.21x higher than NOP at 50k MPS)
- **Memory**: 201-204 MB (5.4x higher than NOP due to sketch data structures)
- **Storage Reduction**: 99.99% reduction (only sketch summaries emitted)
- **Latency**: Comparable to NOP (< 2.6ms P99), actually slightly better at higher loads

### KLL Processor Benchmark

The KLL (K-LL) processor aggregates metrics using the K-LL sketch algorithm for quantile estimation. It supports two modes (aligned with DDSketch):

- **`batch`**: per-batch aggregation and flush; the processor keeps the original gauge series and appends quantile metrics (e.g. `_p50`, `_p90`, `_p99`).
- **`window`**: tumbling-window aggregation with a configurable `window_duration`; the processor accumulates raw samples per series and emits only quantile metrics at each window boundary.

The benchmark runs a **correctness check** after each scenario (scrape port 8889): quantile metrics exist, monotonicity `p50 <= p90 <= p99`, and values within `[0.99, 507]` (Zipf range).

**Processor Configuration (batch):** mode: `batch`, k: 256, quantiles: [0.5, 0.9, 0.99], drop_original: false  
**Processor Configuration (window):** mode: `window`, window_duration: 10s, k: 256, quantiles: [0.5, 0.9, 0.99]

**Results Summary (batch mode):**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Output/Input Ratio | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|--------------------|-------------|-------------|-------------|
| 10,000 MPS  | 9,991 MPS         | 99.91%       | 10.00%  | 42.36 MB    | 1.30x (expansion)  | 1.50 ms     | 2.01 ms     | 2.45 ms     |
| 20,000 MPS  | 20,000 MPS        | 100.00%      | 19.00%  | 43.04 MB    | 1.30x (expansion)  | 1.55 ms     | 2.12 ms     | 2.67 ms     |
| 30,000 MPS  | 30,000 MPS        | 100.00%      | 28.00%  | 43.51 MB    | 1.30x (expansion)  | 1.51 ms     | 2.09 ms     | 2.65 ms     |
| 40,000 MPS  | 39,983 MPS        | 99.96%       | 38.00%  | 44.14 MB    | 1.30x (expansion)  | 1.54 ms     | 2.17 ms     | 2.74 ms     |
| 50,000 MPS  | 49,983 MPS        | 99.97%       | 46.00%  | 44.02 MB    | 1.30x (expansion)  | 1.48 ms     | 1.94 ms     | 2.63 ms     |

**Results Summary (window mode, 10s window):**

| Target Rate | Actual Throughput | Throughput % | Avg CPU | Peak Memory | Data Loss* | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|--------------|---------|-------------|------------|-------------|-------------|-------------|
| 10,000 MPS  | 9,998 MPS         | 99.98%       | 6.00%   | 47.92 MB    | 97.00%*    | 1.47 ms     | 1.85 ms     | 2.15 ms     |
| 20,000 MPS  | 20,000 MPS        | 100.00%      | 11.00%  | 49.30 MB    | 98.50%*    | 1.45 ms     | 1.89 ms     | 2.50 ms     |
| 30,000 MPS  | 29,996 MPS        | 99.99%       | 16.00%  | 53.34 MB    | 98.99%*    | 1.41 ms     | 1.82 ms     | 2.13 ms     |
| 40,000 MPS  | 39,983 MPS        | 99.96%       | 21.00%  | 56.69 MB    | 99.24%*    | 1.44 ms     | 1.92 ms     | 2.37 ms     |
| 50,000 MPS  | 49,983 MPS        | 99.97%       | 26.00%  | 55.18 MB    | 99.39%*    | 1.41 ms     | 1.83 ms     | 2.47 ms     |

\* **Data Loss Note**: Expected in window mode: raw samples are compressed into quantile metrics per window.

**Key Observations:**
- **Batch mode**: Output/input ratio ~1.30x (one original + three quantiles per series); throughput and latency similar to other sketch processors.
- **Window mode**: High “data loss” by design; lower CPU (6–26%) and moderate memory (48–57 MB); correctness check (p50 ≤ p90 ≤ p99, bounds) passes for all rates.

### DDSketch Processor Benchmark

The DDSketch processor aggregates metrics into DDSketch data structures for approximate quantile estimation. It supports two modes:

- `batch`: per-batch aggregation and flush; the processor keeps the original gauge series and appends quantile metrics.
- `window`: tumbling-window aggregation with a configurable `window_duration`; the processor turns many raw samples into a small number of quantile metrics per series per window.

In both modes, the collector receives standard OTLP Gauge metrics from this load generator, builds/merges DDSketches per series, and emits quantile gauges with a `ddsketch_quantile` label (`0.5`, `0.9`, `0.99`). The benchmark now also runs a basic **correctness check** after each scenario by scraping the Prometheus exporter on port `8889` and verifying:

- Quantile metrics exist.
- Monotonicity: `p50 <= p90 <= p99`.
- Bounds: all quantiles fall within `[0.99, 507]`, matching the Zipf generator’s effective range and DDSketch’s configured relative accuracy.

**Status:** At present, only a **partial window-mode table** (10k and 20k MPS) is recorded below for DDSketch; batch-mode and higher-rate window-mode rows will be added once those runs are captured.

**Processor Configuration (window mode example):**
- mode: `window`
- window_duration: `10s` (for the benchmark; `60s` is a common production value)
- relative_accuracy: `0.01`
- emit_ddsketch: `false`
- quantiles: `[0.5, 0.9, 0.99]`

**Results Summary (window mode, 10s window — partial):**

| Target Rate | Actual Throughput | Avg CPU | Peak Memory | Data Loss* | Avg Latency | P95 Latency | P99 Latency |
|------------|-------------------|---------|-------------|-----------|-------------|-------------|-------------|
| 10,000 MPS | 19,983 MPS        | 32.7%   | 218.9 MB    | ~99%      | 1.27 ms     | 1.83 ms     | 1.94 ms     |
| 20,000 MPS | 39,991 MPS        | 17.2%   | 215.2 MB    | 99.24%    | 1.18 ms     | 1.66 ms     | 1.77 ms     |

\* **Data Loss Note:** As with the other sketch processors, this “data loss” is expected. For each host/metric series, millions of raw samples per minute are compressed into a handful of quantile metrics per window.

Batch mode shows the complementary behavior: it keeps the originals and appends quantile metrics, so the exporter sends roughly 4× as many points as the receiver accepts (one original + three quantiles per sample). For that mode the script reports an **Output/Input Ratio** instead of a loss percentage.

**Running Benchmarks:**
```bash
# Run benchmarks from the cmd directory
cd opentelemetry-collector-contrib-patch/cmd

# NOP Processor
./bench.sh nopcol

# CountSketch Processor
./bench.sh countsketchcol

# CountSketch Processor (batch mode)
./bench.sh countsketchcol-batch

# CountSketch Processor (window mode)
./bench.sh countsketchcol-window

# CountMinSketch Processor
./bench.sh countminsketchcol

# KLL Processor (legacy single config)
./bench.sh kll

# KLL Processor (batch mode)
./bench.sh kll-batch

# KLL Processor (window mode)
./bench.sh kll-window

# DDSketch Processor (batch mode)
./bench.sh ddsketchcol-batch

# DDSketch Processor (window mode)
./bench.sh ddsketchcol-window
```

**Note:** All processors use the centralized benchmark script located at `opentelemetry-collector-contrib-patch/cmd/bench.sh`. If you need to build processors that use private modules, ensure `GOPRIVATE` and `GONOSUMDB` environment variables are set appropriately before running the benchmark script.

## Comparative Analysis

### Performance Comparison at 50,000 MPS

**Note:** DDSketch 50k MPS results will be added to this table once dedicated runs are captured; until then, only the other sketch processors are compared here.

| Processor | CPU Usage | Memory Usage | Latency (Avg) | Latency (P99) | Storage Reduction / Ratio |
|-----------|----------|--------------|---------------|---------------|---------------------------|
| **NOP** | 20.82% | 37.85 MB | 1.63 ms | 2.86 ms | 0% (baseline) |
| **CountSketch (batch)** | 221.00% (10.62x) | 36.86 MB (0.97x) | 1.73 ms (+0.10ms) | 2.90 ms (+0.04ms) | 1.01x (expansion) |
| **CountSketch (window)** | 55.00% (2.64x) | 34.80 MB (0.92x) | 1.79 ms (+0.16ms) | 3.02 ms (+0.16ms) | 99.99% |
| **CountMinSketch** | 25.26% (1.21x) | 204.3 MB (5.40x) | 1.55 ms (-0.08ms) | 2.53 ms (-0.33ms) | 99.99% |
| **KLL (batch)** | 46.00% (2.21x) | 44.02 MB (1.16x) | 1.48 ms (-0.15ms) | 2.63 ms (-0.23ms) | 1.30x (expansion) |
| **KLL (window)** | 26.00% (1.25x) | 55.18 MB (1.46x) | 1.41 ms (-0.22ms) | 2.47 ms (-0.39ms) | 99.39% |
| **KLL** (legacy) | 52.49% (2.52x) | 38.40 MB (1.01x) | 1.51 ms (-0.12ms) | 2.50 ms (-0.36ms) | 99.99% |

### Key Insights

1. **CPU Efficiency**: 
   - CountMinSketch is most CPU-efficient (1.21x overhead vs NOP)
   - CountSketch **window** and KLL **window** have moderate CPU overhead (2.6x and 1.25x respectively) with strong storage reduction
   - CountSketch **batch** trades significantly higher CPU for keeping raw metrics plus sketch summaries

2. **Memory Efficiency**:
   - CountSketch (both modes) is slightly more memory-efficient than NOP (~0.9–0.97x)
   - KLL uses similar memory to NOP (1.0–1.5x), making it very memory-efficient for quantile estimation
   - CountMinSketch uses 5.40x more memory due to larger sketch data structures

3. **Latency**:
   - All processors maintain sub-3ms latency (P99 < 3.1ms)
   - KLL and CountMinSketch show slightly better latency than NOP at high loads
   - CountSketch batch/window add small additional latency but stay within tight SLOs

4. **Storage Reduction**:
   - CountSketch (window), CountMinSketch, and KLL (window/legacy) all achieve ~99.99% storage reduction
   - Batch modes (KLL, CountSketch) expand the stream slightly to add quantile or sketch metadata while keeping originals

5. **Trade-offs**:
   - **CountSketch (batch)**: Very high CPU but minimal storage overhead; best when raw metrics and sketch summaries are both required.
   - **CountSketch (window)**: Moderate CPU, low memory, and 99.99% reduction; good for heavy-hitter style analytics with strict storage budgets.
   - **CountMinSketch**: Lower CPU (1.21x), higher memory (5.40x), good for memory-abundant environments
   - **KLL (batch)**: Expansion mode (~1.30x output); quantiles appended to each batch; CPU 2.21x at 50k MPS
   - **KLL (window)**: Lower CPU (1.25x at 50k MPS), moderate memory; quantiles emitted per window; 99%+ storage reduction
   - **KLL** (legacy): Higher CPU (2.52x), similar memory (1.01x), provides quantile estimation with excellent latency
   - All processors maintain excellent throughput and latency characteristics

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