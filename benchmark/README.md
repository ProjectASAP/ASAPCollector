# OTLP Metric Load Generator

A high-performance, multi-threaded load generator for OpenTelemetry Collectors. It sends OTLP metrics over gRPC, allowing for configurable stress testing of receivers and processors.

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