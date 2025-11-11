# Telegraf Benchmark Configs

This directory contains ready-to-run Telegraf configs that exercise two
extremes of the DataCollector pipeline:

| Config | Goal | Notes |
| --- | --- | --- |
| `max-throughput-gorilla-s3.conf` | Push the custom `gorilla_s3` output with huge batches to measure sustained write throughput and buffer backpressure. | Targets AWS S3. Replace the bucket/region and ensure credentials are available via the usual AWS provider chain. |
| `low-latency-file.conf` | Keep end-to-end latency minimal and flush every 500 ms to a local file to validate fast acknowledgement paths. | Useful for debugging ingestion latency without touching remote services. |
| `max-throughput-gorilla-local.conf` | Drive the gorilla encoder hard but persist the compressed `.gorilla` objects to disk for offline inspection. | No AWS dependency: set `local_dir` and omit bucket/region. |

## How to run

1. Build the local Telegraf binary so the gorilla plugin is available:

   ```bash
   cd DataCollector/telegraf
   make telegraf
   ```

2. Launch the desired scenario (adjust paths for your environment):

   ```bash
   ./telegraf \
     --config ../benchmarks/max-throughput-gorilla-s3.conf \
     --pprof-addr localhost:6060
   ```

   or

   ```bash
   ./telegraf \
     --config ../benchmarks/low-latency-file.conf \
     --pprof-addr localhost:6061
   ```

   or

   ```bash
   ./telegraf \
     --config ../benchmarks/max-throughput-gorilla-local.conf \
     --pprof-addr localhost:6062
   ```

3. Start the Fake Prometheus Exporter so the Prometheus input has something
   to scrape (or point the input at your own endpoints):

   ```bash
   cd ../FakePrometheusExporter
   python exporter_with_config.py --config exporter_config.yaml
   ```

4. Watch `internal_*` metrics (exported in both configs) or attach to the
   `pprof` endpoint for CPU/heap sampling (`go tool pprof
   http://localhost:6060/debug/pprof/profile?seconds=30`).

Feel free to fork these configs per test run—keeping them under version
control makes it easy to compare throughput/latency regressions.
