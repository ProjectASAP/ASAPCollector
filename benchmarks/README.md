# Telegraf Benchmark Configs

This directory contains ready-to-run Telegraf configs that exercise two
extremes of the DataCollector pipeline:

| Config | Goal | Notes |
| --- | --- | --- |
| `max-throughput-gorilla-s3.conf` | Push the custom `gorilla_s3` output with huge batches to measure sustained write throughput and buffer backpressure. | Targets AWS S3. Replace the bucket/region and ensure credentials are available via the usual AWS provider chain. |
| `low-latency-file.conf` | Keep end-to-end latency minimal and flush every 500 ms to a local file to validate fast acknowledgement paths. | Useful for debugging ingestion latency without touching remote services. |
| `max-throughput-gorilla-local.conf` | Drive the gorilla encoder hard but persist the compressed `.gorilla` objects to disk for offline inspection. | No AWS dependency: set `local_dir` and omit bucket/region. |
| `max-throughput-prometheus-loop.conf` | Treat Telegraf like a Prometheus “bump in the wire”: scrape fake exporters, skip processing, and re-export via `outputs.prometheus_client`. | Good for validating scrape/flush throughput without S3 or file IO. |
| `max-throughput-prometheus-client.conf` | Replace the scrape input with a raw socket listener and re-export via `prometheus_client`. | Use `send_firehose.py` to push arbitrary line protocol into tcp://localhost:8094. |
| `max-throughput-null.conf` | Measure Telegraf’s internal pipeline limits by pairing the socket firehose with `outputs.discard`. | Ingest on tcp://localhost:8095 and drop immediately while logging internal metrics. |
| `send_firehose_null.py` | Bench the generator itself by streaming to `/dev/null` via a local sink—no Telegraf involved. | Useful to understand the firehose’s ceiling before it hits Telegraf. |

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

   or

   ```bash
   ./telegraf \
     --config ../benchmarks/max-throughput-prometheus-loop.conf \
     --pprof-addr localhost:6063

   or

   ```bash
   ./telegraf \
     --config ../benchmarks/max-throughput-prometheus-client.conf \
     --pprof-addr localhost:6063

   or

   ```bash
   ./telegraf \
     --config ../benchmarks/max-throughput-null.conf \
     --pprof-addr localhost:6064
   ```
   ```

3. Start the Fake Prometheus Exporter so the Prometheus input has something
   to scrape (or point the input at your own endpoints):

   ```bash
   cd ../FakePrometheusExporter
   python exporter_with_config.py --config exporter_config.yaml

   For the socket_listener firehose (`max-throughput-prometheus-client.conf`),
   run the bundled generator:

   ```bash
   python benchmarks/send_firehose.py              # default: localhost:8094
   # use --port 8095 when driving max-throughput-null.conf
   # use --rate to throttle, or leave unset for best-effort firehose
   # example with custom measurement/tags:
   # python benchmarks/send_firehose.py --measurement bench --tags "source=gen01,region=west" --fields value,latency
   # spawn multiple workers for more parallel writers:
   # python benchmarks/send_firehose.py --processes 4 --rate 200000

   To benchmark the generator alone (no Telegraf), run the helper that spins up
   a local `/dev/null` sink:

   ```bash
   python benchmarks/send_firehose_null.py --port 19000
   ```
   ```
   ```

4. Watch `internal_*` metrics (exported in both configs) or attach to the
   `pprof` endpoint for CPU/heap sampling (`go tool pprof
   http://localhost:6060/debug/pprof/profile?seconds=30`).

Feel free to fork these configs per test run—keeping them under version
control makes it easy to compare throughput/latency regressions. When runs
finish, summarize Telegraf's internal throughput/latency with:

```bash
python benchmarks/summarize_telegraf_metrics.py --results-dir benchmarks/results
```

The summarizer now also reports CPU/RSS if the underlying `.lp` files include
`procstat` metrics (as in `max-throughput-null.conf`).
