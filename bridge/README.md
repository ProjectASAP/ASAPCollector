# OTEL - Telegraf Bridge
This folder contains the instructions for setting up OTEL to Telegraf communication, as well as some utility and benchmarking scripts.  
Communication between OTEL and Telegraf is dona via gRPC and the OTLP protocol. OTEL exports data via the `otlp` exporter, and Telegraf receives this data via the `inputs.opentelemetry` plugin. A basic setup is provided in `configs/otel-debug.yaml` and `configs/telegraf-debug.conf`.
# Usage
```bash
./build.sh # you may need to install some additional packages; see the output of the command

./run-debug.sh
# or
./run-benchmark.sh
```
# Directory Structure
1. `build.sh`: Build OTEL and Telegraf. OTEL plugins are derived from `configs/otel-build.yaml` and Telegraf plugins are any that are used in the `configs/` directory.
2. `run-debug.sh`: Pipes in a very small number of metrics (5 by default) to the pipeline with no processors. Telegraf will write what it receives to `out/telegraf-debug.json`.
    - This is meant to be used for manual verification (i.e. insert your processors into the respective config files and check output).
    - Uses `configs/otel-debug.yaml` and `configs/telegraf-debug.conf` by default.
    - See `./run.debug.sh -h` for options.
3. `run-benchmark.sh`: Benchmarks the OTEL - Telegraf pipeline. Outputs metrics per second (for both OTEL and Telegraf), Telegraf latency, and CPU/memory usage.
    - To benchmark your processor, either modify the default configs (see next line) or pass in your own config files.
    - Uses `configs/otel-bench.yaml` and `configs/telegraf-bench.conf` by default.
    - See `./run-benchmark.sh -h` for options.
2. `configs/`: All config files should be placed here
