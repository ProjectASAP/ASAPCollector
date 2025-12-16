## OpenTelemetry App Helpers

- `cmd/fakemetricload`: Emits synthetic metrics with configurable distributions, metric types (DDSketch histogram, Prometheus histogram, raw gauge, counter), total series per metric, and separate export intervals for raw gauge vs. aggregations. Used by `run_raw_gauge.sh` and `run_ddsketch.sh`.

### Running the generators

Two convenience scripts demonstrate typical configurations:

```bash
# Raw gauge stress test
./run_raw_gauge.sh

# DDSketch-only stress test
./run_ddsketch.sh
```

Both scripts accept the standard `GOFLAGS`/`OTEL_EXPORTER_OTLP_ENDPOINT` env overrides because they just call `go run ./cmd/fakemetricload`.
