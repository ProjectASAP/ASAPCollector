# Prometheus client profiling results

This directory is the output target for the producer-side
Prometheus `client_golang` profiling harness.

The harness is intentionally source-side only:

```text
synthetic app -> Prometheus Go client -> /metrics scrape
```

It does not include the collector, gateway, backend, or Prometheus
remote-write path. That makes the results comparable to the PR #195
OTel SDK profiling, which measures client-library cost before data
reaches downstream systems.

Run a smoke pass:

```bash
SOAK_S=20 PROFILE_SECONDS=15 \
CARDINALITIES="1000" \
FREQS="100" \
SCRAPE_INTERVALS="1s" \
UPDATE_MODES="cached dynamic" \
./deploy/scripts/run-prom-client-profiling-eval.sh
```

Run a first PR-quality pass:

```bash
SOAK_S=60 PROFILE_SECONDS=30 \
CARDINALITIES="1000 10000" \
FREQS="10 100 1000" \
SCRAPE_INTERVALS="1s 15s 60s" \
UPDATE_MODES="cached dynamic" \
./deploy/scripts/run-prom-client-profiling-eval.sh
```

Generated files:

- `prom-client-profiling-YYYYMMDD-HHMMSS.csv`: one row per profiling cell.
- `prom-client-profiling-YYYYMMDD-HHMMSS.log`: top-level run log.
- `profiles/*.cpu.pb` and `profiles/*.heap.pb`: raw pprof captures, kept locally.
- `profiles/*.top.txt`: `go tool pprof -top` summaries, kept locally.
- `logs/*.container.log` and `logs/*.scrapes.csv`: per-cell diagnostics, kept locally.

Only the summary CSV, top-level run log, README, and findings are checked
in. The per-cell logs and pprof artifacts are ignored so the PR stays close
to the artifact style used by the SDK-cost PR.
