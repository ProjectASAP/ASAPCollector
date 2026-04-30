# Delta-Transmission Sweep (`bench_delta_sweep.sh`)

Companion to `bench_delta.sh`. Where `bench_delta.sh` runs a single
`(sketch, mode, delta on/off, rate)` configuration, this harness sweeps the
**`window_duration` x `delta_threshold`** matrix for every collector that
has a delta-enabled config.

## What gets swept

* `window_duration` (paper default): `1s, 5s, 30s, 5m`
* `delta_threshold` (paper default): `0, 0.1, 1.0`
* Cross product: 12 cells per processor.

## Which processors

Anything in `opentelemetry-collector-contrib-patch/cmd/*/config-*delta*.yaml`
with a `window` mode and the `window_duration` + `delta_threshold` knobs.
That's currently:

* `countminsketch` (binary: `cmd/countminsketchcol/dist/countminsketchcol`)
* `countsketch`    (binary: `cmd/countsketchcol/dist/countsketchcol`)

`hll` has only a `window-delta` config but with different knob names; not
included in this sweep — extend the script if needed.

## What gets recorded

Per cell, one row in `results/delta_sweep/delta_sweep.csv`:

| column | source |
|---|---|
| `processor`, `window`, `threshold` | sweep cell key |
| `bandwidth_bytes_per_sec` | average response size of `GET /metrics` on :8889, sampled 1 Hz |
| `cpu_pct` | mean of `ps -p <pid> -o %cpu`, sampled 1 Hz |
| `heap_peak_mb` | peak RSS of the collector process from `ps`, in MB |
| `output_input_ratio` | bandwidth_out / SDK avg gRPC input bytes-per-sec |
| `duration_sec`, `rate_mps` | sweep settings |
| `status` | `OK`, `SKIP_NO_BINARY`, or `COLLECTOR_DIED` |

A markdown summary `results/delta_sweep/delta_sweep.md` is rendered after
the run with one table per processor.

## Files

* `bench_delta_sweep.sh` — the harness.
* `delta_sweep_config_template.yaml` — collector config with
  `${PROCESSOR}`, `${PROC_BLOCK}`, `${WINDOW}`, `${THRESHOLD}` placeholders.
  The script does the templating in pure Python (not envsubst, because
  envsubst collapses the multi-line `${PROC_BLOCK}` value).
* `results/delta_sweep/<proc>_w<window>_t<threshold>/` — per-cell artefacts:
  rendered `config.yaml`, `collector.log`, `sdk.log`,
  `collector_resource.csv`, `bandwidth.csv`, plus the SDK summary JSON.

## Smoke-test usage (defaults)

```bash
./bench_delta_sweep.sh --smoke
```

Runs exactly one cell (`countminsketch`, window=1s, threshold=0, 10s) and
proves the plumbing end-to-end. Completes in well under a minute.

## Paper-scale usage

```bash
./bench_delta_sweep.sh \
    --processors "countminsketch countsketch" \
    --windows    "1s 5s 30s 5m" \
    --thresholds "0 0.1 1.0" \
    --duration   60s \
    --rate       30000 \
    --series     1000 \
    --warmup     10
```

24 cells (12 per processor) x 60s + 10s warmup + ~3s teardown ≈ 30 min.

Override the output directory with `--output-dir /path/to/results`.

## Dependencies

* The collector binary for each `--processors` entry must be **pre-built**
  in `opentelemetry-collector-contrib-patch/cmd/<proc>col/dist/`. The
  script does not build collectors; if a binary is missing the cell is
  marked `SKIP_NO_BINARY` and the sweep continues. Build with the OTel
  builder (see `bench_delta.sh::build_collector`).
* The synthetic load generator `opentelemetry-app/cmd/e2esdkbench` must
  compile in the workspace. As of 2026-04-29 it depends on a
  `go.opentelemetry.io/proto/otlp` replacement directory at
  `../opentelemetry-proto/gen/go/...` — if that workspace clone is
  missing, the SDK loadgen will exit non-zero and the cell row will show
  `bandwidth_bytes_per_sec=0` and `output_input_ratio=0`. The collector
  side (CPU/heap/peak) is still sampled and is still meaningful. Status
  stays `OK` because the collector itself ran.
* `python3`, `curl`, `lsof`, `bc`, `ps`, `awk`.

## Smoke-run result (2026-04-29)

```
Cells: 1 (duration 10s per cell, rate 30000 MPS, 1000 series)

| window | threshold | bw B/s | cpu % | heap peak MB | out/in | status |
|---|---|---|---|---|---|---|
| 1s | 0 | 0.00 | 2.60 | 168.79 | 0 | OK |
```

End-to-end plumbing verified: config rendered, collector started and
sampled (CPU 2.6%, RSS 168 MB), CSV row written, markdown summary
generated. Bandwidth and out/in ratio came back zero in this run because
the SDK loadgen (`go run ./cmd/e2esdkbench`) failed to compile against
the local go-workspace `replace` directives — a pre-existing environment
issue, not a sweep-script bug. Once the SDK workspace is intact those
columns will populate.

## How to extend

* Add another sketch type: extend the `processor_binary`,
  `processor_yaml_name`, and `processor_block` shell functions, and pass
  `--processors "...."` on the CLI.
* Add another knob (e.g. `rows`/`columns` for CMS): turn it into another
  templated variable in `delta_sweep_config_template.yaml`, add another
  CLI sweep dimension and another nested loop in the runner.
