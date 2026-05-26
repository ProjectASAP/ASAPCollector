# otel-app trace replay

otel-app's trace-replay mode reads a CSV of
`(timestamp_ms, series_id, value)` rows and emits them as OTLP
Gauges at the recorded pace. This is the
workload-credibility hook — run B0/B1/B2/B3/B5 against **real
production data** instead of the synthetic Zipf default.

## Quickstart: demo dataset

`demo-trace.csv` is checked into the repo — 100 instances × 600
samples (10 minutes at 1 Hz), ~2 MB. Statistically shaped to
resemble Google cluster-trace CPU usage (slow drift + sparse
spikes + rare flatlines + inter-instance diurnal correlation).

Regenerate with a different seed:

```
python3 gen-demo-trace.py 17 demo-trace.csv
```

Run a baseline against the demo trace:

```
docker compose \
  -f base.yml -f agents-N1.yml -f baseline-b3-delta.yml up -d
```

The compose stack already mounts `otel-app/replay-data/`
at `/trace/` in each otel-app container, and the producer is
launched with `-trace-file=/trace/demo-trace.csv` in the
`baseline-*-trace.yml` overlays.

## Swapping in the real Google 2019 cluster trace

The actual trace is in
`gs://clusterdata_2019_a/instance_usage/` (public BigQuery
dataset `google.com:google-cluster-data.instance_usage_*`).

Preprocessing checklist:
  1. Download a window — a single day's `instance_usage` is a few
     dozen GB compressed. 1 hour × 10k instances is a manageable
     ~5 GB shard for local smoke tests.
  2. Extract the three columns we need:
     * `start_time` (microseconds since 2011-01-01 00:00:00 UTC
       per the trace's convention — convert to an arbitrary
       millisecond epoch for the CSV)
     * a stable per-instance key (we use `collection_id ×
       instance_index`, but any hashable string works — it
       becomes the `series_id` label on the OTLP gauge)
     * `cpu_usage` (a float in [0, 1])
  3. Sort by `timestamp_ms` (otel-app sorts internally but
     a pre-sort speeds up the load).
  4. Emit as the same CSV schema as the demo.

Expected file size at real production cardinality (~10k
instances × 12 samples/hour × 24 hours) is ~80 MB — small enough
to bind-mount into a container without a separate loader
service.

## CSV schema

```
timestamp_ms,series_id,value
1700000000000,instance-0000,0.253907
...
```

- `timestamp_ms` (int64): monotonic, Unix-millis or any strictly
  increasing offset from a fixed origin. Only deltas matter.
- `series_id` (string): one label per series. Becomes a single
  `{series_id="…"}` attribute on the emitted gauge.
- `value` (float64): the gauge reading.

`-trace-scale` (default 1.0) scales playback speed —
`10` replays a 1-hour trace in 6 minutes, useful for shorter
sweeps. `-trace-loop` (default true) wraps at EOF for
long soaks.

## Limitations

* Each row is emitted as its own gauge — if the source data has
  aggregated multi-metric rows, flatten them before writing.
* Timestamps are advisory; otel-app uses wallclock at emit
  time for OTLP export. The CSV timestamp only governs the
  inter-sample sleep interval.
* The demo dataset is synthetic — `log-normal + drift` — so
  paper-facing claims that name the Google trace need the real
  preprocessed data.
