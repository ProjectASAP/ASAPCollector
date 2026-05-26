# otel-app

`otel-app` is the single-binary synthetic OTLP metrics producer for the
ASAP SDK-aggregation evaluation. It consolidates the former
`fakemetricload` Zipf load generator and the former env-driven OTLP
producer into one `package main` at the module root:

```
go build -o otel-app .
go run .
```

## Value model

- **Synthetic latency values** are drawn from a **Zipf distribution**
  (`-zipf-s` / `-zipf-v` / `-zipf-max` / `-zipf-mean`).
- The **SDK pre-aggregates** each series per export window. The chosen
  aggregation (`-agg` / `-sketch-type`) is attached to every instrument
  via a single match-all View, so the collector receives one compact
  sketch (or raw) data point per series per window. The patched
  `AggregationDDSketch` / `KLLSketch` / `CountSketch` / `CountMinSketch`
  / `HLLSketch` / `RawBuffer` types live in the local `opentelemetry-go`
  tree.

## Modes

1. **Synthetic** (default) — a Zipf-latency gauge + an event counter per
   series at `-freq-hz`, plus the five-sketch MVP workload
   (`request_size_bytes` / `unique_users_per_min` / `top_endpoint_qps` /
   `endpoint_request_freq`) and the three freshness probes
   (`http_freshness_probe_{raw,warm,archive}`).
2. **Trace replay** (`-trace-file`) — replays a CSV of
   `(timestamp_ms, series_id, value)` rows at the recorded pace. See
   `replay-data/README.md`.

A runtime **control channel** (`-control-addr`) exposes
`POST /control/projection` to swap the SDK View's attribute projection
mid-run without a restart.

## Configuration

Configuration is **command-line flags + an optional YAML config file**.
There are **no environment variables**. Precedence (low to high):

```
built-in defaults  <  YAML (-config <file>)  <  explicitly-set flags
```

The YAML is parsed into the same `Config` struct that flag defaults
populate; then only the flags explicitly present on the command line
(detected via `flag.Visit`) are re-applied on top — so a CLI flag always
wins over the same key in the YAML.

```
otel-app -config config.yaml                       # YAML drives everything
otel-app -config config-kll.yaml -duration=30s     # YAML + a flag override
otel-app -sketch-type=ddsketch -cardinality=10 -freq-hz=2 -target=localhost:4317
```

### Flags

Every flag has an identically-named YAML key (dashes to underscores).

| Flag | YAML key | Default | Description |
|------|----------|---------|-------------|
| `-config` | — | `""` | Path to YAML config file (CLI flags override it). |
| `-target` | `target` | `gateway:4317` | OTLP/gRPC endpoint. |
| `-metric` | `metric` | `http_requests_total` | Base metric name (counter + `_latency_ms` gauge). |
| `-pprof-addr` | `pprof_addr` | `""` | pprof listen addr; empty = off. |
| `-producer-id` | `producer_id` | `""` | `producer_id` label prepended to every series; empty = none. |
| `-seed` | `seed` | `0` | Deterministic per-series PRNG base seed; 0 = auto-random. |
| `-sdk-window` | `sdk_window` | `15s` | SDK PeriodicReader export interval. |
| `-sdk-projection` | `sdk_projection` | `""` | Attribute keep-list: `""` keep-all, `zone,rack` keep listed, `-` drop-all. |
| `-agg` | `sdk_agg` | `default` | SDK aggregation kind (see below). Authoritative over `-sketch-type`. |
| `-max-buffer-per-series` | `max_buffer_per_series` | `0` | `raw-buffer` per-series event cap (0 = unbounded). |
| `-sketch-type` | `sketch_type` | `""` | Convenience alias: `ddsketch\|kll\|countsketch\|countminsketch\|hll\|baseline`. Used only when `-agg` is `default`. |
| `-cardinality` | `cardinality` | `500` | Synthetic: # distinct zone×rack×node×pod attribute sets. |
| `-freq-hz` | `freq_hz` | `10` | Synthetic: per-series event rate (Hz). |
| `-zone-vals` | `zone_vals` | `4` | # distinct zone values. |
| `-rack-vals` | `rack_vals` | `10` | # distinct rack values. |
| `-node-vals` | `node_vals` | `25` | # distinct node values. |
| `-pod-vals` | `pod_vals` | `10` | # distinct pod values. |
| `-zipf-s` | `zipf_s` | `1.1` | Zipf shape (must be > 1). |
| `-zipf-v` | `zipf_v` | `1.0` | Zipf shift (must be >= 1). |
| `-zipf-max` | `zipf_max` | `500` | Zipf integer range upper bound. |
| `-zipf-mean` | `zipf_mean` | `250` | Target mean for value scaling. |
| `-trace-file` | `trace_file` | `""` | If set, switch to trace replay of this CSV. |
| `-trace-scale` | `trace_scale` | `1.0` | Playback speed multiplier. |
| `-trace-loop` | `trace_loop` | `true` | Wrap trace replay at EOF. |
| `-freshness-probes` | `freshness_probes` | `true` | Emit the three freshness probe counters. |
| `-freshness-probe-hz` | `freshness_probe_hz` | `1.0` | Probe tick rate (Hz). |
| `-five-sketch-user-pool` | `five_sketch_user_pool` | `100` | HLL user-id pool size (50..2000). |
| `-five-sketch-endpoints` | `five_sketch_endpoints` | `50` | Zipfian endpoint cardinality (>= 5). |
| `-five-sketch-zipf-s` | `five_sketch_zipf_s` | `1.2` | Endpoint Zipfian s (> 1). |
| `-five-sketch-user-rotate` | `five_sketch_user_rotate` | `60s` | User-window rotate cadence. |
| `-control-addr` | `control_addr` | `""` | HTTP `/control/projection` listen addr; empty = off. |
| `-duration` | `duration` | `0` | Run duration; 0 = run until Ctrl+C. |

### `-agg` values

`default` · `sum` · `raw-buffer` · `dd-full` · `dd-delta` · `kll` ·
`cms-full` · `cms-delta` · `cs-full` · `cs-delta` · `hll-full` ·
`hll-delta`. The `*-delta` variants enable sparse delta encoding in the
cumulative export; `raw-buffer` keeps every event as its own data point
(capped by `-max-buffer-per-series`).

`-sketch-type` maps onto `-agg` as: `ddsketch→dd-full`, `kll→kll`,
`countsketch→cs-full`, `countminsketch→cms-full`, `hll→hll-full`,
`baseline→default`.

## Example config files

| File | Aggregation |
|------|-------------|
| `config.yaml` | DDSketch (full schema reference; all keys) |
| `config-ddsketch.yaml` | DDSketch |
| `config-kll.yaml` | KLL |
| `config-hll.yaml` | HLL |
| `config-countsketch.yaml` | CountSketch |
| `config-countminsketch.yaml` | CountMinSketch |
| `config-baseline.yaml` | none (raw Sum/LastValue) |
