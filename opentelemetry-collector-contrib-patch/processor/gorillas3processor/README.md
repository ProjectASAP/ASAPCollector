# gorillas3processor

Phase 2 of the Gorilla-S3-cold-engine. Compresses incoming Gauge / Sum
metrics with Gorilla XOR-delta encoding on a tumbling window and PUTs
the resulting chunks to an S3-compatible object store (AWS S3, MinIO).

The encoded GORILLA1 block layout is byte-compatible with the Phase 1 Rust
`asap-gorilla` decoder and the Telegraf `gorilla_s3` output plugin
(which carries pre-encoded payloads with the same body shape).

## When to use it

Run on the **asap-otel agent** path when the controller's plan asks
the agent to land raw samples in cold-store rather than forward them
to the gateway. The processor sets `drop_original: true` by default so
the agent does not OTLP-forward the metric further.

This is **not** a paper baseline — see `gorillaprocessor` (`processor:
gorilla`, local-dir-only) for the b5-gorilla baseline that the paper
chart pins.

## Config

| Field             | Type           | Default                                       | Notes                                                                                                                                       |
| ----------------- | -------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `window_interval` | `duration`     | `60s`                                         | Tumbling window. On each tick: encode → PUT → update index.                                                                                 |
| `max_object_bytes`| `int64`        | `0` (unlimited)                               | Cap a single chunk size; multi-chunks per metric per window when exceeded.                                                                  |
| `tenant`          | `string`       | `default`                                     | Substituted into `{tenant}` in `prefix_template`.                                                                                           |
| `endpoint`        | `string`       | `""` (AWS default)                            | e.g. `http://minio:9000` for MinIO.                                                                                                         |
| `bucket`          | `string`       | **required**                                  | Destination bucket. The processor does not create it.                                                                                       |
| `prefix_template` | `string`       | `{tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/`    | UTC-bucketed key layout. Tokens: `{tenant}`, `{metric}`, `{YYYY}`, `{MM}`, `{DD}`, `{HH}`.                                                  |
| `access_key_id`   | `string`       | `""` (default chain)                          | Set together with `secret_access_key`, or both empty.                                                                                       |
| `secret_access_key` | `string`     | `""` (default chain)                          | See above.                                                                                                                                  |
| `use_ssl`         | `bool`         | `false`                                       | http for local MinIO; https for AWS S3.                                                                                                     |
| `region`          | `string`       | `us-east-1`                                   | Required by the SDK even for MinIO.                                                                                                         |
| `drop_original`   | `bool`         | `true`                                        | If true, do not forward metrics to the next consumer.                                                                                       |
| `max_retries`     | `int`          | `3`                                           | PutObject retries with linear backoff.                                                                                                      |
| `retry_backoff`   | `duration`     | `1s`                                          | Step for `(attempt+1) * retry_backoff`.                                                                                                     |
| `upload_timeout`  | `duration`     | `30s`                                         | Per-PUT context timeout.                                                                                                                    |
| `local_spool_dir` | `string`       | `""` (no spool)                               | Optional: when set, S3 PutObject failure spills the chunk to this directory.                                                                |
| `block_format`    | `string`       | `asap`                                        | `asap` \| `prometheus_tsdb` \| `both`. Selects the cold-store on-disk layout. mvp/step2.1.                                                  |
| `tsdb_bucket`     | `string`       | `""` (falls back to `bucket`)                 | Destination bucket for Prometheus TSDB blocks. Strongly recommended to use a separate bucket so the two layouts do not co-mingle.           |
| `tsdb_block_duration` | `duration` | (== `window_interval`)                        | Block-writer block-size hint, in milliseconds. Aligns with the controller's per-window plan cadence.                                        |
| `tsdb_external_labels` | `map[string]string` | `{}`                                | External labels added to every series in the emitted Prometheus block, e.g. `cluster: prod`.                                                |

## Example

```yaml
processors:
  gorillas3:
    window_interval: 60s
    drop_original: true
    endpoint: http://minio:9000
    bucket: asap-gorilla
    region: us-east-1
    use_ssl: false
    access_key_id: ${env:MINIO_ACCESS_KEY}
    secret_access_key: ${env:MINIO_SECRET_KEY}
    prefix_template: "{tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/"
    tenant: default
```

## Object layout

`block_format: asap` (default):

```
<bucket>/<tenant>/<metric>/YYYY/MM/DD/HH/part-<unix>-<NNNNNN>.gor
<bucket>/<tenant>/<metric>/YYYY/MM/DD/HH/index.json
<bucket>/<tenant>/<metric>/YYYY/MM/DD/HH/postings-v1.json
```

`block_format: prometheus_tsdb` (mvp/step2.1):

```
<tsdb_bucket>/<ULID>/chunks/000001     (Prometheus chunks file; XOR + delta-of-delta)
<tsdb_bucket>/<ULID>/index             (Prometheus index file)
<tsdb_bucket>/<ULID>/meta.json         (Prometheus block meta — uploaded LAST)
```

The block ULID encodes the wall-clock time of the flush — the high
48 bits are millisecond-precision timestamp, the low 80 bits are
random — so a `LIST` of `<tsdb_bucket>/` returns blocks in
chronological order without an extra index. `meta.json` is uploaded
last so a Thanos store-gateway scanning the bucket while the upload
is in flight does not pick up a half-written block.

`block_format: both` emits both layouts concurrently from the same
window snapshot. Used during migration / verification only.

Each chunk is one GORILLA1 block (one metric, one or more series). Header:

```
[4]   "GORILLA1"
[1]   version (1)
[4]   uint32 LE  series_count
```

Each series body inside the block:

```
[2]   uint16 LE  metaLen
[*]              metaLen bytes JSON metadata
[4]   uint32 LE  point_count
[8]   uint64 LE  first_ts (UnixNano)
[8]   uint64 LE  first_value_bits (IEEE 754)
[4]   uint32 LE  ts_bits_len
[*]              ceil(ts_bits_len/8) bytes  Gorilla delta-of-delta TS stream
[4]   uint32 LE  val_bits_len
[*]              ceil(val_bits_len/8) bytes Gorilla XOR float stream
```

`index.json` is per-`(metric, hour)` and lists all chunks with their
time range, sample count, label hash and size.

## Self-monitoring metrics

The processor uses the upstream `selfmonitor` package for the standard
processor in/out byte / point counters and adds the following
gorillas3-specific counters under the meter
`github.com/ProjectASAP/opentelemetry-collector-contrib/processor/gorillas3processor`:

* `gorillas3_chunks_written_total`
* `gorillas3_chunk_bytes_written_total`
* `gorillas3_chunk_points_written_total`
* `gorillas3_s3_put_failures_total`

Each carries `processor.id` + `processor.type` attributes.
