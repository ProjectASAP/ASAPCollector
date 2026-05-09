# gorillas3processor

`gorillas3processor` is the ASAP cold-path bridge into Thanos:

```text
Prometheus TSDB block files -> S3/MinIO -> Thanos store-gateway/query -> ASAPQuery ThanosForwardEngine
```

The Gorilla/Prometheus implementation lives in `asap-gorilla-go`. The OTel
processor is only a runtime adapter.

## Delivery Modes

### `delivery_mode: best_effort`

Low-cost path for users who accept that OTel/Telegraf/OTAP transport can drop
data.

```text
raw metrics
  -> edge gorillas3processor role=agent
       streaming XOR fragment encoding
  -> OTel / Telegraf / OTAP transport
  -> gateway gorillas3processor role=gateway_fragment
       TSDB block finalizer: chunks + index + meta.json
  -> S3/MinIO final bucket
  -> Thanos
```

Use this when edge CPU/memory is scarce and lossless delivery is not required.
The edge only emits encoded fragment metrics; the gateway writes the block
index and `meta.json`.

### `delivery_mode: durable_raw`

Recommended lossless-oriented path. Kafka stores raw metrics before Gorilla
compression, so compression/finalization bugs and S3 write failures can be
replayed from raw input.

```text
raw metrics
  -> Kafka durable raw topic
  -> gateway gorillas3processor role=gateway_raw
       streaming reorder + XOR chunks + TSDB block finalizer
  -> S3/MinIO final bucket
  -> Thanos
```

This costs more Kafka bandwidth/storage, but preserves maximum replay ability.
The finalizer must still be idempotent because Kafka can redeliver.

### `delivery_mode: durable_fragment`

Middle path: Kafka stores compressed fragments instead of raw metrics.

```text
raw metrics
  -> edge gorillas3processor role=agent
       streaming XOR fragment encoding
  -> Kafka durable fragment topic
  -> gateway gorillas3processor role=gateway_fragment
       TSDB block finalizer: chunks + index + meta.json
  -> S3/MinIO final bucket
  -> Thanos
```

This reduces Kafka bandwidth, but replay starts from encoded fragments. If the
edge compression logic was wrong, the raw samples are no longer available from
Kafka for recompression.

## Roles

| Role | Input | Output | Intended placement |
| --- | --- | --- | --- |
| `agent` | Raw Gauge/Sum metrics | `asap.gorilla.fragment` metrics | Edge agent |
| `gateway_fragment` | `asap.gorilla.fragment` metrics | Prometheus TSDB block files in S3 | Gateway/backend |
| `gateway_raw` | Raw Gauge/Sum metrics | Prometheus TSDB block files in S3 | Gateway/backend, usually after Kafka raw |

## Config

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `role` | `string` | `gateway_raw` | `agent`, `gateway_fragment`, or `gateway_raw`. |
| `delivery_mode` | `string` | role-dependent | `best_effort`, `durable_raw`, or `durable_fragment`. Documents the pipeline contract. |
| `window_interval` | `duration` | `60s` | Flush cadence for fragments or TSDB blocks. |
| `drop_original` | `bool` | `true` | Edge/gateway usually replace their input with fragments or empty metrics. |
| `source_id` | `string` | `""` | Edge source identity included in fragments. |
| `fragment_samples_per_chunk` | `int` | `120` | Samples per encoded XOR fragment. |
| `tsdb_reorder_grace` | `duration` | `2s` | Event-time lateness bound before samples become watermark-safe. |
| `bucket` | `string` | required except `role=agent` | Destination bucket for gateway roles. |
| `tsdb_bucket` | `string` | falls back to `bucket` | Final Thanos-visible TSDB bucket. |
| `endpoint` | `string` | `""` | S3/MinIO endpoint, e.g. `http://minio:9000`. |
| `region` | `string` | `us-east-1` | AWS SDK region. |
| `access_key_id` / `secret_access_key` | `string` | default chain | Must be set together or both empty. |
| `use_ssl` | `bool` | `false` | HTTP for local MinIO, HTTPS for AWS. |
| `tsdb_external_labels` | `map[string]string` | `{}` | Labels added to every finalized TSDB series. |
| `max_retries` | `int` | `3` | S3 PutObject retries. |
| `retry_backoff` | `duration` | `1s` | Linear retry backoff. |
| `upload_timeout` | `duration` | `30s` | Per-upload timeout. |
| `local_spool_dir` | `string` | `""` | Optional failed-upload spool for gateway roles. |

## Examples

Best effort edge:

```yaml
processors:
  gorillas3/edge:
    role: agent
    delivery_mode: best_effort
    source_id: edge-a
    window_interval: 60s
    tsdb_reorder_grace: 2s
    drop_original: true
```

Best effort gateway:

```yaml
processors:
  gorillas3/gateway:
    role: gateway_fragment
    delivery_mode: best_effort
    window_interval: 60s
    endpoint: http://minio:9000
    bucket: asap-tsdb
    tsdb_bucket: asap-tsdb
    region: us-east-1
    access_key_id: ${env:MINIO_ACCESS_KEY}
    secret_access_key: ${env:MINIO_SECRET_KEY}
```

Durable raw gateway after Kafka:

```yaml
processors:
  gorillas3/kafka-raw-gateway:
    role: gateway_raw
    delivery_mode: durable_raw
    window_interval: 60s
    endpoint: http://minio:9000
    bucket: asap-tsdb
    tsdb_bucket: asap-tsdb
    region: us-east-1
```

## Final Object Layout

Gateway roles upload only Prometheus TSDB blocks:

```text
<tsdb_bucket>/<ULID>/chunks/000001
<tsdb_bucket>/<ULID>/index
<tsdb_bucket>/<ULID>/meta.json
```

`meta.json` is uploaded last so Thanos does not observe a partial block.
