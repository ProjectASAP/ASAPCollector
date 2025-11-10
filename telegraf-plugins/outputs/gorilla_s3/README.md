Gorilla S3 Output (Telegraf plugin)

This is a Telegraf output plugin that compresses numeric time series using Gorilla-style encoding (delta-of-delta for timestamps, XOR for float64 values) and uploads the compressed batches directly to Amazon S3.

Summary
- Groups metrics by measurement + sorted tags + field into individual series.
- Sorts points by timestamp within the current Telegraf write batch.
- Encodes each series: timestamps via delta-of-delta, values via XOR on float64 bit patterns.
- Writes a single binary object per Telegraf write batch containing many series, with a compact header and JSON metadata per series.
- Uploads the object to S3 with a configurable prefix based on time.

Status
- Plugin code provided to be compiled into a custom Telegraf build.
- Uses AWS SDK for Go (v1) `s3.PutObject`.
- Intended as a starting point; you can extend config, error handling, and metrics.

Build & Integration
1) Clone Telegraf repo: https://github.com/influxdata/telegraf
2) Copy this folder into Telegraf under `plugins/outputs/gorilla_s3`.
3) Register the plugin by adding the import in `plugins/outputs/all/all.go`:

   ```go
   // in plugins/outputs/all/all.go
   import (
       _ "github.com/influxdata/telegraf/plugins/outputs/gorilla_s3"
   )
   ```

   If you prefer to keep this plugin in a local checkout (for example
   `~/repos/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3`)
   instead of copying it into the Telegraf tree, point the import at that module
   path and add a `replace` directive in `telegraf/go.mod`, e.g.:

   ```go
   import (
       _ "github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3"
   )

   replace github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3 => ../DataCollector/telegraf-plugins/outputs/gorilla_s3
   ```

   Then add the dependency to Telegraf's `go.mod` (the local module already has
   its own `go.mod` file) so future builds resolve it automatically:

   ```bash
   go get github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3
   ```

4) Build Telegraf:

   ```bash
   make telegraf
   ```

5) Configure in `telegraf.conf` (see sample below) and run your custom binary.

Sample Config
[[outputs.gorilla_s3]]
  bucket = "my-metrics-bucket"
  region = "us-east-1"
  # Prefix supports strftime tokens: %Y, %m, %d, %H, %M, %S
  prefix = "metrics/date=%Y-%m-%d/hour=%H/"
  # Optional: server-side encryption with KMS
  sse_kms_key_id = ""

  # Optional object naming override (default is batch-<unix>-<rand>.gorilla)
  # object_name = ""

  # Optional: size and retry controls
  # max_object_bytes = 67108864     # Split batch into multiple objects when exceeded (0 = no split)
  # multipart_threshold = 8388608   # Use multipart when object >= this size (min 5MiB)
  # multipart_part_bytes = 8388608  # Size per multipart part (min 5MiB)
  # max_retries = 3                 # Upload retries on transient errors
  # retry_backoff = "1s"            # Base backoff between retries (linear backoff)
  # upload_timeout = "30s"          # Timeout per upload attempt

Notes
- Credentials: the AWS SDK uses the default provider chain (env, shared config, EC2/ECS role).
- Permissions: grant PutObject to the bucket/prefix. Prefer write-only role.
- Data model: Only numeric fields (float, int) are encoded. Non-numeric fields are ignored.
- Histograms/summaries: Not supported here; consider extending format or writing in parallel.
- Object format: A simple container with a header, per-series JSON metadata, and the bit-packed payloads.

Object Format (v1)
- Magic: 8 bytes ASCII "GORILLA1"
- Endianness: 1 byte (1 = little-endian)
- SeriesCount: u32
- Repeat per series:
  - MetaLen: u16
  - Meta: JSON bytes (measurement, field, tags, start_ts, end_ts, point_count)
  - PointCount: u32
  - FirstTimestamp: u64 (ns since epoch)
  - FirstValueBits: u64 (bits of float64)
  - TsBitsLen: u32 (length in bits of the timestamp bitstream)
  - TsBits: bytes
  - ValBitsLen: u32 (length in bits of the values bitstream)
  - ValBits: bytes

Decoding
- Reconstruct timestamps with delta-of-delta using the stored first timestamp and a previous delta of 0.
- Reconstruct values using the XOR scheme on float64 bit patterns, reusing leading/trailing zero windows as per Gorilla.

Limitations
- Timestamp fallback uses 64-bit write for very large delta-of-delta values (instead of Gorilla's 32-bit fallback). This is more general and safe; a matching decoder must honor it.
- Sorting of points is done per batch; very late points landing in a subsequent batch are encoded in that batch.

Runtime Telemetry (logs)
- On each object upload, the plugin logs: key, number of series, total points, encoded bytes, estimated raw bytes, compression ratio, and S3 upload latency in milliseconds.
- Example: gorilla_s3 uploaded key=prefix/batch-... series=12 points=15000 bytes=1048576 est_raw=2400000 ratio=0.4369 latency_ms=210
