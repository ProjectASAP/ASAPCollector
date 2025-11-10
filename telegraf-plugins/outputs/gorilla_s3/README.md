# 🐒 Gorilla S3 Output (Telegraf Plugin)

This is a Telegraf output plugin that compresses numeric time series using **Gorilla-style encoding**  
(delta-of-delta for timestamps, XOR for `float64` values) and uploads the compressed batches directly to **Amazon S3**.

---

## 📘 Summary

- Groups metrics by **measurement + sorted tags + field** into individual series.  
- Sorts points by **timestamp** within the current Telegraf write batch.  
- Encodes each series:
  - **Timestamps** via delta-of-delta.
  - **Values** via XOR on `float64` bit patterns.
- Writes a **single binary object** per Telegraf write batch containing many series,  
  with a compact header and JSON metadata per series.
- Uploads the object to **S3** with a configurable prefix based on time.

---

## 🚧 Status

- Plugin code provided to be compiled into a **custom Telegraf build**.  
- Uses **AWS SDK for Go (v1)** → `s3.PutObject`.  
- Intended as a **starting point**; you can extend configuration, error handling, and metrics.

---

## 🏗️ Build & Integration

1. **Clone the DataCollector repository (with submodules)**

   ```bash
   git clone --recurse-submodules git@github.com:approx-telemetry/DataCollector.git
   # or
   git clone --recurse-submodules https://github.com/approx-telemetry/DataCollector.git
   ```

   The vendored Telegraf checkout lives at `DataCollector/telegraf/`, while this
   plugin stays under `DataCollector/telegraf-plugins/outputs/gorilla_s3/`.

2. **Register the plugin** by adding the import in  
   `telegraf/plugins/outputs/all/all.go` (inside the submodule):

   ```go
   // in plugins/outputs/all/all.go
   import (
       _ "github.com/influxdata/telegraf/plugins/outputs/gorilla_s3"
   )
   ```

3. **(Optional) Keep the plugin purely local**

   If you prefer not to copy files into the submodule, point the import at the
   DataCollector path and add a `replace` directive in `telegraf/go.mod`:

   ```go
   import (
       _ "github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3"
   )

   replace github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3 => ../DataCollector/telegraf-plugins/outputs/gorilla_s3
   ```

   Then add the dependency:

   ```bash
   go get github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3
   ```

4. **Build Telegraf from the submodule root**

   ```bash
   cd telegraf
   make telegraf
   ```

5. **Configure** in `telegraf.conf` (sample below) and run your custom binary.

---

## ⚙️ Sample Config

```toml
[[outputs.gorilla_s3]]
  bucket = "my-metrics-bucket"
  region = "us-east-1"
  # Prefix supports strftime tokens: %Y, %m, %d, %H, %M, %S
  prefix = "metrics/date=%Y-%m-%d/hour=%H/"

  # Optional: server-side encryption with KMS
  sse_kms_key_id = ""

  # Optional object naming override (default: batch-<unix>-<rand>.gorilla)
  # object_name = ""

  # Optional: size and retry controls
  # max_object_bytes = 67108864     # Split batch into multiple objects (0 = no split)
  # multipart_threshold = 8388608   # Use multipart when >= this size (min 5MiB)
  # multipart_part_bytes = 8388608  # Size per part (min 5MiB)
  # max_retries = 3                 # Upload retries
  # retry_backoff = "1s"            # Linear backoff
  # upload_timeout = "30s"          # Timeout per upload
```

---

## 🧾 Notes

- **Credentials:** AWS SDK uses the default provider chain (env vars, shared config, or EC2/ECS roles).  
- **Permissions:** Grant `PutObject` to the bucket/prefix (prefer write-only IAM role).  
- **Data model:** Only numeric fields (`float`, `int`) are encoded. Non-numeric fields are ignored.  
- **Histograms/summaries:** Not supported; consider extending or parallel output.  
- **Object format:** Simple container with a header, per-series JSON metadata, and bit-packed payloads.

---

## 📦 Object Format (v1)

| Field | Description |
|:------|:-------------|
| **Magic** | 8 bytes ASCII `"GORILLA1"` |
| **Endianness** | 1 byte (1 = little-endian) |
| **SeriesCount** | `u32` |
| **Per Series** | — |
| MetaLen | `u16` |
| Meta | JSON bytes *(measurement, field, tags, start_ts, end_ts, point_count)* |
| PointCount | `u32` |
| FirstTimestamp | `u64` (ns since epoch) |
| FirstValueBits | `u64` (bits of float64) |
| TsBitsLen | `u32` (length in bits of timestamp bitstream) |
| TsBits | bytes |
| ValBitsLen | `u32` (length in bits of value bitstream) |
| ValBits | bytes |

---

## 🔄 Decoding

- **Timestamps:** Reconstruct using delta-of-delta from the stored first timestamp (initial delta = 0).  
- **Values:** Reconstruct using XOR on `float64` bit patterns, reusing leading/trailing zero windows (per Gorilla algorithm).

---

## ⚠️ Limitations

- Timestamp fallback uses **64-bit writes** for very large delta-of-delta values (instead of Gorilla’s 32-bit fallback).  
  - More general and safe; decoder must handle it.  
- Sorting of points is **per batch**; late-arriving points go into later batches.

---

## 📊 Runtime Telemetry (Logs)

On each object upload, the plugin logs:
- S3 key  
- Number of series  
- Total points  
- Encoded bytes  
- Estimated raw bytes  
- Compression ratio  
- Upload latency (ms)

**Example:**

```
gorilla_s3 uploaded key=prefix/batch-... series=12 points=15000 bytes=1048576 est_raw=2400000 ratio=0.4369 latency_ms=210
```
