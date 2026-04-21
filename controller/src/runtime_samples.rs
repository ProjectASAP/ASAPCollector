//! Receiver for `sketch-runtime::PushExporter` batches — the
//! **push side** of the controller's real-time decision loop.
//!
//! Agents running an embedded `sketch-runtime` Sampler POST
//! batched v1 JSONL records (optionally zstd-compressed) to
//! `/api/v1/runtime-samples`. The handler decodes the batch and
//! appends each record to a bounded ring buffer keyed by
//! `(source, sketch, impl)`. Decision loops in the planner /
//! replanner peek the tail of that buffer to see the freshest
//! throughput / latency / accuracy signal.
//!
//! ## Why a ring buffer, not a stream
//!
//! Real-time decisions want the freshest N records, not a full
//! replay. Bounded memory, O(1) append + peek, no eviction
//! policy beyond FIFO. Post-mortem analysis of a longer window
//! lives in the agent's `FileExporter` artifact.
//!
//! ## Why this handler accepts compressed bodies
//!
//! `PushExporter` ships `Content-Encoding: zstd` by default.
//! We decode on receipt; consumers of `RuntimeSamplesStore`
//! never see compressed bytes. The compression is
//! wire-efficiency only — not a schema concern.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One record as stored in the ring buffer. Kept as
/// `serde_json::Value` so we don't have to define a mirror of
/// every field on `sketch-core::report::Record` and stay
/// schema-forward: the `schema_version` field signals which
/// shape to expect, and consumers parse out the fields they
/// need.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRecord {
    pub source: String,
    pub sketch: String,
    #[serde(rename = "impl")]
    pub impl_name: String,
    /// Remainder of the record (bench/profile sections, labels
    /// on `workload`, timestamp, etc.) preserved as opaque JSON.
    /// Decision loops pull the numeric fields they care about
    /// without forcing a shared schema crate between DC and
    /// sketchlib-bench.
    #[serde(flatten)]
    pub payload: Value,
}

/// Bounded FIFO ring buffer of runtime records, keyed by
/// `(source, sketch, impl)`. Each key gets its own buffer so a
/// chatty source can't starve a quiet one.
pub struct RuntimeSamplesStore {
    buffers: RwLock<HashMap<SampleKey, VecDeque<RuntimeRecord>>>,
    per_key_capacity: usize,
    stats: Arc<RuntimeSamplesStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SampleKey {
    pub source: String,
    pub sketch: String,
    pub impl_name: String,
}

#[derive(Debug, Default)]
pub struct RuntimeSamplesStats {
    pub batches_received: AtomicU64,
    pub records_stored: AtomicU64,
    pub records_evicted: AtomicU64,
    pub decode_errors: AtomicU64,
}

impl RuntimeSamplesStats {
    pub fn snapshot(&self) -> RuntimeSamplesStatsSnapshot {
        RuntimeSamplesStatsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            records_stored: self.records_stored.load(Ordering::Relaxed),
            records_evicted: self.records_evicted.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RuntimeSamplesStatsSnapshot {
    pub batches_received: u64,
    pub records_stored: u64,
    pub records_evicted: u64,
    pub decode_errors: u64,
}

impl RuntimeSamplesStore {
    pub fn new(per_key_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            buffers: RwLock::new(HashMap::new()),
            per_key_capacity,
            stats: Arc::new(RuntimeSamplesStats::default()),
        })
    }

    pub fn stats(&self) -> Arc<RuntimeSamplesStats> {
        Arc::clone(&self.stats)
    }

    fn append(&self, rec: RuntimeRecord) {
        let key = SampleKey {
            source: rec.source.clone(),
            sketch: rec.sketch.clone(),
            impl_name: rec.impl_name.clone(),
        };
        let mut map = self.buffers.write();
        let buf = map
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(self.per_key_capacity));
        if buf.len() >= self.per_key_capacity {
            buf.pop_front();
            self.stats.records_evicted.fetch_add(1, Ordering::Relaxed);
        }
        buf.push_back(rec);
        self.stats.records_stored.fetch_add(1, Ordering::Relaxed);
    }

    /// Peek the latest record for a given key, or `None` if the
    /// key has never been seen. Used by the replanner /
    /// decision loop to read freshness signals.
    pub fn latest(&self, key: &SampleKey) -> Option<RuntimeRecord> {
        self.buffers.read().get(key).and_then(|b| b.back().cloned())
    }

    /// Snapshot the full ring for a key. O(n) clone; use only
    /// from non-hot paths.
    pub fn snapshot(&self, key: &SampleKey) -> Vec<RuntimeRecord> {
        self.buffers
            .read()
            .get(key)
            .map(|b| b.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn keys(&self) -> Vec<SampleKey> {
        self.buffers.read().keys().cloned().collect()
    }
}

/// Axum handler for `POST /api/v1/runtime-samples`. Accepts
/// `application/x-ndjson` body, optionally with
/// `Content-Encoding: zstd`. Parses one `RuntimeRecord` per
/// line and appends them to the store. Returns `204 No Content`
/// on success; malformed bodies / unknown encodings yield
/// `400 Bad Request` but never `500` — the controller must stay
/// up even when agents misbehave.
pub async fn handle_runtime_samples(
    State(store): State<Arc<RuntimeSamplesStore>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    store.stats.batches_received.fetch_add(1, Ordering::Relaxed);

    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let decoded = match encoding {
        "" | "identity" => body.to_vec(),
        "zstd" => match zstd::decode_all(&body[..]) {
            Ok(d) => d,
            Err(e) => {
                store.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                return (StatusCode::BAD_REQUEST, format!("zstd decode failed: {e}"))
                    .into_response();
            }
        },
        other => {
            store.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
            return (
                StatusCode::BAD_REQUEST,
                format!("unsupported Content-Encoding: {other}"),
            )
                .into_response();
        }
    };

    let text = match std::str::from_utf8(&decoded) {
        Ok(s) => s,
        Err(e) => {
            store.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
            return (StatusCode::BAD_REQUEST, format!("non-utf8 body: {e}")).into_response();
        }
    };

    let mut any_ok = false;
    for (lineno, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<RuntimeRecord>(line) {
            Ok(rec) => {
                store.append(rec);
                any_ok = true;
            }
            Err(e) => {
                store.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    lineno,
                    error = %e,
                    "runtime-samples: malformed line, skipping"
                );
            }
        }
    }

    if any_ok {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::BAD_REQUEST.into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::util::ServiceExt;

    fn sample_line() -> String {
        serde_json::json!({
            "source": "data-collector",
            "sketch": "cms",
            "impl": "oxide",
            "schema_version": 1,
            "mode": "runtime",
            "bench": {},
        })
        .to_string()
    }

    #[tokio::test]
    async fn uncompressed_ndjson_stores_one_record() {
        let store = RuntimeSamplesStore::new(16);
        let app = Router::new()
            .route("/api/v1/runtime-samples", post(handle_runtime_samples))
            .with_state(Arc::clone(&store));

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runtime-samples")
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from(format!("{}\n", sample_line())))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let key = SampleKey {
            source: "data-collector".into(),
            sketch: "cms".into(),
            impl_name: "oxide".into(),
        };
        assert!(store.latest(&key).is_some());
        assert_eq!(store.stats.records_stored.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn zstd_compressed_ndjson_decodes_and_stores() {
        let store = RuntimeSamplesStore::new(16);
        let body_text = format!("{}\n{}\n", sample_line(), sample_line());
        let compressed = zstd::encode_all(body_text.as_bytes(), 3).unwrap();
        let app = Router::new()
            .route("/api/v1/runtime-samples", post(handle_runtime_samples))
            .with_state(Arc::clone(&store));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runtime-samples")
                    .header("content-type", "application/x-ndjson")
                    .header("content-encoding", "zstd")
                    .body(Body::from(compressed))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(store.stats.records_stored.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn ring_evicts_oldest_past_capacity() {
        let store = RuntimeSamplesStore::new(3);
        for i in 0..5 {
            let mut v = serde_json::from_str::<Value>(&sample_line()).unwrap();
            v["seq"] = serde_json::json!(i);
            let rec: RuntimeRecord = serde_json::from_value(v).unwrap();
            store.append(rec);
        }
        assert_eq!(store.stats.records_stored.load(Ordering::Relaxed), 5);
        assert_eq!(store.stats.records_evicted.load(Ordering::Relaxed), 2);
        let key = SampleKey {
            source: "data-collector".into(),
            sketch: "cms".into(),
            impl_name: "oxide".into(),
        };
        let snap = store.snapshot(&key);
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].payload["seq"], 2);
        assert_eq!(snap[2].payload["seq"], 4);
    }

    #[tokio::test]
    async fn unsupported_encoding_400s() {
        let store = RuntimeSamplesStore::new(16);
        let app = Router::new()
            .route("/api/v1/runtime-samples", post(handle_runtime_samples))
            .with_state(Arc::clone(&store));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runtime-samples")
                    .header("content-encoding", "gzip")
                    .body(Body::from("{}\n"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn per_key_separation_across_sources() {
        let store = RuntimeSamplesStore::new(16);
        let for_key = |source: &str, sketch: &str, impl_name: &str| {
            serde_json::json!({
                "source": source,
                "sketch": sketch,
                "impl": impl_name,
            })
            .to_string()
        };
        let body = format!(
            "{}\n{}\n{}\n",
            for_key("dc-a", "cms", "oxide"),
            for_key("dc-b", "cms", "oxide"),
            for_key("dc-a", "hll", "lib"),
        );
        let app = Router::new()
            .route("/api/v1/runtime-samples", post(handle_runtime_samples))
            .with_state(Arc::clone(&store));
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/runtime-samples")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(store.keys().len(), 3);
    }
}
