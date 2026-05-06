//! Minimal async object-store interface backing the compactor.
//!
//! Mirrors the same shape used by the backend's
//! `GorillaS3ColdStore::ObjectStore` so a future refactor can pull
//! both behind a single crate. The compactor needs five operations:
//!
//! * `list_prefix` — to discover blocks under a key prefix
//! * `get_object` — to fetch chunk + index + postings bytes
//! * `put_object` — to write merged outputs
//! * `delete_object` — to remove the now-redundant source blocks
//! * `head_object` — to check existence (for idempotency)
//!
//! Production callers wire `S3ObjectStore` (rust-s3 crate). Tests
//! use [`InMemoryObjectStore`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::Mutex;

/// Errors raised by [`ObjectStore`] implementations.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// Object key does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// Underlying transport / S3 SDK failure.
    #[error("backend: {0}")]
    Backend(String),
}

/// Async object-store contract. All methods must be safe to call
/// concurrently from a tokio runtime.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// List all object keys under `prefix`. Order is unspecified —
    /// callers sort if they need a deterministic walk.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;

    /// GET an entire object body. Use [`Self::get_range`] for
    /// partial reads.
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError>;

    /// Range-GET an object's bytes `[offset, offset + length)`.
    /// Used by the verify-mode partial read.
    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, ObjectStoreError>;

    /// PUT an object, replacing any prior contents.
    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<(), ObjectStoreError>;

    /// DELETE an object. Idempotent: deleting a missing key is OK.
    async fn delete_object(&self, key: &str) -> Result<(), ObjectStoreError>;

    /// True iff `key` exists. Used for tmp-prefix-then-rename
    /// idempotency: we skip work if the final-prefix output is
    /// already present.
    async fn object_exists(&self, key: &str) -> Result<bool, ObjectStoreError>;
}

// ─────────────────────────────────────────────────────────────────────
// In-memory mock — drives the compactor's unit + integration tests.
// ─────────────────────────────────────────────────────────────────────

/// In-memory [`ObjectStore`] used by tests and the
/// `--dry-run` plan inspector. Keeps a per-key fetch counter so
/// tests can assert the compactor isn't doing surprise reads.
#[derive(Default)]
pub struct InMemoryObjectStore {
    inner: Mutex<InMemoryState>,
}

#[derive(Default)]
struct InMemoryState {
    objects: HashMap<String, Vec<u8>>,
    get_count: HashMap<String, usize>,
    put_count: HashMap<String, usize>,
    delete_count: HashMap<String, usize>,
}

impl InMemoryObjectStore {
    /// Build a fresh empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap in [`Arc`] for use as `Arc<dyn ObjectStore>`.
    pub fn into_arc(self) -> Arc<dyn ObjectStore> {
        Arc::new(self)
    }

    /// Test helper — number of times [`ObjectStore::get_object`] /
    /// [`ObjectStore::get_range`] was called for `key`.
    pub async fn get_count(&self, key: &str) -> usize {
        let g = self.inner.lock().await;
        g.get_count.get(key).copied().unwrap_or(0)
    }

    /// Test helper — number of PUTs for `key`.
    pub async fn put_count(&self, key: &str) -> usize {
        let g = self.inner.lock().await;
        g.put_count.get(key).copied().unwrap_or(0)
    }

    /// Test helper — number of DELETEs for `key`.
    pub async fn delete_count(&self, key: &str) -> usize {
        let g = self.inner.lock().await;
        g.delete_count.get(key).copied().unwrap_or(0)
    }

    /// Test helper — total number of stored objects.
    pub async fn len(&self) -> usize {
        let g = self.inner.lock().await;
        g.objects.len()
    }

    /// Test helper — sum of all stored object sizes in bytes.
    pub async fn total_bytes(&self) -> usize {
        let g = self.inner.lock().await;
        g.objects.values().map(Vec::len).sum()
    }

    /// Test helper — list of stored object keys (sorted).
    pub async fn keys(&self) -> Vec<String> {
        let g = self.inner.lock().await;
        let mut k: Vec<String> = g.objects.keys().cloned().collect();
        k.sort();
        k
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let g = self.inner.lock().await;
        let mut hits: Vec<String> = g
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        hits.sort();
        Ok(hits)
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        let mut g = self.inner.lock().await;
        *g.get_count.entry(key.to_string()).or_insert(0) += 1;
        g.objects
            .get(key)
            .cloned()
            .ok_or_else(|| ObjectStoreError::NotFound(key.to_string()))
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        let mut g = self.inner.lock().await;
        *g.get_count.entry(key.to_string()).or_insert(0) += 1;
        let body = g
            .objects
            .get(key)
            .ok_or_else(|| ObjectStoreError::NotFound(key.to_string()))?;
        let start = offset as usize;
        let end = start + length as usize;
        if end > body.len() {
            return Err(ObjectStoreError::Backend(format!(
                "range {}..{} out of bounds for object of {} bytes",
                start,
                end,
                body.len()
            )));
        }
        Ok(body[start..end].to_vec())
    }

    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<(), ObjectStoreError> {
        let mut g = self.inner.lock().await;
        *g.put_count.entry(key.to_string()).or_insert(0) += 1;
        g.objects.insert(key.to_string(), body);
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<(), ObjectStoreError> {
        let mut g = self.inner.lock().await;
        *g.delete_count.entry(key.to_string()).or_insert(0) += 1;
        g.objects.remove(key);
        Ok(())
    }

    async fn object_exists(&self, key: &str) -> Result<bool, ObjectStoreError> {
        let g = self.inner.lock().await;
        Ok(g.objects.contains_key(key))
    }
}
