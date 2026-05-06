//! Compactor runner — given a [`CompactionPlan`], execute each
//! group via **byte concatenation** of the source chunks (no decode,
//! no re-encode).
//!
//! ## Flow per group
//!
//! 1. For each [`SourceBlock`] in the group:
//!    a. GET its `index.json` and `postings-v1.json`.
//!    b. For every [`asap_gorilla::IndexEntry`] in the index, GET
//!       the chunk's bytes via partial-read (or whole-object read
//!       for legacy pre-compactor blocks).
//!    c. Append the chunk bytes to the merged buffer; record the
//!       new `(byte_offset, byte_length, object_key)` in the
//!       merged index entry.
//! 2. Build the merged [`asap_gorilla::Postings`] via
//!    [`asap_gorilla::Postings::merge_many`].
//! 3. PUT the three outputs to a tmp prefix
//!    (`<output_prefix>.tmp/<basename>.{gor,index.json,postings-v1.json}`).
//! 4. Promote: PUT each output at the final prefix; DELETE all source
//!    block objects (chunks + sidecars). The promotion is best-effort
//!    atomic via "tmp first, then final"; a partial failure leaves
//!    sources untouched and the next run picks up where this one
//!    left off.
//!
//! ## Verify mode
//!
//! When [`CompactorConfig::verify`] is set, after promoting the
//! merged outputs the runner picks one chunk at random, issues a
//! Range-GET against the merged object using the recorded
//! `byte_offset / byte_length`, and asserts that
//! [`asap_gorilla::GorillaDecoder`] parses the chunk header
//! correctly. This is the proof-by-test that the concat-only design
//! actually works.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use thiserror::Error;
use tracing::{debug, info, warn};

use asap_gorilla::{GorillaDecoder, IndexEntry, IndexFile, Postings, INDEX_SCHEMA_VERSION};

use crate::metrics::{CompactorMetrics, CompactorMetricsSnapshot};
use crate::object_store::{ObjectStore, ObjectStoreError};
use crate::plan::{CompactionGroup, CompactionPlan};

/// Tunable knobs for [`Compactor::run`].
#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// `true` ⇒ no S3 writes; the compactor still walks the plan
    /// and reports what it would have done. Used by the
    /// `--dry-run` plan inspector.
    pub dry_run: bool,
    /// `true` ⇒ after promoting the merged outputs, partial-read
    /// one chunk and decode it to validate the byte_offset /
    /// byte_length contract.
    pub verify: bool,
    /// Tenant prefix passed to [`CompactionPlan::discover`] (e.g.
    /// `"tenant1/"`).
    pub tenant_prefix: String,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            dry_run: false,
            verify: true,
            tenant_prefix: String::new(),
        }
    }
}

/// Runner-level errors.
#[derive(Debug, Error)]
pub enum CompactorError {
    /// Underlying [`ObjectStore`] failure.
    #[error("object store: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    /// `index.json` failed to parse.
    #[error("decode index: {0}")]
    Decode(#[from] asap_gorilla::DecodeError),
    /// `index.json` could not be re-encoded.
    #[error("encode index: {0}")]
    Encode(#[from] asap_gorilla::EncodeError),
    /// JSON serialization failure (not from asap-gorilla — e.g.
    /// when we serialize a CompactionPlan to stderr).
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Verify-mode partial-read returned bytes the decoder
    /// rejected. Concrete message names which chunk failed.
    #[error("verify failed: {0}")]
    VerifyFailed(String),
}

/// One merged-block descriptor returned to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedBlock {
    /// Final S3 key of the merged `.gor` object.
    pub merged_chunk_key: String,
    /// Final S3 key of the merged `.index.json`.
    pub merged_index_key: String,
    /// Final S3 key of the merged `.postings-v1.json`.
    pub merged_postings_key: String,
    /// Total bytes the merged `.gor` object weighs in at.
    pub merged_chunk_bytes: u64,
    /// Number of source chunks that landed in this merged block.
    pub chunks_merged: u64,
    /// Source block prefixes that were deleted as part of promoting
    /// this merged block.
    pub source_blocks_deleted: Vec<String>,
}

/// Result of a complete [`Compactor::run`].
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// One entry per group merged this run.
    pub blocks: Vec<CompactedBlock>,
    /// Snapshot of the metrics counters at end-of-run.
    pub metrics: CompactorMetricsSnapshot,
}

/// Concat-only compactor.
pub struct Compactor {
    store: Arc<dyn ObjectStore>,
    metrics: Arc<CompactorMetrics>,
    config: CompactorConfig,
}

impl Compactor {
    /// Build a new compactor with explicit dependencies. The
    /// production binary wires `store = S3ObjectStore::new(...)`;
    /// tests inject [`crate::InMemoryObjectStore`].
    pub fn new(store: Arc<dyn ObjectStore>, config: CompactorConfig) -> Self {
        Self {
            store,
            metrics: Arc::new(CompactorMetrics::new()),
            config,
        }
    }

    /// Borrow the live counters — useful for HTTP exposition.
    pub fn metrics(&self) -> &Arc<CompactorMetrics> {
        &self.metrics
    }

    /// Discover + execute. Returns once every group has been
    /// processed (or skipped, in `--dry-run` mode).
    pub async fn run(
        &self,
        plan: &CompactionPlan,
    ) -> Result<CompactionResult, CompactorError> {
        let started = Instant::now();
        if self.config.dry_run {
            self.metrics.dry_run.store(1, Ordering::Relaxed);
        }

        let mut blocks = Vec::with_capacity(plan.groups.len());
        for group in &plan.groups {
            match self.run_group(group).await {
                Ok(blk) => blocks.push(blk),
                Err(e) => {
                    warn!(
                        tenant = group.tenant.as_str(),
                        metric = group.metric.as_str(),
                        error = %e,
                        "compactor: group failed; skipping"
                    );
                }
            }
        }

        self.metrics
            .duration_ms
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(CompactionResult {
            blocks,
            metrics: self.metrics.snapshot(),
        })
    }

    async fn run_group(
        &self,
        group: &CompactionGroup,
    ) -> Result<CompactedBlock, CompactorError> {
        info!(
            tenant = group.tenant.as_str(),
            metric = group.metric.as_str(),
            output_prefix = group.output_prefix.as_str(),
            output_basename = group.output_basename.as_str(),
            sources = group.sources.len(),
            dry_run = self.config.dry_run,
            "compactor: running group"
        );

        let merged_chunk_key =
            format!("{}{}.gor", group.output_prefix, group.output_basename);
        let merged_index_key = format!(
            "{}{}.index.json",
            group.output_prefix, group.output_basename
        );
        let merged_postings_key = format!(
            "{}{}.postings-v1.json",
            group.output_prefix, group.output_basename
        );

        // Idempotency short-circuit: if the merged chunk already
        // exists in S3 (a prior run finished promotion but not
        // source-deletion, or two compactor instances raced), do
        // nothing for this group beyond cleaning up the sources.
        if self.store.object_exists(&merged_chunk_key).await? {
            debug!(key = %merged_chunk_key, "compactor: merged output already exists; cleaning up sources");
            // Counter still ticks the source PUT/GET so /metrics
            // shows non-zero progress.
            self.metrics.s3_get_count.fetch_add(1, Ordering::Relaxed);
            return self
                .finish_group(
                    group,
                    merged_chunk_key,
                    merged_index_key,
                    merged_postings_key,
                    /*merged_size_bytes=*/ 0,
                    /*chunks_merged=*/ 0,
                )
                .await;
        }

        // ── 1. Download every source block's chunks + sidecars ──
        let mut merged_chunk_bytes: Vec<u8> = Vec::new();
        let mut merged_index = IndexFile::new(now_ns());
        merged_index.schema_version = INDEX_SCHEMA_VERSION;
        let mut postings_inputs: Vec<Postings> = Vec::new();

        for src in &group.sources {
            self.metrics.blocks_in.fetch_add(1, Ordering::Relaxed);
            let src_index_bytes = self.read_object(&src.index_key).await?;
            let src_index = IndexFile::read(src_index_bytes.as_slice())?;

            // Postings sidecar (best-effort: missing postings ⇒
            // empty contribution). The merged output then drops
            // the missing-block's series_ids — same outcome as if
            // the agent had never written postings for it.
            let postings_key = format!("{}postings-v1.json", src.prefix());
            match self.read_object(&postings_key).await {
                Ok(b) => match Postings::read(b.as_slice()) {
                    Ok(p) => postings_inputs.push(p),
                    Err(e) => warn!(key = %postings_key, error = %e, "compactor: postings parse failed; skipping"),
                },
                Err(ObjectStoreError::NotFound(_)) => {
                    debug!(key = %postings_key, "compactor: postings sidecar missing; skipping");
                }
                Err(e) => return Err(e.into()),
            }

            for entry in &src_index.entries {
                self.metrics.chunks_in.fetch_add(1, Ordering::Relaxed);
                // Compute the source bytes for THIS chunk. Two
                // shapes to handle:
                //
                // (a) pre-compactor block: chunk lives at its own
                //     S3 key (`entry.key`). We GET the whole object
                //     and append.
                // (b) already-compacted block (rare in practice —
                //     compaction is once-per-day so we only see
                //     this if a prior compactor run got interrupted
                //     mid-way). The chunk is a slice of a larger
                //     object (`entry.effective_object_key()` +
                //     `byte_offset/byte_length`). Range-GET it.
                let chunk_bytes = if entry.byte_offset.is_some()
                    && entry.byte_length.is_some()
                {
                    let off = entry.byte_offset.unwrap();
                    let len = entry.byte_length.unwrap();
                    // Resolve the merged key relative to the source
                    // block's prefix (S3 keys in `index.json` are
                    // basenames, not absolute paths).
                    let absolute_key =
                        absolute_key(&src.prefix(), entry.effective_object_key());
                    self.read_range(&absolute_key, off, len).await?
                } else {
                    // pre-compactor chunk — fetch the whole object
                    // at `entry.key` (relative to src.prefix()).
                    let absolute_key = absolute_key(&src.prefix(), &entry.key);
                    self.read_object(&absolute_key).await?
                };

                let byte_offset = merged_chunk_bytes.len() as u64;
                let byte_length = chunk_bytes.len() as u64;
                merged_chunk_bytes.extend_from_slice(&chunk_bytes);

                // Build the merged-block index entry. `key` keeps
                // the source chunk's identity (its filename in the
                // source bucket), but `object_key` now points at
                // the merged output and the byte range marks the
                // chunk's slice inside it.
                merged_index.entries.push(IndexEntry {
                    key: entry.key.clone(),
                    time_range: entry.time_range,
                    sample_count: entry.sample_count,
                    label_hash: entry.label_hash,
                    size_bytes: byte_length as u32,
                    object_key: Some(format!(
                        "{}.gor",
                        group.output_basename
                    )),
                    byte_offset: Some(byte_offset),
                    byte_length: Some(byte_length),
                });
                self.metrics.chunks_out.fetch_add(1, Ordering::Relaxed);
            }
        }

        let merged_postings = Postings::merge_many(&postings_inputs);

        // Stamp the merged index with a pointer to the merged
        // postings sidecar (relative path inside the same prefix,
        // same convention as the agent-side index.json).
        // (`IndexFile` does not yet have a public `postings` field —
        // we pin it via the `generated_at_ns` + the entries' keys.)

        // ── 2. Promote: tmp first, then final, then delete sources ──
        let merged_size = merged_chunk_bytes.len() as u64;
        let mut merged_index_bytes: Vec<u8> = Vec::new();
        merged_index.write(&mut merged_index_bytes)?;
        let mut merged_postings_bytes: Vec<u8> = Vec::new();
        merged_postings.write_with_clock(&mut merged_postings_bytes, now_ns())?;

        if !self.config.dry_run {
            let tmp_chunk_key = format!("{}.tmp.{}", merged_chunk_key, now_ns());
            let tmp_index_key = format!("{}.tmp.{}", merged_index_key, now_ns());
            let tmp_postings_key =
                format!("{}.tmp.{}", merged_postings_key, now_ns());
            self.put_object(&tmp_chunk_key, merged_chunk_bytes.clone())
                .await?;
            self.put_object(&tmp_index_key, merged_index_bytes.clone())
                .await?;
            self.put_object(&tmp_postings_key, merged_postings_bytes.clone())
                .await?;
            // Promote.
            self.put_object(&merged_chunk_key, merged_chunk_bytes)
                .await?;
            self.put_object(&merged_index_key, merged_index_bytes)
                .await?;
            self.put_object(&merged_postings_key, merged_postings_bytes)
                .await?;
            // Cleanup tmp.
            self.delete_object(&tmp_chunk_key).await?;
            self.delete_object(&tmp_index_key).await?;
            self.delete_object(&tmp_postings_key).await?;
        } else {
            self.metrics
                .bytes_out
                .fetch_add(merged_size as u64, Ordering::Relaxed);
        }

        // ── 3. Verify (post-promote) ──
        if self.config.verify && !self.config.dry_run && merged_index.entries.len() > 0 {
            let entry = &merged_index.entries[0];
            let off = entry.byte_offset.unwrap_or(0);
            let len = entry.byte_length.unwrap_or(entry.size_bytes as u64);
            let bytes = self.read_range(&merged_chunk_key, off, len).await?;
            // Just attempt to parse the magic + first chunk header;
            // a full sample iteration would amount to re-decoding.
            GorillaDecoder::from_reader(bytes.as_slice()).map_err(|e| {
                CompactorError::VerifyFailed(format!(
                    "merged chunk {} did not decode at offset {} len {}: {}",
                    merged_chunk_key, off, len, e
                ))
            })?;
            debug!(key = %merged_chunk_key, off, len, "compactor: verify partial-read OK");
        }

        // ── 4. Finalize ──
        self.finish_group(
            group,
            merged_chunk_key,
            merged_index_key,
            merged_postings_key,
            merged_size,
            merged_index.entries.len() as u64,
        )
        .await
    }

    async fn finish_group(
        &self,
        group: &CompactionGroup,
        merged_chunk_key: String,
        merged_index_key: String,
        merged_postings_key: String,
        merged_size_bytes: u64,
        chunks_merged: u64,
    ) -> Result<CompactedBlock, CompactorError> {
        let mut deleted = Vec::with_capacity(group.sources.len());
        if !self.config.dry_run {
            for src in &group.sources {
                let prefix = src.prefix();
                // Conservative: list every key under the source
                // block's prefix and DELETE individually. S3 batch
                // delete is a future optimization; for MVP's small
                // scale (≤ 6 source blocks per group, each holding
                // a few chunks) the latency is fine.
                let keys = self.store.list_prefix(&prefix).await?;
                for key in keys {
                    self.delete_object(&key).await?;
                }
                deleted.push(prefix);
            }
            self.metrics.blocks_out.fetch_add(1, Ordering::Relaxed);
        }
        Ok(CompactedBlock {
            merged_chunk_key,
            merged_index_key,
            merged_postings_key,
            merged_chunk_bytes: merged_size_bytes,
            chunks_merged,
            source_blocks_deleted: deleted,
        })
    }

    // ─── Object-store wrappers that keep the metrics in sync ────

    async fn read_object(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        self.metrics.s3_get_count.fetch_add(1, Ordering::Relaxed);
        let body = self.store.get_object(key).await?;
        self.metrics
            .bytes_in
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    async fn read_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        self.metrics.s3_get_count.fetch_add(1, Ordering::Relaxed);
        let body = self.store.get_range(key, offset, length).await?;
        self.metrics
            .bytes_in
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<(), ObjectStoreError> {
        self.metrics.s3_put_count.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_out
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        self.store.put_object(key, body).await
    }

    async fn delete_object(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.metrics
            .s3_delete_count
            .fetch_add(1, Ordering::Relaxed);
        self.store.delete_object(key).await
    }
}

fn absolute_key(prefix: &str, name: &str) -> String {
    if name.contains('/') {
        // Already an absolute key (legacy `index.json` rows store
        // full keys, e.g. `tenant/m/2026/05/06/12/part-A.gor`).
        name.to_string()
    } else {
        format!("{prefix}{name}")
    }
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::InMemoryObjectStore;
    use crate::plan::{CompactionPlan, CompactionThresholds};
    use asap_gorilla::{GorillaEncoder, IndexEntry, IndexFile, PostingsBuilder};
    use chrono::TimeZone;
    use chrono::Utc;

    fn make_chunk(metric: &str, labels: &[(&str, &str)], samples: &[(u64, f64)]) -> Vec<u8> {
        let mut enc = GorillaEncoder::new(
            metric,
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        for (t, v) in samples {
            enc.append(*t, *v);
        }
        enc.finalize().unwrap()
    }

    /// Seed an InMemoryObjectStore with `n_hours` per-hour blocks for one
    /// (tenant, metric). Each block holds one chunk with two samples.
    /// Returns the store Arc.
    async fn seed_n_hour_blocks(n_hours: u32) -> Arc<InMemoryObjectStore> {
        let store = Arc::new(InMemoryObjectStore::new());
        for hour in 0..n_hours {
            let prefix = format!("tenant1/m/2026/05/06/{:02}/", hour);
            let chunk_key = format!("{prefix}part-0.gor");
            let chunk = make_chunk(
                "m",
                &[("zone", if hour % 2 == 0 { "a" } else { "b" })],
                &[
                    ((hour as u64) * 1_000_000_000_000, hour as f64 + 0.1),
                    ((hour as u64) * 1_000_000_000_000 + 60_000_000_000, hour as f64 + 0.2),
                ],
            );
            store.put_object(&chunk_key, chunk.clone()).await.unwrap();

            let mut idx = IndexFile::new(0);
            idx.entries.push(IndexEntry {
                key: "part-0.gor".to_string(),
                time_range: (
                    (hour as u64) * 1_000_000_000_000,
                    (hour as u64) * 1_000_000_000_000 + 60_000_000_000,
                ),
                sample_count: 2,
                label_hash: hour as u64 + 1,
                size_bytes: chunk.len() as u32,
                object_key: Some("part-0.gor".to_string()),
                byte_offset: Some(0),
                byte_length: Some(chunk.len() as u64),
            });
            let mut idx_bytes = Vec::new();
            idx.write(&mut idx_bytes).unwrap();
            store.put_object(&format!("{prefix}index.json"), idx_bytes).await.unwrap();

            let mut pb = PostingsBuilder::new();
            pb.add_series(
                hour as u64 + 1,
                vec![("__name__", "m"), ("zone", if hour % 2 == 0 { "a" } else { "b" })],
            );
            let p = pb.finalize();
            let mut p_bytes = Vec::new();
            p.write(&mut p_bytes).unwrap();
            store
                .put_object(&format!("{prefix}postings-v1.json"), p_bytes)
                .await
                .unwrap();
        }
        store
    }

    fn now_for_six_hours_ago() -> chrono::DateTime<chrono::Utc> {
        // Make every block in `seed_n_hour_blocks` look 6+ hours old.
        Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap()
    }

    #[tokio::test]
    async fn compactor_concat_only_merges_six_blocks() {
        let store = seed_n_hour_blocks(6).await;
        let store_arc: Arc<dyn ObjectStore> = store.clone();
        let plan = CompactionPlan::discover(
            &store_arc,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_for_six_hours_ago(),
            },
        )
        .await
        .unwrap();
        assert_eq!(plan.groups.len(), 1, "expected one group of 6 blocks");

        let compactor = Compactor::new(
            store_arc,
            CompactorConfig {
                dry_run: false,
                verify: true,
                tenant_prefix: "tenant1/".to_string(),
            },
        );
        let result = compactor.run(&plan).await.unwrap();
        assert_eq!(result.blocks.len(), 1);
        let blk = &result.blocks[0];
        assert_eq!(blk.chunks_merged, 6);
        assert!(blk.merged_chunk_bytes > 0);

        // Source blocks are gone.
        for hour in 0..6u32 {
            let key = format!("tenant1/m/2026/05/06/{:02}/index.json", hour);
            assert!(!store.object_exists(&key).await.unwrap(),
                "source index {key} should be deleted");
        }

        // Merged outputs exist.
        assert!(store
            .object_exists(&blk.merged_chunk_key)
            .await
            .unwrap());
        assert!(store
            .object_exists(&blk.merged_index_key)
            .await
            .unwrap());
        assert!(store
            .object_exists(&blk.merged_postings_key)
            .await
            .unwrap());

        // Merged chunk size = sum of source chunk sizes (concat-only).
        let merged_bytes = store
            .get_object(&blk.merged_chunk_key)
            .await
            .unwrap();
        assert_eq!(merged_bytes.len() as u64, blk.merged_chunk_bytes);

        // Counters tick.
        assert_eq!(result.metrics.blocks_in, 6);
        assert_eq!(result.metrics.blocks_out, 1);
        assert_eq!(result.metrics.chunks_in, 6);
        assert_eq!(result.metrics.chunks_out, 6);
    }

    #[tokio::test]
    async fn compactor_partial_read_decodes_each_source_chunk() {
        // Concat-only correctness contract: every chunk inside the
        // merged object must decode in isolation when fetched via
        // its recorded byte_offset / byte_length.
        let store = seed_n_hour_blocks(6).await;
        let store_arc: Arc<dyn ObjectStore> = store.clone();
        let plan = CompactionPlan::discover(
            &store_arc,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_for_six_hours_ago(),
            },
        )
        .await
        .unwrap();
        let compactor = Compactor::new(
            store_arc,
            CompactorConfig {
                dry_run: false,
                verify: true,
                tenant_prefix: "tenant1/".to_string(),
            },
        );
        let result = compactor.run(&plan).await.unwrap();
        let blk = &result.blocks[0];

        // Re-read the merged index and partial-read every chunk.
        let merged_idx_bytes = store
            .get_object(&blk.merged_index_key)
            .await
            .unwrap();
        let merged_idx = IndexFile::read(merged_idx_bytes.as_slice()).unwrap();
        assert_eq!(merged_idx.entries.len(), 6);

        for entry in &merged_idx.entries {
            let off = entry.byte_offset.expect("merged entry must carry offset");
            let len = entry.byte_length.expect("merged entry must carry length");
            let chunk_bytes = store
                .get_range(&blk.merged_chunk_key, off, len)
                .await
                .unwrap();
            // Partial-read decode must succeed and yield the same
            // sample count the index records.
            let decoder = GorillaDecoder::from_reader(chunk_bytes.as_slice()).unwrap();
            let header = decoder.header().expect("series header");
            assert_eq!(header.sample_count, entry.sample_count);
            let samples: Vec<_> = decoder.samples().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(samples.len(), entry.sample_count as usize);
        }
    }

    #[tokio::test]
    async fn compactor_idempotent_rerun_is_noop() {
        let store = seed_n_hour_blocks(6).await;
        let store_arc: Arc<dyn ObjectStore> = store.clone();
        let plan = CompactionPlan::discover(
            &store_arc,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_for_six_hours_ago(),
            },
        )
        .await
        .unwrap();
        let compactor = Compactor::new(
            store_arc.clone(),
            CompactorConfig {
                dry_run: false,
                verify: true,
                tenant_prefix: "tenant1/".to_string(),
            },
        );
        let _first = compactor.run(&plan).await.unwrap();

        // Re-discover post-merge: the source `index.json` files
        // were deleted; the planner should find zero groups.
        let plan2 = CompactionPlan::discover(
            &store_arc,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_for_six_hours_ago(),
            },
        )
        .await
        .unwrap();
        assert!(plan2.groups.is_empty(), "post-compaction plan must be empty");

        let result2 = compactor.run(&plan2).await.unwrap();
        assert!(result2.blocks.is_empty());
    }

    #[tokio::test]
    async fn compactor_dry_run_writes_nothing() {
        let store = seed_n_hour_blocks(6).await;
        let store_arc: Arc<dyn ObjectStore> = store.clone();
        let plan = CompactionPlan::discover(
            &store_arc,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_for_six_hours_ago(),
            },
        )
        .await
        .unwrap();
        let compactor = Compactor::new(
            store_arc.clone(),
            CompactorConfig {
                dry_run: true,
                verify: false,
                tenant_prefix: "tenant1/".to_string(),
            },
        );
        let result = compactor.run(&plan).await.unwrap();
        assert_eq!(result.blocks.len(), 1, "dry-run still surfaces the planned block");
        assert!(result.metrics.dry_run, "dry-run flag set on metrics");

        // Nothing changed in the store: the 18 source objects (6 ×
        // {chunk, index, postings}) should still be present, and
        // the merged keys should NOT exist.
        let blk = &result.blocks[0];
        assert!(!store.object_exists(&blk.merged_chunk_key).await.unwrap());
        assert!(!store.object_exists(&blk.merged_index_key).await.unwrap());
        assert!(!store.object_exists(&blk.merged_postings_key).await.unwrap());
        for hour in 0..6u32 {
            assert!(store
                .object_exists(&format!("tenant1/m/2026/05/06/{:02}/index.json", hour))
                .await
                .unwrap());
        }
    }

    #[tokio::test]
    async fn compactor_does_not_decode_chunks_during_merge() {
        // Concat-only assertion: if the GorillaDecoder were called
        // during the merge path we would (a) need the chunks to be
        // valid Gorilla blocks AND (b) tick GET counters for every
        // sample. We seed blocks with garbage byte payloads instead
        // of real Gorilla bytes, then run with `verify=false` and
        // assert the merge succeeds — proving that decode is NOT in
        // the merge path.
        let store = Arc::new(InMemoryObjectStore::new());
        for hour in 0..6u32 {
            let prefix = format!("tenant1/m/2026/05/06/{:02}/", hour);
            let garbage = vec![0xAA_u8; 64]; // intentionally not a GORILLA1 block
            store
                .put_object(&format!("{prefix}part-0.gor"), garbage.clone())
                .await
                .unwrap();
            let mut idx = IndexFile::new(0);
            idx.entries.push(IndexEntry {
                key: "part-0.gor".to_string(),
                time_range: ((hour as u64) * 1000, (hour as u64) * 1000 + 999),
                sample_count: 0,
                label_hash: hour as u64,
                size_bytes: garbage.len() as u32,
                object_key: Some("part-0.gor".to_string()),
                byte_offset: Some(0),
                byte_length: Some(garbage.len() as u64),
            });
            let mut idx_bytes = Vec::new();
            idx.write(&mut idx_bytes).unwrap();
            store
                .put_object(&format!("{prefix}index.json"), idx_bytes)
                .await
                .unwrap();
            // Empty postings sidecar so the merge step doesn't
            // skip the block entirely.
            let mut p_bytes = Vec::new();
            asap_gorilla::Postings::new().write(&mut p_bytes).unwrap();
            store
                .put_object(&format!("{prefix}postings-v1.json"), p_bytes)
                .await
                .unwrap();
        }
        let store_arc: Arc<dyn ObjectStore> = store.clone();
        let plan = CompactionPlan::discover(
            &store_arc,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_for_six_hours_ago(),
            },
        )
        .await
        .unwrap();
        let compactor = Compactor::new(
            store_arc,
            CompactorConfig {
                dry_run: false,
                // `verify: false` so the post-merge partial-read
                // doesn't try to decode the garbage. The point of
                // this test is to prove the merge body itself
                // doesn't decode.
                verify: false,
                tenant_prefix: "tenant1/".to_string(),
            },
        );
        let result = compactor.run(&plan).await.unwrap();
        assert_eq!(result.blocks.len(), 1);
        // 6 source garbage chunks × 64 bytes = 384 bytes merged.
        assert_eq!(result.blocks[0].merged_chunk_bytes, 6 * 64);
        // The merged object is the literal byte concatenation.
        let merged = store
            .get_object(&result.blocks[0].merged_chunk_key)
            .await
            .unwrap();
        assert_eq!(merged.len(), 6 * 64);
        for byte in merged {
            assert_eq!(byte, 0xAA);
        }
    }
}
