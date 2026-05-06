//! Compactor counters for end-of-run reporting + Prometheus
//! scraping. Pure-stdlib `AtomicU64`s; the binary glues them onto a
//! `/metrics` HTTP endpoint or a CSV dump (caller's choice).
//!
//! These counters are also surfaced in the `CompactionResult` so
//! integration tests can assert "the compactor read N bytes and
//! wrote M bytes", without scraping `/metrics`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-run counters.
#[derive(Debug, Default)]
pub struct CompactorMetrics {
    /// Number of source blocks the compactor walked (across all
    /// groups). For pre-compactor blocks this equals the number of
    /// `index.json` files read.
    pub blocks_in: AtomicU64,
    /// Number of merged blocks emitted.
    pub blocks_out: AtomicU64,
    /// Number of source chunks consumed.
    pub chunks_in: AtomicU64,
    /// Number of chunks recorded in the merged index files. Equal to
    /// [`Self::chunks_in`] under concat-only — kept distinct so the
    /// metric remains meaningful if a future revision starts merging
    /// adjacent chunks.
    pub chunks_out: AtomicU64,
    /// Bytes pulled from S3 (chunks + index + postings).
    pub bytes_in: AtomicU64,
    /// Bytes written back to S3 (merged chunk + index + postings).
    pub bytes_out: AtomicU64,
    /// Count of S3 PUT operations issued.
    pub s3_put_count: AtomicU64,
    /// Count of S3 GET operations issued.
    pub s3_get_count: AtomicU64,
    /// Count of S3 DELETE operations issued.
    pub s3_delete_count: AtomicU64,
    /// Wall-clock duration of the whole run, in milliseconds.
    pub duration_ms: AtomicU64,
    /// Set to 1 when the run was a `--dry-run` (no S3 writes).
    pub dry_run: AtomicU64,
}

impl CompactorMetrics {
    /// Build a fresh counter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a thread-safe snapshot of the current counter values.
    pub fn snapshot(&self) -> CompactorMetricsSnapshot {
        CompactorMetricsSnapshot {
            blocks_in: self.blocks_in.load(Ordering::Relaxed),
            blocks_out: self.blocks_out.load(Ordering::Relaxed),
            chunks_in: self.chunks_in.load(Ordering::Relaxed),
            chunks_out: self.chunks_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            s3_put_count: self.s3_put_count.load(Ordering::Relaxed),
            s3_get_count: self.s3_get_count.load(Ordering::Relaxed),
            s3_delete_count: self.s3_delete_count.load(Ordering::Relaxed),
            duration_ms: self.duration_ms.load(Ordering::Relaxed),
            dry_run: self.dry_run.load(Ordering::Relaxed) != 0,
        }
    }

    /// Render Prometheus text exposition for `/metrics` endpoint /
    /// CSV-equivalent dump. Each counter goes out as a single
    /// `compactor_<name> <value>` line.
    pub fn render_prometheus(&self) -> String {
        let s = self.snapshot();
        format!(
            concat!(
                "# HELP compactor_blocks_in Number of source blocks walked.\n",
                "# TYPE compactor_blocks_in counter\n",
                "compactor_blocks_in {}\n",
                "# HELP compactor_blocks_out Number of merged blocks emitted.\n",
                "# TYPE compactor_blocks_out counter\n",
                "compactor_blocks_out {}\n",
                "# HELP compactor_chunks_in Number of source chunks consumed.\n",
                "# TYPE compactor_chunks_in counter\n",
                "compactor_chunks_in {}\n",
                "compactor_chunks_out {}\n",
                "compactor_bytes_in {}\n",
                "compactor_bytes_out {}\n",
                "compactor_s3_put_count {}\n",
                "compactor_s3_get_count {}\n",
                "compactor_s3_delete_count {}\n",
                "compactor_duration_ms {}\n",
                "compactor_dry_run {}\n",
            ),
            s.blocks_in,
            s.blocks_out,
            s.chunks_in,
            s.chunks_out,
            s.bytes_in,
            s.bytes_out,
            s.s3_put_count,
            s.s3_get_count,
            s.s3_delete_count,
            s.duration_ms,
            if s.dry_run { 1 } else { 0 },
        )
    }
}

/// Plain-old-data snapshot of the live counters, suitable for
/// returning by value out of [`crate::Compactor::run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactorMetricsSnapshot {
    /// See [`CompactorMetrics::blocks_in`].
    pub blocks_in: u64,
    /// See [`CompactorMetrics::blocks_out`].
    pub blocks_out: u64,
    /// See [`CompactorMetrics::chunks_in`].
    pub chunks_in: u64,
    /// See [`CompactorMetrics::chunks_out`].
    pub chunks_out: u64,
    /// See [`CompactorMetrics::bytes_in`].
    pub bytes_in: u64,
    /// See [`CompactorMetrics::bytes_out`].
    pub bytes_out: u64,
    /// See [`CompactorMetrics::s3_put_count`].
    pub s3_put_count: u64,
    /// See [`CompactorMetrics::s3_get_count`].
    pub s3_get_count: u64,
    /// See [`CompactorMetrics::s3_delete_count`].
    pub s3_delete_count: u64,
    /// See [`CompactorMetrics::duration_ms`].
    pub duration_ms: u64,
    /// See [`CompactorMetrics::dry_run`].
    pub dry_run: bool,
}
