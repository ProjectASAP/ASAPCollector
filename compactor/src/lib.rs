//! ASAP S3 cold-store compactor — **concat-only** block merger.
//!
//! mvp/v5 ships this as a sidecar binary that walks a Gorilla-S3
//! bucket, finds groups of small per-hour blocks, and merges each
//! group into one big per-day block. The merge is **byte
//! concatenation** of the source `part-*.gor` files; chunks are
//! NEVER decoded or re-encoded.
//!
//! ## Why concat-only
//!
//! Gorilla bit-streams are stateful: delta-of-delta timestamps + XOR
//! values both reference prior samples *within the chunk*. Naive byte
//! concatenation across chunk boundaries corrupts the decoder.
//! Boundary-fixup (re-encode just the first sample of B relative to
//! A's last) doesn't help because the fixed-up sample's bit-length
//! differs from the original prefix, forcing a bit-shift of all
//! subsequent bits — i.e. the same work as full decode + re-encode.
//!
//! Instead, we treat each source `part-*.gor` as an atomic,
//! individually-decodable Gorilla chunk that stays byte-identical
//! inside the merged file. The backend then uses
//! `byte_offset / byte_length` from the new `index.json` to issue
//! `Range: bytes=START-END` partial reads, so query-time fetches are
//! still per-chunk.
//!
//! ## Wins (without decode)
//!
//! * ~6× fewer S3 PUTs at compaction time vs each hour individually
//! * Fewer S3 LIST/GET calls when answering long-range queries
//! * One merged `postings-v1.json` per merged block → query-time
//!   GET is a single fetch instead of 6
//! * Source chunks stay byte-identical to what the agent wrote —
//!   the verify mode does a partial read against the merged object
//!   and a sample decode to prove the chunk is intact.
//!
//! ## Future-work optimization (NOT in scope here)
//!
//! Decode + re-encode adjacent same-series chunks into a single
//! longer chunk for ~10–30% additional Gorilla compression. That's
//! the natural follow-up after this MVP lands. See
//! `// TODO(post-mvp)` markers in this file.
//!
//! ## High-level flow
//!
//! 1. List the bucket under `<tenant>/<metric>/YYYY/MM/DD/HH/`
//!    prefixes; group adjacent hours that meet age/count thresholds.
//! 2. For each group, fetch every chunk's bytes (no decoding) +
//!    parse its index.json + parse its postings-v1.json.
//! 3. Append all chunk bytes to a single output buffer; track each
//!    chunk's `(byte_offset, byte_length)` inside the merged buffer.
//! 4. Build the merged `IndexFile` (chunks point at the merged
//!    object key; offsets/lengths set) and the merged postings via
//!    [`asap_gorilla::Postings::merge_many`].
//! 5. Atomically PUT the three outputs (merged .gor, merged
//!    index.json, merged postings) under a tmp prefix, then rename
//!    to the final prefix and DELETE the source-block objects.
//!
//! Idempotent: re-running on already-compacted blocks finds no
//! groups and emits zero work.

pub mod metrics;
pub mod object_store;
pub mod plan;
pub mod run;

pub use metrics::CompactorMetrics;
pub use object_store::{InMemoryObjectStore, ObjectStore, ObjectStoreError};
pub use plan::{CompactionGroup, CompactionPlan, CompactionThresholds, SourceBlock};
pub use run::{CompactedBlock, CompactionResult, Compactor, CompactorConfig, CompactorError};

// Re-exported for convenience in tests.
pub use asap_gorilla::{IndexEntry, IndexFile, Postings, PostingsBuilder};
