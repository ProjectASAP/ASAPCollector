//! Per-hour-bucket `index.json` catalog of cold-store chunks.
//!
//! The Telegraf-side `gorilla_s3` output writes one or more
//! `<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor` chunks per hour.
//! A sibling `<tenant>/<metric>/YYYY/MM/DD/HH/index.json` enumerates
//! them so the backend `GorillaQueryEngine` can prune by time / label
//! without listing the bucket. This module owns the on-wire format
//! for that index file.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::error::{DecodeError, EncodeError};

/// Schema version of [`IndexFile`]. Bump if the JSON layout changes
/// in a backwards-incompatible way.
///
/// **mvp/v5**: still `1`. The `byte_offset` / `byte_length` /
/// `object_key` extension on [`IndexEntry`] is purely additive — old
/// chunks (no compactor merge) keep all three at their defaults
/// (`object_key = key`, `byte_offset = 0`, `byte_length = size_bytes`)
/// and load fine on both old and new readers. See module docs for
/// the contract.
pub const INDEX_SCHEMA_VERSION: u8 = 1;

/// One entry in the per-hour [`IndexFile`].
///
/// ### mvp/v5 — chunk-manifest extension for compactor-merged blocks
///
/// The compactor produces ONE big `block-NNNN-MMMM.gor` object that
/// is the literal byte concatenation of N source `part-*.gor` files.
/// Each source chunk remains an atomic, individually-decodable
/// Gorilla chunk inside the merged object. The new manifest fields
/// record where each chunk lives:
///
/// * `object_key`  → the merged-object's key (e.g. `block-0000-0006.gor`)
/// * `byte_offset` → offset of this chunk within the merged object
/// * `byte_length` → length of this chunk's bytes inside the merged object
///
/// The query engine issues `Range: bytes=START-END` partial reads
/// against `object_key` to fetch exactly the chunk it wants.
///
/// **Backward compat**: `object_key` is `Option<String>` and
/// `byte_offset` / `byte_length` are `Option<u64> / Option<u32>`. Old
/// fixtures parse with all three at `None` and resolve to "fetch
/// whole object at `key`". The `effective_*` helpers below pick the
/// right value regardless of producer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Full S3 key for this *chunk* (legacy field). For pre-compactor
    /// blocks this is the key of the chunk's own object. For compactor-
    /// merged blocks it points at the chunk within the merged file —
    /// the [`Self::object_key`] field then carries the merged file's
    /// key, and [`Self::byte_offset`] / [`Self::byte_length`] mark the
    /// partial-read window. Readers that don't consult the new fields
    /// will read the whole merged file by mistake — see
    /// [`Self::effective_object_key`] for the correct lookup.
    pub key: String,
    /// `(start_ts_ns, end_ts_ns)` covered by the chunk.
    pub time_range: (u64, u64),
    /// Number of samples in the chunk.
    pub sample_count: u32,
    /// 64-bit hash of the canonical sorted label set, for prune-by-
    /// label-equality without fetching the chunk.
    pub label_hash: u64,
    /// Size of the on-S3 object in bytes (pre-compactor) **or** of
    /// this chunk's slice inside a merged block (post-compactor).
    pub size_bytes: u32,
    /// **mvp/v5**: the S3 key of the merged-block object that
    /// physically holds this chunk. `None` ⇒ this chunk is its own
    /// S3 object at [`Self::key`] (pre-compactor layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    /// **mvp/v5**: byte offset of this chunk inside the merged-block
    /// object identified by [`Self::object_key`]. `None` ⇒ start of
    /// object (pre-compactor layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_offset: Option<u64>,
    /// **mvp/v5**: byte length of this chunk's slice inside the
    /// merged-block object. `None` ⇒ whole object (pre-compactor
    /// layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<u64>,
}

impl IndexEntry {
    /// S3 key the engine should issue a GET / Range-GET against to
    /// read this chunk's bytes. Falls back to [`Self::key`] if the
    /// chunk is its own object (pre-compactor layout).
    pub fn effective_object_key(&self) -> &str {
        self.object_key.as_deref().unwrap_or(&self.key)
    }

    /// `(byte_offset, byte_length)` window inside
    /// [`Self::effective_object_key`] that holds this chunk's bytes.
    /// Returns `None` if the entry has no offset/length pair AND no
    /// `size_bytes` info — in that case the caller should GET the
    /// full object. For pre-compactor entries (no `byte_offset`),
    /// returns `Some((0, size_bytes))` so the partial-read path can
    /// be unconditional.
    pub fn effective_byte_range(&self) -> Option<(u64, u64)> {
        match (self.byte_offset, self.byte_length) {
            (Some(off), Some(len)) => Some((off, len)),
            // Pre-compactor: chunk is its own object, range starts at
            // 0 with the recorded size_bytes.
            _ if self.size_bytes > 0 => Some((0, self.size_bytes as u64)),
            _ => None,
        }
    }
}

/// Per-hour-bucket catalog of [`IndexEntry`]s. Encoded as JSON for
/// human-debuggability (matches the Telegraf-side index.json contract).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFile {
    /// On-wire schema version. Always [`INDEX_SCHEMA_VERSION`] for
    /// freshly built indexes; a [`DecodeError::UnsupportedIndexVersion`]
    /// is returned when reading any other value.
    pub schema_version: u8,
    /// Wall-clock nanoseconds-since-Unix-epoch at which the index
    /// file was written.
    pub generated_at_ns: u64,
    /// One row per chunk. The Phase 1 cold-engine writes them in
    /// time-ascending order; the prune iterators do not assume that.
    pub entries: Vec<IndexEntry>,
}

impl IndexFile {
    /// Build an empty index pinned to the current schema version.
    pub fn new(generated_at_ns: u64) -> Self {
        Self {
            schema_version: INDEX_SCHEMA_VERSION,
            generated_at_ns,
            entries: Vec::new(),
        }
    }

    /// Read a JSON-encoded index from `r`.
    pub fn read<R: Read>(mut r: R) -> Result<Self, DecodeError> {
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        let me: Self = serde_json::from_slice(&buf)?;
        if me.schema_version != INDEX_SCHEMA_VERSION {
            return Err(DecodeError::UnsupportedIndexVersion(me.schema_version));
        }
        Ok(me)
    }

    /// Write a pretty-printed JSON encoding of the index to `w`.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), EncodeError> {
        let bytes = serde_json::to_vec_pretty(self)?;
        w.write_all(&bytes)?;
        Ok(())
    }

    /// Iterate the entries that overlap `range = (start_ns, end_ns)`.
    /// Both endpoints are inclusive. Two intervals overlap when
    /// `entry.start <= range.end && entry.end >= range.start`.
    pub fn prune_by_time(&self, range: (u64, u64)) -> impl Iterator<Item = &IndexEntry> {
        let (qs, qe) = range;
        self.entries
            .iter()
            .filter(move |e| e.time_range.0 <= qe && e.time_range.1 >= qs)
    }

    /// Iterate the entries whose [`IndexEntry::label_hash`] equals
    /// `hash`. Used by the backend to pre-filter chunks for an
    /// exact-equality matcher (e.g. `instance="i-12345"`).
    pub fn prune_by_label_hash(&self, hash: u64) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter().filter(move |e| e.label_hash == hash)
    }
}
