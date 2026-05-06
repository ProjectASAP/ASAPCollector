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
pub const INDEX_SCHEMA_VERSION: u8 = 1;

/// One entry in the per-hour [`IndexFile`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Full S3 key, e.g. `<tenant>/<metric>/YYYY/MM/DD/HH/part-N.gor`.
    pub key: String,
    /// `(start_ts_ns, end_ts_ns)` covered by the chunk.
    pub time_range: (u64, u64),
    /// Number of samples in the chunk.
    pub sample_count: u32,
    /// 64-bit hash of the canonical sorted label set, for prune-by-
    /// label-equality without fetching the chunk.
    pub label_hash: u64,
    /// Size of the on-S3 object in bytes.
    pub size_bytes: u32,
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
