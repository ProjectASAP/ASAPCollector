//! On-wire layout of a single Gorilla block.
//!
//! This format is byte-compatible with the canonical Go implementation
//! at `opentelemetry-collector-contrib-patch/processor/gorillaprocessor/
//! {compress,encoder,bitwriter}.go`. A "block" (a.k.a. an "object" in
//! Go terminology) is a `GORILLA1` container holding one or more
//! per-series chunks. The Telegraf-side `gorilla_s3` output is a
//! passthrough uploader; the chunk encoding lives in the Go
//! `gorillaprocessor` and is mirrored here.
//!
//! ## Block (object) layout
//!
//! All multi-byte integers are little-endian.
//!
//! | offset | size | field         | description                                  |
//! |-------:|-----:|---------------|----------------------------------------------|
//! | 0      | 8    | magic         | ASCII `"GORILLA1"`                           |
//! | 8      | 1    | version       | block-format version (currently `1`)         |
//! | 9      | 4    | series_count  | number of [`SeriesChunk`]s that follow       |
//! | 13     | …    | chunks        | back-to-back encoded series chunks           |
//!
//! Each [`SeriesChunk`] is laid out as:
//!
//! | size      | field          | description                                            |
//! |----------:|----------------|--------------------------------------------------------|
//! | 2         | meta_len       | length of the JSON metadata blob (`u16`)               |
//! | meta_len  | meta_json      | UTF-8 [`SeriesMeta`] JSON (no trailing NUL)            |
//! | 4         | point_count    | number of `(ts, value)` samples (`u32`)                |
//! | 8         | first_ts       | first timestamp, raw `int64` cast to `u64`             |
//! | 8         | first_val_bits | `f64::to_bits` of the first value                      |
//! | 4         | ts_bits_len    | length **in bits** of the timestamp body (`u32`)       |
//! | …         | ts_bits        | byte-padded XOR/delta-of-delta timestamp bit stream    |
//! | 4         | val_bits_len   | length **in bits** of the value body (`u32`)           |
//! | …         | val_bits       | byte-padded XOR-encoded value bit stream               |
//!
//! `ts_bits` and `val_bits` are encoded by [`crate::encoder`] using
//! the Gorilla 2015 paper §4 algorithm (delta-of-delta for timestamps,
//! XOR with leading/significant-bit-window encoding for values).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Magic bytes prefixed to every encoded block.
pub const MAGIC: [u8; 8] = *b"GORILLA1";

/// Block-format version emitted (and accepted) by this crate.
pub const BLOCK_VERSION: u8 = 1;

/// Length in bytes of the fixed block header (magic + version + series_count).
pub const HEADER_LEN: usize = 8 + 1 + 4;

/// Per-series JSON metadata header. Field names match the Go
/// `seriesMeta` struct in `gorillaprocessor/encoder.go` exactly so the
/// JSON wire format is byte-identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesMeta {
    /// Metric name (e.g. `node_cpu_seconds_total`).
    pub metric_name: String,
    /// Label / attribute set, encoded as a string→string map. The Go
    /// side uses `map[string]string` which serializes as a JSON object
    /// with a non-deterministic key order. We use a [`BTreeMap`] so
    /// `asap-gorilla` always emits sorted JSON keys; that is a
    /// deliberate strengthening of the contract — see
    /// `tests/byte_compat.rs` for what this means for byte-parity.
    pub attributes: BTreeMap<String, String>,
    /// First sample's timestamp (UnixNano), as recorded by the encoder
    /// after sorting.
    pub start_ts: i64,
    /// Last sample's timestamp (UnixNano).
    pub end_ts: i64,
    /// Number of samples in the chunk.
    pub point_count: usize,
}

/// One series's worth of compressed samples plus its metadata header.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesChunk {
    /// JSON metadata describing the series.
    pub meta: SeriesMeta,
    /// First timestamp; subsequent timestamps reconstruct from
    /// `ts_bits`.
    pub first_ts: i64,
    /// Raw `f64` bit pattern of the first value.
    pub first_val_bits: u64,
    /// Length in **bits** (not bytes) of the timestamp bit stream.
    pub ts_bits_len: u32,
    /// Byte-aligned timestamp bit stream.
    pub ts_bits: Vec<u8>,
    /// Length in **bits** (not bytes) of the value bit stream.
    pub val_bits_len: u32,
    /// Byte-aligned value bit stream.
    pub val_bits: Vec<u8>,
}

impl SeriesChunk {
    /// Serialized length in bytes that this chunk will occupy inside
    /// the enclosing block.
    pub fn encoded_len(&self) -> usize {
        // meta_len(2) + meta + point_count(4) + first_ts(8) +
        // first_val_bits(8) + ts_bits_len(4) + ts_bits + val_bits_len(4) +
        // val_bits.
        let meta_len = serde_json::to_vec(&self.meta).map(|v| v.len()).unwrap_or(0);
        2 + meta_len + 4 + 8 + 8 + 4 + self.ts_bits.len() + 4 + self.val_bits.len()
    }
}
