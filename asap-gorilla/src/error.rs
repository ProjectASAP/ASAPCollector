//! Error types for the encoder, decoder, and index file.

use std::io;

use thiserror::Error;

/// Errors raised by [`crate::encoder::GorillaEncoder`] and
/// [`crate::index::IndexFile::write`].
#[derive(Debug, Error)]
pub enum EncodeError {
    /// Wrapping I/O error from the underlying writer.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// JSON serialization failed (only for the per-series metadata or
    /// the per-hour `IndexFile`).
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The per-series JSON metadata exceeded the on-wire `u16` length
    /// prefix that the GORILLA1 format permits.
    #[error("series metadata too large: {0} bytes (max {max})", max = u16::MAX)]
    MetadataTooLarge(usize),
    /// The encoder received a sample count that does not fit a `u32`.
    #[error("sample count {0} exceeds u32::MAX")]
    SampleCountOverflow(usize),
}

/// Errors raised by [`crate::decoder::GorillaDecoder`] and
/// [`crate::index::IndexFile::read`].
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Wrapping I/O error from the underlying reader.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// JSON deserialization failed.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Magic bytes (`"GORILLA1"`) did not match.
    #[error("bad magic: expected {expected:?}, got {got:?}")]
    BadMagic {
        /// Expected magic bytes.
        expected: [u8; 8],
        /// Magic bytes the reader actually returned.
        got: [u8; 8],
    },
    /// Block format version was not understood by this build of the
    /// crate.
    #[error("unsupported block version {0} (this build supports {})", crate::block::BLOCK_VERSION)]
    UnsupportedVersion(u8),
    /// Index file's `schema_version` was not understood.
    #[error("unsupported index schema_version {0} (this build supports {})", crate::index::INDEX_SCHEMA_VERSION)]
    UnsupportedIndexVersion(u8),
    /// The bit stream ended before the encoded number of samples could
    /// be reconstructed.
    #[error("bit stream truncated at sample {sample_idx}")]
    Truncated {
        /// Sample index where the bit stream ran out.
        sample_idx: usize,
    },
    /// Encoder state machine invariant was violated — typically only
    /// reachable on malformed input.
    #[error("malformed bit stream: {0}")]
    Malformed(&'static str),
}
