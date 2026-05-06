//! `asap-gorilla` — Gorilla XOR-delta block format encoder + decoder
//! for the ASAP cold-store engine.
//!
//! This crate is the Rust mirror of the canonical `GORILLA1` block
//! format defined by the Go `gorillaprocessor` plugin under
//! `opentelemetry-collector-contrib-patch/processor/gorillaprocessor/`.
//! Bytes written by either implementation are interchangeable; see
//! [`crate::block`] for the on-wire layout and `tests/byte_compat.rs`
//! for the round-trip-against-fixtures contract.
//!
//! # Phase 1 deliverable
//!
//! Phase 1 of the Gorilla-S3-cold-engine is just the format codec:
//! - [`encoder::GorillaEncoder`] produces blocks
//! - [`decoder::GorillaDecoder`] streams samples back out
//! - [`index::IndexFile`] is the per-hour-bucket catalog the backend
//!   uses to prune chunks before issuing range reads
//!
//! Subsequent phases will wire this crate into the
//! `ASAPQuery-backend` GorillaQueryEngine and the controller's
//! cold-store-aware planner.
//!
//! # Quick start
//!
//! ```
//! use asap_gorilla::{GorillaDecoder, GorillaEncoder};
//!
//! let mut enc = GorillaEncoder::new(
//!     "node_cpu_seconds_total",
//!     vec![
//!         ("instance".to_string(), "i-1".to_string()),
//!         ("mode".to_string(), "user".to_string()),
//!     ],
//! );
//! enc.append(1_700_000_000_000_000_000, 0.5);
//! enc.append(1_700_000_001_000_000_000, 0.6);
//! enc.append(1_700_000_002_000_000_000, 0.6);
//! let bytes = enc.finalize().unwrap();
//!
//! let dec = GorillaDecoder::from_reader(&bytes[..]).unwrap();
//! let header = dec.header().unwrap();
//! assert_eq!(header.metric, "node_cpu_seconds_total");
//! assert_eq!(header.sample_count, 3);
//! let samples: Vec<_> = dec.samples().collect::<Result<Vec<_>, _>>().unwrap();
//! assert_eq!(samples.len(), 3);
//! ```

#![warn(missing_docs)]

pub mod block;
pub mod decoder;
pub mod encoder;
pub mod error;
pub mod index;

pub use block::{SeriesChunk, SeriesMeta, BLOCK_VERSION, HEADER_LEN, MAGIC};
pub use decoder::{DecodedHeader, GorillaDecoder, SampleIter};
pub use encoder::{write_block, GorillaEncoder};
pub use error::{DecodeError, EncodeError};
pub use index::{IndexEntry, IndexFile, INDEX_SCHEMA_VERSION};
