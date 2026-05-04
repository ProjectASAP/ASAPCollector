//! Real sketch wrappers over [`asap_sketchlib`] types.
//!
//! Mirrors `asap-precompute-go/sketches/{ddsketch,kll,hll,countsketch,cms}.go`.
//! Each wrapper adapts a concrete sketch implementation from
//! [`asap_sketchlib`] to the host-neutral [`crate::precompute::Sketch`]
//! trait family so a [`crate::precompute::Precompute`] instance can own
//! it as a generic sketch.
//!
//! # Wire format
//!
//! Wrappers serialize via `asap_sketchlib::proto::sketchlib::SketchEnvelope`
//! (prost-encoded). The Go reference uses
//! `sketchlib-go::SerializePortable + proto.Marshal`, which produces
//! the same envelope shape from the same proto definitions
//! (`asap_sketchlib/proto/*.proto`). Field-level byte-parity depends
//! on:
//!
//! - Identical proto field tags / wire types (guaranteed by the shared
//!   proto file).
//! - Identical numeric encoding (integer values map identically across
//!   prost / google.golang.org/protobuf for fixed-tag varints).
//! - Identical floating-point bit patterns (no platform divergence
//!   for finite f64 in IEEE-754 round-trip).
//!
//! See `tests/cross_language_parity.rs` for byte-level verification
//! against Go-generated golden fixtures.
//!
//! # API surface caveats
//!
//! `asap_sketchlib` does not currently expose:
//! - Per-sketch `compute_delta(prev_bytes, threshold)` helpers — Go's
//!   `sketchlib-go` ships these (`ddsketch.ComputeDelta`,
//!   `hll.ComputeRegisterDelta`, `countsketch.ComputeDelta`,
//!   `cms.ComputeDelta`).  The wrappers therefore fall back to
//!   "always-full" delta encoding (the runtime emits `ProtoFull`
//!   envelopes every window). This is correct but bandwidth-inefficient
//!   versus Go. A follow-up that lands `compute_delta` in
//!   `asap_sketchlib` will let these wrappers emit `ProtoDelta` frames.
//! - `DeserializeXxxFromProtoBytes` round-trip helpers for the
//!   high-throughput sketch types. The wrappers decode the wire-format
//!   `SketchEnvelope` envelope and reconstruct the wire-aligned
//!   sketch struct (`DdSketch`, `HllSketch`, …) directly from the
//!   inner state proto.

pub mod cms;
pub mod countsketch;
pub mod ddsketch;
pub mod hll;
pub mod kll;

pub use cms::{CMSObserver, CMSWrapper};
pub use countsketch::{CountSketchObserver, CountSketchWrapper};
pub use ddsketch::{DDSketchObserver, DDSketchWrapper};
pub use hll::{HLLObserver, HLLWrapper};
pub use kll::{KLLObserver, KLLWrapper};
