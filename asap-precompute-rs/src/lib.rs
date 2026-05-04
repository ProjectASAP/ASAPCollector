//! `asap-precompute-rs` is the Rust mirror of `asap-precompute-go`, the
//! host-neutral edge precompute runtime described in
//! `docs/design-asap-edge-framework.md` §6 and pinned by ADR-0002.
//!
//! This crate owns the windowing, snapshot caching, and delta encoding
//! state machine that today lives inside `ASAPQuery-backend`'s ingest
//! path (`asap-query-engine/src/precompute_operators/*.rs` +
//! `drivers/ingest/otel.rs::apply_modified_otlp_delta_bytes`).
//! Per-platform Adapter implementations (the Layer-4 shims) translate
//! their host's native event into [`Observation`], hand it to a
//! [`Precompute`], and translate the runtime's emitted
//! [`SketchEnvelope`] back to the host's native event.
//!
//! # Bootstrap status (Phase 3 step 1)
//!
//! This module is currently the **bootstrap skeleton** — types and
//! trait surface only. State-machine methods (window rotate, observe
//! routing, snapshot/delta computation, scheduler) are defined as
//! `unimplemented!()` and migrate from `ASAPQuery-backend`'s ingest
//! path in subsequent PRs (Phase 3 step 2+). The API surface here is
//! the contract those migrations must hit so the cross-PR work stays
//! safe.
//!
//! # Mirror map to `asap-precompute-go`
//!
//! | Go file | Rust module |
//! | --- | --- |
//! | `observation.go` | [`observation`] |
//! | `envelope.go` | [`envelope`] |
//! | `precompute.go` | [`precompute`] |
//! | `window.go` | [`window`] |
//! | `snapshot_cache.go` | [`snapshot_cache`] |
//! | `matchers.go` | [`matchers`] |
//! | `config.go` | [`config`] |
//! | `adapter.go` | [`adapter`] |
//! | `controlchannel/channel.go` | [`control_channel`] |
//!
//! Method names in the Go side (`SeriesKeyFor`, `ObserveEnvelope`, …)
//! are renamed to snake_case in the Rust API per Rust convention.

#![warn(missing_docs)]

pub mod adapter;
pub mod config;
pub mod control_channel;
pub mod envelope;
pub mod matchers;
pub mod observation;
pub mod precompute;
pub mod snapshot_cache;
pub mod window;

pub use config::{
    AggId, AggregationMode, OnOverflow, PrecomputeConfig, PrecomputeConfigSet, SketchParams,
    WindowSpec,
};
pub use envelope::{Encoding, SketchEnvelope, SketchType};
pub use matchers::{LabelMatcher, MatchOp};
pub use observation::{KeyValue, Observation, ObservationValue, ObservationValueKind};
pub use precompute::{
    CardinalitySketch, FrequencyEntry, FrequencySketch, Precompute, QuantileSketch, Sketch,
    SketchObserver,
};
