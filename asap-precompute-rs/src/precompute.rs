//! [`Sketch`] trait family + [`Precompute`] trait. Mirrors
//! `asap-precompute-go/precompute.go`.
//!
//! Bootstrap status: trait surface is final; the concrete
//! [`PrecomputeImpl`] struct's state-machine methods
//! (`observe`, `observe_envelope`, `tick`, `drain`) are
//! `unimplemented!()` and migrate from `ASAPQuery-backend`'s ingest
//! path in Phase 3 step 2.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use thiserror::Error;

use crate::config::{PrecomputeConfig, PrecomputeConfigSet};
use crate::envelope::{SketchEnvelope, SketchType};
use crate::observation::{Observation, ObservationValue};
use crate::snapshot_cache::SnapshotCache;
use crate::window::WindowState;

/// Narrow interface the Layer-3 runtime needs from a Layer-1 sketch
/// implementation.
///
/// Mirrors Go `precompute.Sketch`. Real sketches (DDSketch, KLL, HLL,
/// CountSketch, CountMinSketch) live in [`asap_sketchlib`]; thin
/// wrappers in subsequent phases impl this trait against each
/// concrete sketch.
///
/// The interface is intentionally minimal — `observe` is per-sketch
/// (because each sketch has different value-shapes: float, hash,
/// bytes), so the routing layer above lives in the per-shim glue
/// (see [`SketchObserver`]), not here.
pub trait Sketch: Send + Sync {
    /// Serializes the current sketch state to a portable
    /// proto-encoded byte slice.
    ///
    /// Used by [`crate::snapshot_cache::SnapshotCache::compute_delta`]
    /// and as the `ProtoFull` payload.
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError>;

    /// Computes a sparse delta between this sketch and the previous
    /// snapshot bytes.
    ///
    /// If the resulting delta is at least as large as a full snapshot
    /// scaled by `threshold`, returns the full state with
    /// `is_full = true` so the caller can avoid wasted work
    /// re-marshaling. `threshold` is the per-sketch absolute count
    /// cap (see today's `computeDDSketchDelta` /
    /// `countsketch.ComputeDelta`).
    fn compute_delta_against(
        &self,
        prev: &[u8],
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError>;

    /// Merges a previously-computed delta into this sketch in place.
    ///
    /// Used by [`Precompute::observe_envelope`] when an inbound
    /// envelope is encoded as `ProtoDelta`.
    fn apply_delta(&mut self, delta: &[u8]) -> Result<(), PrecomputeError>;

    /// Folds another sketch (typically a freshly-decoded envelope
    /// payload) into this one.
    ///
    /// Used by [`Precompute::observe_envelope`] on `ProtoFull`
    /// inbound envelopes.
    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError>;

    /// Zeros the sketch in place. Used by window rotation and by
    /// sketch object pools.
    fn reset(&mut self);
}

/// Output of [`Sketch::compute_delta_against`].
///
/// Mirrors Go's `(payload []byte, isFull bool, err error)` triple.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeltaResult {
    /// Wire bytes — either a sparse delta (when `is_full = false`)
    /// or the full snapshot (when `is_full = true`).
    pub payload: Vec<u8>,
    /// Whether `payload` is the full snapshot rather than a sparse
    /// delta. Caller uses this to set
    /// [`crate::envelope::Encoding::ProtoFull`] vs
    /// [`crate::envelope::Encoding::ProtoDelta`].
    pub is_full: bool,
}

/// Implemented by sketches that can answer quantile queries.
///
/// DDSketch and KLL are the two `QuantileSketch` implementations.
/// The runtime never type-asserts to `QuantileSketch` — only adapter
/// code does, when materializing typed quantile output (e.g.
/// emitting one gauge per configured quantile when
/// `transmit_sketch=false`).
pub trait QuantileSketch: Sketch {
    /// Returns the q-th rank value (`0 ≤ q ≤ 1`) from the sketch's
    /// current state. Implementations should clamp `q` to `[0, 1]`
    /// and return a finite value (NaN is acceptable for an empty
    /// sketch).
    fn quantile(&self, q: f64) -> f64;
}

/// Implemented by sketches that answer distinct-count queries.
///
/// HyperLogLog is the canonical `CardinalitySketch`. Adapter code
/// downcasts to call [`Self::estimate_cardinality`] when emitting a
/// typed cardinality gauge from an HLL-backed envelope.
pub trait CardinalitySketch: Sketch {
    /// Returns the sketch's current distinct-element estimate as a
    /// float (HLL's bias-corrected estimator returns a non-integer;
    /// callers round if they want an integer gauge).
    fn estimate_cardinality(&self) -> f64;
}

/// One entry in a [`FrequencySketch::top_k`] result. Mirrors Go
/// `precompute.FrequencyEntry`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FrequencyEntry {
    /// Opaque byte slice the sketch indexes by (the same shape
    /// passed to [`crate::observation::ObservationValue::bytes`]).
    pub key: Vec<u8>,
    /// Estimated frequency.
    pub count: f64,
}

/// Implemented by sketches that answer count / top-k queries.
///
/// CountSketch and CountMinSketch are the two `FrequencySketch`
/// implementations. Adapter code downcasts to materialize per-key
/// counts or top-k tables.
pub trait FrequencySketch: Sketch {
    /// Returns the estimated frequency for the given key.
    /// Implementations may return a non-integer value (e.g.
    /// CountSketch's median-of-rows estimator).
    fn estimate_count(&self, key: &[u8]) -> f64;

    /// Returns the top-k highest-frequency entries observed by the
    /// sketch. Order is descending by `count`; implementations may
    /// return fewer than `k` entries when the sketch hasn't seen
    /// enough distinct keys.
    fn top_k(&self, k: usize) -> Vec<FrequencyEntry>;
}

/// Implemented by the per-shim glue that knows how to feed a raw
/// observation value (Float / Hash / Bytes) into the concrete
/// sketch.
///
/// Mirrors Go `precompute.SketchObserver`. NOT part of the [`Sketch`]
/// trait itself because the value type is sketch-specific (DDSketch
/// wants float, HLL wants hash, set-aggregator wants bytes), and
/// forcing all sketches to accept all kinds would push pointless
/// match-arms into every Layer-1 implementation.
pub trait SketchObserver: Send + Sync {
    /// Applies the observation's value to the sketch.
    ///
    /// Implementations should panic-proof against unsupported kinds
    /// (return an error) and call [`Sketch`] methods directly.
    fn observe(&self, sketch: &mut dyn Sketch, v: &ObservationValue)
        -> Result<(), PrecomputeError>;
}

/// Errors returned by the precompute runtime.
///
/// Mirrors the Go-side sentinel errors (`ErrSeriesCapExceeded`,
/// `ErrLateData`, `ErrNoConfig`, `ErrAggIDMismatch`,
/// `ErrSketchTypeMismatch`) plus a generic `Other` for nested
/// adapter / sketch errors.
#[derive(Debug, Error)]
pub enum PrecomputeError {
    /// `MaxSeries` is reached and `OnOverflow` is `Drop`. Mirrors Go
    /// `ErrSeriesCapExceeded`.
    #[error("precompute: series cap exceeded")]
    SeriesCapExceeded,
    /// Observation timestamp is older than the active window's lower
    /// bound minus `AllowedLateness`. Mirrors Go `ErrLateData`.
    #[error("precompute: observation timestamp outside allowed lateness")]
    LateData,
    /// Precompute has no `PrecomputeConfig`. Mirrors Go
    /// `ErrNoConfig`.
    #[error("precompute: no config installed")]
    NoConfig,
    /// Envelope's `agg_id` doesn't match this Precompute's config.
    /// Mirrors Go `ErrAggIDMismatch`.
    #[error(
        "precompute: envelope agg_id does not match config: envelope={envelope} config={config}"
    )]
    AggIdMismatch {
        /// Envelope-side `agg_id`.
        envelope: u64,
        /// Config-side `agg_id`.
        config: u64,
    },
    /// Envelope's `sketch_type` doesn't match this Precompute's
    /// config. Mirrors Go `ErrSketchTypeMismatch`.
    #[error(
        "precompute: envelope sketch_type does not match config: envelope={envelope:?} config={config:?}"
    )]
    SketchTypeMismatch {
        /// Envelope-side type.
        envelope: SketchType,
        /// Config-side type.
        config: SketchType,
    },
    /// Generic catch-all for nested errors (proto decode, sketch
    /// internal failure, etc.).
    #[error("precompute: {0}")]
    Other(String),
}

/// Host-neutral state machine described in design-doc §6.2 and
/// ADR-0002 §"Public API".
///
/// Mirrors Go `precompute.Precompute`. One [`Precompute`] instance
/// owns one sketch type (see [`crate::config::PrecomputeConfig::sketch_type`]);
/// a deployment with multiple sketch types runs multiple instances
/// side-by-side.
pub trait Precompute: Send + Sync {
    /// Routes a raw observation into the active window. May return
    /// [`PrecomputeError::SeriesCapExceeded`] or
    /// [`PrecomputeError::LateData`]; other errors indicate
    /// config / state problems.
    fn observe(&self, obs: &Observation) -> Result<(), PrecomputeError>;

    /// Merges a pre-aggregated upstream sketch into the active
    /// window.
    ///
    /// The envelope's bytes are NEVER expanded to scalar samples
    /// (design-doc §5.2 bandwidth invariant).
    fn observe_envelope(&self, env: &SketchEnvelope) -> Result<(), PrecomputeError>;

    /// Rotates the active window (when due) and returns the closed
    /// window's series as `Vec<SketchEnvelope>` ready for emit.
    ///
    /// `now_ms` is the wall-clock time the caller (e.g.
    /// `Adapter::schedule_tick`) considers "now"; the runtime uses
    /// it to decide whether the window is due for rotation.
    fn tick(&self, now_ms: u64) -> Vec<SketchEnvelope>;

    /// Forces rotation of the active window regardless of
    /// wall-clock time and returns any envelopes that result.
    ///
    /// Use this on shutdown paths to flush pending observations
    /// that haven't reached their natural window boundary.
    /// Distinct from [`Self::tick`]: `tick` only rotates when
    /// `now_ms >= active_end_ms`, which silently drops mid-window
    /// data on early termination. After `drain` the next active
    /// window's bounds are advanced to the same boundary `tick`
    /// would have used at the natural rotation point.
    fn drain(&self) -> Vec<SketchEnvelope>;

    /// Atomically swaps the active config.
    ///
    /// The in-flight window is preserved (matchers / `aggregate_by`
    /// may change, but bytes already accumulated stay where they
    /// are); see ADR-0003 §3.
    fn update_config(&self, cs: &PrecomputeConfigSet);

    /// Returns the live counters; safe to call concurrently.
    fn stats(&self) -> StatsSnapshot;

    /// Flushes any in-progress state; intended for the shim's
    /// shutdown path to run a final tick before returning.
    fn shutdown(&self) -> Result<(), PrecomputeError>;
}

/// Point-in-time snapshot of [`Precompute`] runtime counters.
///
/// Mirrors Go `precompute.StatsSnapshot`. Values across fields may be
/// drawn from slightly different instants — adapters that need an
/// atomic multi-counter view must add an explicit lock.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StatsSnapshot {
    /// Total `observe()` calls (all kinds).
    pub input_observations: u64,
    /// Total `observe_envelope()` calls.
    pub input_envelopes: u64,
    /// Envelopes emitted via `tick`.
    pub output_envelopes: u64,
    /// Current size of the per-`(agg_id, label_key)` map in the
    /// active window. Negative values are not expected but tolerated
    /// for atomic-decrement safety on series eviction.
    pub active_series: i64,
    /// Observations dropped due to `MaxSeries`.
    pub dropped_overflow: u64,
    /// Observations dropped due to `AllowedLateness`.
    pub dropped_late: u64,
    /// Wall-clock timestamp of the last `tick` call.
    pub last_tick_ms: u64,
    /// Count returned by the most recent `tick` (snapshot of
    /// one-tick output volume).
    pub last_emitted_envelopes: u64,
}

/// Type-erased boxed [`SketchObserver`].
///
/// Mirrors Go's interface-typed observer field. The default impl in
/// [`PrecomputeImpl`] holds this in an `Option` because either
/// `cfg` or `observer` may be `None` at construction time.
pub type BoxedObserver = Box<dyn SketchObserver>;

/// Factory function that constructs an empty [`Sketch`] of the type
/// owned by a specific [`Precompute`] instance.
///
/// Mirrors Go `precompute.SketchFactory`. Phase 3 step 2 keeps
/// construction per-instance rather than registry-based to avoid
/// global state.
pub type SketchFactory = Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync>;

/// Concrete implementation of [`Precompute`].
///
/// Mirrors the Go `precompute` (lowercase, package-private) struct
/// but is exposed publicly here so tests and downstream binaries
/// can construct one directly. Fields are private; construction
/// goes through [`PrecomputeImpl::new`].
///
/// **Phase 3 step 1 (this PR):** the state-machine methods (`observe`,
/// `observe_envelope`, `tick`, `drain`) are
/// [`unimplemented!()`](core::unimplemented). The full
/// implementation migrates from `ASAPQuery-backend/asap-query-engine/
/// src/precompute_operators/*.rs` and `drivers/ingest/otel.rs::
/// apply_modified_otlp_delta_bytes` in Phase 3 step 2.
pub struct PrecomputeImpl {
    cfg: Mutex<Option<PrecomputeConfig>>,
    // Phase 3 step 2: wired into observe()/observe_envelope() once
    // the state-machine bodies migrate from ASAPQuery-backend.
    #[allow(dead_code)]
    sketch_factory: Option<SketchFactory>,
    #[allow(dead_code)]
    observer: Option<BoxedObserver>,
    #[allow(dead_code)]
    window: Mutex<WindowState>,
    #[allow(dead_code)]
    snapshot_cache: SnapshotCache,
    stats: Mutex<StatsSnapshot>,
    sketch_type: SketchType,
    closed: AtomicBool,
}

impl PrecomputeImpl {
    /// Constructs a [`PrecomputeImpl`] given an initial config, a
    /// sketch factory that produces empty sketches of the configured
    /// type, and a [`SketchObserver`] that knows how to apply
    /// observation values to the sketch.
    ///
    /// Either `initial_cfg` or `observer` may be `None` at
    /// construction time, but [`Precompute::observe`] will return
    /// [`PrecomputeError::NoConfig`] until [`Precompute::update_config`]
    /// is called.
    ///
    /// Mirrors Go `precompute.New`.
    pub fn new(
        initial_cfg: Option<PrecomputeConfig>,
        sketch_factory: Option<SketchFactory>,
        observer: Option<BoxedObserver>,
    ) -> Self {
        let sketch_type = initial_cfg
            .as_ref()
            .map(|c| c.sketch_type)
            .unwrap_or(SketchType::Unspecified);
        Self {
            cfg: Mutex::new(initial_cfg),
            sketch_factory,
            observer,
            window: Mutex::new(WindowState::new()),
            snapshot_cache: SnapshotCache::new(),
            stats: Mutex::new(StatsSnapshot::default()),
            sketch_type,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the configured sketch type. Mirrors Go's
    /// `precompute.sketchType` field accessor (used by tests).
    pub fn sketch_type(&self) -> SketchType {
        self.sketch_type
    }

    /// Returns whether the instance has been shut down.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Precompute for PrecomputeImpl {
    fn observe(&self, _obs: &Observation) -> Result<(), PrecomputeError> {
        // PHASE 3 STEP 2: migrate from
        // ASAPQuery-backend/asap-query-engine/src/precompute_operators/*.rs
        // and drivers/ingest/otel.rs::apply_modified_otlp_delta_bytes.
        // Reference: asap-precompute-go/precompute.go::Observe and
        // window.go::observe.
        unimplemented!(
            "PrecomputeImpl::observe — migrates from ASAPQuery-backend ingest path in Phase 3 step 2; \
             see asap-precompute-go/precompute.go::Observe + window.go::observe for the contract"
        )
    }

    fn observe_envelope(&self, _env: &SketchEnvelope) -> Result<(), PrecomputeError> {
        // PHASE 3 STEP 2: migrate the envelope-merge path from
        // ASAPQuery-backend (sketch_envelope_accumulator + per-sketch
        // accumulator ApplyDelta paths). Reference:
        // asap-precompute-go/precompute.go::ObserveEnvelope and
        // window.go::observeEnvelope.
        unimplemented!(
            "PrecomputeImpl::observe_envelope — migrates from ASAPQuery-backend ingest path in \
             Phase 3 step 2; see asap-precompute-go/precompute.go::ObserveEnvelope + \
             window.go::observeEnvelope for the contract"
        )
    }

    fn tick(&self, _now_ms: u64) -> Vec<SketchEnvelope> {
        // PHASE 3 STEP 2: window rotation + serializeSeries lands here.
        // Reference: asap-precompute-go/precompute.go::Tick +
        // window.go::rotate + precompute.go::serializeSeries.
        unimplemented!(
            "PrecomputeImpl::tick — migrates in Phase 3 step 2; see \
             asap-precompute-go/precompute.go::Tick + window.go::rotate"
        )
    }

    fn drain(&self) -> Vec<SketchEnvelope> {
        // PHASE 3 STEP 2: shutdown / batch-flush rotation. Reference:
        // asap-precompute-go/precompute.go::Drain + window.go::drain.
        unimplemented!(
            "PrecomputeImpl::drain — migrates in Phase 3 step 2; see \
             asap-precompute-go/precompute.go::Drain + window.go::drain"
        )
    }

    fn update_config(&self, cs: &PrecomputeConfigSet) {
        if cs.configs.is_empty() {
            return;
        }
        let mut guard = self.cfg.lock().expect("config lock poisoned");
        let active_agg_id = guard.as_ref().map(|c| c.agg_id);
        let chosen = match active_agg_id {
            Some(id) => cs
                .configs
                .iter()
                .find(|c| c.agg_id == id)
                .or_else(|| cs.configs.first()),
            None => cs.configs.first(),
        };
        if let Some(c) = chosen {
            *guard = Some(c.clone());
        }
    }

    fn stats(&self) -> StatsSnapshot {
        *self.stats.lock().expect("stats lock poisoned")
    }

    fn shutdown(&self) -> Result<(), PrecomputeError> {
        // CompareAndSwap: only the first shutdown does work. Phase 3
        // step 2 will run a final tick here, mirroring Go's Shutdown.
        let _ = self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PrecomputeConfig;

    #[test]
    fn new_with_no_config_starts_unspecified() {
        let p = PrecomputeImpl::new(None, None, None);
        assert_eq!(p.sketch_type(), SketchType::Unspecified);
        assert!(!p.is_closed());
    }

    #[test]
    fn update_config_picks_matching_agg_id() {
        let initial = PrecomputeConfig {
            agg_id: 7,
            sketch_type: SketchType::DDSketch,
            ..Default::default()
        };
        let p = PrecomputeImpl::new(Some(initial), None, None);
        let new_set = PrecomputeConfigSet {
            version: 2,
            configs: vec![
                PrecomputeConfig {
                    agg_id: 1,
                    sketch_type: SketchType::HLLSketch,
                    ..Default::default()
                },
                PrecomputeConfig {
                    agg_id: 7,
                    sketch_type: SketchType::DDSketch,
                    metric_name: "bumped".into(),
                    ..Default::default()
                },
            ],
        };
        p.update_config(&new_set);
        let cfg = p.cfg.lock().unwrap();
        assert_eq!(cfg.as_ref().unwrap().agg_id, 7);
        assert_eq!(cfg.as_ref().unwrap().metric_name, "bumped");
    }

    #[test]
    fn shutdown_is_idempotent() {
        let p = PrecomputeImpl::new(None, None, None);
        p.shutdown().expect("first shutdown");
        p.shutdown().expect("second shutdown");
        assert!(p.is_closed());
    }
}
