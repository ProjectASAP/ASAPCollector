//! Per-[`crate::precompute::Precompute`] window manager. Mirrors
//! `asap-precompute-go/window.go`.
//!
//! Bootstrap status: types only. The state-machine bodies (`observe`,
//! `observe_envelope`, `rotate`, `drain`, `advance_window`) are
//! `unimplemented!()` and migrate from
//! `ASAPQuery-backend/asap-query-engine/src/precompute_operators/*.rs`
//! plus `drivers/ingest/otel.rs::apply_modified_otlp_delta_bytes` in
//! Phase 3 step 2.

use std::collections::HashMap;

use crate::config::PrecomputeConfig;
use crate::envelope::SketchEnvelope;
use crate::observation::{KeyValue, Observation};
use crate::precompute::{BoxedObserver, PrecomputeError, Sketch, SketchFactory, StatsSnapshot};
use crate::snapshot_cache::SnapshotCache;

/// Per-series state held inside a window.
///
/// Mirrors Go `seriesEntry`. Owns one [`Sketch`] instance and the
/// labels needed to reconstruct the [`SketchEnvelope`] at flush
/// time.
pub struct SeriesEntry {
    /// Running sketch for this series. Owned here; the window calls
    /// `Sketch::reset` on rotation when the entry is recycled in
    /// place, OR drops the reference when `MaxSeries` triggers
    /// eviction.
    pub sketch: Box<dyn Sketch>,
    /// Resource-scope attribute set captured when the series was
    /// first observed. Stored alongside `labels` so the adapter's
    /// encode path can faithfully reconstruct the
    /// `pmetric::ResourceMetrics → ScopeMetrics → Metric` hierarchy
    /// (or its non-OTel equivalent) on emission.
    pub resource_labels: Vec<KeyValue>,
    /// Host-neutral data-point attribute set used by series-key
    /// construction and by the encode path at the adapter boundary.
    pub labels: Vec<KeyValue>,
    /// Most recent observation timestamp; used for
    /// [`crate::config::OnOverflow::EvictOldest`].
    pub last_seen_ms: u64,
    /// Total observation count accumulated for this series in the
    /// active window. Incremented once per scalar observation;
    /// envelope-valued observations contribute the upstream
    /// envelope's `count` when present (so chained pre-aggregation
    /// preserves the running sample count). Copied into
    /// [`SketchEnvelope::count`] at flush time so the OTel adapter
    /// can set `dp.SetCount()`.
    pub count: u64,
}

/// Per-Precompute window manager. Tumbling-only for Phase 3 step 2;
/// Sliding lands in a follow-up.
///
/// Mirrors Go `windowState`.
///
/// **Locking (target shape, lands in Phase 3 step 2):** a single
/// `RwLock` guards the entire series map plus `active_start_ms` /
/// `active_end_ms` window bounds. Read paths take the read lock to
/// look up an existing series and upgrade only if a new series
/// needs creation.
pub struct WindowState {
    /// Map from series-key (see [`crate::matchers::series_key`]) to
    /// the active series entry.
    pub(crate) series: HashMap<String, SeriesEntry>,
    /// Inclusive lower bound of the active window (Unix ms).
    // Phase 3 step 2: read by observe()/rotate() once the state
    // machine migrates from ASAPQuery-backend.
    #[allow(dead_code)]
    pub(crate) active_start_ms: u64,
    /// Exclusive upper bound of the active window (Unix ms).
    #[allow(dead_code)]
    pub(crate) active_end_ms: u64,
    /// Whether `active_start_ms` / `active_end_ms` have been
    /// initialized for the active config. Lazy-init avoids needing
    /// the constructor to know the config up front.
    #[allow(dead_code)]
    pub(crate) initialized: bool,
}

impl Default for WindowState {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowState {
    /// Constructs an empty window. Mirrors Go `newWindowState`.
    pub fn new() -> Self {
        Self {
            series: HashMap::new(),
            active_start_ms: 0,
            active_end_ms: 0,
            initialized: false,
        }
    }

    /// Returns the current series count. Mirrors Go
    /// `(*windowState).activeSeriesCount`.
    pub fn active_series_count(&self) -> usize {
        self.series.len()
    }

    /// Lazily computes the first window's bounds based on a
    /// reference timestamp.
    ///
    /// Mirrors Go `(*windowState).initWindow`. **Phase 3 step 2:**
    /// migrates the byte-level body from
    /// `asap-precompute-go/window.go::initWindow`.
    pub fn init_window(&mut self, _ref_ms: u64, _cfg: &PrecomputeConfig) {
        unimplemented!(
            "WindowState::init_window — migrates in Phase 3 step 2; see \
             asap-precompute-go/window.go::initWindow"
        )
    }

    /// Routes an observation into the window.
    ///
    /// Mirrors Go `(*windowState).observe`. Creates a new series
    /// entry if needed; honors `OnOverflow`. Returns
    /// [`PrecomputeError::SeriesCapExceeded`] or
    /// [`PrecomputeError::LateData`] where applicable.
    ///
    /// **Phase 3 step 2:** migrates from
    /// `ASAPQuery-backend/asap-query-engine/src/precompute_operators/`.
    pub fn observe(
        &mut self,
        _obs: &Observation,
        _cfg: &PrecomputeConfig,
        _sketch_factory: &SketchFactory,
        _observer: &BoxedObserver,
        _stats: &mut StatsSnapshot,
    ) -> Result<(), PrecomputeError> {
        unimplemented!(
            "WindowState::observe — migrates in Phase 3 step 2; see \
             asap-precompute-go/window.go::observe"
        )
    }

    /// Applies an inbound envelope to the appropriate series via
    /// the sketch's `apply_delta` or `merge` depending on encoding.
    ///
    /// Mirrors Go `(*windowState).observeEnvelope`. Strategy A
    /// enforcement (design-doc §5.2): inbound envelopes are merged
    /// into the local sketch as sketches, never expanded to scalar
    /// samples.
    ///
    /// **Phase 3 step 2:** migrates from `ASAPQuery-backend`'s
    /// per-accumulator `apply_proto_delta_bytes` paths.
    pub fn observe_envelope(
        &mut self,
        _env: &SketchEnvelope,
        _cfg: &PrecomputeConfig,
        _sketch_factory: &SketchFactory,
        _snapshot_cache: &SnapshotCache,
        _stats: &mut StatsSnapshot,
    ) -> Result<(), PrecomputeError> {
        unimplemented!(
            "WindowState::observe_envelope — migrates in Phase 3 step 2; see \
             asap-precompute-go/window.go::observeEnvelope"
        )
    }

    /// Atomically drains the active window and returns the
    /// closed-window series for emission.
    ///
    /// Mirrors Go `(*windowState).rotate`. For tumbling, rotation
    /// triggers when `now_ms >= active_end_ms` OR when the window
    /// has uninitialized bounds with at least one series (Batch
    /// mode). Returns `(closed_series, [start, end))`. If the
    /// active window isn't yet due, returns an empty `Vec`.
    ///
    /// **Phase 3 step 2:** migrates the body.
    pub fn rotate(
        &mut self,
        _now_ms: u64,
        _cfg: &PrecomputeConfig,
    ) -> (Vec<SeriesEntry>, [u64; 2]) {
        unimplemented!(
            "WindowState::rotate — migrates in Phase 3 step 2; see \
             asap-precompute-go/window.go::rotate"
        )
    }

    /// Unconditionally rotates the active window regardless of
    /// wall-clock time. Used by `Precompute::drain` on shutdown
    /// paths.
    ///
    /// Mirrors Go `(*windowState).drain`. When the active window is
    /// already empty `drain` is a no-op.
    pub fn drain(&mut self, _cfg: &PrecomputeConfig) -> (Vec<SeriesEntry>, [u64; 2]) {
        unimplemented!(
            "WindowState::drain — migrates in Phase 3 step 2; see \
             asap-precompute-go/window.go::drain"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_starts_empty_and_uninitialized() {
        let w = WindowState::new();
        assert_eq!(w.active_series_count(), 0);
        assert!(!w.initialized);
        assert_eq!(w.active_start_ms, 0);
        assert_eq!(w.active_end_ms, 0);
    }
}
