package precompute

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// seriesEntry is the per-series state held inside a window. It
// owns one Sketch instance and the labels needed to reconstruct
// the SketchEnvelope at flush time.
type seriesEntry struct {
	// Sketch is the running sketch for this series. Owned here;
	// the window calls Reset on rotation when the entry is
	// recycled in place, OR drops the reference when MaxSeries
	// triggers eviction.
	Sketch Sketch
	// ResourceLabels is the resource-scope attribute set captured
	// when the series was first observed. Stored alongside Labels
	// so the adapter's Encode path can faithfully reconstruct the
	// pmetric ResourceMetrics → ScopeMetrics → Metric hierarchy
	// (or its non-OTel equivalent) on emission. Stored as a copy.
	ResourceLabels []KeyValue
	// Labels is the host-neutral data-point attribute set used by
	// SeriesKey and by Encode at the adapter boundary. Stored as
	// a copy so the caller's slice is free to be reused.
	Labels []KeyValue
	// LastSeenMs tracks the most recent observation timestamp;
	// used for OnOverflowEvictOldest.
	LastSeenMs uint64
	// Count is the total observation count accumulated for this
	// series in the active window. Incremented once per scalar
	// observation; envelope-valued observations contribute the
	// upstream envelope's Count when present (so chained pre-
	// aggregation preserves the running sample count). Copied into
	// SketchEnvelope.Count at flush time so the OTel adapter can
	// set dp.SetCount().
	Count uint64
}

// windowState is the per-Precompute window manager. Tumbling-only
// for Phase 2; Sliding lands in a follow-up.
//
// Locking: a single RWMutex guards the entire series map plus the
// activeStart/activeEnd window bounds. Read paths (Observe) take
// RLock to look up an existing series, then upgrade only if a new
// series needs creation. Tick takes the write lock to swap the
// active map atomically.
type windowState struct {
	mu            sync.RWMutex
	series        map[string]*seriesEntry
	activeStartMs uint64
	activeEndMs   uint64
	// initialized tracks whether activeStart/End have been
	// initialized for the active config. Lazy-init avoids needing
	// New() to know the config up front.
	initialized bool
}

// newWindowState constructs an empty windowState.
func newWindowState() *windowState {
	return &windowState{
		series: make(map[string]*seriesEntry),
	}
}

// initWindow lazily computes the first window's bounds based on a
// reference timestamp. Caller must hold the write lock.
func (w *windowState) initWindow(refMs uint64, cfg *PrecomputeConfig) {
	if w.initialized {
		return
	}
	size := windowSizeMs(cfg)
	if size == 0 {
		// Batch mode: window covers a single observation set; use
		// a sentinel range that Tick treats as always-flushable.
		w.activeStartMs = refMs
		w.activeEndMs = refMs
		w.initialized = true
		return
	}
	// Align to size boundaries so multiple Precompute instances on
	// the same host produce comparable window edges.
	w.activeStartMs = (refMs / size) * size
	w.activeEndMs = w.activeStartMs + size
	w.initialized = true
}

// windowSizeMs returns the active window size in milliseconds, or
// zero for unsized (Batch) configs.
func windowSizeMs(cfg *PrecomputeConfig) uint64 {
	if cfg == nil || cfg.Window.Size <= 0 {
		return 0
	}
	return uint64(cfg.Window.Size / time.Millisecond)
}

// observe routes an Observation into the window. Creates a new
// series entry if needed; honors OnOverflow. Returns
// ErrSeriesCapExceeded or ErrLateData where applicable.
func (w *windowState) observe(
	obs *Observation,
	cfg *PrecomputeConfig,
	sketchFactory SketchFactory,
	observer SketchObserver,
	stats *PrecomputeStats,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.initWindow(obs.TimestampMs, cfg)

	// Late-data check.
	if cfg.Window.AllowedLateness > 0 {
		latenessMs := uint64(cfg.Window.AllowedLateness / time.Millisecond)
		if obs.TimestampMs+latenessMs < w.activeStartMs {
			return ErrLateData
		}
	}

	key := cfg.SeriesKeyFor(obs)
	entry, ok := w.series[key]
	if !ok {
		// New series — check cap.
		if cfg.MaxSeries > 0 && uint64(len(w.series)) >= cfg.MaxSeries {
			switch cfg.OnOverflow {
			case OnOverflowDrop, OnOverflowBlock:
				// Block is degraded to Drop in Phase 2: latency-
				// hostile semantics belong to integration tests
				// and aren't worth blocking the runtime hot path.
				return ErrSeriesCapExceeded
			case OnOverflowEvictOldest:
				// Find and evict the oldest series.
				var (
					oldestKey string
					oldestMs  uint64 = ^uint64(0)
				)
				for k, e := range w.series {
					if e.LastSeenMs < oldestMs {
						oldestMs = e.LastSeenMs
						oldestKey = k
					}
				}
				if oldestKey != "" {
					delete(w.series, oldestKey)
					if stats != nil {
						stats.ActiveSeries.Add(-1)
					}
				}
			}
		}
		sketch := sketchFactory()
		// Honor the parity-mode flags by stripping the labels we
		// promised not to surface. GlobalAggregation collapses
		// everything; OmitResourceAttrs zeroes only the resource
		// segment. The output envelope reads ResourceLabels/Labels
		// straight from the entry, so this is what controls what
		// shows up on the wire.
		var resourceCopy, labelsCopy []KeyValue
		if !cfg.GlobalAggregation {
			labelsCopy = make([]KeyValue, len(obs.Labels))
			copy(labelsCopy, obs.Labels)
			if !cfg.OmitResourceAttrs {
				resourceCopy = make([]KeyValue, len(obs.ResourceLabels))
				copy(resourceCopy, obs.ResourceLabels)
			}
		}
		entry = &seriesEntry{
			Sketch:         sketch,
			ResourceLabels: resourceCopy,
			Labels:         labelsCopy,
			LastSeenMs:     obs.TimestampMs,
		}
		w.series[key] = entry
		if stats != nil {
			stats.ActiveSeries.Add(1)
		}
	} else {
		if obs.TimestampMs > entry.LastSeenMs {
			entry.LastSeenMs = obs.TimestampMs
		}
	}

	if err := observer.Observe(entry.Sketch, obs.Value); err != nil {
		return fmt.Errorf("sketch observe: %w", err)
	}
	entry.Count++
	return nil
}

// observeEnvelope applies an inbound envelope to the appropriate
// series via the sketch's ApplyDelta or Merge depending on encoding.
//
// Strategy A enforcement (design-doc §5.2): inbound envelopes are
// merged into the local sketch as sketches, never expanded to
// scalar samples.
func (w *windowState) observeEnvelope(
	env *SketchEnvelope,
	cfg *PrecomputeConfig,
	sketchFactory SketchFactory,
	snapshotCache *SnapshotCache,
	stats *PrecomputeStats,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Use the envelope's window-end as the reference timestamp;
	// this lets a fresh Precompute initialize its window aligned
	// with the upstream sender.
	refMs := env.WindowEndMs
	if refMs == 0 {
		refMs = env.WindowStartMs
	}
	w.initWindow(refMs, cfg)

	// Envelopes carry a single flat labels list (the upstream sender
	// already collapsed any resource/datapoint distinction), so
	// resource labels are empty in this path. We still route through
	// SeriesKeyForEntry so GlobalAggregation collapses inbound
	// envelopes into the same global bucket as the scalar path.
	key := cfg.SeriesKeyForEntry(nil, env.Labels)
	entry, ok := w.series[key]
	if !ok {
		if cfg.MaxSeries > 0 && uint64(len(w.series)) >= cfg.MaxSeries {
			if cfg.OnOverflow == OnOverflowDrop || cfg.OnOverflow == OnOverflowBlock {
				return ErrSeriesCapExceeded
			}
		}
		sketch := sketchFactory()
		labelsCopy := make([]KeyValue, len(env.Labels))
		copy(labelsCopy, env.Labels)
		entry = &seriesEntry{
			Sketch:     sketch,
			Labels:     labelsCopy,
			LastSeenMs: refMs,
		}
		w.series[key] = entry
		if stats != nil {
			stats.ActiveSeries.Add(1)
		}
	}

	switch env.Encoding {
	case EncodingProtoDelta:
		// Delta apply path: feed the delta bytes directly into the
		// sketch; the wrapper knows the on-the-wire delta format.
		if err := entry.Sketch.ApplyDelta(env.Payload); err != nil {
			return fmt.Errorf("apply delta: %w", err)
		}
		// Reconstruct the new full snapshot for cached inbound use.
		snap, snapErr := entry.Sketch.Snapshot()
		if snapErr == nil && snapshotCache != nil {
			snapshotCache.CacheInbound(key, snap)
		}
	case EncodingProtoFull, EncodingMsgpack, EncodingUnspecified:
		// Full-state path: deserialize into a temporary sketch and
		// merge. The Layer-3 runtime doesn't hold a deserialize
		// hook (those are sketch-specific); we go through the
		// SketchFactory + ApplyDelta-as-merge convention.
		other := sketchFactory()
		// ApplyDelta with a full payload is the documented merge
		// path in sketchlib-go's Apply* functions when the
		// previous-state was empty. For envelopes with full
		// proto-state, the wrapper implementation should handle
		// both — we delegate via Merge if Apply isn't enough.
		if err := mergeFullEnvelope(other, entry.Sketch, env.Payload); err != nil {
			return fmt.Errorf("merge full envelope: %w", err)
		}
		if snapshotCache != nil {
			snapshotCache.CacheInbound(key, env.Payload)
		}
	default:
		return fmt.Errorf("precompute: unsupported envelope encoding %s", env.Encoding)
	}
	// Carry the upstream envelope's observation count into our
	// running entry so the next emission reflects the merged total.
	// Envelopes with Count==0 (older senders that don't populate
	// the field) contribute zero, which is a no-op.
	entry.Count += env.Count
	return nil
}

// mergeFullEnvelope is a small helper kept separate so wrappers can
// override behavior. Today it asks `other` to apply the payload as
// a delta from empty (sketchlib-go's Apply* functions handle
// empty-base semantics) and merges the resulting sketch into dst.
func mergeFullEnvelope(other, dst Sketch, payload []byte) error {
	if other == nil {
		return errors.New("nil temp sketch")
	}
	if err := other.ApplyDelta(payload); err != nil {
		return err
	}
	return dst.Merge(other)
}

// rotate atomically drains the active window and returns the
// closed-window series for emission. For tumbling, rotation
// triggers when nowMs >= activeEndMs OR when the window has
// uninitialized bounds with at least one series (Batch mode).
//
// Returns the slice of closed series and the window range
// [start, end) in millis. If the active window isn't yet due,
// returns an empty slice.
func (w *windowState) rotate(nowMs uint64, cfg *PrecomputeConfig) ([]*seriesEntry, [2]uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return nil, [2]uint64{0, 0}
	}

	size := windowSizeMs(cfg)
	if size > 0 && nowMs < w.activeEndMs {
		// Window not yet due.
		return nil, [2]uint64{0, 0}
	}

	if len(w.series) == 0 {
		// Slide the window forward but emit nothing.
		w.advanceWindow(nowMs, cfg)
		return nil, [2]uint64{0, 0}
	}

	closedSeries := make([]*seriesEntry, 0, len(w.series))
	for _, entry := range w.series {
		closedSeries = append(closedSeries, entry)
	}
	rng := [2]uint64{w.activeStartMs, w.activeEndMs}

	// Reset the series map for the next window.
	w.series = make(map[string]*seriesEntry)
	w.advanceWindow(nowMs, cfg)
	return closedSeries, rng
}

// advanceWindow rolls the active window bounds forward. Caller
// holds the write lock.
//
// Tumbling: when nowMs is at least one full window past activeEnd,
// jump to the bucket containing nowMs so we don't churn through
// many empty windows in a row. Otherwise advance by one size.
//
// Batch: collapses to a no-op since size is zero.
func (w *windowState) advanceWindow(nowMs uint64, cfg *PrecomputeConfig) {
	size := windowSizeMs(cfg)
	if size == 0 {
		// Batch / unsized — use the latest observation timestamp
		// as the new window start.
		w.activeStartMs = nowMs
		w.activeEndMs = nowMs
		return
	}
	// Snap to the bucket containing nowMs to avoid lock-step
	// churn after long idle gaps.
	bucketStart := (nowMs / size) * size
	if bucketStart <= w.activeStartMs {
		// Defensive: at minimum move forward by one window.
		w.activeStartMs = w.activeEndMs
	} else {
		w.activeStartMs = bucketStart
	}
	w.activeEndMs = w.activeStartMs + size
}

// activeSeriesCount returns the current series count. For tests
// and the telemetry layer.
func (w *windowState) activeSeriesCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.series)
}
