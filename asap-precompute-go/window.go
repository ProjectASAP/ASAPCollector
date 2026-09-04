package precompute

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// seriesEntry is the per-series state held inside a window. It
// owns one Sketch instance and the labels needed to reconstruct
// the SketchEnvelope at flush time.
type seriesEntry struct {
	seriesKey string
	heapIndex int
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

	// deltaWindowStartMs / deltaWindowEndMs record the [start,end) range
	// of the inbound delta envelope most recently applied to this entry's
	// sketch (zero when no delta has been applied — e.g. scalar-fed or
	// full-state-merged entries). observeEnvelope uses them to detect a
	// NEW upstream window arriving for the same series-key in one consumer
	// window: under the per-window-reset (PWR) model each delta is that
	// window's own state against empty, so folding a second window's delta
	// onto the first would over-count additive families (CMS/CountSketch).
	// When the range differs the sketch is reset to empty before applying
	// the new delta, enforcing the one-delta-per-series-per-window
	// invariant. Same-range re-delivery (an idempotent retransmit) keeps
	// the existing additive merge.
	deltaWindowStartMs uint64
	deltaWindowEndMs   uint64

	// subWindowAcked / ackVal track the threshold-driven sub-window producer's
	// divergence reference for this series: ackVal is the family-metric value at
	// the last sub-window emit (Sum value / count N / HLL cardinality;
	// Count-Sketch tracks its own cell snapshot in the wrapper). subWindowAcked
	// is false until the first emit, so the first sub-window emit of a window
	// always fires (and ships full state). Reset to zero on rotation (the series
	// map is rebuilt), so each window's first sub-emit is full.
	subWindowAcked bool
	ackVal         float64
}

type seriesEvictionHeap []*seriesEntry

func (h seriesEvictionHeap) Len() int { return len(h) }
func (h seriesEvictionHeap) Less(i, j int) bool {
	if h[i].LastSeenMs == h[j].LastSeenMs {
		return h[i].seriesKey < h[j].seriesKey
	}
	return h[i].LastSeenMs < h[j].LastSeenMs
}
func (h seriesEvictionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}
func (h *seriesEvictionHeap) Push(value any) {
	e := value.(*seriesEntry)
	e.heapIndex = len(*h)
	*h = append(*h, e)
}
func (h *seriesEvictionHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.heapIndex = -1
	*h = old[:n-1]
	return e
}

// windowState is the per-Precompute window manager. It supports
// Tumbling, Batch, and Sliding modes.
//
// Tumbling / Batch use `series` as the single active window's map:
// rotation drains it wholesale and emits one envelope per series.
//
// Sliding adds a ring of closed PANES (`panes`). The active `series`
// map IS the current (newest) pane. A "slide" event (Tick crossing
// activeEndMs, where activeEndMs == curPaneStart + slide) closes the
// current pane into the ring, trims the ring to panesPerWindow, and
// emits one MERGED envelope per series-key covering every pane still
// in the window. The window thus has length panesPerWindow × slide
// and advances by one slide each Tick (overlapping panes are
// re-emitted in successive windows — that is the defining property of
// a sliding window).
//
// Locking: a single RWMutex guards the entire series map, the pane
// ring, and the activeStart/activeEnd window bounds. Observe takes the
// write lock to admit/record; Tick takes it to rotate.
type windowState struct {
	mu            sync.RWMutex
	series        map[string]*seriesEntry
	eviction      seriesEvictionHeap
	snapshotCache *SnapshotCache
	sketchSink    *atomic.Pointer[SketchSink]
	activeStartMs uint64
	activeEndMs   uint64
	// initialized tracks whether activeStart/End have been
	// initialized for the active config. Lazy-init avoids needing
	// New() to know the config up front.
	initialized bool

	// panes is the ring of closed slide-panes for Sliding mode, oldest
	// first, holding at most panesPerWindow entries. Each pane is a
	// snapshot of the `series` map captured when its slide closed. nil /
	// empty for Tumbling and Batch.
	panes []slidingPane

	// sketchFactory is captured from the observe path so the Sliding
	// rotate can build fresh throwaway sketches to merge panes into,
	// without threading the factory through rotate/drain (whose
	// signatures the test suite pins). Set on first observe; nil before
	// any observation, in which case the empty-window rotate paths never
	// dereference it.
	sketchFactory SketchFactory

	// monitorHook, when non-nil, is invoked on every admitted observation
	// (under w.mu) for continuous intra-window threshold monitoring
	// (Discipline B). It must be cheap: read the series' additive value and
	// run the slack compare — arithmetic plus a non-blocking enqueue, never
	// network or lock I/O. nil (the default) costs one nil-check on the hot
	// path. Installed by Precompute via setMonitorHook.
	monitorHook func(entry *seriesEntry, windowStartMs uint64)
	// monitorResetHook, when non-nil, is invoked from advanceWindow on every
	// rotation with the NEW window start, so the monitor engine begins a fresh
	// epoch aligned exactly to the window bounds (one window = one CDM epoch).
	monitorResetHook func(newWindowStartMs uint64)
	// monitorSampleHook, when non-nil, is invoked on each freshly-created series
	// sketch for the active (new) window — i.e. at the first observation a series
	// receives in a window, before any data is folded in. It stamps the
	// coordinator-granted distributed-NitroSketch sampling probability onto the
	// wrapper via WithSampleP (family-gated; a no-op for non-sampling families).
	// Because the sketch is empty when stamped, the same p governs the whole
	// window's updates and every merge operand — the invariant that WithSampleP
	// must NEVER be applied mid-window. Installed by Precompute via
	// setMonitorHooks alongside the observe/reset hooks. nil ⇒ one nil-check.
	monitorSampleHook func(s Sketch)
	// wakeHook, when non-nil, is invoked from recordLocked whenever the
	// just-observed sketch reports (via the wakeSignaler interface) that an
	// insert-time GOS threshold crossing happened on this observation — the
	// out-of-cycle counterpart to monitorHook, requesting an immediate
	// sub-window flush instead of waiting for the next tick. Installed once at
	// construction via Precompute.SetWakeHook (unlike the monitor hooks, this
	// is not re-installed per config swap). nil ⇒ one nil-check on the hot
	// path.
	wakeHook func()
}

func (w *windowState) hasAccumulatedState() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if len(w.series) != 0 {
		return true
	}
	for _, pane := range w.panes {
		if len(pane.series) != 0 {
			return true
		}
	}
	return false
}

// wakeSignaler is implemented by a Sketch that can trigger an out-of-cycle
// flush from insert-time GOS threshold detection
// (design-gos-unified-edge-telemetry.md §11). ConsumeWakeSignal reports
// whether a crossing happened since the last call, clearing the flag —
// checked once per observation so a burst of crossings between two flushes
// wakes the loop exactly once. Sketches that don't support GOS (or have it
// disabled) simply don't implement this, so the type-assert in recordLocked
// is a one-time no-op for them.
type wakeSignaler interface {
	ConsumeWakeSignal() bool
}

// slidingPane is one closed slide-interval's worth of per-series
// sketches, tagged with the [start,end) range the pane covered.
type slidingPane struct {
	series  map[string]*seriesEntry
	startMs uint64
	endMs   uint64
}

// newWindowState constructs an empty windowState.
func newWindowState() *windowState {
	return &windowState{
		series: make(map[string]*seriesEntry),
	}
}

// initWindow lazily computes the first window's bounds based on a
// reference timestamp. Caller must hold the write lock.
//
// For Sliding mode the bounds describe the CURRENT PANE (length =
// slide), not the full window — rotation steps one pane at a time and
// the full window is reconstructed from the pane ring at emit time.
func (w *windowState) initWindow(refMs uint64, cfg *PrecomputeConfig) {
	if w.initialized {
		return
	}
	size := rotationStepMs(cfg)
	if size == 0 {
		// Batch mode: window covers a single observation set; use
		// a sentinel range that Tick treats as always-flushable.
		w.activeStartMs = refMs
		w.activeEndMs = refMs
		w.initialized = true
		return
	}
	// Align to step boundaries so multiple Precompute instances on
	// the same host produce comparable window edges.
	w.activeStartMs = (refMs / size) * size
	w.activeEndMs = w.activeStartMs + size
	w.initialized = true
}

// windowSizeMs returns the configured window size in milliseconds, or
// zero for unsized (Batch) configs. For Sliding this is the full
// window length (panesPerWindow × slide).
func windowSizeMs(cfg *PrecomputeConfig) uint64 {
	if cfg == nil || cfg.Window.Size <= 0 {
		return 0
	}
	return uint64(cfg.Window.Size / time.Millisecond)
}

// slideMs returns the slide interval (pane length) in milliseconds for
// Sliding mode. Falls back to the full window size when Slide is unset
// or non-positive (Slide == Size ⇒ degenerates to Tumbling). Returns 0
// only when the window itself is unsized.
func slideMs(cfg *PrecomputeConfig) uint64 {
	if cfg == nil {
		return 0
	}
	if cfg.Window.Slide > 0 {
		return uint64(cfg.Window.Slide / time.Millisecond)
	}
	return windowSizeMs(cfg)
}

// panesPerWindow returns N, the number of slide-panes that tile one
// full sliding window: N = ceil(windowSize / slide), at least 1. With
// the conventional window_size = N × slide_interval this is exact.
func panesPerWindow(cfg *PrecomputeConfig) int {
	size := windowSizeMs(cfg)
	slide := slideMs(cfg)
	if size == 0 || slide == 0 {
		return 1
	}
	n := int((size + slide - 1) / slide) // ceil
	if n < 1 {
		n = 1
	}
	return n
}

// rotationStepMs returns the per-Tick rotation period in milliseconds:
// the slide interval for Sliding mode, the full window size otherwise,
// and 0 for Batch. This is the unit the active pane / window bounds
// advance by on each rotation.
func rotationStepMs(cfg *PrecomputeConfig) uint64 {
	if cfg != nil && cfg.Mode == Sliding {
		return slideMs(cfg)
	}
	return windowSizeMs(cfg)
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

	if w.sketchFactory == nil {
		w.sketchFactory = sketchFactory
	}
	w.initWindow(obs.TimestampMs, cfg)

	if err := w.validateTimestampLocked(obs.TimestampMs, cfg); err != nil {
		return err
	}

	// Build the lookup key into a pooled byte buffer so the common
	// case (an already-admitted series) costs no allocation: the
	// `w.series[string(sc.buf)]` index is the compiler's zero-alloc
	// string-from-bytes form. The retained string key is allocated
	// only when a new series is admitted below.
	sc := getSeriesKeyScratch()
	defer putSeriesKeyScratch(sc)
	cfg.buildSeriesKey(sc, obs)
	entry, ok := w.series[string(sc.buf)]
	if !ok {
		var err error
		if entry, err = w.admitSeriesLocked(string(sc.buf), obs, cfg, sketchFactory, stats); err != nil {
			return err
		}
	}
	return w.recordLocked(entry, obs, observer)
}

// observeKeyed is the shared-key entry point for the fused asap_edge
// processor: the caller built the series key once (and decoded the obs
// labels once) so this skips cfg.buildSeriesKey entirely. The key MUST be
// byte-identical to cfg.SeriesKeyFor(obs) — asap_edge derives it the same
// way — so keyed and unkeyed admits land on the same series.
func (w *windowState) observeKeyed(
	key string,
	obs *Observation,
	cfg *PrecomputeConfig,
	sketchFactory SketchFactory,
	observer SketchObserver,
	stats *PrecomputeStats,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.sketchFactory == nil {
		w.sketchFactory = sketchFactory
	}
	w.initWindow(obs.TimestampMs, cfg)

	if err := w.validateTimestampLocked(obs.TimestampMs, cfg); err != nil {
		return err
	}

	entry, ok := w.series[key]
	if !ok {
		var err error
		if entry, err = w.admitSeriesLocked(key, obs, cfg, sketchFactory, stats); err != nil {
			return err
		}
	}
	return w.recordLocked(entry, obs, observer)
}

// validateTimestampLocked prevents host scheduling jitter from changing
// timestamp-defined window semantics. A future-window sample is returned to
// the host for rotate-and-retry; it is never folded into the current sketch.
func (w *windowState) validateTimestampLocked(timestampMs uint64, cfg *PrecomputeConfig) error {
	if timestampMs >= w.activeEndMs {
		return ErrFutureData
	}
	if timestampMs < w.activeStartMs {
		latenessMs := uint64(cfg.Window.AllowedLateness / time.Millisecond)
		if w.activeStartMs-timestampMs > latenessMs {
			return ErrLateData
		}
	}
	return nil
}

// admitSeriesLocked creates + registers a new series for key (caller holds
// w.mu and confirmed it absent), honoring MaxSeries/OnOverflow and the
// parity-mode label-stripping flags. Shared by observe (unkeyed) and
// observeKeyed.
func (w *windowState) admitSeriesLocked(
	key string,
	obs *Observation,
	cfg *PrecomputeConfig,
	sketchFactory SketchFactory,
	stats *PrecomputeStats,
) (*seriesEntry, error) {
	// WholeStream collapses to a single bucket per AggID, so the
	// series-cardinality cap is a no-op (cardinality is 1) — never reject the
	// lone global series even when MaxSeries is set small.
	if cfg.MaxSeries > 0 && !cfg.isWholeStream() && uint64(len(w.series)) >= cfg.MaxSeries {
		switch cfg.OnOverflow {
		case OnOverflowDrop, OnOverflowBlock:
			// Block is degraded to Drop in Phase 2: latency-hostile
			// semantics belong to integration tests and aren't worth
			// blocking the runtime hot path.
			return nil, ErrSeriesCapExceeded
		case OnOverflowEvictOldest:
			oldestKey, _ := w.evictOldestLocked()
			if oldestKey != "" {
				if stats != nil {
					stats.ActiveSeries.Add(-1)
				}
			}
		}
	}
	sketch := sketchFactory()
	// Stamp the coordinator-granted distributed-NitroSketch sampling probability
	// onto this brand-new (empty) series wrapper. Applying WithSampleP here —
	// when the series first appears in a window, before any observation is folded
	// in — guarantees the same p governs the whole window and every merge operand
	// (never mid-window). The hook is family-gated: a no-op for sketches that
	// don't support coordinated sampling.
	if w.monitorSampleHook != nil {
		w.monitorSampleHook(sketch)
	}
	// A physical-plan probability is authoritative for this immutable plan
	// generation and overrides the legacy live grant on each new window sketch.
	if cfg.SampleP > 0 {
		if setter, ok := sketch.(interface{ SetSampleP(float64) }); ok {
			setter.SetSampleP(cfg.SampleP)
		}
	}
	// Honor the parity-mode flags by stripping the labels we promised not
	// to surface. WholeStream (incl. the legacy GlobalAggregation alias)
	// collapses everything; OmitResourceAttrs zeroes only the resource
	// segment. The output envelope reads ResourceLabels/Labels straight from
	// the entry.
	var resourceCopy, labelsCopy []KeyValue
	if !cfg.isWholeStream() {
		labelsCopy = make([]KeyValue, len(obs.Labels))
		copy(labelsCopy, obs.Labels)
		if !cfg.OmitResourceAttrs {
			resourceCopy = make([]KeyValue, len(obs.ResourceLabels))
			copy(resourceCopy, obs.ResourceLabels)
		}
	}
	entry := &seriesEntry{
		Sketch:         sketch,
		seriesKey:      key,
		heapIndex:      -1,
		ResourceLabels: resourceCopy,
		Labels:         labelsCopy,
		LastSeenMs:     obs.TimestampMs,
	}
	w.series[key] = entry
	heap.Push(&w.eviction, entry)
	if stats != nil {
		stats.ActiveSeries.Add(1)
	}
	return entry, nil
}

func (w *windowState) evictOldestLocked() (string, *seriesEntry) {
	if len(w.eviction) == 0 {
		return "", nil
	}
	entry := heap.Pop(&w.eviction).(*seriesEntry)
	delete(w.series, entry.seriesKey)
	if w.snapshotCache != nil {
		w.snapshotCache.Delete(entry.seriesKey)
	}
	if w.sketchSink != nil {
		if sink := w.sketchSink.Load(); sink != nil && *sink != nil {
			(*sink)(entry.Sketch)
			entry.Sketch = nil
		}
	}
	if entry.Sketch != nil {
		entry.Sketch.Reset()
	}
	return entry.seriesKey, entry
}

// recordLocked feeds one observation into a series' sketch and advances its
// bookkeeping. Caller holds w.mu. Shared by observe and observeKeyed.
func (w *windowState) recordLocked(entry *seriesEntry, obs *Observation, observer SketchObserver) error {
	if obs.TimestampMs > entry.LastSeenMs {
		entry.LastSeenMs = obs.TimestampMs
		if entry.heapIndex >= 0 {
			heap.Fix(&w.eviction, entry.heapIndex)
		}
	}
	if err := observer.Observe(entry.Sketch, obs.Value); err != nil {
		return fmt.Errorf("sketch observe: %w", err)
	}
	entry.Count++
	// Continuous intra-window monitoring (Discipline B): cheap nil-checked
	// hook. windowStart is the active window's lower bound, which equals the
	// monitoring epoch id; the engine uses it to detect boundary crossings.
	if w.monitorHook != nil {
		w.monitorHook(entry, w.activeStartMs)
	}
	// Insert-time GOS wake: cheap nil-checked hook + narrow interface assert.
	// A sketch with GOS disabled (or that doesn't support it) never
	// implements wakeSignaler, so this costs one type-assert on the hot path.
	if w.wakeHook != nil {
		if ws, ok := entry.Sketch.(wakeSignaler); ok && ws.ConsumeWakeSignal() {
			w.wakeHook()
		}
	}
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

	if w.sketchFactory == nil {
		w.sketchFactory = sketchFactory
	}

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
			switch cfg.OnOverflow {
			case OnOverflowDrop, OnOverflowBlock:
				return ErrSeriesCapExceeded
			case OnOverflowEvictOldest:
				evictedKey, evicted := w.evictOldestLocked()
				if evictedKey != "" {
					if snapshotCache != nil {
						snapshotCache.Delete(evictedKey)
					}
					if evicted != nil && evicted.Sketch != nil {
						evicted.Sketch.Reset()
					}
					if stats != nil {
						stats.ActiveSeries.Add(-1)
					}
				}
			}
		}
		sketch := sketchFactory()
		labelsCopy := make([]KeyValue, len(env.Labels))
		copy(labelsCopy, env.Labels)
		entry = &seriesEntry{
			Sketch:     sketch,
			seriesKey:  key,
			heapIndex:  -1,
			Labels:     labelsCopy,
			LastSeenMs: refMs,
		}
		w.series[key] = entry
		heap.Push(&w.eviction, entry)
		if stats != nil {
			stats.ActiveSeries.Add(1)
		}
	}

	switch env.Encoding {
	case EncodingProtoDelta, EncodingMsgpackDelta:
		// Delta apply path: feed the delta bytes directly into the
		// sketch; the wrapper knows the on-the-wire delta format (proto
		// sparse cells, or the msgpack DELTA-HEAP frame). Under the
		// per-window-reset model each delta is that window's own state
		// against empty, and the runtime already starts each window with
		// a fresh per-series sketch, so the apply reconstructs the
		// window's state.
		//
		// Double-count guard (P0-3): a delta encodes one upstream
		// window's full state against empty. If a SECOND envelope for the
		// SAME series-key but a DIFFERENT window range arrives before this
		// consumer window rotates, folding its delta onto the first would
		// over-count additive families (CMS/CountSketch). Reset the
		// per-series sketch to empty first so each distinct upstream
		// window replaces rather than accumulates. A same-range
		// re-delivery (idempotent retransmit) is left to merge as before.
		envStart, envEnd := env.WindowStartMs, env.WindowEndMs
		if entry.deltaWindowEndMs != 0 &&
			(entry.deltaWindowStartMs != envStart || entry.deltaWindowEndMs != envEnd) {
			entry.Sketch.Reset()
		}
		if err := entry.Sketch.ApplyDelta(env.Payload); err != nil {
			return fmt.Errorf("apply delta: %w", err)
		}
		entry.deltaWindowStartMs = envStart
		entry.deltaWindowEndMs = envEnd
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
	if refMs > entry.LastSeenMs {
		entry.LastSeenMs = refMs
		if entry.heapIndex >= 0 {
			heap.Fix(&w.eviction, entry.heapIndex)
		}
	}
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

	step := rotationStepMs(cfg)
	if step > 0 && nowMs < w.activeEndMs {
		// Window / pane not yet due.
		return nil, [2]uint64{0, 0}
	}

	return w.rotateLocked(nowMs, cfg)
}

// drain unconditionally rotates the active window regardless of
// wall-clock time. Used by Precompute.Drain on shutdown paths to
// flush pending mid-window observations that would otherwise be
// silently dropped by Tick's `nowMs < activeEndMs` gate.
//
// Returns the slice of closed series and the window range
// [start, end) in millis. The next active window starts at the
// same boundary Tick would have used at natural rotation
// (activeStartMs := old activeEndMs; activeEndMs += size).
//
// When the active window is already empty drain is a no-op:
// it neither emits envelopes nor advances the window bounds,
// so callers can invoke Drain multiple times on shutdown without
// fast-forwarding the window through empty buckets.
func (w *windowState) drain(cfg *PrecomputeConfig) ([]*seriesEntry, [2]uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return nil, [2]uint64{0, 0}
	}
	// For Sliding, there may be buffered state in the pane ring even
	// when the current pane is empty, so drain whenever either holds
	// data. For Tumbling/Batch only the current series matters.
	if cfg != nil && cfg.Mode == Sliding {
		if len(w.series) == 0 && len(w.panes) == 0 {
			return nil, [2]uint64{0, 0}
		}
	} else if len(w.series) == 0 {
		return nil, [2]uint64{0, 0}
	}
	// Hand a "now" pegged to the active end so advanceWindow
	// snaps the next window forward by exactly one step — the
	// same boundary Tick would have used had it fired naturally.
	return w.rotateLocked(w.activeEndMs, cfg)
}

// rotateLocked is the shared rotation body for rotate and drain.
// Caller must hold w.mu (write lock). Captures the active series,
// resets the map, and advances the window bounds.
func (w *windowState) rotateLocked(nowMs uint64, cfg *PrecomputeConfig) ([]*seriesEntry, [2]uint64) {
	if cfg != nil && cfg.Mode == Sliding {
		return w.rotateSlidingLocked(nowMs, cfg)
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
	w.eviction = nil
	w.advanceWindow(nowMs, cfg)
	return closedSeries, rng
}

// rotateSlidingLocked closes the current pane (the active `series` map)
// into the pane ring, trims the ring to panesPerWindow, and emits one
// MERGED entry per series-key spanning every pane in the window. Caller
// holds w.mu.
//
// The returned entries own FRESH, throwaway sketches (built via
// sketchFactory and Merge-folded from the panes' live sketches), so
// finishRotate is free to hand them to the sketch pool without
// disturbing the still-live pane sketches that the next slide will
// re-emit. The pane sketches themselves are only released when their
// pane ages out of the ring.
func (w *windowState) rotateSlidingLocked(nowMs uint64, cfg *PrecomputeConfig) ([]*seriesEntry, [2]uint64) {
	n := panesPerWindow(cfg)
	curStart, curEnd := w.activeStartMs, w.activeEndMs

	// Close the current pane into the ring (even when empty: an empty
	// pane still advances the window and ages out an old pane).
	w.panes = append(w.panes, slidingPane{
		series:  w.series,
		startMs: curStart,
		endMs:   curEnd,
	})
	// Start a fresh current pane and advance the bounds by one slide.
	w.series = make(map[string]*seriesEntry)
	w.eviction = nil
	w.advanceWindow(nowMs, cfg)

	// Trim the ring to the most recent N panes (drop the oldest,
	// releasing its sketches for GC).
	if len(w.panes) > n {
		w.panes = append(w.panes[:0], w.panes[len(w.panes)-n:]...)
	}

	// Merge every pane in the window per series-key.
	merged := make(map[string]*seriesEntry)
	for pi := range w.panes {
		pane := &w.panes[pi]
		for key, src := range pane.series {
			dst, ok := merged[key]
			if !ok {
				// Build a fresh throwaway sketch and copy the series
				// identity so the emitted envelope carries the right
				// labels. The merge below folds in this pane's state.
				dst = &seriesEntry{
					Sketch:         w.sketchFactory(),
					ResourceLabels: src.ResourceLabels,
					Labels:         src.Labels,
					LastSeenMs:     src.LastSeenMs,
				}
				merged[key] = dst
			}
			if dst.Sketch != nil && src.Sketch != nil {
				// Best-effort: a merge error (dimension/type mismatch)
				// should never happen for same-config sketches, but if
				// it does we simply skip this pane's contribution rather
				// than fail the whole rotate.
				_ = dst.Sketch.Merge(src.Sketch)
			}
			dst.Count += src.Count
			if src.LastSeenMs > dst.LastSeenMs {
				dst.LastSeenMs = src.LastSeenMs
			}
		}
	}

	if len(merged) == 0 {
		return nil, [2]uint64{0, 0}
	}

	out := make([]*seriesEntry, 0, len(merged))
	for _, e := range merged {
		out = append(out, e)
	}
	// The emitted window spans from the oldest retained pane's start to
	// the just-closed pane's end.
	rng := [2]uint64{w.panes[0].startMs, w.panes[len(w.panes)-1].endMs}
	return out, rng
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
	step := rotationStepMs(cfg)
	if step == 0 {
		// Batch / unsized — use the latest observation timestamp
		// as the new window start.
		w.activeStartMs = nowMs
		w.activeEndMs = nowMs
		w.fireMonitorReset()
		return
	}
	// Snap to the bucket containing nowMs to avoid lock-step
	// churn after long idle gaps. For Sliding `step` is the slide,
	// so this advances by exactly one pane (or jumps forward over
	// idle panes).
	bucketStart := (nowMs / step) * step
	if bucketStart <= w.activeStartMs {
		// Defensive: at minimum move forward by one step.
		w.activeStartMs = w.activeEndMs
	} else {
		w.activeStartMs = bucketStart
	}
	w.activeEndMs = w.activeStartMs + step
	w.fireMonitorReset()
}

// fireMonitorReset notifies the monitor engine that a new epoch has begun,
// aligned to the freshly-advanced window start. Caller holds w.mu.
func (w *windowState) fireMonitorReset() {
	if w.monitorResetHook != nil {
		w.monitorResetHook(w.activeStartMs)
	}
}

// setMonitorHooks installs (or clears) the per-observation monitor hook and the
// epoch-reset hook under the window lock so they are race-free against the
// observe / rotate paths that read them.
func (w *windowState) setMonitorHooks(
	observe func(entry *seriesEntry, windowStartMs uint64),
	reset func(newWindowStartMs uint64),
	sample func(s Sketch),
) {
	w.mu.Lock()
	w.monitorHook = observe
	w.monitorResetHook = reset
	w.monitorSampleHook = sample
	w.mu.Unlock()
}

// setWakeHook installs (or clears) the per-observation wake hook (see
// wakeHook / recordLocked), race-free against the observe path.
func (w *windowState) setWakeHook(fn func()) {
	w.mu.Lock()
	w.wakeHook = fn
	w.mu.Unlock()
}

// activeSeriesCount returns the current series count. For tests
// and the telemetry layer.
func (w *windowState) activeSeriesCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.series)
}

// resetForScopeChange drops all in-flight window state (the active series map
// and any sliding panes) without emitting it. Called by Precompute.UpdateConfig
// when the aggregation SCOPE flips (PerSeries <-> WholeStream): the two scopes
// key the series map incompatibly (one bucket per AggID vs one per series), so
// the old map's entries cannot be reused under the new scope. Discarding the
// partial window is the conservative choice — at most the current sub-window's
// observations are lost on a live scope change, which only happens on a control-
// plane reconfiguration. A same-scope config change leaves the window untouched.
//
// The window bounds are kept (initialized stays true) so the next rotate still
// advances on the established cadence; only the accumulated sketches are
// dropped. Pooling is intentionally NOT invoked here (the sketches are released
// to GC) because UpdateConfig has no access to the sketch sink and a scope
// change is rare.
func (w *windowState) resetForScopeChange() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.series = make(map[string]*seriesEntry)
	w.eviction = nil
	w.panes = nil
}

// subWindowVisit walks the active (NOT-yet-rotated) series under the window
// lock and calls visit once per live series. It is the read-side of a
// sub-window emit: the runtime serializes each series' CURRENT accumulated
// state into an incremental delta WITHOUT draining the map, resetting any
// sketch, or advancing the window bounds — so accumulation continues into the
// same window after the emit. Returns the active window range [start, end) that
// every sub-window envelope must be stamped with (the SAME range the eventual
// boundary rotate stamps, so the backend's per-window base rotation accumulates
// all of a window's sub-emits and only rotates when window_start changes).
//
// visit runs while w.mu is held, so it must not call back into the window
// (no re-entrant Observe/Tick); it only reads/serializes the sketch. Sliding
// mode has no stable in-window base (panes rotate), so it is a no-op (zero
// range) and the caller skips emission.
func (w *windowState) subWindowVisit(
	cfg *PrecomputeConfig,
	visit func(entry *seriesEntry),
) [2]uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.initialized || (cfg != nil && cfg.Mode == Sliding) {
		return [2]uint64{0, 0}
	}
	if len(w.series) == 0 {
		return [2]uint64{w.activeStartMs, w.activeEndMs}
	}
	for _, entry := range w.series {
		visit(entry)
	}
	return [2]uint64{w.activeStartMs, w.activeEndMs}
}
