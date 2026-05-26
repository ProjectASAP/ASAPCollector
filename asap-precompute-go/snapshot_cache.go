package precompute

import (
	"fmt"
	"sync"
)

// SnapshotCache stores per-series sketch payloads for delta encoding
// (outbound) and inbound delta-apply against cached upstream
// snapshots. Mirrors today's per-processor `snapshots map[string][]byte`
// (outbound) and `IngestState.sketch_snapshots` (inbound) with explicit
// per-direction methods.
//
// Two maps avoid cross-direction collisions: the same series key may
// be both produced (outbound) and consumed (inbound) by the same
// host when running as a forwarder, and the two byte streams are not
// interchangeable (a remote sender's snapshot is not what we'd emit
// locally).
type SnapshotCache struct {
	mu       sync.RWMutex
	outbound map[string][]byte
	inbound  map[string][]byte
}

// NewSnapshotCache constructs an empty cache.
func NewSnapshotCache() *SnapshotCache {
	return &SnapshotCache{
		outbound: make(map[string][]byte),
		inbound:  make(map[string][]byte),
	}
}

// CacheOutbound stores the latest full sketch payload, keyed by
// seriesKey. Returns true if this is the first snapshot for that
// key (caller can use this to force a PROTO_FULL on next emit).
//
// Stores a defensive copy of payload so a caller mutating its slice
// after caching does not race the cache reader.
func (c *SnapshotCache) CacheOutbound(seriesKey string, payload []byte) (firstTime bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, existed := c.outbound[seriesKey]
	cp := make([]byte, len(payload))
	copy(cp, payload)
	c.outbound[seriesKey] = cp
	return !existed
}

// GetOutbound returns the cached outbound payload, or nil. The
// returned slice MUST NOT be mutated by the caller — it aliases the
// cache's storage. (Callers that need to mutate should copy first.)
func (c *SnapshotCache) GetOutbound(seriesKey string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.outbound[seriesKey]
}

// CacheInbound stores an upstream snapshot for delta apply.
func (c *SnapshotCache) CacheInbound(seriesKey string, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	c.inbound[seriesKey] = cp
}

// GetInbound returns the cached upstream snapshot or nil.
func (c *SnapshotCache) GetInbound(seriesKey string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inbound[seriesKey]
}

// emptyBaseDeltaSketch is implemented by sketches that opt in to
// "true per-window deltas" (delta-baseline-contract.md §3).
//
// DeltaAgainstEmptyBase returns the snapshot bytes the SnapshotCache
// caches as the outbound base AFTER each window-close emit — i.e. the
// snapshot of an EMPTY sketch of the same shape. The next window's
// ComputeDeltaAgainst then diffs against empty, so its delta is that
// window's own full per-window state encoded as a delta (no
// cross-window subtraction). This is what makes the backend's future
// per-window base rotation correct.
//
// Only DDSketch implements this in the first per-window-delta rollout; CMS /
// CountSketch / HLL / KLL do NOT, so they keep the legacy
// always-refresh behavior via the type-assertion miss below.
type emptyBaseDeltaSketch interface {
	DeltaAgainstEmptyBase() ([]byte, error)
}

// ComputeDelta diffs current sketch state against the cached outbound
// snapshot for that seriesKey. Returns (payload, isFull, err) where
// isFull means the runtime should emit PROTO_FULL (either no prior
// snapshot existed, or the delta exceeded threshold).
//
// Base refresh — two modes:
//
//   - Legacy always-refresh (CMS / KLL / HLL / CountSketch): every call
//     updates the cached previous snapshot to the current sketch state,
//     so successive sub-threshold deltas are each computed against the
//     immediately preceding window. This matches the established
//     behavior of the legacy OTel sketch processors.
//   - Per-window deltas (DDSketch, via emptyBaseDeltaSketch):
//     after each window-close emit the cached base is reset to the
//     EMPTY-sketch snapshot, so the next window diffs against empty and
//     transmits its OWN per-window state as a delta (no cross-window
//     subtraction). See delta-baseline-contract.md §3.
//
// The Sketch interface's ComputeDeltaAgainst does the actual diff
// using the algorithm-specific delta-encoding rules from sketchlib-go.
func (c *SnapshotCache) ComputeDelta(
	seriesKey string,
	current Sketch,
	threshold uint64,
) (payload []byte, isFull bool, err error) {
	if current == nil {
		return nil, false, fmt.Errorf("compute delta: nil sketch")
	}
	c.mu.RLock()
	prev := c.outbound[seriesKey]
	c.mu.RUnlock()
	// Compute the wire payload first (full on first call or above
	// threshold; sparse delta otherwise).
	if prev == nil {
		// First time — emit full.
		full, snapErr := current.Snapshot()
		if snapErr != nil {
			return nil, false, fmt.Errorf("snapshot: %w", snapErr)
		}
		payload = full
		isFull = true
	} else {
		delta, full, dErr := current.ComputeDeltaAgainst(prev, threshold)
		if dErr != nil {
			return nil, false, fmt.Errorf("compute delta: %w", dErr)
		}
		payload = delta
		isFull = full
	}
	// Refresh the cached outbound base for the NEXT window's delta.
	//
	// Per-window deltas: if the sketch opts in via
	// emptyBaseDeltaSketch (DDSketch this phase), reset the cached base
	// to the EMPTY-sketch snapshot after this window-close emit, so the
	// next window diffs against empty and transmits its own per-window
	// state as a delta — no cross-window subtraction.
	if eb, ok := current.(emptyBaseDeltaSketch); ok {
		emptyBase, ebErr := eb.DeltaAgainstEmptyBase()
		if ebErr != nil {
			return nil, false, fmt.Errorf("empty base: %w", ebErr)
		}
		c.CacheOutbound(seriesKey, emptyBase)
		return payload, isFull, nil
	}
	// Legacy always-refresh: update the cached outbound to the latest
	// full snapshot so the next ComputeDelta call diffs against the
	// just-emitted window. When isFull=true the wire payload IS the
	// snapshot, so reuse it; otherwise serialize a fresh full snapshot
	// for the cache. Both branches end with c.outbound[seriesKey] ==
	// latest full state.
	if isFull {
		c.CacheOutbound(seriesKey, payload)
	} else {
		full, snapErr := current.Snapshot()
		if snapErr != nil {
			return nil, false, fmt.Errorf("snapshot: %w", snapErr)
		}
		c.CacheOutbound(seriesKey, full)
	}
	return payload, isFull, nil
}

// Reset clears all cached state (used in tests and on shutdown).
func (c *SnapshotCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outbound = make(map[string][]byte)
	c.inbound = make(map[string][]byte)
}

// LenOutbound returns the number of cached outbound snapshots; for
// tests and the telemetry layer.
func (c *SnapshotCache) LenOutbound() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.outbound)
}

// LenInbound returns the number of cached inbound snapshots; for
// tests and the telemetry layer.
func (c *SnapshotCache) LenInbound() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.inbound)
}
