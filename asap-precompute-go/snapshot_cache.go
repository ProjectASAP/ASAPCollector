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
	mu         sync.RWMutex
	outbound   map[string][]byte
	inbound    map[string][]byte
	generation uint64
	seen       map[string]uint64
}

// NewSnapshotCache constructs an empty cache.
func NewSnapshotCache() *SnapshotCache {
	return &SnapshotCache{
		outbound:   make(map[string][]byte),
		inbound:    make(map[string][]byte),
		generation: 1,
		seen:       make(map[string]uint64),
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
	c.seen[seriesKey] = c.generation
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
	c.seen[seriesKey] = c.generation
}

// BeginGeneration advances the reusable liveness epoch used by rotation.
func (c *SnapshotCache) BeginGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	return c.generation
}

// Touch marks a retained series without allocating a per-rotation keep set.
func (c *SnapshotCache) Touch(seriesKey string, generation uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation == c.generation {
		c.seen[seriesKey] = generation
	}
}

// EndGeneration removes snapshots not touched by the closed generation. Cache
// writes racing with serialization mark themselves in the current generation.
func (c *SnapshotCache) EndGeneration(generation uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return
	}
	for key := range c.outbound {
		if c.seen[key] != generation {
			delete(c.outbound, key)
		}
	}
	for key := range c.inbound {
		if c.seen[key] != generation {
			delete(c.inbound, key)
		}
	}
	for key, seen := range c.seen {
		if seen != generation {
			delete(c.seen, key)
		}
	}
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
// DDSketch, CMS, CountSketch, and HLL implement this; KLL does NOT
// (it stays full-only), so KLL keeps the legacy always-refresh behavior
// via the type-assertion miss below. A family that opts in only
// conditionally returns a nil/empty base from DeltaAgainstEmptyBase to
// fall back to legacy refresh for that call (e.g. CMS msgpack mode).
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
//   - Legacy always-refresh (KLL, and any family that does not opt in):
//     every call updates the cached previous snapshot to the current
//     sketch state, so successive sub-threshold deltas are each computed
//     against the immediately preceding window. This matches the
//     established behavior of the legacy OTel sketch processors.
//   - Per-window deltas (DDSketch / CMS / CountSketch / HLL, via
//     emptyBaseDeltaSketch): after each window-close emit the cached base
//     is reset to the EMPTY-sketch snapshot, so the next window diffs
//     against empty and transmits its OWN per-window state as a delta (no
//     cross-window subtraction). See delta-baseline-contract.md §3.
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
	// emptyBaseDeltaSketch (DDSketch / CMS / CountSketch / HLL), reset
	// the cached base to the EMPTY-sketch snapshot after this
	// window-close emit, so the next window diffs against empty and
	// transmits its own per-window state as a delta — no cross-window
	// subtraction.
	//
	// A family may opt in only conditionally (e.g. CMS in proto mode but
	// not msgpack mode, which cannot carry deltas). DeltaAgainstEmptyBase
	// then returns a nil/empty base to signal "not this call": we fall
	// through to the legacy always-refresh below for that case.
	if eb, ok := current.(emptyBaseDeltaSketch); ok {
		emptyBase, ebErr := eb.DeltaAgainstEmptyBase()
		if ebErr != nil {
			return nil, false, fmt.Errorf("empty base: %w", ebErr)
		}
		if len(emptyBase) > 0 {
			c.CacheOutbound(seriesKey, emptyBase)
			return payload, isFull, nil
		}
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

// RetainKeys prunes the cache down to ONLY the keys present in retain,
// deleting every outbound/inbound entry whose key is not in the set.
//
// Without this the outbound/inbound maps only ever grow: with delta
// transmission every series key ever observed retains a snapshot copy
// forever, even after the series vanishes, pinning agent memory for the
// process lifetime (MaxSeries bounds only the live window map, not the
// cache). finishRotate calls this after each window-close emit with the
// just-closed window's key set, so a series that did not reappear this
// window has its cached snapshots evicted.
//
// A nil/empty retain set is treated as "keep nothing" and clears both
// maps — callers that mean "no series this window" want the cache empty.
func (c *SnapshotCache) RetainKeys(retain map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.outbound {
		if _, keep := retain[k]; !keep {
			delete(c.outbound, k)
		}
	}
	for k := range c.inbound {
		if _, keep := retain[k]; !keep {
			delete(c.inbound, k)
		}
	}
}

// Delete removes a single series key's cached outbound and inbound
// snapshots. Used when a series is explicitly evicted; a no-op when the
// key is absent.
func (c *SnapshotCache) Delete(seriesKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.outbound, seriesKey)
	delete(c.inbound, seriesKey)
	delete(c.seen, seriesKey)
}

// Reset clears all cached state (used in tests and on shutdown).
func (c *SnapshotCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outbound = make(map[string][]byte)
	c.inbound = make(map[string][]byte)
	c.seen = make(map[string]uint64)
	c.generation++
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

// ComputeSubWindowDelta diffs the current (still-accumulating) sketch against
// the immediately-preceding emit for seriesKey and refreshes the cached base to
// the just-emitted full state — the always-refresh path applied WITHIN a window
// between sub-window emits. Unlike the boundary ComputeDelta it NEVER applies
// the per-window empty-base reset (that reset is the boundary action; doing it
// mid-window would re-ship the whole window-to-date and defeat the savings) and
// does NOT rotate/reset the sketch. The first emit of a window (no prior base)
// is full; thereafter sub-window emits are sparse deltas against the previous
// emit.
func (c *SnapshotCache) ComputeSubWindowDelta(
	seriesKey string,
	current Sketch,
	threshold uint64,
) (payload []byte, isFull bool, err error) {
	if current == nil {
		return nil, false, fmt.Errorf("compute sub-window delta: nil sketch")
	}
	c.mu.RLock()
	prev := c.outbound[seriesKey]
	c.mu.RUnlock()
	if prev == nil {
		full, snapErr := current.Snapshot()
		if snapErr != nil {
			return nil, false, fmt.Errorf("snapshot: %w", snapErr)
		}
		c.CacheOutbound(seriesKey, full)
		return full, true, nil
	}
	delta, full, dErr := current.ComputeDeltaAgainst(prev, threshold)
	if dErr != nil {
		return nil, false, fmt.Errorf("compute delta: %w", dErr)
	}
	// Always-refresh: cache the current full state as the base for the NEXT
	// sub-window emit so it diffs against THIS emit (incremental), not empty.
	if full {
		c.CacheOutbound(seriesKey, delta)
	} else {
		fullSnap, snapErr := current.Snapshot()
		if snapErr != nil {
			return nil, false, fmt.Errorf("snapshot: %w", snapErr)
		}
		c.CacheOutbound(seriesKey, fullSnap)
	}
	return delta, full, nil
}
