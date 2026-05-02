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

// ComputeDelta diffs current sketch state against the cached outbound
// snapshot for that seriesKey. Returns (payload, isFull, err) where
// isFull means the runtime should emit PROTO_FULL (either no prior
// snapshot existed, or the delta exceeded threshold).
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
	if prev == nil {
		// First time — emit full and cache.
		full, err := current.Snapshot()
		if err != nil {
			return nil, false, fmt.Errorf("snapshot: %w", err)
		}
		c.CacheOutbound(seriesKey, full)
		return full, true, nil
	}
	delta, isFull, err := current.ComputeDeltaAgainst(prev, threshold)
	if err != nil {
		return nil, false, fmt.Errorf("compute delta: %w", err)
	}
	if isFull {
		// Above threshold; refresh the cached outbound to the new
		// full snapshot so the next delta is computed against it.
		full, err := current.Snapshot()
		if err != nil {
			return nil, false, fmt.Errorf("snapshot: %w", err)
		}
		c.CacheOutbound(seriesKey, full)
		return full, true, nil
	}
	// Below threshold; the cached outbound stays at `prev` —
	// the next delta computes against the same baseline so all
	// recipients can apply against it. (Mirrors today's
	// ddsketch processor behavior.)
	return delta, false, nil
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
