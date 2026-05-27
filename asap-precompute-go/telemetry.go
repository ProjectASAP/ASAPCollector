package precompute

import "sync/atomic"

// PrecomputeStats is the host-neutral telemetry surface. Adapters
// read these counters via Adapter.EmitTelemetry() and translate to
// platform-native metrics (OTel scope metric, Telegraf field, etc.).
//
// All fields are atomics so callers (Observe / ObserveEnvelope /
// Tick / window rotate) can update without coordinating with the
// outer Precompute lock. Read paths via Snapshot() take a copy of
// each counter, so adapters never observe a torn read.
type PrecomputeStats struct {
	// InputObservations counts total Observe() calls (all kinds).
	InputObservations atomic.Uint64
	// InputEnvelopes counts total ObserveEnvelope() calls.
	InputEnvelopes atomic.Uint64
	// OutputEnvelopes counts envelopes emitted via Tick.
	OutputEnvelopes atomic.Uint64
	// ActiveSeries is the current size of the per-(agg_id, label_key)
	// map in the active window. Negative values are not expected
	// but tolerated for atomic-decrement safety on series eviction.
	ActiveSeries atomic.Int64
	// DroppedOverflow counts observations dropped due to MaxSeries.
	DroppedOverflow atomic.Uint64
	// DroppedLate counts observations dropped due to AllowedLateness.
	DroppedLate atomic.Uint64
	// DroppedSerialize counts closed-window series that failed to
	// serialize into an emittable envelope at flush time (a Snapshot /
	// ComputeDelta error, or a nil/empty payload). Without this counter
	// the host-neutral runtime — which has no logger — would drop the
	// series silently in finishRotate, making the loss unobservable.
	DroppedSerialize atomic.Uint64
	// LastTickMs is the wall-clock timestamp of the last Tick() call.
	LastTickMs atomic.Uint64
	// LastEmittedEnvelopes is the count returned by the most recent
	// Tick() (snapshot of one-tick output volume).
	LastEmittedEnvelopes atomic.Uint64
}

// NewStats returns a zero-valued stats struct.
func NewStats() *PrecomputeStats { return &PrecomputeStats{} }

// NewPrecomputeStats is an alias for NewStats kept for symmetry with
// the type name; the precompute constructor uses this name.
func NewPrecomputeStats() *PrecomputeStats { return NewStats() }

// StatsSnapshot captures the counters as a flat struct for safe read
// access (no atomic vs non-atomic field aliasing).
type StatsSnapshot struct {
	InputObservations    uint64
	InputEnvelopes       uint64
	OutputEnvelopes      uint64
	ActiveSeries         int64
	DroppedOverflow      uint64
	DroppedLate          uint64
	DroppedSerialize     uint64
	LastTickMs           uint64
	LastEmittedEnvelopes uint64
}

// Snapshot returns a point-in-time copy of the counters. Each field
// is read independently, so values across fields may be drawn from
// slightly different instants — adapters that need an atomic
// multi-counter view must add an explicit lock.
func (s *PrecomputeStats) Snapshot() StatsSnapshot {
	if s == nil {
		return StatsSnapshot{}
	}
	return StatsSnapshot{
		InputObservations:    s.InputObservations.Load(),
		InputEnvelopes:       s.InputEnvelopes.Load(),
		OutputEnvelopes:      s.OutputEnvelopes.Load(),
		ActiveSeries:         s.ActiveSeries.Load(),
		DroppedOverflow:      s.DroppedOverflow.Load(),
		DroppedLate:          s.DroppedLate.Load(),
		DroppedSerialize:     s.DroppedSerialize.Load(),
		LastTickMs:           s.LastTickMs.Load(),
		LastEmittedEnvelopes: s.LastEmittedEnvelopes.Load(),
	}
}
