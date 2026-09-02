// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"encoding/binary"
	"fmt"
	"math"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// SumWrapper is the scalar Sum aggregate as a first-class precompute
// family. It is NOT a sketch — the envelope's AggregationKind is Sum — but
// it implements the same precompute.Sketch surface so a precompute.Precompute
// can own it generically, exactly like the sketch wrappers. Its state is a
// running {sum, count} that is additively mergeable (Merge folds partials,
// which is what lets cross-shard / cross-host Sum merge at flush/query time).
//
// Wire format: Sum is an aggregation, NOT a sketch, so it deliberately does
// NOT ride the sketchlib sketch-envelope proto (keeping the Sum aggregate out
// of the public sketch proto entirely). Snapshot emits a small self-contained
// payload — float64 sum (little-endian) followed by uint64 count
// (little-endian), 16 bytes — carried in the modified-OTLP SumAgg metric's
// bytes field. The backend decodes the same fixed layout (no proto, no
// sketchlib dependency).
//
// Delta: Sum is additively mergeable. ComputeDeltaAgainst computes the
// INCREMENTAL {Δsum,Δcount} = current−prev by default; when GOS insert-time
// mode is active (SetGosMode), it instead drains the pending GOS delta (see
// below) and ignores prev entirely.
type SumWrapper struct {
	sum   float64
	count uint64

	// gosEpsilon/gosSites configure the GOS isotropic insert-time delta gate
	// (design-gos-unified-edge-telemetry.md §11, derivations §8.1): when
	// gosEpsilon>0, Update checks the accumulated-since-last-crossing
	// magnitude against the closed-form threshold T=ε·|sum|/k immediately,
	// in place of the periodic decode-prev-diff sub-window model
	// (subWindowDivergence in precompute.go). gosEpsilon<=0 (the default)
	// leaves Update/ComputeDeltaAgainst/Snapshot on the pre-existing
	// diff-two-totals path, byte-identical to before GOS existed. Set via
	// SetGosMode.
	gosEpsilon float64
	gosSites   uint32

	// gosSinceCrossSum/gosSinceCrossCount track the amount added since the
	// last insert-time threshold crossing — the degenerate single-scalar
	// analogue of CountSketch's per-cell "since last touch" accumulator.
	// Reset to 0 ("subtract the reported amount") the instant a crossing is
	// captured, per design-gos-unified-edge-telemetry.md §11's per-family
	// reset table ("Sum | scalar | zero it (subtract reported amount) |
	// degenerate 1-cell case"), so the next threshold check starts fresh
	// against the (now larger) current total.
	gosSinceCrossSum   float64
	gosSinceCrossCount uint64

	// gosReadySum/gosReadyCount stage already-crossed (already-reset)
	// amounts awaiting the next drain (ComputeDeltaAgainst at flush time). A
	// burst of multiple crossings between two flushes folds additively into
	// these two scalars — Sum is associative, so summing several captured
	// crossings is exactly equivalent to one crossing of their total,
	// unlike CountSketch's per-cell gosDirty list.
	gosReadySum   float64
	gosReadyCount uint64

	// gosWake is armed the instant gosReady{Sum,Count} transitions from
	// "nothing captured" to "something captured" since the last drain, and
	// consumed exactly once by ConsumeWakeSignal — a burst of many
	// crossings between two flushes wakes the out-of-cycle flush loop once,
	// not once per crossing.
	gosWake bool
}

// NewSumWrapper builds an empty Sum aggregate.
func NewSumWrapper() *SumWrapper { return &SumWrapper{} }

// SetGosMode configures the GOS isotropic insert-time delta gate. epsilon<=0
// disables it (existing diff-two-totals path, unchanged behavior).
// Idempotent — callers (the Sum factory, at series creation, and the
// runtime's applyGosMode, at flush, on every already-live series) may call
// this repeatedly with the same config; it just re-stamps the two scalars.
func (w *SumWrapper) SetGosMode(epsilon float64, sites uint32) {
	w.gosEpsilon = epsilon
	w.gosSites = sites
}

// Update folds one observation into the running sum, and — when GOS mode is
// active — checks the accumulated-since-last-crossing magnitude against the
// closed-form threshold T=ε·|sum|/k (SumIsotropicThreshold), recomputed from
// the CURRENT total on every insert. Crossing captures the accumulated
// amount into the pending-drain accumulators, resets the since-crossing
// accumulator in place, and arms the wake signal.
func (w *SumWrapper) Update(v float64) {
	w.sum += v
	w.count++
	if w.gosEpsilon <= 0 {
		return
	}
	w.gosSinceCrossSum += v
	w.gosSinceCrossCount++
	threshold := SumIsotropicThreshold(w.gosEpsilon, math.Abs(w.sum), w.gosSites)
	if math.Abs(w.gosSinceCrossSum) < threshold {
		return
	}
	if w.gosReadyCount == 0 {
		w.gosWake = true // armed on the first capture since the last drain
	}
	w.gosReadySum += w.gosSinceCrossSum
	w.gosReadyCount += w.gosSinceCrossCount
	w.gosSinceCrossSum = 0
	w.gosSinceCrossCount = 0
}

// ApplyAdmittedOccurrence applies a d=1 source-SDK admission decision. The
// SDK sends only admitted occurrences, so bit zero must be present. Scaling
// the summand by 1/p gives the Horvitz--Thompson Sum estimator; the observation
// count remains the raw admitted count and is not an estimated event count.
func (w *SumWrapper) ApplyAdmittedOccurrence(v float64, admittedRows uint64, sampleP float64) error {
	if admittedRows != 1 {
		return fmt.Errorf("SumWrapper: admitted_rows must be 1 for a one-row aggregate, got %#x", admittedRows)
	}
	if sampleP <= 0 || sampleP > 1 || math.IsNaN(sampleP) {
		return fmt.Errorf("SumWrapper: sample_p must be in (0,1], got %v", sampleP)
	}
	w.Update(v / sampleP)
	return nil
}

// ConsumeWakeSignal implements the runtime's narrow wake-signal interface
// (asap-precompute-go window.go's recordLocked): reports whether an
// insert-time GOS threshold crossing happened since the last call, clearing
// the flag. Always false when GOS mode is inactive.
func (w *SumWrapper) ConsumeWakeSignal() bool {
	if !w.gosWake {
		return false
	}
	w.gosWake = false
	return true
}

// drainGosDelta serializes whatever has been captured (crossed + reset)
// since the last drain as the {Δsum,Δcount} wire payload — the insert-time
// counterpart of the old decode-prev-diff path: the amount was already
// accumulated cell-by-cell (insert-by-insert) at Update time, so no previous
// snapshot needs decoding here. Returns (nil, false, nil) when nothing has
// crossed since the last drain — the caller (precompute.SnapshotCache's
// compute-delta paths) treats a nil payload as "nothing to emit".
func (w *SumWrapper) drainGosDelta() ([]byte, bool, error) {
	if w.gosReadyCount == 0 {
		return nil, false, nil
	}
	b := make([]byte, sumPayloadLen)
	binary.LittleEndian.PutUint64(b[0:8], math.Float64bits(w.gosReadySum))
	binary.LittleEndian.PutUint64(b[8:16], w.gosReadyCount)
	w.gosReadySum = 0
	w.gosReadyCount = 0
	return b, false, nil
}

// sumPayloadLen is the fixed Sum payload size: float64 sum || uint64 count.
const sumPayloadLen = 16

// Snapshot emits the fixed 16-byte {sum,count} payload (little-endian). An
// empty window (count == 0) emits nothing (nil), matching the sketch
// wrappers' empty-window behavior.
//
// GOS mode subtlety: when GOS is active, Snapshot excludes whatever has
// already been captured (crossed + reset) and is staged in
// gosReadySum/gosReadyCount awaiting its own drain via ComputeDeltaAgainst.
// Without this exclusion, the ONE call site that can still invoke Snapshot
// under GOS — SnapshotCache's prev==nil "first emit ever for this series"
// fallback (both ComputeDelta and ComputeSubWindowDelta take this branch
// instead of calling ComputeDeltaAgainst when there is no cached base yet) —
// would report the captured amount, and a later drain of gosReadySum would
// report that SAME amount again: additive reconstruction (the backend sums
// every fragment it receives, design-gos-unified-edge-telemetry.md §11)
// would then double-count it. Excluding gosReadySum here makes the full
// snapshot and the later drained delta disjoint, non-overlapping fragments
// that sum to the true total, matching CountSketch's equivalent property
// (its full snapshot reads the actual matrix cells, which are already
// zeroed in place at crossing time, so it never needs a subtraction here).
// A no-op (gosReadySum/gosReadyCount are always 0) when GOS is inactive, so
// the wire bytes are byte-identical to before GOS existed.
func (w *SumWrapper) Snapshot() ([]byte, error) {
	if w.count == 0 {
		return nil, nil
	}
	sum, count := w.sum, w.count
	if w.gosEpsilon > 0 {
		sum -= w.gosReadySum
		count -= w.gosReadyCount
	}
	b := make([]byte, sumPayloadLen)
	binary.LittleEndian.PutUint64(b[0:8], math.Float64bits(sum))
	binary.LittleEndian.PutUint64(b[8:16], count)
	return b, nil
}

// ComputeDeltaAgainst returns the INCREMENTAL delta {Δsum, Δcount} = current −
// prev. The backend's ApplyDelta is additive, so a sequence of these (against
// the per-window base reset to empty at each boundary, see DeltaAgainstEmptyBase)
// reconstructs the window total — which is what makes Sum sub-window-capable
// (multiple emits per window accumulate instead of over-counting full state).
// prev is always non-nil here (the SnapshotCache handles the first-emit-full
// case via Snapshot).
//
// GOS mode: cells (here, the single scalar accumulator) were already
// detected + reset at insert time (Update), so the delta is just draining
// the pending gosReadySum/gosReadyCount — prev is never consulted (nothing
// to decode: the mechanism doesn't need a "previous total" reference at
// all). Mirrors CountSketchWrapper.ComputeDeltaAgainst's GOS branch.
func (w *SumWrapper) ComputeDeltaAgainst(prev []byte, _ uint64) ([]byte, bool, error) {
	if w.gosEpsilon > 0 {
		return w.drainGosDelta()
	}
	var prevSum float64
	var prevCount uint64
	if len(prev) >= sumPayloadLen {
		prevSum = math.Float64frombits(binary.LittleEndian.Uint64(prev[0:8]))
		prevCount = binary.LittleEndian.Uint64(prev[8:16])
	}
	dSum := w.sum - prevSum
	dCount := w.count - prevCount
	if dSum == 0 && dCount == 0 {
		return nil, false, nil // no change since the previous emit
	}
	b := make([]byte, sumPayloadLen)
	binary.LittleEndian.PutUint64(b[0:8], math.Float64bits(dSum))
	binary.LittleEndian.PutUint64(b[8:16], dCount)
	return b, false, nil
}

// DeltaAgainstEmptyBase opts Sum into the per-window-reset (PWR) delta model used
// by the other delta families: after each window-close emit the cached base is
// reset to the EMPTY {0,0} sum, so the next window's emits are deltas from zero
// (its own per-window total). Returns an explicit 16-byte zero payload (len>0) so
// the SnapshotCache takes the reset branch; without it, window N+1's first delta
// would be (currentₙ₊₁ − fullₙ), a bogus cross-window subtraction.
func (w *SumWrapper) DeltaAgainstEmptyBase() ([]byte, error) {
	return make([]byte, sumPayloadLen), nil
}

// ApplyDelta loads a 16-byte {sum,count} payload and folds it into this
// aggregate (additive). The runtime's mergeFullEnvelope path builds a temp
// sketch and calls ApplyDelta before Merge-ing; for Sum "apply" == add.
func (w *SumWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if len(payload) < sumPayloadLen {
		return fmt.Errorf("sum.ApplyDelta: payload too short (%d bytes, want %d)", len(payload), sumPayloadLen)
	}
	w.sum += math.Float64frombits(binary.LittleEndian.Uint64(payload[0:8]))
	w.count += binary.LittleEndian.Uint64(payload[8:16])
	return nil
}

// Merge folds another SumWrapper into this one (associative add).
func (w *SumWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*SumWrapper)
	if !ok {
		return fmt.Errorf("SumWrapper: Merge with %T", other)
	}
	w.sum += o.sum
	w.count += o.count
	return nil
}

// Reset zeros the aggregate in place, including any pending GOS accumulators
// (mirrors CountSketchWrapper.Reset). Does NOT clear gosEpsilon/gosSites —
// mode config persists across Reset, orthogonal to per-window accumulation,
// and is re-stamped by applyGosMode at every flush regardless.
func (w *SumWrapper) Reset() {
	w.sum = 0
	w.count = 0
	w.gosSinceCrossSum = 0
	w.gosSinceCrossCount = 0
	w.gosReadySum = 0
	w.gosReadyCount = 0
	w.gosWake = false
}

// Sum returns the accumulated sum (used by the otel adapter encode path to
// stamp the emitted Sum data point's value).
func (w *SumWrapper) Sum() float64 { return w.sum }

// Count returns the accumulated observation count.
func (w *SumWrapper) Count() uint64 { return w.count }

// SumObserver implements precompute.SketchObserver: a KindFloat observation
// is folded via Update (the numeric value is the summand). Inbound SumState
// envelopes (KindEnvelope) are routed through Precompute.ObserveEnvelope by
// the runtime and never reach this observer.
type SumObserver struct{}

// Observe folds a precompute.ObservationValue into the wrapped Sum.
func (SumObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*SumWrapper)
	if !ok {
		return fmt.Errorf("SumObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		if v.RowSampled {
			return w.ApplyAdmittedOccurrence(v.Float, v.AdmittedRows, v.SampleP)
		}
		w.Update(v.Float)
		return nil
	default:
		return fmt.Errorf("SumObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that SumWrapper satisfies the Sketch surface.
var (
	_ precompute.Sketch         = (*SumWrapper)(nil)
	_ precompute.SketchObserver = SumObserver{}
)
