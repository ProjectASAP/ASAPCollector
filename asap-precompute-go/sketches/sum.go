// Copyright The OpenTelemetry Authors
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
// Delta: Sum is additively mergeable, but this wrapper is FULL-ONLY for now
// (like KLLWrapper) — ComputeDeltaAgainst returns the full snapshot. The
// 16-byte payload makes a delta pointless. True per-window delta is a
// documented follow-up.
type SumWrapper struct {
	sum   float64
	count uint64
}

// NewSumWrapper builds an empty Sum aggregate.
func NewSumWrapper() *SumWrapper { return &SumWrapper{} }

// Update folds one observation into the running sum.
func (w *SumWrapper) Update(v float64) {
	w.sum += v
	w.count++
}

// sumPayloadLen is the fixed Sum payload size: float64 sum || uint64 count.
const sumPayloadLen = 16

// Snapshot emits the fixed 16-byte {sum,count} payload (little-endian). An
// empty window (count == 0) emits nothing (nil), matching the sketch
// wrappers' empty-window behavior.
func (w *SumWrapper) Snapshot() ([]byte, error) {
	if w.count == 0 {
		return nil, nil
	}
	b := make([]byte, sumPayloadLen)
	binary.LittleEndian.PutUint64(b[0:8], math.Float64bits(w.sum))
	binary.LittleEndian.PutUint64(b[8:16], w.count)
	return b, nil
}

// ComputeDeltaAgainst returns the full snapshot (Sum is full-only for now;
// see the type doc). isFull = true so the runtime tags the frame PROTO_FULL.
func (w *SumWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
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

// Reset zeros the aggregate in place.
func (w *SumWrapper) Reset() {
	w.sum = 0
	w.count = 0
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
