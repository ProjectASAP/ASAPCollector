// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"errors"
	"fmt"

	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// CountSketchWrapper adapts a sketchlib-go *countsketch.CountSketch to
// the host-neutral precompute.Sketch + precompute.FrequencySketch
// interfaces.
//
// The wrapper preserves the exact wire format the legacy CountSketch
// processor emitted: sketchlib-go's SerializeProtoBytes on a state
// proto wrapped in a SketchEnvelope (legacy serializeCountSketch ==
// SerializePortable + proto.Marshal == SerializeProtoBytes). That's
// what makes the parity-harness byte-equality invariant honest:
// runtime and legacy paths both call the same serializer on the same
// sketch state.
type CountSketchWrapper struct {
	cs   *countsketch.CountSketch
	rows int
	cols int
}

// NewCountSketchWrapper constructs a fresh CountSketch with the given
// (rows, cols) — derived from epsilon/delta the same way the legacy
// processor's newConfiguredCountSketch did. Returns an error if
// sketchlib's constructor rejects the dimensions.
func NewCountSketchWrapper(rows, cols int) (*CountSketchWrapper, error) {
	cs, err := countsketch.NewCountSketch(rows, cols)
	if err != nil {
		return nil, fmt.Errorf("sketches: NewCountSketch(%d, %d): %w", rows, cols, err)
	}
	return &CountSketchWrapper{cs: cs, rows: rows, cols: cols}, nil
}

// UpdateString mirrors the legacy ws.cs.UpdateString(itemKey, value)
// call. Adapters that route a key/count pair (rather than an
// ObservationValue) call this directly.
func (w *CountSketchWrapper) UpdateString(key string, count float64) {
	w.cs.UpdateString(key, count)
}

// Snapshot returns the canonical proto-encoded SketchEnvelope bytes,
// byte-identical to the legacy processor's serializeCountSketch
// output (SerializePortable + proto.Marshal).
func (w *CountSketchWrapper) Snapshot() ([]byte, error) {
	if w.cs == nil {
		return nil, nil
	}
	return w.cs.SerializeProtoBytes()
}

// ComputeDeltaAgainst mirrors the legacy delta-encoding path:
// deserialize the previous snapshot, compute a delta against the
// current sketch, return SerializeDelta bytes. On any decode/compute
// failure (e.g. no previous snapshot), fall back to a full snapshot
// with isFull=true so the runtime emits a PROTO_FULL frame.
func (w *CountSketchWrapper) ComputeDeltaAgainst(prev []byte, threshold uint64) ([]byte, bool, error) {
	if w.cs == nil {
		return nil, true, nil
	}
	if len(prev) == 0 {
		full, err := w.Snapshot()
		return full, true, err
	}
	prevCS, err := countsketch.DeserializeCountSketchFromProtoBytes(prev)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	deltaMsg, err := countsketch.ComputeDelta(prevCS, w.cs, float64(threshold))
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	payload, err := countsketch.SerializeDelta(deltaMsg)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return payload, false, nil
}

// ApplyDelta merges a sparse delta payload into this sketch in place.
// Used by Precompute.ObserveEnvelope when an upstream agent forwards
// a PROTO_DELTA-encoded CountSketchDataPoint.
func (w *CountSketchWrapper) ApplyDelta(delta []byte) error {
	if len(delta) == 0 {
		return errors.New("CountSketchWrapper: ApplyDelta with empty payload")
	}
	if w.cs == nil {
		cs, err := countsketch.NewCountSketch(w.rows, w.cols)
		if err != nil {
			return err
		}
		w.cs = cs
	}
	deltaMsg, err := countsketch.DeserializeDelta(delta)
	if err != nil {
		return fmt.Errorf("CountSketchWrapper: DeserializeDelta: %w", err)
	}
	countsketch.ApplyDelta(w.cs, deltaMsg)
	return nil
}

// Merge folds another CountSketch into this one. The runtime calls
// this when an envelope-valued observation arrives encoded as
// PROTO_FULL (the snapshot bytes are first decoded via
// DeserializeCountSketchFromProtoBytes by the runtime, then this
// wrapper's Merge is called).
func (w *CountSketchWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*CountSketchWrapper)
	if !ok {
		return fmt.Errorf("CountSketchWrapper: Merge with %T", other)
	}
	if o.cs == nil {
		return nil
	}
	if w.cs == nil {
		cs, err := countsketch.NewCountSketch(w.rows, w.cols)
		if err != nil {
			return err
		}
		w.cs = cs
	}
	return w.cs.Merge(o.cs)
}

// Reset zeros the sketch in place, preserving (rows, cols). Mirrors
// the legacy windowSketchPool path's `ws.cs.Reset()` call.
func (w *CountSketchWrapper) Reset() {
	if w.cs != nil {
		w.cs.Reset()
	}
}

// EstimateCount implements precompute.FrequencySketch. The key is
// the opaque byte slice the sketch indexes by (the same shape passed
// to ObservationValue.Bytes); CountSketch's median-of-rows estimator
// returns a non-negative integer count which we surface as float64
// per the host-neutral contract.
func (w *CountSketchWrapper) EstimateCount(key []byte) float64 {
	if w.cs == nil || len(key) == 0 {
		return 0
	}
	return float64(w.cs.EstimateStringCount(string(key)))
}

// TopK implements precompute.FrequencySketch. Returns up to k entries
// from the sketch's internal TopK heap, sorted descending by Count.
// sketchlib's heap is min-rooted so the wrapper sort-descends after
// copying; insertion sort is fine because k is bounded by sketchlib's
// TOPK_SIZE (small constant).
func (w *CountSketchWrapper) TopK(k int) []precompute.FrequencyEntry {
	if k <= 0 || w.cs == nil || w.cs.TopK == nil {
		return nil
	}
	heap := w.cs.TopK.Heap
	if len(heap) == 0 {
		return nil
	}
	out := make([]precompute.FrequencyEntry, 0, len(heap))
	for _, item := range heap {
		out = append(out, precompute.FrequencyEntry{
			Key:   []byte(item.Key),
			Count: float64(item.Count),
		})
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Count > out[j-1].Count; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	if len(out) > k {
		out = out[:k]
	}
	return out
}

// CountSketchObserver routes a KindFloat observation into the wrapped
// CountSketch via UpdateString. Legacy hot path:
// ws.cs.UpdateString(metric.Name(), value). The metric name travels
// through the host-neutral interface as ObservationValue.Bytes (set
// by the adapter's observe path); when absent, the observer falls
// back to DefaultKey.
type CountSketchObserver struct {
	// DefaultKey is used when ObservationValue.Bytes is empty. Adapter
	// code typically sets this to the metric name so the wire format
	// stays compatible with the legacy CountSketch processor.
	DefaultKey string
}

// Observe routes a precompute.ObservationValue into the wrapped
// CountSketch via UpdateString.
func (o CountSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*CountSketchWrapper)
	if !ok {
		return fmt.Errorf("CountSketchObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindFloat {
		return fmt.Errorf("CountSketchObserver: unsupported value kind %s", v.Kind)
	}
	key := o.DefaultKey
	if len(v.Bytes) > 0 {
		key = string(v.Bytes)
	}
	w.UpdateString(key, v.Float)
	return nil
}

// Compile-time assertions that CountSketchWrapper satisfies both the
// base Sketch trait (used by the runtime's window logic) and
// the FrequencySketch query trait (used by adapter code that needs
// typed frequency queries).
var (
	_ precompute.Sketch          = (*CountSketchWrapper)(nil)
	_ precompute.FrequencySketch = (*CountSketchWrapper)(nil)
	_ precompute.SketchObserver  = CountSketchObserver{}
)
