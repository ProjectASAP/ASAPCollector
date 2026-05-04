// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"fmt"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// HLLWrapper adapts a sketchlib-go *hll.HyperLogLog to the
// precompute.Sketch + precompute.CardinalitySketch interfaces so a
// precompute.Precompute can own it as a generic Sketch.
//
// Byte-format invariant: SerializeProtoBytes for full snapshots and
// ComputeRegisterDelta + SerializeRegisterDelta for deltas produce
// the same wire bytes the legacy processor emitted via
// serializeHLLSketch / SerializeRegisterDelta, so wire payloads stay
// byte-identical pre/post-refactor (ADR-0002 §"Behavior preservation").
//
// Encoding selection: the wrapper emits proto bytes by default; the
// msgpack path is selected at the adapter's encode layer rather than
// here, because the runtime's Sketch.Snapshot signature is a simple
// `([]byte, error)` and cannot carry the encoding tag back out — proto
// is the canonical format for delta computation, and msgpack is a
// legacy emit-side mode that only applies when DeltaTransmission=false.
type HLLWrapper struct {
	sk *hll.HyperLogLog
}

// NewHLLWrapper builds an empty HLL sketch. The sketchlib-go
// constructor is parameterless (precision is hard-coded to
// hll.HLLPrecision = 14); the adapter's encoding choice is honored
// at the encode layer, not here.
func NewHLLWrapper() *HLLWrapper {
	return &HLLWrapper{sk: hll.NewHyperLogLog()}
}

// UpdateValue feeds a single observation into the underlying HLL
// sketch via UpdateValue (matching legacy hllprocessor's batch and
// window paths that call bs.sketch.UpdateValue(dp.DoubleValue())).
func (w *HLLWrapper) UpdateValue(v float64) {
	if w.sk != nil {
		w.sk.UpdateValue(v)
	}
}

// Snapshot serializes via SerializeProtoBytes — the canonical wire
// format the backend's modified-OTLP HLL decoder expects (matching
// DeserializeHyperLogLogFromProtoBytes).
func (w *HLLWrapper) Snapshot() ([]byte, error) {
	if w.sk == nil {
		return nil, nil
	}
	return w.sk.SerializeProtoBytes()
}

// ComputeDeltaAgainst computes a sparse RegisterDelta against the
// previous snapshot bytes. When prev is empty (first window), returns
// the full snapshot with isFull=true. The threshold parameter is
// unused for HLL — register deltas are always sparse and never larger
// than the full state, so the threshold short-circuit isn't relevant.
func (w *HLLWrapper) ComputeDeltaAgainst(prev []byte, _ uint64) ([]byte, bool, error) {
	if w.sk == nil {
		return nil, true, nil
	}
	if len(prev) == 0 {
		full, err := w.Snapshot()
		return full, true, err
	}
	prevSk, err := hll.DeserializeHyperLogLogFromProtoBytes(prev)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	deltaMsg := hll.ComputeRegisterDelta(prevSk, w.sk)
	payload, err := hll.SerializeRegisterDelta(deltaMsg)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return payload, false, nil
}

// ApplyDelta merges a payload into the underlying HLL. Dispatches on
// payload shape: a RegisterDelta (the sketchlib-go delta wire format)
// is applied via ApplyRegisterDelta; otherwise the payload is treated
// as a full proto-encoded HyperLogLog and merged in directly. The
// runtime's mergeFullEnvelope helper calls ApplyDelta on a fresh
// sketch as its "merge from empty" path, so accepting both shapes
// keeps the inbound full / delta envelope handling uniform.
func (w *HLLWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if w.sk == nil {
		w.sk = hll.NewHyperLogLog()
	}
	// Try delta first: RegisterDelta is the more constrained shape;
	// proto-encoded HyperLogLogState envelopes won't decode as a
	// RegisterDelta so the fallback path catches them cleanly.
	if deltaMsg, err := hll.DeserializeRegisterDelta(payload); err == nil && deltaMsg != nil {
		hll.ApplyRegisterDelta(w.sk, deltaMsg)
		return nil
	}
	other, err := hll.DeserializeHyperLogLogFromProtoBytes(payload)
	if err != nil {
		return fmt.Errorf("hll.DeserializeHyperLogLogFromProtoBytes: %w", err)
	}
	return w.sk.Merge(other)
}

// Merge folds another HLLWrapper into this one. The runtime only
// ever calls Merge between sketches owned by the same Precompute
// (same precision), so the type assertion is safe.
func (w *HLLWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*HLLWrapper)
	if !ok {
		return fmt.Errorf("HLLWrapper: Merge with %T", other)
	}
	if o.sk == nil {
		return nil
	}
	if w.sk == nil {
		w.sk = hll.NewHyperLogLog()
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch in place by replacing it with a fresh HLL.
// Window rotation calls this when the runtime decides to recycle
// entries.
func (w *HLLWrapper) Reset() {
	w.sk = hll.NewHyperLogLog()
}

// EstimateCardinality satisfies precompute.CardinalitySketch — adapter
// code type-asserts s.(CardinalitySketch) when emitting a typed
// cardinality gauge from an HLL-backed envelope.
func (w *HLLWrapper) EstimateCardinality() float64 {
	if w.sk == nil {
		return 0
	}
	return float64(w.sk.Estimate())
}

// Estimate returns the integer cardinality estimate. Used by adapter
// encode paths so emitted typed HLLSketch dps advertise the same
// dp.SetCardinality(...) the legacy emit set.
func (w *HLLWrapper) Estimate() uint64 {
	if w.sk == nil {
		return 0
	}
	return uint64(w.sk.Estimate())
}

// HLLObserver implements precompute.SketchObserver for KindFloat
// observations: the legacy processor's accumulateGaugeMetric called
// sk.UpdateValue(double); inbound HLLSketch envelopes (KindEnvelope)
// are routed through Precompute.ObserveEnvelope by the runtime and
// never reach this observer.
type HLLObserver struct{}

// Observe routes a precompute.ObservationValue into the wrapped HLL
// sketch via UpdateValue.
func (HLLObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*HLLWrapper)
	if !ok {
		return fmt.Errorf("HLLObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.UpdateValue(v.Float)
		return nil
	default:
		return fmt.Errorf("HLLObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that HLLWrapper satisfies the trait surface
// for CardinalitySketch implementations.
var (
	_ precompute.Sketch            = (*HLLWrapper)(nil)
	_ precompute.CardinalitySketch = (*HLLWrapper)(nil)
	_ precompute.SketchObserver    = HLLObserver{}
)
