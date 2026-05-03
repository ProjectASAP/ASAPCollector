// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllprocessor

import (
	"fmt"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// hllSketchWrapper adapts a sketchlib-go *hll.HyperLogLog to the
// precompute.Sketch + precompute.CardinalitySketch interface so a
// precompute.Precompute can own it as a generic Sketch. This is the
// production wrapper consumed by the HLL processor shim; the parity
// harness has its own test-only wrapper at
// integration/parity/harness/sketches.go that shares the same shape
// but lives outside the production import graph.
//
// Byte-format invariant: SerializeProtoBytes for full snapshots and
// ComputeRegisterDelta + SerializeRegisterDelta for deltas produce
// the same wire bytes the legacy processor emitted via
// serializeHLLSketch / SerializeRegisterDelta, so wire payloads stay
// byte-identical pre/post-refactor (ADR-0002 §"Behavior preservation").
//
// Encoding selection: the wrapper emits proto bytes by default; the
// msgpack path is selected at the shim's encode layer (see encode.go
// `serializeHLLSketch`) rather than here, because the runtime's
// Sketch.Snapshot signature is a simple `([]byte, error)` and cannot
// carry the encoding tag back out — proto is the canonical format
// for delta computation, and msgpack is a legacy emit-side mode that
// only applies when DeltaTransmission=false.
type hllSketchWrapper struct {
	sk *hll.HyperLogLog
}

// newHLLSketchWrapper builds an empty HLL sketch. The sketchlib-go
// constructor is parameterless (precision is hard-coded to
// hll.HLLPrecision = 14); the shim's Config.Encoding choice is honored
// at the encode layer, not here.
func newHLLSketchWrapper() *hllSketchWrapper {
	return &hllSketchWrapper{sk: hll.NewHyperLogLog()}
}

// updateValue feeds a single observation into the underlying HLL
// sketch via UpdateValue (matching legacy hllprocessor's batch and
// window paths that call bs.sketch.UpdateValue(dp.DoubleValue())).
func (w *hllSketchWrapper) updateValue(v float64) {
	if w.sk != nil {
		w.sk.UpdateValue(v)
	}
}

// Snapshot serializes via SerializeProtoBytes — the canonical wire
// format the backend's modified-OTLP HLL decoder expects (matching
// DeserializeHyperLogLogFromProtoBytes).
func (w *hllSketchWrapper) Snapshot() ([]byte, error) {
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
func (w *hllSketchWrapper) ComputeDeltaAgainst(prev []byte, _ uint64) ([]byte, bool, error) {
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
func (w *hllSketchWrapper) ApplyDelta(payload []byte) error {
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

// Merge folds another hllSketchWrapper into this one. The runtime
// only ever calls Merge between sketches owned by the same Precompute
// (same precision), so the type assertion is safe.
func (w *hllSketchWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*hllSketchWrapper)
	if !ok {
		return fmt.Errorf("hllSketchWrapper: Merge with %T", other)
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
func (w *hllSketchWrapper) Reset() {
	w.sk = hll.NewHyperLogLog()
}

// EstimateCardinality satisfies precompute.CardinalitySketch — the
// adapter layer type-asserts s.(CardinalitySketch) when emitting a
// typed cardinality gauge from an HLL-backed envelope.
func (w *hllSketchWrapper) EstimateCardinality() float64 {
	if w.sk == nil {
		return 0
	}
	return float64(w.sk.Estimate())
}

// estimate returns the integer cardinality estimate. Used by the
// shim's encode path so emitted typed HLLSketch dps advertise the
// same dp.SetCardinality(...) the legacy emit set.
func (w *hllSketchWrapper) estimate() uint64 {
	if w.sk == nil {
		return 0
	}
	return uint64(w.sk.Estimate())
}

// hllSketchObserver implements precompute.SketchObserver for KindFloat
// observations: the legacy processor's accumulateGaugeMetric called
// sk.UpdateValue(double); inbound HLLSketch envelopes (KindEnvelope)
// are routed through Precompute.ObserveEnvelope by the runtime and
// never reach this observer.
type hllSketchObserver struct{}

func (hllSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*hllSketchWrapper)
	if !ok {
		return fmt.Errorf("hllSketchObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.updateValue(v.Float)
		return nil
	default:
		return fmt.Errorf("hllSketchObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that hllSketchWrapper satisfies the trait
// surface ADR-0002 / PR #224 pinned for CardinalitySketch
// implementations.
var (
	_ precompute.Sketch            = (*hllSketchWrapper)(nil)
	_ precompute.CardinalitySketch = (*hllSketchWrapper)(nil)
	_ precompute.SketchObserver    = hllSketchObserver{}
)

// cloneHLL returns a deep copy of h suitable for use as a delta
// snapshot. Retained at the package level (not on the wrapper) so the
// existing delta_transmission_test.go keeps compiling without a
// rewrite — it calls cloneHLL directly on a *hll.HyperLogLog
// reconstructed from the emitted wire bytes.
func cloneHLL(h *hll.HyperLogLog) *hll.HyperLogLog {
	if h == nil {
		return nil
	}
	data, err := h.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := hll.DeserializeHyperLogLogFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
