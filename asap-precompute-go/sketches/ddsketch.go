// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package sketches provides production-grade wrappers that adapt the
// concrete sketch types in github.com/ProjectASAP/sketchlib-go to the
// host-neutral precompute.Sketch interface family
// (Sketch + QuantileSketch / CardinalitySketch / FrequencySketch).
//
// Every adapter — OTel processors, Telegraf, Vector, OTAP — that wires
// a precompute.Precompute to a real sketch consumes these wrappers
// instead of carrying its own copy. The wrappers are platform-
// independent (no pdata / Telegraf / Vector / Arrow imports) and live
// alongside the runtime they're built for.
//
// Byte-format invariant: the wrappers' Snapshot / ComputeDeltaAgainst
// outputs are byte-identical to what the legacy OTel processors emitted
// pre-shim — the parity harness in integration/parity exercises this.
package sketches

import (
	"errors"
	"fmt"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	"google.golang.org/protobuf/proto"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// DDSketchWrapper adapts a sketchlib-go *ddsketch.DDSketch to the
// precompute.QuantileSketch interface so a precompute.Precompute can
// own it as a generic Sketch.
//
// The byte-format invariant: SerializePortable + proto.Marshal produces
// the same envelope bytes the legacy DDSketch processor emitted via
// serializeDDSketch, so the wire payload stays byte-identical
// pre/post-refactor (ADR-0002 §"Behavior preservation").
type DDSketchWrapper struct {
	sk    *ddsketch.DDSketch
	alpha float64
}

// NewDDSketchWrapper builds an empty DDSketch with the configured
// relative-accuracy alpha. Callers must keep alpha within (0, 1);
// sketchlib-go's NewDDSketch panics otherwise.
func NewDDSketchWrapper(alpha float64) *DDSketchWrapper {
	return &DDSketchWrapper{sk: ddsketch.NewDDSketch(alpha), alpha: alpha}
}

// Update feeds a single observation into the underlying DDSketch.
// Used by DDSketchObserver; exposed publicly so adapter code that
// already has a typed handle can bypass the observer interface.
func (w *DDSketchWrapper) Update(v float64) { w.sk.Update(v) }

// Snapshot serializes via SerializePortable + proto.Marshal — the
// canonical wire format the backend's modified-OTLP DDSketch decoder
// expects (`asap_sketchlib::SketchEnvelope{DDSketchState}`).
func (w *DDSketchWrapper) Snapshot() ([]byte, error) {
	if w.sk == nil {
		return nil, nil
	}
	env, err := w.sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("ddsketch.SerializePortable: %w", err)
	}
	return proto.Marshal(env)
}

// ComputeDeltaAgainst mirrors the legacy computeDDSketchDelta: decode
// the prior snapshot envelope, then call sketchlib-go's ComputeDelta.
// On any decode/compute failure, fall back to a full snapshot so the
// emit path always produces a valid payload.
func (w *DDSketchWrapper) ComputeDeltaAgainst(prev []byte, threshold uint64) ([]byte, bool, error) {
	if w.sk == nil {
		return nil, true, nil
	}
	if len(prev) == 0 {
		full, err := w.Snapshot()
		return full, true, err
	}
	prevSk, err := decodeDDSketchEnvelope(prev)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	delta, err := ddsketch.ComputeDelta(prevSk, w.sk, threshold)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return delta, false, nil
}

// ApplyDelta merges a payload into the underlying DDSketch. The
// runtime invokes this for both delta-encoded inbound envelopes and
// full-state envelopes (the runtime's mergeFullEnvelope helper calls
// ApplyDelta on a fresh sketch as its "merge from empty" path). We
// dispatch on payload shape: an envelope-wrapped DDSketchState (the
// legacy processor's wire format) takes the NewFromState + Merge
// path; otherwise sketchlib-go's ApplyDelta consumes a DDSketchDelta.
func (w *DDSketchWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if w.sk == nil {
		w.sk = ddsketch.NewDDSketch(w.alpha)
	}
	if other, err := decodeDDSketchEnvelope(payload); err == nil && other != nil {
		return w.sk.Merge(other)
	}
	if other, err := ddsketch.NewFromStateProtoBytes(payload); err == nil && other != nil {
		return w.sk.Merge(other)
	}
	return ddsketch.ApplyDelta(w.sk, payload)
}

// Merge folds another DDSketchWrapper into this one. The runtime
// only ever calls Merge between sketches owned by the same Precompute
// (same alpha), so the type assertion is safe.
func (w *DDSketchWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*DDSketchWrapper)
	if !ok {
		return fmt.Errorf("DDSketchWrapper: Merge with %T", other)
	}
	if o.sk == nil {
		return nil
	}
	if w.sk == nil {
		w.sk = ddsketch.NewDDSketch(w.alpha)
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch IN PLACE, preserving the bucket store's
// backing-array capacity so a wrapper recycled through a sketch pool
// across windows does not re-allocate its store. Previously this
// reallocated via NewDDSketch, which defeated pooling.
func (w *DDSketchWrapper) Reset() {
	if w.sk == nil {
		w.sk = ddsketch.NewDDSketch(w.alpha)
		return
	}
	w.sk.Clear()
}

// clampQuantile clamps q to the [0,1] range required by the
// precompute.QuantileSketch contract. NaN (which compares false to both
// bounds) is mapped to 0 so the query is always well-defined rather than
// passing NaN into the underlying sketch's quantile lookup.
func clampQuantile(q float64) float64 {
	if q != q { // NaN
		return 0
	}
	if q < 0 {
		return 0
	}
	if q > 1 {
		return 1
	}
	return q
}

// Quantile returns the q-th rank value as a float64; (0, false) from
// the underlying sketch (empty / out-of-range) collapses to 0 per
// the QuantileSketch contract used by adapter code. q is clamped to
// [0,1] per the contract before querying.
func (w *DDSketchWrapper) Quantile(q float64) float64 {
	if w.sk == nil {
		return 0
	}
	v, ok := w.sk.Quantile(clampQuantile(q))
	if !ok {
		return 0
	}
	return v
}

// decodeDDSketchEnvelope unwraps a SerializePortable envelope into a
// reconstructed *ddsketch.DDSketch.
func decodeDDSketchEnvelope(b []byte) (*ddsketch.DDSketch, error) {
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(b, &env); err != nil {
		return nil, err
	}
	st := env.GetDdsketch()
	if st == nil {
		return nil, errors.New("envelope did not carry DDSketchState")
	}
	return ddsketch.NewFromState(st)
}

// DDSketchObserver implements precompute.SketchObserver for KindFloat
// observations: the legacy processor's accumulateGaugeMetric called
// `sk.Update(double)`, and the legacy accumulateDDSketchMetric path
// folded inbound envelopes via ObserveEnvelope (handled directly by
// the runtime, not this observer).
type DDSketchObserver struct{}

// Observe routes a precompute.ObservationValue into the wrapped
// DDSketch via Update.
func (DDSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*DDSketchWrapper)
	if !ok {
		return fmt.Errorf("DDSketchObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.Update(v.Float)
		return nil
	default:
		return fmt.Errorf("DDSketchObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that DDSketchWrapper satisfies the trait
// surface for QuantileSketch implementations.
var (
	_ precompute.Sketch         = (*DDSketchWrapper)(nil)
	_ precompute.QuantileSketch = (*DDSketchWrapper)(nil)
	_ precompute.SketchObserver = DDSketchObserver{}
)
