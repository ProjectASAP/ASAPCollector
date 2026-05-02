// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"errors"
	"fmt"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	"google.golang.org/protobuf/proto"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// ddSketchWrapper adapts a sketchlib-go *ddsketch.DDSketch to the
// precompute.QuantileSketch interface so a precompute.Precompute can
// own it as a generic Sketch. This is the production wrapper consumed
// by the DDSketch processor shim; the parity harness has its own
// test-only wrapper at integration/parity/harness/sketches.go that
// shares the same shape but lives outside the production import graph.
//
// The byte-format invariant: SerializePortable + proto.Marshal here
// produces the same envelope bytes the legacy processor emitted via
// serializeDDSketch, so the wire payload stays byte-identical
// pre/post-refactor (ADR-0002 §"Behavior preservation").
type ddSketchWrapper struct {
	sk    *ddsketch.DDSketch
	alpha float64
}

// newDDSketchWrapper builds an empty DDSketch with the configured
// relative-accuracy alpha. Callers must keep alpha within (0, 1);
// sketchlib-go's NewDDSketch panics otherwise.
func newDDSketchWrapper(alpha float64) *ddSketchWrapper {
	return &ddSketchWrapper{sk: ddsketch.NewDDSketch(alpha), alpha: alpha}
}

// update feeds a single observation into the underlying DDSketch.
// Used by the shim's SketchObserver.
func (w *ddSketchWrapper) update(v float64) { w.sk.Update(v) }

// Snapshot serializes via SerializePortable + proto.Marshal — the
// canonical wire format the backend's modified-OTLP DDSketch decoder
// expects (`asap_sketchlib::SketchEnvelope{DDSketchState}`).
func (w *ddSketchWrapper) Snapshot() ([]byte, error) {
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
func (w *ddSketchWrapper) ComputeDeltaAgainst(prev []byte, threshold uint64) ([]byte, bool, error) {
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
func (w *ddSketchWrapper) ApplyDelta(payload []byte) error {
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

// Merge folds another ddSketchWrapper into this one. The runtime
// only ever calls Merge between sketches owned by the same Precompute
// (same alpha), so the type assertion is safe.
func (w *ddSketchWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*ddSketchWrapper)
	if !ok {
		return fmt.Errorf("ddSketchWrapper: Merge with %T", other)
	}
	if o.sk == nil {
		return nil
	}
	if w.sk == nil {
		w.sk = ddsketch.NewDDSketch(w.alpha)
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch in place by replacing it with a fresh
// DDSketch of the same alpha. Window rotation calls this when the
// runtime decides to recycle entries.
func (w *ddSketchWrapper) Reset() {
	w.sk = ddsketch.NewDDSketch(w.alpha)
}

// Quantile returns the q-th rank value as a float64; (0, false) from
// the underlying sketch (empty / out-of-range) collapses to NaN per
// the QuantileSketch contract.
func (w *ddSketchWrapper) Quantile(q float64) float64 {
	if w.sk == nil {
		return 0
	}
	v, ok := w.sk.Quantile(q)
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

// ddSketchObserver implements precompute.SketchObserver for KindFloat
// observations: the legacy processor's accumulateGaugeMetric called
// `sk.Update(double)`, and the legacy accumulateDDSketchMetric path
// folded inbound envelopes via ObserveEnvelope (handled directly by
// the runtime, not this observer).
type ddSketchObserver struct{}

func (ddSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*ddSketchWrapper)
	if !ok {
		return fmt.Errorf("ddSketchObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.update(v.Float)
		return nil
	default:
		return fmt.Errorf("ddSketchObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that ddSketchWrapper satisfies the trait
// surface ADR-0002 / PR #224 pinned for QuantileSketch implementations.
var (
	_ precompute.Sketch         = (*ddSketchWrapper)(nil)
	_ precompute.QuantileSketch = (*ddSketchWrapper)(nil)
	_ precompute.SketchObserver = ddSketchObserver{}
)
