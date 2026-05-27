// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"fmt"

	"github.com/ProjectASAP/sketchlib-go/common"
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
	// sampleP is the per-sketch hash-threshold sampling probability in
	// (0,1]. 1.0 (the default) disables sampling so the sketch is
	// byte-identical to an unsampled one. Set via WithSampleP; preserved
	// across the re-construction paths (Reset / Merge / ApplyDelta) so a
	// sampled wrapper stays sampled for its whole lifetime.
	sampleP float64
}

// NewHLLWrapper builds an empty HLL sketch. The sketchlib-go
// constructor is parameterless (precision is hard-coded to
// hll.HLLPrecision = 14); the adapter's encoding choice is honored
// at the encode layer, not here.
//
// Sampling is disabled (sampleP=1.0) by default — call WithSampleP to
// enable it. The default keeps the emitted wire bytes byte-identical to
// the pre-sampling format.
func NewHLLWrapper() *HLLWrapper {
	w := &HLLWrapper{sampleP: 1.0}
	w.sk = w.newSketch()
	return w
}

// WithSampleP enables per-sketch hash-threshold element sampling at
// probability p in (0,1]. p>=1 (or NaN) disables sampling (exact, the
// default); p<=0 keeps nothing, which sketchlib-go clamps to disabled.
// Returns the receiver for fluent construction. The probability is
// stamped on the SketchEnvelope by sketchlib-go so the backend rescales
// cardinality by 1/p at query time.
func (w *HLLWrapper) WithSampleP(p float64) *HLLWrapper {
	if p >= 1.0 || p != p { // p != p ⇒ NaN
		w.sampleP = 1.0
	} else {
		w.sampleP = p
	}
	if w.sk != nil {
		w.sk.WithSampleP(w.sampleP)
	}
	return w
}

// SampleP returns the configured sampling probability (1.0 when disabled).
func (w *HLLWrapper) SampleP() float64 {
	if w.sampleP <= 0 {
		return 1.0
	}
	return w.sampleP
}

// newSketch builds a fresh sketchlib-go HLL carrying the wrapper's
// configured sampling probability. Centralises the construction so every
// re-creation path (New / Reset / Merge / ApplyDelta) keeps sampleP.
func (w *HLLWrapper) newSketch() *hll.HyperLogLog {
	sk := hll.NewHyperLogLog()
	if sk != nil && w.sampleP > 0 && w.sampleP < 1.0 {
		sk.WithSampleP(w.sampleP)
	}
	return sk
}

// UpdateValue feeds a single observation into the underlying HLL
// sketch via UpdateValue (matching legacy hllprocessor's batch and
// window paths that call bs.sketch.UpdateValue(dp.DoubleValue())).
func (w *HLLWrapper) UpdateValue(v float64) {
	if w.sk != nil {
		w.sk.UpdateValue(v)
	}
}

// UpdateBytes feeds the canonical hash of an opaque byte key (e.g. an
// item_label attribute VALUE such as a user_id) into the HLL. The hash is
// computed via common.FromBytes — the SAME canonical-seed XXH3 path the CMS /
// CountSketch observers use for their string keys — so the inner-dimension
// cardinality is measured over the label value, not the numeric sample. Used
// by the fused asap_edge item_label path so unique_users_per_min counts
// DISTINCT user_ids per group instead of degenerating to one cardinality-1
// HLL per user_id. An empty key is a no-op (no element to add).
func (w *HLLWrapper) UpdateBytes(b []byte) {
	if w.sk == nil || len(b) == 0 {
		return
	}
	w.sk.InsertWithHash(common.FromBytes(b).Hash)
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

// ComputeDeltaAgainst computes a sparse RegisterDelta against the previous
// snapshot bytes. When prev is empty (first window), returns the full snapshot
// with isFull=true. The threshold parameter is unused for HLL (register deltas
// are lossless). The delta is CLAMPED to the full frame: the full HLL state is
// sparse-packed (HLLSparseRegisters), so when few registers are set a
// per-register-update delta can be LARGER than the full sparse frame; in that
// case the full frame is emitted so a delta is never larger than the
// equivalent full frame at the same cadence.
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
	full, fErr := w.Snapshot()
	if fErr == nil && len(payload) >= len(full) {
		return full, true, nil
	}
	return payload, false, nil
}

// DeltaAgainstEmptyBase returns the snapshot of an EMPTY HLL (same
// precision / sampling probability). The precompute.SnapshotCache caches
// this as the outbound base after each window-close emit
// (delta-baseline-contract.md §3): the next window's ComputeDeltaAgainst
// then diffs against this empty base, so the emitted RegisterDelta is
// that window's own per-window register state (every non-zero register)
// encoded as a delta — no cross-window subtraction.
//
// HLL merges by register-wise MAX, so a per-window delta over an empty
// base is mandatory for window-scoped cardinality correctness: without
// resetting the base each window, a never-reset base would over-count
// (delta-baseline-contract.md §1.5 / §2.3). An empty HLL's
// SerializeProtoBytes is a non-empty envelope (it encodes the all-zero
// register array + precision), so ComputeDeltaAgainst takes its
// decode-and-diff path rather than the len(prev)==0 full-snapshot
// fallback.
func (w *HLLWrapper) DeltaAgainstEmptyBase() ([]byte, error) {
	empty := w.newSketch()
	if empty == nil {
		return nil, nil
	}
	b, err := empty.SerializeProtoBytes()
	if err != nil {
		return nil, fmt.Errorf("hll.SerializeProtoBytes(empty): %w", err)
	}
	return b, nil
}

// ApplyDelta merges a payload into the underlying HLL. Dispatches on
// payload shape: try a full-state SketchEnvelope FIRST, then fall back
// to a sparse RegisterDelta. The runtime's mergeFullEnvelope helper
// calls ApplyDelta on a fresh sketch as its "merge from empty" path, so
// accepting both shapes keeps the inbound full / delta envelope
// handling uniform.
//
// Order matters and full-state MUST be attempted first. A full-state
// HyperLogLogState is carried in a SketchEnvelope (oneof field 12),
// whereas a delta is a bare HLLDelta whose `updates` lives at field 1.
// proto3's HLLDelta tolerates the envelope's unknown fields and decodes
// to an EMPTY delta (no updates) without error — so trying delta first
// would silently merge a real full-state envelope to cardinality 0
// (the bug this ordering fixes). Conversely a bare HLLDelta fails the
// envelope decode (field-1 wire-type mismatch / GetHll()==nil), so the
// delta fallback catches it cleanly. This mirrors CMSWrapper.ApplyDelta.
func (w *HLLWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if w.sk == nil {
		w.sk = w.newSketch()
	}
	if other, err := hll.DeserializeHyperLogLogFromProtoBytes(payload); err == nil && other != nil {
		return w.sk.Merge(other)
	}
	if deltaMsg, err := hll.DeserializeRegisterDelta(payload); err == nil && deltaMsg != nil {
		hll.ApplyRegisterDelta(w.sk, deltaMsg)
		return nil
	}
	return fmt.Errorf("HLLWrapper: payload is neither a full proto state nor a register delta")
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
		w.sk = w.newSketch()
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch in place by replacing it with a fresh HLL
// (carrying the same sampling probability). Window rotation calls this
// when the runtime decides to recycle entries.
func (w *HLLWrapper) Reset() {
	w.sk = w.newSketch()
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
// sketch. KindFloat hashes the numeric value (the legacy
// accumulateGaugeMetric path); KindBytes hashes an opaque byte key — the
// item_label attribute VALUE — so the fused asap_edge item_label path can
// measure the cardinality of a label dimension (e.g. distinct user_ids)
// rather than the numeric sample. The two kinds share the same canonical
// hash family, so a KindFloat(x) and a KindBytes(float-bytes-of-x) are NOT
// interchangeable — callers pick the kind that matches the cardinality
// subject they intend.
func (HLLObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*HLLWrapper)
	if !ok {
		return fmt.Errorf("HLLObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.UpdateValue(v.Float)
		return nil
	case precompute.KindBytes:
		w.UpdateBytes(v.Bytes)
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
