// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"errors"
	"fmt"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// CMSWrapper adapts a sketchlib-go *cms.CountMinSketch to the
// precompute.Sketch + precompute.FrequencySketch interfaces so a
// precompute.Precompute can own it as a generic Sketch.
//
// Byte-format invariant: the legacy emit-side serializer is
// SerializeProtoBytesFO (Frequency-Only proto, omitting Sum/Sum2 to
// halve the wire size). The wrapper's Snapshot() mirrors that exactly
// when the configured encoding is "proto", and switches to
// SerializeMsgpack when "msgpack" — preserving wire-bytes parity
// pre/post-refactor (ADR-0002 §"Behavior preservation").
//
// Delta semantics: the wrapper now drives real delta computation
// through the runtime's SnapshotCache (which always-refreshes the
// cached snapshot after every emit — see snapshot_cache.go). The
// earlier "always isFull=true" stub existed only to work around the
// SnapshotCache's previous "refresh-only-on-full" bug, which is now
// fixed.
type CMSWrapper struct {
	sk         *cms.CountMinSketch
	rows       int
	cols       int
	useMsgpack bool
	// sampleP is the per-sketch sampling probability in (0,1]. 1.0 (the
	// default) disables sampling so the sketch is byte-identical to an
	// unsampled one. Set via WithSampleP; preserved across the
	// re-construction paths (Reset / Merge / ApplyDelta) so a sampled
	// wrapper stays sampled for its whole lifetime.
	sampleP float64
}

// cmsSampleSeed is the fixed seed handed to sketchlib-go's geometric
// sampler. A constant seed keeps the admitted-subset reproducible across
// runs (sketchlib-go's NewGeometricSampler doc) and across the wrapper's
// internal re-constructions; the value is arbitrary and only matters for
// determinism, never for correctness (any seed yields an unbiased sample).
const cmsSampleSeed int64 = 0x5A4D_5043 // "ZMPC"

// NewCMSWrapper builds an empty CMS with the configured rows / cols.
// useMsgpack=true selects the legacy msgpack emit path (no delta
// transmission supported in that mode); useMsgpack=false uses the
// proto SerializeProtoBytesFO format and supports delta transmission.
//
// Sampling is disabled (sampleP=1.0) by default — call WithSampleP to
// enable it. The default keeps the emitted wire bytes byte-identical to
// the pre-sampling format.
func NewCMSWrapper(rows, cols int, useMsgpack bool) *CMSWrapper {
	w := &CMSWrapper{rows: rows, cols: cols, useMsgpack: useMsgpack, sampleP: 1.0}
	w.sk = w.newSketch()
	return w
}

// WithSampleP enables per-sketch geometric admission sampling at
// probability p in (0,1]. p>=1 (or NaN) disables sampling (exact, the
// default); p<=0 is clamped to a tiny positive probability by sketchlib-go
// rather than dropping the whole stream. Returns the receiver for fluent
// construction. The probability is stamped on the SketchEnvelope by
// sketchlib-go so the backend rescales frequency estimates by 1/p at
// query time.
func (w *CMSWrapper) WithSampleP(p float64) *CMSWrapper {
	if p >= 1.0 || p != p { // p != p ⇒ NaN
		w.sampleP = 1.0
	} else {
		w.sampleP = p
	}
	// Re-apply to the live sketch so a WithSampleP after construction
	// takes effect immediately.
	if w.sk != nil {
		w.sk.WithSampleP(w.sampleP, cmsSampleSeed)
	}
	return w
}

// SampleP returns the configured sampling probability (1.0 when disabled).
func (w *CMSWrapper) SampleP() float64 {
	if w.sampleP <= 0 {
		return 1.0
	}
	return w.sampleP
}

// newSketch builds a fresh sketchlib-go CMS carrying the wrapper's
// configured sampling probability. Centralises the construction so every
// re-creation path (New / Reset / Merge / ApplyDelta) keeps sampleP.
func (w *CMSWrapper) newSketch() *cms.CountMinSketch {
	sk, _ := cms.NewCountMinSketch(w.rows, w.cols)
	if sk != nil && w.sampleP > 0 && w.sampleP < 1.0 {
		sk.WithSampleP(w.sampleP, cmsSampleSeed)
	}
	return sk
}

// InsertHash mirrors the legacy CMS processor's
// `ws.cms.InsertWithHash(common.FromString(flowKey).Hash)` call.
// Adapter code feeds the encoded data-point attribute set as bytes
// here; the hash agrees with what the legacy processor computes from
// the same attrs because both go through common.FromString /
// common.FromBytes.
func (w *CMSWrapper) InsertHash(h uint64) {
	if w.sk != nil {
		w.sk.InsertWithHash(h)
	}
}

// Snapshot serializes via SerializeProtoBytesFO (the legacy emit
// path's default) or SerializeMsgpack when the wrapper was configured
// for msgpack. The backend's modified-OTLP CMS decoder accepts both
// wire formats; the chosen format must match the encoding tag the
// adapter's encode path stamps on the data point.
func (w *CMSWrapper) Snapshot() ([]byte, error) {
	if w.sk == nil {
		return nil, nil
	}
	if w.useMsgpack {
		b, err := w.sk.SerializeMsgpack()
		if err != nil {
			return nil, fmt.Errorf("cms.SerializeMsgpack: %w", err)
		}
		return b, nil
	}
	b, err := w.sk.SerializeProtoBytesFO()
	if err != nil {
		return nil, fmt.Errorf("cms.SerializeProtoBytesFO: %w", err)
	}
	return b, nil
}

// ComputeDeltaAgainst computes a sparse delta against the previous
// snapshot bytes via sketchlib-go's cms.ComputeDelta. When prev is
// empty (first window), returns the full snapshot with isFull=true.
// On any decode/compute failure, falls back to a full snapshot so
// the emit path always produces a valid payload.
//
// Msgpack mode does not support delta transmission — Snapshot is
// returned unconditionally so the runtime emits PROTO_FULL frames.
func (w *CMSWrapper) ComputeDeltaAgainst(prev []byte, threshold uint64) ([]byte, bool, error) {
	if w.sk == nil {
		return nil, true, nil
	}
	if w.useMsgpack {
		full, err := w.Snapshot()
		return full, true, err
	}
	if len(prev) == 0 {
		full, err := w.Snapshot()
		return full, true, err
	}
	prevSk, err := cms.DeserializeCountMinSketchFromProtoBytes(prev)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	deltaMsg, err := cms.ComputeDelta(prevSk, w.sk, float64(threshold))
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	payload, err := cms.SerializeDelta(deltaMsg)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return payload, false, nil
}

// ApplyDelta merges an inbound payload into the underlying sketch.
// The runtime invokes this for both delta-encoded inbound envelopes
// (the runtime's mergeFullEnvelope helper calls ApplyDelta on a fresh
// sketch as its "merge from empty" path) and full-state envelopes.
// Dispatch on payload shape: try full state first (the legacy
// processor's wire format on successful proto round-trip), then fall
// back to delta.
func (w *CMSWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if w.sk == nil {
		w.sk = w.newSketch()
	}
	if other, err := cms.DeserializeCountMinSketchFromProtoBytes(payload); err == nil && other != nil {
		return w.sk.Merge(other)
	}
	if d, err := cms.DeserializeDelta(payload); err == nil && d != nil {
		cms.ApplyDelta(w.sk, d)
		return nil
	}
	return errors.New("CMSWrapper: payload is neither a full proto state nor a delta")
}

// Merge folds another CMSWrapper into this one. The runtime only
// ever calls Merge between sketches owned by the same Precompute
// (same rows / cols), so the type assertion is safe.
func (w *CMSWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*CMSWrapper)
	if !ok {
		return fmt.Errorf("CMSWrapper: Merge with %T", other)
	}
	if o.sk == nil {
		return nil
	}
	if w.sk == nil {
		w.sk = w.newSketch()
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch in place by replacing it with a fresh
// CountMinSketch of the same dimensions (and the same sampling
// probability). Window rotation calls this when the runtime decides to
// recycle entries.
func (w *CMSWrapper) Reset() {
	w.sk = w.newSketch()
}

// EstimateCount returns the estimated frequency for a hashed key.
// Adapters hash the key the same way the observer does (via
// common.FromBytes) so callers can pass the raw byte key.
func (w *CMSWrapper) EstimateCount(key []byte) float64 {
	if w.sk == nil {
		return 0
	}
	return w.sk.FastEstimateWithHash(common.FromBytes(key).Hash)
}

// TopK is not natively supported by CountMinSketch (which is a
// frequency estimator over a known key set, not a top-k tracker).
// Returning an empty slice keeps the FrequencySketch interface
// satisfied; callers wanting top-k functionality should use
// CountSketchWrapper instead. The legacy CMS processor never queries
// TopK; the FrequencySketch contract is fulfilled at compile-time only.
func (w *CMSWrapper) TopK(_ int) []precompute.FrequencyEntry { return nil }

// CMSObserver implements precompute.SketchObserver for KindBytes
// observations: the adapter translates each observation's encoded
// data-point attribute set into bytes (matching the legacy processor's
// `common.FromString(flowKey)` hash) and routes here.
type CMSObserver struct{}

// Observe routes a precompute.ObservationValue (KindBytes) into the
// wrapped CMS via InsertHash.
func (CMSObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*CMSWrapper)
	if !ok {
		return fmt.Errorf("CMSObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindBytes {
		return fmt.Errorf("CMSObserver: expected KindBytes, got %s", v.Kind)
	}
	w.InsertHash(common.FromBytes(v.Bytes).Hash)
	return nil
}

// Compile-time assertions that CMSWrapper satisfies the trait surface
// for FrequencySketch implementations.
var (
	_ precompute.Sketch          = (*CMSWrapper)(nil)
	_ precompute.FrequencySketch = (*CMSWrapper)(nil)
	_ precompute.SketchObserver  = CMSObserver{}
)
