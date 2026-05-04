// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"errors"
	"fmt"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// cmsSketchWrapper adapts a sketchlib-go *cms.CountMinSketch to the
// precompute.Sketch + precompute.FrequencySketch interfaces so a
// precompute.Precompute can own it as a generic Sketch. This is the
// production wrapper consumed by the CMS processor shim; the parity
// harness has its own test-only wrapper at
// integration/parity/harness/sketches.go that shares the same shape
// but lives outside the production import graph.
//
// Byte-format invariant: the legacy emit-side serializer is
// SerializeProtoBytesFO (Frequency-Only proto, omitting Sum/Sum2 to
// halve the wire size). The wrapper's Snapshot() mirrors that exactly
// when the configured encoding is "proto", and switches to
// SerializeMsgpack when "msgpack" — preserving wire-bytes parity
// pre/post-refactor (ADR-0002 §"Behavior preservation").
type cmsSketchWrapper struct {
	sk         *cms.CountMinSketch
	rows       int
	cols       int
	useMsgpack bool
}

// newCMSSketchWrapper builds an empty CMS with the configured rows /
// cols. The legacy newProcessor allocates `cms.NewCountMinSketch(rows,
// cols)` lazily on first observation; we allocate eagerly here so the
// wrapper's Sketch interface is always valid.
func newCMSSketchWrapper(rows, cols int, useMsgpack bool) *cmsSketchWrapper {
	sk, _ := cms.NewCountMinSketch(rows, cols)
	return &cmsSketchWrapper{sk: sk, rows: rows, cols: cols, useMsgpack: useMsgpack}
}

// insertHash mirrors the legacy CMS processor's
// `ws.cms.InsertWithHash(common.FromString(flowKey).Hash)` call. The
// shim feeds the encoded data-point attribute set as bytes here; the
// hash agrees with what the legacy processor computes from the same
// attrs because both go through common.FromString / common.FromBytes.
func (w *cmsSketchWrapper) insertHash(h uint64) {
	if w.sk != nil {
		w.sk.InsertWithHash(h)
	}
}

// Snapshot serializes via SerializeProtoBytesFO (the legacy emit
// path's default) or SerializeMsgpack when the wrapper was configured
// for msgpack. The backend's modified-OTLP CMS decoder accepts both
// wire formats; the chosen format must match the encoding tag the
// shim's encode path stamps on the data point.
func (w *cmsSketchWrapper) Snapshot() ([]byte, error) {
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

// ComputeDeltaAgainst is a no-op for the production CMS wrapper:
// the shim configures the runtime with DeltaTransmission=false (so
// the runtime always emits full snapshots) and post-processes the
// emit stream itself in a per-series prev cache held at the shim
// layer. See cmsProcessor.applyDeltaTransmission in shim_helpers.go.
//
// Why not use the runtime's snapshot cache? The legacy CMS processor
// refreshes the per-partition snapshot after EVERY emit (full or
// delta), then computes the next delta against that fresh snapshot.
// The runtime's SnapshotCache only refreshes when the wrapper
// returns isFull=true, which would force every emit to be a full
// payload. Routing delta tracking to the shim sidesteps that gap
// without modifying asap-precompute-go.
//
// The wrapper still implements the contract for completeness — any
// caller that DOES enable runtime-side delta gets a full snapshot
// (isFull=true) on every call.
func (w *cmsSketchWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

// ApplyDelta merges an inbound payload into the underlying sketch.
// The runtime invokes this for both delta-encoded inbound envelopes
// (the runtime's mergeFullEnvelope helper calls ApplyDelta on a fresh
// sketch as its "merge from empty" path) and full-state envelopes.
// Dispatch on payload shape: try full state first (the legacy
// processor's wire format on successful proto round-trip), then fall
// back to delta.
func (w *cmsSketchWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	if w.sk == nil {
		w.sk, _ = cms.NewCountMinSketch(w.rows, w.cols)
	}
	if other, err := cms.DeserializeCountMinSketchFromProtoBytes(payload); err == nil && other != nil {
		return w.sk.Merge(other)
	}
	if d, err := cms.DeserializeDelta(payload); err == nil && d != nil {
		cms.ApplyDelta(w.sk, d)
		return nil
	}
	return errors.New("cmsSketchWrapper: payload is neither a full proto state nor a delta")
}

// Merge folds another cmsSketchWrapper into this one. The runtime
// only ever calls Merge between sketches owned by the same Precompute
// (same rows / cols), so the type assertion is safe.
func (w *cmsSketchWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*cmsSketchWrapper)
	if !ok {
		return fmt.Errorf("cmsSketchWrapper: Merge with %T", other)
	}
	if o.sk == nil {
		return nil
	}
	if w.sk == nil {
		w.sk, _ = cms.NewCountMinSketch(w.rows, w.cols)
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch in place by replacing it with a fresh
// CountMinSketch of the same dimensions. Window rotation calls this
// when the runtime decides to recycle entries.
func (w *cmsSketchWrapper) Reset() {
	w.sk, _ = cms.NewCountMinSketch(w.rows, w.cols)
}

// EstimateCount returns the estimated frequency for a hashed key.
// The shim hashes the key the same way the observer does (via
// common.FromBytes) so callers can pass the raw byte key.
func (w *cmsSketchWrapper) EstimateCount(key []byte) float64 {
	if w.sk == nil {
		return 0
	}
	return w.sk.FastEstimateWithHash(common.FromBytes(key).Hash)
}

// TopK is not natively supported by CountMinSketch (which is a
// frequency estimator over a known key set, not a top-k tracker).
// Returning an empty slice keeps the FrequencySketch interface
// satisfied; callers wanting top-k functionality should use
// CountSketch instead. Today's CMS processor never queries TopK;
// the FrequencySketch contract is fulfilled at compile-time only.
func (w *cmsSketchWrapper) TopK(k int) []precompute.FrequencyEntry { return nil }

// cmsSketchObserver implements precompute.SketchObserver for KindBytes
// observations: the shim translates each observation's encoded
// data-point attribute set into bytes (matching the legacy processor's
// `common.FromString(flowKey)` hash) and routes here.
type cmsSketchObserver struct{}

func (cmsSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*cmsSketchWrapper)
	if !ok {
		return fmt.Errorf("cmsSketchObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindBytes {
		return fmt.Errorf("cmsSketchObserver: expected KindBytes, got %s", v.Kind)
	}
	w.insertHash(common.FromBytes(v.Bytes).Hash)
	return nil
}

// Compile-time assertions that cmsSketchWrapper satisfies the trait
// surface ADR-0002 / PR #224 pinned for FrequencySketch implementations.
var (
	_ precompute.Sketch          = (*cmsSketchWrapper)(nil)
	_ precompute.FrequencySketch = (*cmsSketchWrapper)(nil)
	_ precompute.SketchObserver  = cmsSketchObserver{}
)

// serializeCMS / deserializeCMS are retained as thin helpers so the
// existing TestRoundTripIngestProtoSketch (PR #222) keeps compiling
// without rewriting test logic. Production code paths construct
// sketches via newCMSSketchWrapper / cmsSketchObserver.
func serializeCMS(s *cms.CountMinSketch) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	return s.SerializeProtoBytesFO()
}

func deserializeCMS(data []byte) (*cms.CountMinSketch, error) {
	return cms.DeserializeCountMinSketchFromProtoBytes(data)
}
