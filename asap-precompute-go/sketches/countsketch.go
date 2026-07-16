// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/ProjectASAP/sketchlib-go/common"
	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// countSketchSampleSeed seeds the geometric update-sampler so the admitted
// subset is deterministic across runs (matches the CMS wrapper convention).
const countSketchSampleSeed int64 = 0x5a3e06d

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

	// heapMsgpack selects the heap-bearing MessagePack emit path: when
	// true, Snapshot() emits the `{sketch, topk_heap, heap_size}` wire
	// format the ASAPQuery backend reads via
	// CountMinSketchWithHeap::from_msgpack, promoting the sid to
	// CountSketchWithHeap (Capability::FrequencyTopk) so `topk(metric)`
	// queries route here instead of returning "No result". When false
	// (the default) Snapshot() emits the legacy proto full state, exactly
	// byte-parity with the original wrapper.
	//
	// Delta transmission IS supported in heap-msgpack mode via the
	// DELTA-HEAP wire form (encoding tag MSGPACK_DELTA): window 1 ships a
	// full heap frame; each later window ships a sparse matrix delta + the
	// full top-k heap (SerializeMsgpackWithHeapDelta). The delta is
	// computed against an EMPTY base per the per-window-reset model (PWR,
	// delta-baseline-contract.md §3), so the backend reconstructs the
	// window's own state by applying the delta onto a rotated-empty base.
	heapMsgpack bool
	// heapSize is the bounded top-k heap capacity carried in the wire
	// payload (the backend stores it and uses min(self,other) on merge).
	heapSize int

	// ackedCells is the cell matrix snapshot at the last sub-window emit, used
	// by the threshold-driven sub-window producer to measure L2 divergence
	// (Frobenius norm of current − acked) without decoding the wire base. nil
	// until the first MarkSubWindowEmitted; cleared on Reset.
	ackedCells [][]float64

	// GOS delta mode (set per emit by the runtime from PrecomputeConfig). When
	// gosEpsilon>0, ComputeDeltaAgainst overrides the passed threshold with the
	// GOS relative isotropic threshold (O(1) memory). The anisotropic
	// (gradient-weighted per-cell {T_j}) mode that used to live alongside this
	// has been removed pending an Activity_j redesign — see gos_threshold.go.
	gosEpsilon float64
	gosSites   uint32

	// sampler implements NitroSketch geometric update-sampling. When non-nil
	// (sampleP<1), UpdateString admits each item with probability sampleP and
	// upweights the admitted insert by 1/sampleP, so the frequency estimate stays
	// unbiased while ~(1−sampleP) of the per-item d-row counter work is skipped —
	// the distributed-NitroSketch CPU lever (the coordinator hands each edge a
	// sampleP via Grant.SampleP; see docs/distributed-nitrosketch-coordinated-
	// sampling.md). nil (sampleP=1) ⇒ every item updates, byte-identical to today.
	sampler *common.GeometricSampler
	sampleP float64
}

// WithSampleP enables geometric update-sampling at probability p (0<p<1). p>=1
// (or NaN) disables it. Admitted inserts are upweighted by 1/p so the estimate
// is unbiased without any backend rescale. Returns the wrapper for chaining.
func (w *CountSketchWrapper) WithSampleP(p float64) *CountSketchWrapper {
	if w == nil {
		return w
	}
	if p >= 1.0 || p != p || p <= 0 { // p!=p ⇒ NaN
		w.sampler = nil
		w.sampleP = 1.0
		return w
	}
	w.sampleP = p
	w.sampler = common.NewGeometricSampler(p, countSketchSampleSeed)
	return w
}

// SetSampleP applies the sampling probability via WithSampleP, discarding the
// chained receiver so *CountSketchWrapper satisfies precompute.SampleSetter (the
// coordinated-sampling stamp path).
func (w *CountSketchWrapper) SetSampleP(p float64) { w.WithSampleP(p) }

// SampleP returns the active update-sampling probability (1.0 when disabled).
func (w *CountSketchWrapper) SampleP() float64 {
	if w == nil || w.sampleP <= 0 {
		return 1.0
	}
	return w.sampleP
}

// L2DivergenceSinceEmit reports the L2 (Frobenius) magnitude of the change in
// the count matrix since the last MarkSubWindowEmitted, and the current matrix
// CellMatrix returns a copy of the current rows×cols signed-count cell matrix —
// the whole-sketch state the F2 monitor squares/merges and ships over the wire
// (via asapmsgpack.MarshalCountSketch). Returns nil for an uninitialized sketch.
func (w *CountSketchWrapper) CellMatrix() [][]float64 {
	if w.cs == nil {
		return nil
	}
	m := make([][]float64, w.rows)
	for r := 0; r < w.rows; r++ {
		m[r] = make([]float64, w.cols)
		for c := 0; c < w.cols; c++ {
			m[r][c] = w.cs.GetCell(r, c)
		}
	}
	return m
}

// L2 norm. The threshold-driven sub-window producer emits when
// div >= ε·norm, giving the backend a Count-Sketch within ε·‖f‖₂ of the true
// current state (the √rows factor cancels in the ratio, so the raw Frobenius
// norms suffice).
func (w *CountSketchWrapper) L2DivergenceSinceEmit() (div, norm float64) {
	if w.cs == nil {
		return 0, 0
	}
	var d2, n2 float64
	for r := 0; r < w.rows; r++ {
		for c := 0; c < w.cols; c++ {
			cur := w.cs.GetCell(r, c)
			n2 += cur * cur
			prev := 0.0
			if r < len(w.ackedCells) && c < len(w.ackedCells[r]) {
				prev = w.ackedCells[r][c]
			}
			d := cur - prev
			d2 += d * d
		}
	}
	return math.Sqrt(d2), math.Sqrt(n2)
}

// MarkSubWindowEmitted captures the current cell matrix as the divergence
// reference for the next L2DivergenceSinceEmit.
func (w *CountSketchWrapper) MarkSubWindowEmitted() {
	if w.cs == nil {
		return
	}
	if len(w.ackedCells) != w.rows {
		w.ackedCells = make([][]float64, w.rows)
		for r := range w.ackedCells {
			w.ackedCells[r] = make([]float64, w.cols)
		}
	}
	for r := 0; r < w.rows; r++ {
		for c := 0; c < w.cols; c++ {
			w.ackedCells[r][c] = w.cs.GetCell(r, c)
		}
	}
}

// SetGosMode configures the GOS delta mode. When epsilon>0, ComputeDeltaAgainst
// gates the delta with the GOS relative isotropic threshold instead of the
// passed fixed value (one scalar, O(1) memory). epsilon≤0 disables GOS
// (unchanged behavior). The runtime calls this from PrecomputeConfig per emit.
func (w *CountSketchWrapper) SetGosMode(epsilon float64, k uint32) {
	w.gosEpsilon = epsilon
	w.gosSites = k
}

// GosDeltaThreshold computes the F2 isotropic GOS per-cell delta threshold
// `T = ε·‖Ĉ‖/(2k√(dw))` from the current sketch norm and dims, rounded up to an
// integer (never below 1 = lossless). Used by the sub-window emit path to gate
// the sparse delta with a relative, norm-adaptive threshold instead of a fixed
// configured value. Returns 1 when ε ≤ 0 (GOS disabled → lossless).
func (w *CountSketchWrapper) GosDeltaThreshold(epsilon float64, k uint32) uint64 {
	if epsilon <= 0 || w.cs == nil {
		return 1
	}
	_, norm := w.L2DivergenceSinceEmit()
	t := F2IsotropicThreshold(epsilon, norm, k, w.rows, w.cols)
	if !math.IsInf(t, 1) && !math.IsNaN(t) && t > 1.0 {
		return uint64(math.Ceil(t))
	}
	return 1
}

// defaultCountSketchHeapSize mirrors sketchlib-go's CountSketch TOPK_SIZE
// default (the heap the producer's Space-Saving tracker feeds). Used when
// the caller passes heapSize <= 0.
const defaultCountSketchHeapSize = 100

// NewCountSketchWrapper constructs a fresh CountSketch with the given
// (rows, cols) — derived from epsilon/delta the same way the legacy
// processor's newConfiguredCountSketch did. Returns an error if
// sketchlib's constructor rejects the dimensions (it requires cols to
// be a power of two and both dims positive).
//
// In addition to sketchlib's checks, this rejects dimensions whose
// per-row hash slices would overflow the single 64-bit item hash:
// sketchlib bit-slices the hash as row*ceil(log2(cols)) and once
// rows*ceil(log2(cols)) > 64 the high rows read shifted-out (zero) bits
// and silently collapse onto column 0. NewCountSketchWrapper already
// returns an error (unlike NewCMSWrapper), so it rejects rather than
// clamps — surfacing the misconfiguration to the caller.
func NewCountSketchWrapper(rows, cols int) (*CountSketchWrapper, error) {
	cs, err := countsketch.NewCountSketch(rows, cols)
	if err != nil {
		return nil, fmt.Errorf("sketches: NewCountSketch(%d, %d): %w", rows, cols, err)
	}
	// cols is a power of two here (sketchlib rejected it otherwise), so
	// bits.TrailingZeros gives exactly log2(cols) = the per-row bit width.
	if bitsPerRow := bits.TrailingZeros(uint(cols)); bitsPerRow > 0 && rows*bitsPerRow > maxRowHashBits {
		return nil, fmt.Errorf(
			"sketches: NewCountSketch(%d, %d): rows*ceil(log2(cols))=%d exceeds the %d-bit row-hash budget; reduce rows or cols",
			rows, cols, rows*bitsPerRow, maxRowHashBits)
	}
	return &CountSketchWrapper{cs: cs, rows: rows, cols: cols, heapSize: defaultCountSketchHeapSize}, nil
}

// NewCountSketchWithHeapWrapper builds a CountSketch wrapper whose
// Snapshot() emits the heap-bearing MessagePack wire format the
// ASAPQuery backend detects as `CountSketchWithHeap` (see
// sketchlib-go CountSketch.SerializeMsgpackWithHeap and
// ASAPQuery-backend ingest/otel.rs::sketch_kind_handle_for). heapSize
// bounds the transmitted top-k heap (<=0 → defaultCountSketchHeapSize).
// Same dimension validation as NewCountSketchWrapper.
//
// IMPORTANT: the top-k heap is populated from the producer's internal
// Space-Saving candidate tracker, which is fed ONLY by UpdateString
// (the keyed-observe path). Route observations through the
// CountSketchObserver (KindFloat with the item key in
// ObservationValue.Bytes) so the heap is non-empty — the backend only
// promotes to FrequencyTopk when the decoded heap is non-empty.
func NewCountSketchWithHeapWrapper(rows, cols, heapSize int) (*CountSketchWrapper, error) {
	w, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		return nil, err
	}
	if heapSize <= 0 {
		heapSize = defaultCountSketchHeapSize
	}
	w.heapMsgpack = true
	w.heapSize = heapSize
	return w, nil
}

// UpdateString mirrors the legacy ws.cs.UpdateString(itemKey, value)
// call. Adapters that route a key/count pair (rather than an
// ObservationValue) call this directly.
func (w *CountSketchWrapper) UpdateString(key string, count float64) {
	// Nil guard: a wrapper whose constructor failed (e.g. dimensions
	// exceeding the 64-bit row-hash budget) can be left with cs == nil if
	// a caller discarded the constructor error. Skip the update rather
	// than panic on the first sample (P0-1).
	if w == nil || w.cs == nil {
		return
	}
	if w.sampler != nil {
		// PER-ROW geometric admission (design §3.1/§3.2): the sampler decides
		// which of the d rows this item updates; the key is hashed only if ≥1 row
		// is admitted, and admitted rows carry the 1/p weight. Per-row (not
		// per-item) admission decorrelates the row estimates so the median-of-rows
		// concentrates the sampling error. Replaces the older whole-item admit.
		w.cs.UpdateStringSampledPerRow(key, count, w.sampler)
		return
	}
	w.cs.UpdateString(key, count)
}

// ApplyAdmittedOccurrence applies a row-admission decision made UPSTREAM —
// typically by an OTel SDK running NitroSketch admission at Record() time,
// before the occurrence was ever serialized (metricdata.RowSampledSketch;
// see AggregationRowSampledSketch in the SDK). admittedRows is a bitmask
// over this sketch's rows (bit r set ⇒ row r admits); count is the
// occurrence's raw magnitude and sampleP is the admission probability in
// effect when the SDK made the decision.
//
// admittedRows == 0 (R(x)=∅) is a no-op — the SDK is expected to have
// already dropped such occurrences before they ever reached the wire.
// Unlike UpdateString, this NEVER consults w.sampler: the admission
// decision is given, not made here.
//
// This does NOT touch w.sampleP or stamp anything on the envelope: the
// 1/sampleP correction is baked into the cell here (mirrors
// UpdateStringSampledPerRow's contract) — a query-time consumer must NOT
// also rescale by this sketch's envelope p, or the correction applies
// twice.
func (w *CountSketchWrapper) ApplyAdmittedOccurrence(key string, count float64, admittedRows uint64, sampleP float64) {
	if w == nil || w.cs == nil {
		return
	}
	w.cs.UpdateStringAtRows(key, count, admittedRows, sampleP)
}

// Snapshot returns the canonical proto-encoded SketchEnvelope bytes,
// byte-identical to the legacy processor's serializeCountSketch
// output (SerializePortable + proto.Marshal). In heap-msgpack mode it
// instead returns the heap-bearing MessagePack wire format the backend
// reads via CountMinSketchWithHeap::from_msgpack (carrying the count
// matrix + top-k heap), which the emit path tags EncodingMsgpack.
func (w *CountSketchWrapper) Snapshot() ([]byte, error) {
	// Nil guard (P0-1): a wrapper left with cs == nil (constructor error
	// discarded by a caller) returns an empty snapshot rather than
	// panicking; the runtime treats a nil payload as "skip this series".
	if w == nil || w.cs == nil {
		return nil, nil
	}
	if w.heapMsgpack {
		return w.cs.SerializeMsgpackWithHeap(w.heapSize)
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
	// GOS delta gating: override the fixed threshold with the relative GOS
	// isotropic threshold.
	if w.gosEpsilon > 0 {
		threshold = w.GosDeltaThreshold(w.gosEpsilon, w.gosSites)
	}
	// Heap-msgpack mode: produce a DELTA-HEAP frame — a sparse matrix
	// delta of this window's sketch against the cached base (an empty
	// heap-msgpack frame under PWR) plus the FULL top-k heap. On any
	// decode/compute failure (e.g. no/garbled base) fall back to the full
	// heap snapshot tagged isFull so the runtime emits a full MSGPACK frame
	// the backend can decode standalone.
	if w.heapMsgpack {
		if len(prev) == 0 {
			full, err := w.Snapshot()
			return full, true, err
		}
		base, err := countsketch.DeserializeMsgpackWithHeapMatrix(prev)
		if err != nil {
			full, fErr := w.Snapshot()
			return full, true, fErr
		}
		delta, err := w.cs.SerializeMsgpackWithHeapDelta(base, w.heapSize, float64(threshold))
		if err != nil {
			full, fErr := w.Snapshot()
			return full, true, fErr
		}
		// Clamp: never emit a delta larger than the equivalent full frame.
		full, fErr := w.Snapshot()
		if fErr == nil && len(delta) >= len(full) {
			return full, true, nil
		}
		return delta, false, nil
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
	// Clamp: never emit a delta larger than the equivalent full frame.
	full, fErr := w.Snapshot()
	if fErr == nil && len(payload) >= len(full) {
		return full, true, nil
	}
	return payload, false, nil
}

// DeltaAgainstEmptyBase returns the snapshot of an EMPTY CountSketch of
// the same dimensions. The precompute.SnapshotCache caches this as the
// outbound base after each window-close emit (delta-baseline-contract.md
// §3): the next window's ComputeDeltaAgainst then diffs against this
// empty base, so the emitted delta is that window's own full (signed)
// per-cell matrix encoded as a delta — no cross-window subtraction.
//
// An empty CountSketch's SerializeProtoBytes is a non-empty envelope (it
// encodes the all-zero matrix + dimensions), so ComputeDeltaAgainst
// takes its decode-and-diff path rather than the len(prev)==0
// full-snapshot fallback.
func (w *CountSketchWrapper) DeltaAgainstEmptyBase() ([]byte, error) {
	empty, err := countsketch.NewCountSketch(w.rows, w.cols)
	if err != nil {
		return nil, fmt.Errorf("sketches: NewCountSketch(empty): %w", err)
	}
	// Heap-msgpack mode: the cached base must itself be a heap-bearing
	// full frame so the NEXT window's ComputeDeltaAgainst decodes its
	// matrix via DeserializeMsgpackWithHeapMatrix. The empty sketch has an
	// empty heap; only its (all-zero) matrix is consumed as the base.
	if w.heapMsgpack {
		b, sErr := empty.SerializeMsgpackWithHeap(w.heapSize)
		if sErr != nil {
			return nil, fmt.Errorf("countsketch.SerializeMsgpackWithHeap(empty): %w", sErr)
		}
		return b, nil
	}
	b, err := empty.SerializeProtoBytes()
	if err != nil {
		return nil, fmt.Errorf("countsketch.SerializeProtoBytes(empty): %w", err)
	}
	return b, nil
}

// ApplyDelta merges an inbound payload into this sketch in place.
// Dispatch on payload shape: try a full-state SketchEnvelope FIRST,
// then fall back to a sparse delta. The runtime invokes this for both
// full-state envelopes (mergeFullEnvelope's "merge from empty" path
// calls ApplyDelta on a fresh sketch) and PROTO_DELTA-encoded
// CountSketchDataPoints.
//
// Order matters and full-state MUST be attempted first. A full-state
// CountSketchState is carried in a SketchEnvelope (oneof field 11),
// whereas a delta is a bare CountSketchDelta. Without the full-state
// branch (the original bug) a full-state envelope was never recognised
// and DeserializeDelta would decode it to an empty delta, silently
// merging it to an estimate of 0. A bare delta fails the envelope
// decode (GetCountSketch()==nil / field wire-type mismatch) so the
// delta fallback catches it cleanly. This mirrors CMSWrapper.ApplyDelta.
func (w *CountSketchWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("CountSketchWrapper: ApplyDelta with empty payload")
	}
	if w.cs == nil {
		cs, err := countsketch.NewCountSketch(w.rows, w.cols)
		if err != nil {
			return err
		}
		w.cs = cs
	}
	// Heap-msgpack frames first: a DELTA-HEAP frame (4-element array,
	// is_delta marker) applies the sparse matrix delta + replaces the heap;
	// a full heap frame (3-element array) merges the decoded matrix. Both
	// are tried before the proto branches because the proto decoders would
	// mis-accept the msgpack bytes as an empty state otherwise.
	if countsketch.IsMsgpackWithHeapDelta(payload) {
		return w.cs.ApplyMsgpackWithHeapDelta(payload)
	}
	if other, err := countsketch.DeserializeMsgpackWithHeapMatrix(payload); err == nil && other != nil {
		return w.cs.Merge(other)
	}
	if other, err := countsketch.DeserializeCountSketchFromProtoBytes(payload); err == nil && other != nil {
		return w.cs.Merge(other)
	}
	if deltaMsg, err := countsketch.DeserializeDelta(payload); err == nil && deltaMsg != nil {
		countsketch.ApplyDelta(w.cs, deltaMsg)
		return nil
	}
	return errors.New("CountSketchWrapper: payload is neither a full proto state nor a delta")
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
	// Nil guard (P0-1): tolerate a wrapper whose cs is nil (discarded
	// constructor error) so window rotation / pool recycle never panics.
	if w == nil || w.cs == nil {
		return
	}
	w.cs.Reset()
	w.ackedCells = nil
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

// Observe routes a precompute.ObservationValue into the wrapped CountSketch
// via UpdateString — or, when v.RowSampled, via ApplyAdmittedOccurrence with
// the SDK's pre-decided admission bitmask, using the same key and v.Float
// weight the plain path would have used.
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
	if v.RowSampled {
		w.ApplyAdmittedOccurrence(key, v.Float, v.AdmittedRows, v.SampleP)
		return nil
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
