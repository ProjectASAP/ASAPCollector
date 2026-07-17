// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// maxRowHashBits is the width, in bits, of the single 64-bit per-item hash
// that sketchlib's CMS / CountSketch bit-slice across rows. Each row consumes
// ceil(log2(cols)) bits at offset row*bitsPerRow. Once
// rows*bitsPerRow exceeds 64, the high rows read shifted-out (zero) bits and
// silently collapse onto column 0, degrading those rows to useless. The
// wrappers clamp/round dimensions so the slicing always fits.
const maxRowHashBits = 64

// nextPow2 rounds n up to the next power of two (n itself when already a
// power of two). Returns 1 for n <= 1. sketchlib's CMS folds inserts with
// `% cols` but its hash bit-slicing and column mask assume a power-of-two
// width, so a non-pow2 cols silently mis-indexes; rounding up keeps the mask
// and the live column count consistent.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	if n&(n-1) == 0 {
		return n
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// clampRowsForHashBits reduces rows so that rows*ceil(log2(cols)) <= 64.
// Beyond that the per-row hash slices overflow the single 64-bit hash and the
// extra rows degrade to zero-width (always column 0), so we cap rows at the
// largest value that still fits. cols is assumed already rounded to a power of
// two. Returns at least 1.
func clampRowsForHashBits(rows, cols int) int {
	if rows < 1 {
		return 1
	}
	bitsPerRow := bits.TrailingZeros(uint(cols)) // == log2(cols) for pow2 cols
	if bitsPerRow <= 0 {
		// cols == 1 ⇒ 0 bits per row; every row maps to col 0 regardless,
		// so the slicing never overflows. Leave rows as-is.
		return rows
	}
	maxRows := maxRowHashBits / bitsPerRow
	if maxRows < 1 {
		maxRows = 1
	}
	if rows > maxRows {
		return maxRows
	}
	return rows
}

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
	// emptyBaseBytes memoizes the SerializeProtoBytesFO of an EMPTY sketch of
	// this wrapper's exact shape + sampleP (the bytes DeltaAgainstEmptyBase
	// hands the SnapshotCache as the outbound base). It is deterministic for a
	// given (rows, cols, sampleP), so caching it once lets ComputeDeltaAgainst
	// recognise the per-window-reset empty base by a cheap bytes.Equal and skip
	// the redundant DeserializeCountMinSketchFromProtoBytes(prev) — which
	// otherwise re-materialises + zeroes a full d×w matrix on EVERY emit purely
	// to subtract zero (the profile's top flat cost). Lazily populated by
	// DeltaAgainstEmptyBase / ensureEmptyBaseBytes; nil in msgpack mode.
	// sampleP is the per-sketch sampling probability in (0,1]. 1.0 (the
	// default) disables sampling so the sketch is byte-identical to an
	// unsampled one. Set via WithSampleP; preserved across the
	// re-construction paths (Reset / Merge / ApplyDelta) so a sampled
	// wrapper stays sampled for its whole lifetime.
	sampleP float64

	emptyBaseBytes []byte

	// gosEpsilon/gosSites configure the GOS isotropic insert-time delta gate
	// (design-gos-unified-edge-telemetry.md §11, derivations §8.2): when
	// gosEpsilon>0, InsertHash checks each just-touched cell against the
	// closed-form threshold T=ε·N/k immediately (N = the sketch's current
	// total mass, tracked incrementally — see currentMass), in place of the
	// periodic decode-prev-diff sub-window model. gosEpsilon<=0 (the
	// default) leaves InsertHash/ComputeDeltaAgainst on the pre-existing
	// fixed-DeltaThreshold path, unchanged. Set via SetGosMode.
	gosEpsilon float64
	gosSites   uint32
	// gosDirty accumulates cells that crossed the insert-time GOS threshold
	// since the last drainGosDelta call. Each entry's Delta already equals
	// that cell's full accumulation since it was last sent (sketchlib zeroes
	// it in place at the moment of crossing), so no separate per-cell
	// accumulator is needed — draining is just serializing this list.
	gosDirty []cms.GOSCellUpdate
	// gosWake is armed on the FIRST cell added to gosDirty since the last
	// drain, and consumed exactly once by ConsumeWakeSignal — a burst of many
	// crossings between two flushes wakes the out-of-cycle flush loop once,
	// not once per crossing (the pending flush picks up everything
	// accumulated by the time it runs).
	gosWake bool
	// gosL1Baseline snapshots the sketch's own per-row L1 accumulator (w.sk.L1,
	// already incrementally maintained by sketchlib on every insert and
	// adjusted on every GOS reset) at the last drain, so drainGosDelta can
	// report each row's L1 change since then in O(rows) — cheap even though
	// it isn't itself insert-time-incremental, since rows is small (typically
	// ≤8), unlike the O(rows·cols) matrix scan this whole mechanism replaces.
	// CMS has no L2 (sketchlib-go#78 — a min-composed estimator can't validly
	// use sum-of-squares the way CountSketch's median-of-signed-rows does).
	gosL1Baseline []float64
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
// Dimension normalization (the signature can NOT return an error —
// callers in sibling modules, e.g. the edge processor's warm_sketch.go,
// use the value directly — so invalid dims are repaired here rather
// than rejected):
//   - cols is ROUNDED UP to the next power of two. sketchlib's CMS folds
//     inserts with `% cols` but masks/bit-slices the query hash assuming a
//     power-of-two width; a non-pow2 cols mis-indexes and can index out of
//     range. Rounding up keeps the column mask and the live column count
//     consistent (matches NewCountSketchWrapper's pow2 requirement, but
//     without breaking callers).
//   - rows is CLAMPED so rows*ceil(log2(cols)) <= 64; beyond that the
//     per-row hash slices overflow the single 64-bit item hash and the
//     extra rows silently degrade to zero width. See clampRowsForHashBits.
//
// Sampling is disabled (sampleP=1.0) by default — call WithSampleP to
// enable it. The default keeps the emitted wire bytes byte-identical to
// the pre-sampling format.
func NewCMSWrapper(rows, cols int, useMsgpack bool) *CMSWrapper {
	if cols < 1 {
		cols = 1
	}
	cols = nextPow2(cols)
	rows = clampRowsForHashBits(rows, cols)
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
	// sampleP rides on the SketchEnvelope, so the empty-base bytes change with
	// it; invalidate the memo so it is recomputed for the new probability.
	w.emptyBaseBytes = nil
	return w
}

// SetSampleP applies the sampling probability via WithSampleP, discarding the
// chained receiver so *CMSWrapper satisfies the runtime's precompute.SampleSetter
// interface (used by the coordinated-sampling path to stamp a coordinator-granted
// p onto a fresh window's wrapper).
func (w *CMSWrapper) SetSampleP(p float64) { w.WithSampleP(p) }

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

// SetGosMode configures the GOS isotropic insert-time delta gate. epsilon<=0
// disables it (fixed DeltaThreshold path, unchanged behavior). Idempotent —
// callers (the CMS factory, at series creation, and the runtime's
// applyGosMode, at flush, on every already-live series) may call this
// repeatedly with the same config; it just re-stamps the two scalars.
func (w *CMSWrapper) SetGosMode(epsilon float64, sites uint32) {
	w.gosEpsilon = epsilon
	w.gosSites = sites
}

// currentMass returns N, the sketch's current total (nonnegative) mass, in
// O(rows) via the sketch's own incrementally-maintained per-row L1
// accumulator (cms.CM_L1, which already takes the conservative min across
// rows — the same convention CMS's min-composed point query uses) — cheap
// enough to call on every insert, unlike an O(rows·cols) full-matrix scan.
func (w *CMSWrapper) currentMass() float64 {
	if w.sk == nil {
		return 0
	}
	return w.sk.CM_L1()
}

// GosDeltaThreshold computes the CMS isotropic GOS per-cell delta threshold
// T=ε·N/k (derivations §8.2) from the current sketch mass, rounded up to an
// integer (never below 1 = lossless). Returns 1 when ε<=0 (GOS disabled).
func (w *CMSWrapper) GosDeltaThreshold(epsilon float64, k uint32) uint64 {
	if epsilon <= 0 || w.sk == nil {
		return 1
	}
	t := CMSIsotropicThreshold(epsilon, w.currentMass(), k)
	if !math.IsInf(t, 1) && !math.IsNaN(t) && t > 1.0 {
		return uint64(math.Ceil(t))
	}
	return 1
}

// recordDirty appends newly-crossed cells to the pending GOS drain list and
// arms the wake signal on the first addition since the last drain.
func (w *CMSWrapper) recordDirty(cells []cms.GOSCellUpdate) {
	if len(cells) == 0 {
		return
	}
	if len(w.gosDirty) == 0 {
		w.gosWake = true
	}
	w.gosDirty = append(w.gosDirty, cells...)
}

// ConsumeWakeSignal implements the runtime's narrow wake-signal interface
// (asap-precompute-go window.go's recordLocked): reports whether an
// insert-time GOS threshold crossing happened since the last call, clearing
// the flag. Always false when GOS isotropic mode is inactive.
func (w *CMSWrapper) ConsumeWakeSignal() bool {
	if !w.gosWake {
		return false
	}
	w.gosWake = false
	return true
}

// drainGosDelta serializes the cells accumulated in gosDirty since the last
// drain as a sparse CMS delta — the insert-time counterpart of the old
// decode-prev-diff path (cms.ComputeDelta): the dirty list was already built
// cell-by-cell at insert time (InsertHash -> InsertWithHashGOS), so no
// previous snapshot needs decoding or scanning here. Returns (nil, false,
// nil) when nothing has crossed since the last drain — the caller
// (precompute.SnapshotCache.ComputeSubWindowDelta) treats a nil payload as
// "nothing to emit" (design-gos-unified-edge-telemetry.md §11: Gate 1's
// periodic divergence pre-check is redundant for a GOS-converted family — an
// empty dirty set at flush time already IS "nothing to send").
func (w *CMSWrapper) drainGosDelta() ([]byte, bool, error) {
	if len(w.gosDirty) == 0 {
		return nil, false, nil
	}
	d := &cms.Delta{
		Rows:  uint32(w.rows),
		Cols:  uint32(w.cols),
		Cells: make([]cms.CellDelta, len(w.gosDirty)),
		L1:    make([]float64, w.rows),
	}
	for i, c := range w.gosDirty {
		d.Cells[i] = cms.CellDelta{Row: c.Row, Col: c.Col, DValue: c.Delta}
	}
	w.gosDirty = w.gosDirty[:0]
	if len(w.gosL1Baseline) != w.rows {
		w.gosL1Baseline = make([]float64, w.rows)
	}
	for r := 0; r < w.rows; r++ {
		curL1 := w.sk.L1[r]
		d.L1[r] = curL1 - w.gosL1Baseline[r]
		w.gosL1Baseline[r] = curL1
	}
	payload, err := cms.SerializeDelta(d)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return payload, false, nil
}

// InsertHash mirrors the legacy CMS processor's
// `ws.cms.InsertWithHash(common.FromString(flowKey).Hash)` call.
// Adapter code feeds the encoded data-point attribute set as bytes
// here; the hash agrees with what the legacy processor computes from
// the same attrs because both go through common.FromString /
// common.FromBytes.
func (w *CMSWrapper) InsertHash(h uint64) {
	if w.sk == nil {
		return
	}
	if w.gosEpsilon > 0 {
		threshold := float64(w.GosDeltaThreshold(w.gosEpsilon, w.gosSites))
		w.recordDirty(w.sk.InsertWithHashGOS(h, 1.0, threshold))
		return
	}
	w.sk.InsertWithHash(h)
}

// ApplyAdmittedOccurrence applies a row-admission decision made UPSTREAM —
// typically by an OTel SDK running NitroSketch admission at Record() time,
// before the occurrence was ever serialized (metricdata.RowSampledSketch;
// see AggregationRowSampledSketch in the SDK). h is the SAME hash
// convention InsertHash takes (common.FromBytes/common.FromString);
// admittedRows is a bitmask over this sketch's rows (bit r set ⇒ row r
// admits); value is the occurrence's raw magnitude (1.0 for pure frequency
// counting) and sampleP is the admission probability in effect when the SDK
// made the decision.
//
// When GOS is active, this composes with insert-time detection the same way
// InsertHash does — the per-row update (InsertWithHashAtRowsGOS) checks each
// ADMITTED row's touched cell against threshold and reports/resets any
// crossing, so an SDK-row-sampled occurrence still participates in GOS
// rather than silently bypassing it.
//
// admittedRows == 0 (R(x)=∅) is a no-op — the SDK is expected to have
// already dropped such occurrences before they ever reached the wire.
//
// This does NOT touch w.sampleP or stamp anything on the envelope: the
// 1/sampleP correction is baked into the cell here (mirrors
// InsertWithHashSampledPerRow's "exact envelope, no double-correct"
// contract) — a query-time consumer must NOT also rescale by this sketch's
// envelope p, or the correction applies twice.
func (w *CMSWrapper) ApplyAdmittedOccurrence(h uint64, value float64, admittedRows uint64, sampleP float64) {
	if w.sk == nil {
		return
	}
	if w.gosEpsilon > 0 {
		threshold := float64(w.GosDeltaThreshold(w.gosEpsilon, w.gosSites))
		w.recordDirty(w.sk.InsertWithHashAtRowsGOS(h, value, admittedRows, sampleP, threshold))
		return
	}
	w.sk.InsertWithHashAtRows(h, value, admittedRows, sampleP)
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
	// GOS isotropic mode: cells were already detected + reset at insert time
	// (InsertHash -> InsertWithHashGOS), so the delta is just draining the
	// pending list — prev is never consulted (nothing to decode: the
	// mechanism doesn't need a "previous full state" reference at all).
	// Msgpack mode isn't GOS-converted (no delta transmission at all in that
	// mode), so it falls through to the existing full-snapshot path below.
	if w.gosEpsilon > 0 && !w.useMsgpack {
		return w.drainGosDelta()
	}
	if w.useMsgpack {
		full, err := w.Snapshot()
		return full, true, err
	}
	if len(prev) == 0 {
		full, err := w.Snapshot()
		return full, true, err
	}

	// Fast path (per-window-reset / empty-base contract): under PWR the cache
	// always hands back the snapshot of an EMPTY sketch as `prev` (see
	// DeltaAgainstEmptyBase). That base is deterministic for this shape, so we
	// recognise it by comparing against the memoized empty-base bytes — a cheap
	// O(len) bytes.Equal — and, when it matches, compute the delta DIRECTLY from
	// the current matrix's non-zero cells via ComputeDeltaAgainstEmpty. This
	// skips DeserializeCountMinSketchFromProtoBytes(prev), which would otherwise
	// re-materialise + zero a full d×w matrix every emit only to subtract zero
	// (the profile's dominant alloc + memclr cost). The emitted bytes are
	// byte-identical to the decode-and-diff path: ComputeDeltaAgainstEmpty
	// produces the same Delta as ComputeDelta(zeroSketch, current), and the
	// clamp below is unchanged.
	if eb := w.ensureEmptyBaseBytes(); eb != nil && bytes.Equal(prev, eb) {
		deltaMsg, err := cms.ComputeDeltaAgainstEmpty(w.sk, float64(threshold))
		if err == nil {
			payload, sErr := cms.SerializeDelta(deltaMsg)
			if sErr == nil {
				// Clamp: never emit a delta larger than the equivalent full frame.
				full, fErr := w.Snapshot()
				if fErr == nil && len(payload) >= len(full) {
					return full, true, nil
				}
				return payload, false, nil
			}
		}
		// On any failure, fall through to the standard decode-and-diff path,
		// which itself falls back to a full snapshot on error.
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
	// Clamp: never emit a delta larger than the equivalent full frame.
	full, fErr := w.Snapshot()
	if fErr == nil && len(payload) >= len(full) {
		return full, true, nil
	}
	return payload, false, nil
}

// ensureEmptyBaseBytes lazily computes and memoizes the SerializeProtoBytesFO
// of an empty sketch of this wrapper's shape + sampleP — the exact bytes the
// SnapshotCache caches as the outbound base under the PWR contract. Returns nil
// in msgpack mode (no delta transmission) or on serialization failure, in which
// case the empty-base fast path is simply skipped. The result is content-stable
// across a wrapper's lifetime (dimensions and sampleP never change after
// construction; WithSampleP would, but it invalidates the cache).
func (w *CMSWrapper) ensureEmptyBaseBytes() []byte {
	if w.useMsgpack {
		return nil
	}
	if w.emptyBaseBytes != nil {
		return w.emptyBaseBytes
	}
	empty := w.newSketch()
	if empty == nil {
		return nil
	}
	b, err := empty.SerializeProtoBytesFO()
	if err != nil {
		return nil
	}
	w.emptyBaseBytes = b
	return w.emptyBaseBytes
}

// DeltaAgainstEmptyBase returns the snapshot of an EMPTY CMS of the
// same dimensions (and sampling probability). The precompute.SnapshotCache
// caches this as the outbound base after each window-close emit
// (delta-baseline-contract.md §3): the next window's ComputeDeltaAgainst
// then diffs against this empty base, so the emitted delta is that
// window's own full per-cell matrix encoded as a delta — no cross-window
// subtraction.
//
// An empty CMS's SerializeProtoBytesFO is a non-empty envelope (it
// encodes the all-zero matrix + dimensions), so ComputeDeltaAgainst
// takes its decode-and-diff path rather than the len(prev)==0
// full-snapshot fallback. Msgpack mode does not support delta
// transmission, so the cache must keep its legacy always-refresh
// behavior there — return nil so the SnapshotCache treats this wrapper
// as a non-opted (legacy) family.
func (w *CMSWrapper) DeltaAgainstEmptyBase() ([]byte, error) {
	if w.useMsgpack {
		return nil, nil
	}
	// Reuse the memoized bytes so the base the cache stores and the bytes the
	// ComputeDeltaAgainst fast path compares against are guaranteed identical.
	if b := w.ensureEmptyBaseBytes(); b != nil {
		return b, nil
	}
	// ensureEmptyBaseBytes only returns nil on construction/serialization
	// failure; surface that as an error to preserve the prior contract.
	return nil, fmt.Errorf("cms.SerializeProtoBytesFO(empty): construction failed")
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
	w.gosDirty = nil
	w.gosWake = false
	w.gosL1Baseline = nil
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

// Observe routes a precompute.ObservationValue (KindBytes) into the wrapped
// CMS via InsertHash — or, when v.RowSampled, via ApplyAdmittedOccurrence
// with the SDK's pre-decided admission bitmask (value 1.0: this observer's
// sole purpose is frequency counting, matching InsertHash's implicit
// weight-1 semantics).
func (CMSObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*CMSWrapper)
	if !ok {
		return fmt.Errorf("CMSObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindBytes {
		return fmt.Errorf("CMSObserver: expected KindBytes, got %s", v.Kind)
	}
	h := common.FromBytes(v.Bytes).Hash
	if v.RowSampled {
		w.ApplyAdmittedOccurrence(h, 1.0, v.AdmittedRows, v.SampleP)
		return nil
	}
	w.InsertHash(h)
	return nil
}

// Compile-time assertions that CMSWrapper satisfies the trait surface
// for FrequencySketch implementations.
var (
	_ precompute.Sketch          = (*CMSWrapper)(nil)
	_ precompute.FrequencySketch = (*CMSWrapper)(nil)
	_ precompute.SketchObserver  = CMSObserver{}
)
