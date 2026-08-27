// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"fmt"
	"sort"

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
	// sparse selects the in-memory SPARSE base sketch
	// (hll.NewSparseHyperLogLog) instead of the dense one
	// (hll.NewHyperLogLog). It is preserved across every re-construction path
	// (Reset / Merge / ApplyDelta via newSketch) so a wrapper built sparse
	// stays sparse for its whole lifetime. The sparse base is API-compatible
	// by design — byte-identical serialization, interoperable Merge/ApplyDelta
	// with dense snapshots, and Reset returns to the empty sparse state — so
	// the wrapper drives it exclusively through the same public methods used
	// for the dense base and never touches the exported Registers field (which
	// would force dense materialization and defeat the memory win).
	sparse bool

	// gosTau configures the GOS register-change adapter's Layer-D insert-time
	// delta gate (sampling-cdm-gos-derivations.md §8.7, design-gos-unified-
	// edge-telemetry.md §11/§12 item 4): >0 routes every insert through the
	// per-register linearized-threshold check (recordGosCrossing) instead of
	// the pre-existing decode-prev-diff RegisterDelta path
	// (hll.ComputeRegisterDelta). <=0 (the default) leaves
	// UpdateValue/UpdateBytes/ComputeDeltaAgainst exactly as before.
	//
	// Knob mapping (a deliberate config-surface decision — see SetGosMode):
	// gosTau REUSES PrecomputeConfig.GosDeltaEpsilon, REINTERPRETED as τ (a
	// count of "doublings" of a register's linearized 2^C contribution)
	// rather than as CountSketch's ε relative-error budget. HLL and
	// CountSketch never share a live sketch instance, so overloading the one
	// generic float64 field with a per-SketchType meaning avoids adding a
	// second GOS config field for a single family. See config.go's
	// PrecomputeConfig.GosDeltaEpsilon doc for the full rationale.
	gosTau float64
	// gosLastSent[i] is the register value AT WHICH register i was last
	// reported to a caller (0 = never reported). This is per-register POLICY
	// state the wrapper must track that sketchlib-go's HyperLogLog does NOT:
	// unlike CountSketch (whose cell is reset to 0 on send, so "since last
	// sent" is always readable straight off the cell), an HLL register is
	// NEVER reset — so "how far has this register moved since it was last
	// sent" cannot be read off the register itself; only its raw CURRENT
	// value is available there. Lazily allocated (hll.HLLRegisterCount bytes)
	// on the first GOS-gated insert, so a non-GOS wrapper pays nothing extra.
	gosLastSent []uint8
	// gosDirty accumulates registers that crossed the insert-time GOS
	// threshold since the last drainGosDelta call. Each entry carries the
	// register's CURRENT value (never a subtractive delta — HLL deltas are
	// always "here is the value", since MAX-merge is idempotent and
	// monotone). Analogous to CountSketchWrapper.gosDirty, but — critically —
	// the underlying registers are NEVER reset when this list is drained; see
	// drainGosDelta.
	gosDirty []hll.RegisterUpdate
	// gosWake mirrors CountSketchWrapper.gosWake: armed on the first register
	// added to gosDirty since the last drain, consumed exactly once by
	// ConsumeWakeSignal so a burst of crossings between two flushes wakes the
	// out-of-cycle flush loop once, not once per crossing.
	gosWake bool
}

// SetGosMode configures the GOS register-change adapter (sampling-cdm-gos-
// derivations.md §8.7). tau<=0 disables it (the pre-existing decode-prev-diff
// RegisterDelta path, unchanged behavior). Idempotent — callers (the HLL
// factory, at series creation, and the runtime's applyGosMode, at flush, on
// every already-live series) may call this repeatedly with the same tau; it
// just re-stamps the scalar.
//
// sites is accepted (so *HLLWrapper has the SAME structural method shape,
// SetGosMode(float64, uint32), as *CountSketchWrapper — precompute.go's
// applyGosMode calls it via one generic interface assert with no
// SketchType-specific branching) but UNUSED: HLL's boxed formula
// |2^C'-2^C|>=2^τ has no multi-site k term the way CountSketch's isotropic F2
// threshold T=ε‖Ĉ‖/(2k√(dw)) does.
func (w *HLLWrapper) SetGosMode(tau float64, _ uint32) {
	w.gosTau = tau
}

// gosRegisterCrossed implements sampling-cdm-gos-derivations.md §8.7's boxed
// formula |2^C'-2^C|>=2^τ, where C=last (the value at which this register was
// last reported, 0 if never) and C'=cur (its current value after this
// insert), PLUS the first-nonzero-write mitigation the design doc proposes
// (§12 open item 4) for the small-cardinality regime: a register's
// first-ever nonzero write (last==0, cur>0) is always reported, regardless
// of τ. THAT MITIGATION IS EXPLICITLY UNVERIFIED per the design doc — it is
// a cheap, plausible fix, not a proven one.
//
// C, C' are small non-negative integers (<= hll.HLLRegisterBits+1, i.e. well
// under 64), so 1<<C is EXACT integer arithmetic here — never a floating
// math.Pow call, per the design doc.
func gosRegisterCrossed(last, cur uint8, tau float64) bool {
	if cur <= last {
		return false // MAX-merge is monotone non-decreasing; nothing to report
	}
	if last == 0 {
		return true // first-ever nonzero write: unconditional (unverified mitigation, see doc comment above)
	}
	diff := (uint64(1) << cur) - (uint64(1) << last)
	threshold := uint64(1)
	if tau > 0 {
		threshold = uint64(1) << uint(tau)
	}
	return diff >= threshold
}

// recordGosCrossing applies gosRegisterCrossed to a register that just
// mechanically changed (reported by InsertWithHashReportingChange /
// UpdateValueReportingChange), and — if it crosses — appends it to gosDirty
// and arms the wake signal. A register that changed but did NOT cross is
// left alone: its gosLastSent entry is untouched, so the next crossing check
// for that register is measured against the same last-sent baseline (the
// linearized gap keeps growing across subsequent inserts until it finally
// crosses, at which point the whole accumulated jump is reported in one
// shot and gosLastSent catches up to the just-reported value).
func (w *HLLWrapper) recordGosCrossing(index int, newVal uint8) {
	if w.gosLastSent == nil {
		w.gosLastSent = make([]uint8, hll.HLLRegisterCount)
	}
	last := w.gosLastSent[index]
	if !gosRegisterCrossed(last, newVal, w.gosTau) {
		return
	}
	w.gosLastSent[index] = newVal
	if len(w.gosDirty) == 0 {
		w.gosWake = true
	}
	w.gosDirty = append(w.gosDirty, hll.RegisterUpdate{Index: uint32(index), Value: newVal})
}

// ConsumeWakeSignal implements the runtime's narrow wake-signal interface
// (asap-precompute-go window.go's recordLocked): reports whether an
// insert-time GOS threshold crossing happened since the last call, clearing
// the flag. Always false when GOS mode is inactive (gosTau<=0, so
// recordGosCrossing/gosWake are never touched).
func (w *HLLWrapper) ConsumeWakeSignal() bool {
	if !w.gosWake {
		return false
	}
	w.gosWake = false
	return true
}

// drainGosDelta serializes the registers accumulated in gosDirty since the
// last drain as a sparse hll.RegisterDelta — the insert-time counterpart of
// the decode-prev-diff path (hll.ComputeRegisterDelta): each dirty entry was
// already individually threshold-checked at insert time (UpdateValue /
// UpdateBytes -> recordGosCrossing), so no previous snapshot needs decoding
// or diffing here.
//
// CRITICAL correctness difference from every other GOS-converted family in
// this workstream: this does NOT reset any register. HLL registers are
// monotone (MAX-merge) and must never regress; the underlying
// *hll.HyperLogLog is completely untouched by this call — only the
// pending-to-send LIST (gosDirty) is drained. Re-sending an unchanged or
// already-known value is harmless downstream (max(x,x)=x), which is exactly
// what makes this safe.
//
// A register that crossed more than once between two drains is de-duplicated
// to its LATEST (highest) value — monotonicity guarantees the latest value
// always supersedes any earlier one already queued — and the result is
// sorted ascending by register index: hll.SerializeRegisterDelta's
// varint-packed wire form requires strictly increasing indices.
func (w *HLLWrapper) drainGosDelta() ([]byte, bool, error) {
	if len(w.gosDirty) == 0 {
		return nil, false, nil
	}
	latest := make(map[uint32]uint8, len(w.gosDirty))
	for _, u := range w.gosDirty {
		latest[u.Index] = u.Value
	}
	updates := make([]hll.RegisterUpdate, 0, len(latest))
	for idx, val := range latest {
		updates = append(updates, hll.RegisterUpdate{Index: idx, Value: val})
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].Index < updates[j].Index })
	w.gosDirty = w.gosDirty[:0]
	payload, err := hll.SerializeRegisterDelta(&hll.RegisterDelta{Updates: updates})
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

// NewHLLWrapperSparse builds an empty HLL sketch backed by the in-memory
// SPARSE base (hll.NewSparseHyperLogLog) rather than the dense one. A
// low-cardinality warm series then holds only its observed registers instead of
// the dense ~16KB register array, so memory per series scales with cardinality
// until the base promotes itself to dense automatically.
//
// Everything else is identical to NewHLLWrapper: precision is the fixed
// hll.HLLPrecision = 14, sampling is disabled (sampleP=1.0) by default, and the
// emitted wire bytes are byte-identical to the dense wrapper for the same
// inputs (the sparse base serializes to the same proto). The sparse flag is
// stamped on the wrapper so Reset / Merge / ApplyDelta rebuild a sparse base.
func NewHLLWrapperSparse() *HLLWrapper {
	w := &HLLWrapper{sampleP: 1.0, sparse: true}
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
// configured sampling probability and base representation. Centralises the
// construction so every re-creation path (New / Reset / Merge / ApplyDelta)
// keeps both sampleP and the sparse/dense choice: a sparse wrapper rebuilds a
// sparse base on Reset (and when Merge/ApplyDelta lazily re-init a nil base),
// so it never silently reverts to the dense ~16KB footprint.
func (w *HLLWrapper) newSketch() *hll.HyperLogLog {
	var sk *hll.HyperLogLog
	if w.sparse {
		sk = hll.NewSparseHyperLogLog()
	} else {
		sk = hll.NewHyperLogLog()
	}
	if sk != nil && w.sampleP > 0 && w.sampleP < 1.0 {
		sk.WithSampleP(w.sampleP)
	}
	return sk
}

// UpdateValue feeds a single observation into the underlying HLL
// sketch via UpdateValue (matching legacy hllprocessor's batch and
// window paths that call bs.sketch.UpdateValue(dp.DoubleValue())).
func (w *HLLWrapper) UpdateValue(v float64) {
	if w.sk == nil {
		return
	}
	if w.gosTau > 0 {
		idx, _, newVal, changed := w.sk.UpdateValueReportingChange(v)
		if changed {
			w.recordGosCrossing(idx, newVal)
		}
		return
	}
	w.sk.UpdateValue(v)
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
	hash := common.FromBytes(b).Hash
	if w.gosTau > 0 {
		idx, _, newVal, changed := w.sk.InsertWithHashReportingChange(hash)
		if changed {
			w.recordGosCrossing(idx, newVal)
		}
		return
	}
	w.sk.InsertWithHash(hash)
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
	// GOS mode: registers were already detected + threshold-checked at
	// insert time (UpdateValue/UpdateBytes -> recordGosCrossing), so the
	// delta is just draining the pending list — prev is never consulted
	// (nothing to decode: the mechanism doesn't need a "previous full
	// state" reference at all). Mirrors CountSketchWrapper's GOS bypass.
	if w.gosTau > 0 {
		return w.drainGosDelta()
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
//
// Also clears the GOS per-window bookkeeping (gosLastSent/gosDirty/gosWake,
// mirroring CountSketchWrapper.Reset): the fresh sketch's registers all
// start at 0, so "last sent" must restart at 0 too — a new window is a new,
// independent register set, consistent with the pre-existing per-window
// (PWR) HLL delta model. gosTau itself is NOT cleared: it is a mode
// configuration, not per-window accumulation state, and survives rotation
// exactly like gosEpsilon does on CountSketchWrapper.
func (w *HLLWrapper) Reset() {
	w.sk = w.newSketch()
	w.gosLastSent = nil
	w.gosDirty = nil
	w.gosWake = false
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
