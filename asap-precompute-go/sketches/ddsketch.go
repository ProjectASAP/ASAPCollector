// Copyright ProjectASAP Authors
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
	"math"

	ddpb "github.com/ProjectASAP/sketchlib-go/proto/ddsketch"
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
// ddSampleSeed is the fixed seed handed to sketchlib-go's geometric sampler so
// the admitted subset is reproducible across runs.
const ddSampleSeed int64 = 0x4444_5350 // "DDSP"

type DDSketchWrapper struct {
	sk    *ddsketch.DDSketch
	alpha float64
	// sampleP is the warm-sketch sampling probability in (0,1]. 1.0 (default)
	// disables sampling so the sketch is byte-identical to an unsampled one.
	// Preserved across Reset so a sampled wrapper stays sampled for its life.
	sampleP float64
	// externalSampling is true after the wrapper receives an SDK-admitted
	// occurrence in the current window. Its p must remain constant for that
	// window or one envelope would describe a mixture of sampling rates.
	externalSampling bool

	// gosEpsilon/gosSites configure the GOS isotropic insert-time delta gate
	// (design-gos-unified-edge-telemetry.md §11): when gosEpsilon>0, Update
	// checks the just-touched bucket against the closed-form threshold
	// T=ε·N/(k·B) (derivations §8.4) immediately, in place of the periodic
	// decode-prev-diff delta model. gosEpsilon<=0 (the default) leaves
	// Update/ComputeDeltaAgainst on the pre-existing ComputeDelta path,
	// unchanged. Set via SetGosMode.
	gosEpsilon float64
	gosSites   uint32
	// gosDirty accumulates buckets that crossed the insert-time GOS
	// threshold since the last drainGosDelta call. Each entry's Count
	// already equals that bucket's full accumulation since it was last sent
	// (sketchlib zeroes it in place at the moment of crossing), so no
	// separate per-bucket accumulator is needed — draining is just
	// serializing this list.
	gosDirty []ddsketch.DDSketchGOSUpdate
	// gosWake is armed on the FIRST bucket added to gosDirty since the last
	// drain, and consumed exactly once by ConsumeWakeSignal — a burst of many
	// crossings between two flushes wakes the out-of-cycle flush loop once,
	// not once per crossing (the pending flush picks up everything
	// accumulated by the time it runs).
	gosWake bool
}

// NewDDSketchWrapper builds an empty DDSketch with the configured
// relative-accuracy alpha. Callers must keep alpha within (0, 1);
// sketchlib-go's NewDDSketch panics otherwise. Sampling is disabled by
// default — call WithSampleP to enable it.
func NewDDSketchWrapper(alpha float64) *DDSketchWrapper {
	return &DDSketchWrapper{sk: ddsketch.NewDDSketch(alpha), alpha: alpha, sampleP: 1.0}
}

// RelativeAccuracy returns the DDSketch alpha (relative accuracy) this
// wrapper was built with. The OTel encoder stamps it onto the emitted
// pmetric.DDSketch container's relative_accuracy field so the backend
// records a non-zero ε on registration. Without it the container defaults
// to 0.0 — a degenerate sketch the backend can't answer quantiles from, so
// `quantile_over_time(...)` capability-misses to the archive and returns
// empty. The standalone ddsketchprocessor sets this via its config; the
// fused asap_edge path lost it because the runtime envelope didn't carry it.
func (w *DDSketchWrapper) RelativeAccuracy() float64 { return w.alpha }

// WithSampleP enables NitroSketch geometric skip-sampling at probability p in
// (0,1]. p>=1 (or NaN) disables sampling (exact, the default). Unlike HLL's
// hash-threshold sampling, the skip decision is value-independent, so a skipped
// value avoids the whole record (bucket-index mapping + store grow + increment)
// — the warm-path CPU saving is real. Quantiles are rank-preserving and need no
// rescale; a total-count query rescales ×1/p (sample_p rides on the envelope).
func (w *DDSketchWrapper) WithSampleP(p float64) *DDSketchWrapper {
	if p >= 1.0 || p != p { // p != p ⇒ NaN
		w.sampleP = 1.0
	} else {
		w.sampleP = p
	}
	if w.sk != nil {
		w.sk.WithSampleP(w.sampleP, ddSampleSeed)
	}
	return w
}

// SetSampleP applies the sampling probability via WithSampleP, discarding the
// chained receiver so *DDSketchWrapper satisfies precompute.SampleSetter (the
// coordinated-sampling stamp path).
func (w *DDSketchWrapper) SetSampleP(p float64) { w.WithSampleP(p) }

// SampleP returns the configured sampling probability (1.0 when disabled).
func (w *DDSketchWrapper) SampleP() float64 {
	if w.sampleP <= 0 {
		return 1.0
	}
	return w.sampleP
}

// SetGosMode configures the GOS isotropic insert-time delta gate. epsilon<=0
// disables it (fixed ComputeDelta path, unchanged behavior). Idempotent —
// callers (the DDSketch factory, at series creation, and the runtime's
// applyGosMode, at flush, on every already-live series) may call this
// repeatedly with the same config; it just re-stamps the two scalars.
func (w *DDSketchWrapper) SetGosMode(epsilon float64, sites uint32) {
	w.gosEpsilon = epsilon
	w.gosSites = sites
}

// GosDeltaThreshold computes the DDSketch isotropic GOS insert-time bucket
// threshold T=ε·N/(k·B) (derivations §8.4) from the sketch's current total
// count (N) and incrementally-tracked populated-bucket count (B), rounded up
// to an integer (never below 1 = ship-on-first-touch, the "cold start is a
// feature" floor). Returns 1 when ε<=0 or the sketch is nil (GOS disabled /
// unusable) — mirrors CountSketchWrapper.GosDeltaThreshold's convention.
func (w *DDSketchWrapper) GosDeltaThreshold(epsilon float64, k uint32) uint64 {
	if epsilon <= 0 || w.sk == nil {
		return 1
	}
	t := DDSketchIsotropicThreshold(epsilon, float64(w.sk.Count()), k, w.sk.PopulatedBuckets())
	if !math.IsInf(t, 1) && !math.IsNaN(t) && t > 1.0 {
		return uint64(math.Ceil(t))
	}
	return 1
}

// recordDirty appends a newly-crossed bucket to the pending GOS drain list
// and arms the wake signal on the first addition since the last drain.
func (w *DDSketchWrapper) recordDirty(u ddsketch.DDSketchGOSUpdate) {
	if len(w.gosDirty) == 0 {
		w.gosWake = true
	}
	w.gosDirty = append(w.gosDirty, u)
}

// ConsumeWakeSignal implements the runtime's narrow wake-signal interface
// (asap-precompute-go window.go's recordLocked): reports whether an
// insert-time GOS threshold crossing happened since the last call, clearing
// the flag. Always false when GOS isotropic mode is inactive.
func (w *DDSketchWrapper) ConsumeWakeSignal() bool {
	if !w.gosWake {
		return false
	}
	w.gosWake = false
	return true
}

// drainGosDelta serializes the buckets accumulated in gosDirty since the
// last drain as a proto-marshalled DDSketchDelta — the insert-time
// counterpart of the old decode-prev-diff path (ddsketch.ComputeDelta): the
// dirty list was already built bucket-by-bucket at insert time
// (Update -> sk.UpdateGOS), so no previous snapshot needs decoding or
// scanning here. Returns (nil, false, nil) when nothing has crossed since
// the last drain — the caller (precompute.SnapshotCache.ComputeSubWindowDelta)
// treats a nil payload as "nothing to emit" (design-gos-unified-edge-
// telemetry.md §11: Gate 1's periodic divergence pre-check is redundant for
// a GOS-converted family — an empty dirty set at flush time already IS
// "nothing to send").
func (w *DDSketchWrapper) drainGosDelta() ([]byte, bool, error) {
	if len(w.gosDirty) == 0 {
		return nil, false, nil
	}
	delta := &ddpb.DDSketchDelta{Buckets: make([]*ddpb.DDSketchBucketDelta, len(w.gosDirty))}
	for i, u := range w.gosDirty {
		delta.Buckets[i] = &ddpb.DDSketchBucketDelta{Index: u.Index, DCount: u.Count}
	}
	w.gosDirty = w.gosDirty[:0]
	payload, err := proto.Marshal(delta)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return payload, false, nil
}

// Update feeds a single observation into the underlying DDSketch.
// Used by DDSketchObserver; exposed publicly so adapter code that
// already has a typed handle can bypass the observer interface.
func (w *DDSketchWrapper) Update(v float64) {
	if w.gosEpsilon > 0 {
		threshold := w.GosDeltaThreshold(w.gosEpsilon, w.gosSites)
		if crossed, upd := w.sk.UpdateGOS(v, threshold); crossed {
			w.recordDirty(upd)
		}
		return
	}
	w.sk.Update(v)
}

// ApplyAdmittedOccurrence applies a d=1 admission decision already made by
// the source SDK. It disables the wrapper's internal sampler to avoid sampling
// the same occurrence twice, records the admitted value once, and stamps p on
// the wire envelope. DDSketch quantiles use the admitted empirical
// distribution directly; count-like consumers rescale the raw count by 1/p.
func (w *DDSketchWrapper) ApplyAdmittedOccurrence(v float64, admittedRows uint64, sampleP float64) error {
	if admittedRows != 1 {
		return fmt.Errorf("DDSketchWrapper: admitted_rows must be 1 for a one-row sketch, got %#x", admittedRows)
	}
	if sampleP <= 0 || sampleP > 1 || math.IsNaN(sampleP) {
		return fmt.Errorf("DDSketchWrapper: sample_p must be in (0,1], got %v", sampleP)
	}
	if w.externalSampling && w.sampleP != sampleP {
		return fmt.Errorf("DDSketchWrapper: sample_p changed within a window: %v to %v", w.sampleP, sampleP)
	}
	w.sampleP = sampleP
	w.externalSampling = true
	w.sk.WithSampleP(1, ddSampleSeed)
	w.sk.SetWireSampleP(sampleP)
	w.Update(v)
	return nil
}

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
	// GOS isotropic mode: buckets were already detected + reset at insert
	// time (Update -> sk.UpdateGOS), so the delta is just draining the
	// pending list — prev is never consulted (nothing to decode: the
	// mechanism doesn't need a "previous full state" reference at all).
	if w.gosEpsilon > 0 {
		return w.drainGosDelta()
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
	// Clamp: never emit a delta that isn't strictly smaller than the full
	// frame. A DDSketch full state packs bucket counts as a contiguous
	// positional array (no per-bucket index), so on dense buckets a sparse
	// indexed delta can be larger; emit the full frame in that case so a delta
	// is never larger than the equivalent full frame at the same cadence.
	full, fErr := w.Snapshot()
	if fErr == nil && len(delta) >= len(full) {
		return full, true, nil
	}
	return delta, false, nil
}

// DeltaAgainstEmptyBase returns the snapshot of an EMPTY DDSketch of
// the same relative-accuracy alpha. The precompute.SnapshotCache caches
// this as the outbound base after each window-close emit
// (delta-baseline-contract.md §3): the next window's ComputeDeltaAgainst
// then diffs against this empty base, so the emitted delta is that
// window's own full per-window bucket store encoded as a delta — no
// cross-window subtraction.
//
// We encode the empty sketch's envelope (rather than returning empty
// bytes) so ComputeDeltaAgainst takes its decode-and-diff path instead
// of the len(prev)==0 full-snapshot fallback.
func (w *DDSketchWrapper) DeltaAgainstEmptyBase() ([]byte, error) {
	env, err := ddsketch.NewDDSketch(w.alpha).SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("ddsketch.SerializePortable(empty): %w", err)
	}
	return proto.Marshal(env)
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
	} else {
		w.sk.Clear()
	}
	// Re-apply sampling so a sampled wrapper stays sampled across window resets.
	if w.sampleP > 0 && w.sampleP < 1.0 {
		w.sk.WithSampleP(w.sampleP, ddSampleSeed)
	}
	w.externalSampling = false
	// gosEpsilon/gosSites are per-series CONFIG (survive resets, like
	// sampleP); gosDirty/gosWake are per-WINDOW state that must not leak into
	// the next window (sk.Clear() above already zeroed d.gosPopulated/d.count
	// on the underlying sketch, so a threshold computed after this Reset
	// starts fresh too).
	w.gosDirty = nil
	w.gosWake = false
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

// LinearReadout realizes the additive "linear functional over DDSketch buckets"
// monitor functional (monitor.FunctionalLinearBuckets) in its MONOTONE,
// non-negative form: a VALUE-RANGE COUNT — the number of recorded samples whose
// bucket value lies in [lo, hi]. A value-range count is an additive
// non-negative aggregate, so it is monotone non-decreasing within a tumbling
// window (bucket counts only grow), which is exactly what the slack-countdown
// monitoring protocol requires.
//
// v1 convention: the coeffs slice carries the value bounds —
//
//	coeffs[0]            → lo
//	coeffs[1] (optional) → hi (defaults to +Inf, i.e. "count of samples ≥ lo")
//
// An empty slice counts nothing. The generic signed / positional linear
// combination the cost-analysis doc also describes (the DDSketch
// quantile-threshold form with a negative coefficient, and Count-Sketch's
// signed cells) is NON-monotone and intentionally NOT implemented here;
// monitor.Spec.Validate already rejects negative coefficients so such specs
// never reach this path.
func (w *DDSketchWrapper) LinearReadout(coeffs []float64) float64 {
	if w.sk == nil || len(coeffs) == 0 {
		return 0
	}
	lo := coeffs[0]
	hi := math.Inf(1)
	if len(coeffs) >= 2 {
		hi = coeffs[1]
	}
	var total float64
	w.sk.EachBucket(func(k int32, count uint64) {
		v := w.sk.BucketValue(k)
		if v >= lo && v <= hi {
			total += float64(count)
		}
	})
	return total
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
		if v.RowSampled {
			return w.ApplyAdmittedOccurrence(v.Float, v.AdmittedRows, v.SampleP)
		}
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
