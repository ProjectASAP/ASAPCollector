// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"fmt"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"google.golang.org/protobuf/proto"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// KLLWrapper adapts a sketchlib-go *kll.KLLSketch to the
// precompute.QuantileSketch interface so a precompute.Precompute can
// own it as a generic Sketch.
//
// Determinism: the wrapper accepts a nullable seed. When seed is nil
// the constructor uses sketchlib-go's time-seeded path (the production
// default). When seed is set, NewKLLSketchWithSeed is used so two
// processors fed identical input produce byte-identical sketch state
// (parity harness, deterministic-replay tests).
//
// Byte-format invariant: SerializePortable + proto.Marshal produces
// the same envelope bytes the legacy KLL processor emitted via
// serializeKLLSketch, so wire payloads stay byte-identical
// pre/post-refactor (ADR-0002 §"Behavior preservation").
type KLLWrapper struct {
	sk   *kll.KLLSketch
	k    int
	seed *int64

	// windowTotal is N — the WINDOW's running observation count since the
	// window itself started (design-gos-unified-edge-telemetry.md §11;
	// derivations doc §8.6 "Emit trigger"). Unlike every other piece of
	// state in this wrapper, it must NOT be reset when the sketch resets
	// on a GOS-triggered segment emit (Reset() below): the trigger
	// R>=epsilon*N compares the just-reset segment's own count R
	// (w.sk.Count(), which DOES zero on Reset) against this window-
	// lifetime total, so the two counters have to live independently. A
	// window BOUNDARY rotation (as opposed to a mid-window segment reset)
	// never calls Reset() on a live wrapper at all — the runtime discards
	// the whole seriesEntry and builds a brand-new wrapper via the
	// factory for the next window (precompute.go's finishRotate) — so
	// windowTotal only ever needs to survive the mid-window segment
	// Reset(), never an explicit zeroing of its own.
	windowTotal uint64
	// gosEpsilon configures the GOS insert-time emit trigger; <=0 (the
	// default) disables it and leaves Update byte-identical to before.
	// Unlike CountSketchWrapper's SetGosMode, KLL's formula (R>=epsilon*N)
	// has no per-cell/sites term, so there is no second scalar to store.
	gosEpsilon float64
	// gosWake is armed the first time an insert's R>=epsilon*N check
	// crosses since the last ConsumeWakeSignal, and consumed exactly once
	// — mirrors CountSketchWrapper.gosWake (countsketch.go): a burst of
	// crossings between two flushes wakes the out-of-cycle flush loop
	// once, not once per insert.
	gosWake bool
}

// NewKLLWrapper builds an empty KLL sketch honoring the provided
// (k, seed). seed may be nil for the time-seeded production default.
func NewKLLWrapper(k int, seed *int64) *KLLWrapper {
	return &KLLWrapper{sk: buildKLL(k, seed), k: k, seed: seed}
}

// buildKLL constructs a fresh sketchlib-go KLL respecting the seed
// nullability contract.
func buildKLL(k int, seed *int64) *kll.KLLSketch {
	if seed != nil {
		sk, err := kll.NewKLLSketchWithSeed(k, *seed)
		if err != nil {
			return nil
		}
		return sk
	}
	sk, err := kll.NewKLLSketch(k)
	if err != nil {
		return nil
	}
	return sk
}

// Update feeds a single observation into the underlying KLL sketch, then
// (when GOS mode is enabled) checks the insert-time emit trigger R>=epsilon*N
// — derivations doc §8.6 "Emit trigger" — where R is the sketch's own
// since-last-reset item count (Count(), which zeros on the segment Reset()
// below) and N is windowTotal, the window-lifetime count that never resets.
// A crossing arms gosWake; it does NOT emit or reset here — that still
// happens later through the existing EmitSubWindow/segment-mode path
// (precompute.go), which the wake only requests out-of-cycle.
func (w *KLLWrapper) Update(v float64) {
	if w.sk == nil {
		return
	}
	w.sk.Update(v)
	w.windowTotal++
	if w.gosEpsilon > 0 {
		r := float64(w.sk.Count())
		n := float64(w.windowTotal)
		if r >= w.gosEpsilon*n {
			w.gosWake = true
		}
	}
}

// SetGosMode configures the GOS insert-time emit trigger. epsilon<=0
// disables it (Update behaves exactly as before: segment resets remain
// driven solely by the pre-existing SubWindowInterval periodic tick's
// external-count check). Idempotent — callers (the KLL factory, at series
// creation, and the runtime's applyGosMode, at flush, on every already-live
// series) may call this repeatedly with the same epsilon; it just re-stamps
// the scalar. No `k`/sites parameter: derivations doc §8.6's R>=epsilon*N
// trigger has no per-cell/sites term, unlike CountSketch's threshold.
func (w *KLLWrapper) SetGosMode(epsilon float64) {
	w.gosEpsilon = epsilon
}

// ConsumeWakeSignal implements the runtime's narrow wake-signal interface
// (asap-precompute-go window.go's wakeSignaler / recordLocked): reports
// whether an insert-time GOS threshold crossing (R>=epsilon*N) happened
// since the last call, clearing the flag. Always false when GOS mode is
// inactive.
func (w *KLLWrapper) ConsumeWakeSignal() bool {
	if !w.gosWake {
		return false
	}
	w.gosWake = false
	return true
}

// Snapshot serializes via SerializePortableRawF64 + proto.Marshal — the
// raw-f64 items[] wire format the backend's modified-OTLP KLL decoder
// expects (matching DeserializeKLLSketchFromProtoBytes).
//
// We deliberately use the RAW-F64 form, NOT the value-offset fixed-point
// form (SerializePortable). The fixed-point encoding (offset/value_scale/
// residuals, KLLState fields 7–9) is a newer bandwidth optimization that
// leaves items[] empty; the ASAPQuery backend's KLL decoder does not yet
// understand those fields, so a fixed-point frame decodes as a degenerate /
// empty sketch (observed in the field as `KllState.k must be >= 8 (got 0)`
// for integer-valued metrics such as http_requests_total_latency_ms, whose
// exact-integer samples always trip the fixed-point path while continuous
// double metrics like request_size_bytes mostly stay on the raw-f64 path).
// Forcing raw-f64 keeps EVERY emitted KLL frame in the format the backend
// reconstructs correctly. Empty windows still emit nothing (nil payload).
func (w *KLLWrapper) Snapshot() ([]byte, error) {
	if w.sk == nil || w.sk.GetSize() == 0 {
		return nil, nil
	}
	env, err := w.sk.SerializePortableRawF64()
	if err != nil {
		return nil, fmt.Errorf("kll.SerializePortableRawF64: %w", err)
	}
	return proto.Marshal(env)
}

// ComputeDeltaAgainst always returns the full snapshot. KLL uses
// random compaction and is not additively mergeable, so delta
// transmission is not defined for KLL sketches; the legacy KLL
// processor explicitly rejects DeltaTransmission=true.
func (w *KLLWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

// ApplyDelta deserializes a full proto-encoded KLL payload (the
// only encoding we accept inbound) and merges it into this sketch.
// The runtime's mergeFullEnvelope path constructs a temp sketch and
// calls ApplyDelta(payload) before Merge-ing; we make ApplyDelta
// mean "load full state into self via merge" which is the only
// inbound path KLL supports.
func (w *KLLWrapper) ApplyDelta(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	other, err := kll.DeserializeKLLSketchFromProtoBytes(payload)
	if err != nil {
		return fmt.Errorf("kll.DeserializeKLLSketchFromProtoBytes: %w", err)
	}
	if w.sk == nil {
		w.sk = buildKLL(w.k, w.seed)
	}
	return w.sk.Merge(other)
}

// Merge folds another KLLWrapper into this one. The runtime only
// ever calls Merge between sketches owned by the same Precompute
// (same k), so the type assertion is safe.
func (w *KLLWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*KLLWrapper)
	if !ok {
		return fmt.Errorf("KLLWrapper: Merge with %T", other)
	}
	if o.sk == nil {
		return nil
	}
	if w.sk == nil {
		w.sk = buildKLL(w.k, w.seed)
	}
	return w.sk.Merge(o.sk)
}

// Reset zeros the sketch in place by rebuilding from scratch with
// the same (k, seed) so deterministic seeds replay identically. This is the
// disjoint-SEGMENT reset (precompute.go's subWindowSegmentMode/EmitSubWindow):
// called mid-window, right after a segment has been successfully emitted, so
// the next segment starts fresh. windowTotal is DELIBERATELY left untouched —
// it tracks the window's lifetime count N, a different quantity from the
// segment's own R (=Count()), which this rebuild does zero. gosWake is
// cleared since the segment that armed it has just been emitted-and-reset.
func (w *KLLWrapper) Reset() {
	w.sk = buildKLL(w.k, w.seed)
	w.gosWake = false
}

// Quantile returns the q-th rank value via the KLL CDF query. q is
// clamped to [0,1] per the precompute.QuantileSketch contract before
// querying.
func (w *KLLWrapper) Quantile(q float64) float64 {
	if w.sk == nil || w.sk.GetSize() == 0 {
		return 0
	}
	return w.sk.CDF().Query(clampQuantile(q))
}

// Count returns the underlying sketch's accumulated sample count.
// Used by adapter encode-quantile paths so emitted gauges advertise
// the same dp.SetCount() the legacy emit set.
func (w *KLLWrapper) Count() int {
	if w.sk == nil {
		return 0
	}
	return w.sk.Count()
}

// KLLObserver implements precompute.SketchObserver for KindFloat
// observations: the legacy processor's accumulateGaugeMetric called
// sk.Update(double); inbound KLLSketch envelopes (KindEnvelope) are
// routed through Precompute.ObserveEnvelope by the runtime and never
// reach this observer.
type KLLObserver struct{}

// Observe routes a precompute.ObservationValue into the wrapped KLL
// sketch via Update.
func (KLLObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*KLLWrapper)
	if !ok {
		return fmt.Errorf("KLLObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.Update(v.Float)
		return nil
	default:
		return fmt.Errorf("KLLObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that KLLWrapper satisfies the trait surface
// for QuantileSketch implementations.
var (
	_ precompute.Sketch         = (*KLLWrapper)(nil)
	_ precompute.QuantileSketch = (*KLLWrapper)(nil)
	_ precompute.SketchObserver = KLLObserver{}
)
