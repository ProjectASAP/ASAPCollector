// Copyright The OpenTelemetry Authors
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

// Update feeds a single observation into the underlying KLL sketch.
func (w *KLLWrapper) Update(v float64) {
	if w.sk != nil {
		w.sk.Update(v)
	}
}

// Snapshot serializes via SerializePortable + proto.Marshal — the
// canonical wire format the backend's modified-OTLP KLL decoder
// expects (matching DeserializeKLLSketchFromProtoBytes).
func (w *KLLWrapper) Snapshot() ([]byte, error) {
	if w.sk == nil || w.sk.GetSize() == 0 {
		return nil, nil
	}
	env, err := w.sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("kll.SerializePortable: %w", err)
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
// the same (k, seed) so deterministic seeds replay identically.
func (w *KLLWrapper) Reset() {
	w.sk = buildKLL(w.k, w.seed)
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
