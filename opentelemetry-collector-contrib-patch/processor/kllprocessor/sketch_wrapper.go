// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllprocessor

import (
	"fmt"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"google.golang.org/protobuf/proto"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// kllSketchWrapper adapts a sketchlib-go *kll.KLLSketch to the
// precompute.QuantileSketch interface so a precompute.Precompute can
// own it as a generic Sketch. This is the production wrapper consumed
// by the KLL processor shim; the parity harness has its own test-only
// wrapper at integration/parity/harness/sketches.go that shares the
// same shape but lives outside the production import graph.
//
// Determinism: the wrapper accepts a nullable seed. When seed is nil
// the constructor uses sketchlib-go's time-seeded path (the production
// default). When seed is set, NewKLLSketchWithSeed is used so two
// processors fed identical input produce byte-identical sketch state
// (parity harness, deterministic-replay tests).
//
// Byte-format invariant: SerializePortable + proto.Marshal produces
// the same envelope bytes the legacy processor emitted via
// serializeKLLSketch, so wire payloads stay byte-identical
// pre/post-refactor (ADR-0002 §"Behavior preservation").
type kllSketchWrapper struct {
	sk   *kll.KLLSketch
	k    int
	seed *int64
}

// newKLLSketchWrapper builds an empty KLL sketch honoring cfg.Seed.
// Mirrors the legacy newKLLSketch helper but returns the wrapper
// implementing precompute.QuantileSketch.
func newKLLSketchWrapper(k int, seed *int64) *kllSketchWrapper {
	return &kllSketchWrapper{sk: buildKLL(k, seed), k: k, seed: seed}
}

// buildKLL constructs a fresh sketchlib-go KLL respecting the seed
// nullability contract documented on Config.Seed.
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

// update feeds a single observation into the underlying KLL sketch.
// Used by the shim's SketchObserver.
func (w *kllSketchWrapper) update(v float64) {
	if w.sk != nil {
		w.sk.Update(v)
	}
}

// Snapshot serializes via SerializePortable + proto.Marshal — the
// canonical wire format the backend's modified-OTLP KLL decoder
// expects (matching DeserializeKLLSketchFromProtoBytes).
func (w *kllSketchWrapper) Snapshot() ([]byte, error) {
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
// processor explicitly rejects DeltaTransmission=true (see
// Config.Validate).
func (w *kllSketchWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

// ApplyDelta deserializes a full proto-encoded KLL payload (the
// only encoding we accept inbound) and merges it into this sketch.
// The runtime's mergeFullEnvelope path constructs a temp sketch and
// calls ApplyDelta(payload) before Merge-ing; we make ApplyDelta
// mean "load full state into self via merge" which is the only
// inbound path KLL supports.
func (w *kllSketchWrapper) ApplyDelta(payload []byte) error {
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

// Merge folds another kllSketchWrapper into this one. The runtime
// only ever calls Merge between sketches owned by the same Precompute
// (same k), so the type assertion is safe.
func (w *kllSketchWrapper) Merge(other precompute.Sketch) error {
	if other == nil {
		return nil
	}
	o, ok := other.(*kllSketchWrapper)
	if !ok {
		return fmt.Errorf("kllSketchWrapper: Merge with %T", other)
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
func (w *kllSketchWrapper) Reset() {
	w.sk = buildKLL(w.k, w.seed)
}

// Quantile returns the q-th rank value via the KLL CDF query.
func (w *kllSketchWrapper) Quantile(q float64) float64 {
	if w.sk == nil || w.sk.GetSize() == 0 {
		return 0
	}
	return w.sk.CDF().Query(q)
}

// count returns the underlying sketch's accumulated sample count.
// Used by the shim's encode-quantile path so emitted gauges advertise
// the same dp.SetCount() the legacy emit set.
func (w *kllSketchWrapper) count() int {
	if w.sk == nil {
		return 0
	}
	return w.sk.Count()
}

// kllSketchObserver implements precompute.SketchObserver for KindFloat
// observations: the legacy processor's accumulateGaugeMetric called
// sk.Update(double); inbound KLLSketch envelopes (KindEnvelope) are
// routed through Precompute.ObserveEnvelope by the runtime and never
// reach this observer.
type kllSketchObserver struct{}

func (kllSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*kllSketchWrapper)
	if !ok {
		return fmt.Errorf("kllSketchObserver: sketch is %T", s)
	}
	switch v.Kind {
	case precompute.KindFloat:
		w.update(v.Float)
		return nil
	default:
		return fmt.Errorf("kllSketchObserver: unsupported value kind %s", v.Kind)
	}
}

// Compile-time assertions that kllSketchWrapper satisfies the trait
// surface ADR-0002 / PR #224 pinned for QuantileSketch implementations.
var (
	_ precompute.Sketch         = (*kllSketchWrapper)(nil)
	_ precompute.QuantileSketch = (*kllSketchWrapper)(nil)
	_ precompute.SketchObserver = kllSketchObserver{}
)

// newKLLSketch is retained as a thin helper around buildKLL so the
// existing TestRoundTripIngestProtoSketch (PR #222) keeps compiling
// without rewriting test logic. Production code paths construct
// sketches via newKLLSketchWrapper / kllSketchObserver.
func newKLLSketch(cfg *Config) *kll.KLLSketch {
	return buildKLL(cfg.K, cfg.Seed)
}

// serializeKLLSketch is retained for the same reason as newKLLSketch:
// the existing round-trip test calls it directly. The implementation
// matches the legacy emit-side serializer (SerializePortable +
// proto.Marshal) so the wire bytes stay byte-identical.
func serializeKLLSketch(sk *kll.KLLSketch) ([]byte, error) {
	if sk == nil {
		return nil, nil
	}
	env, err := sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("kll.SerializePortable: %w", err)
	}
	return proto.Marshal(env)
}
