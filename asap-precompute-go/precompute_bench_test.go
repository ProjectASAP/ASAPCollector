// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// Phase 2.11 path A — testing.B benchmarks for Precompute.Observe
// ns/op across the five sketch types in sketchlib-go. ADR-0002
// §"Performance contract" pins the post-shim p99 within 10% of the
// pre-shim p99.
//
// Compatibility: this file is constructed to compile against BOTH
// the pre-shim baseline (commit 6b3258d, no sketches/ wrapper
// package yet) and post-shim HEAD. To stay portable across the
// commit boundary, the file does NOT import asap-precompute-go's
// `sketches/` subpackage; instead each bench wires a tiny
// `benchXxxWrapper` directly against sketchlib-go and constructs an
// inline `benchXxxObserver` that satisfies precompute.SketchObserver.
// The wrappers implement only the methods Observe needs (Observe →
// SketchObserver.Observe → Sketch.Update or equivalent); Snapshot /
// Merge / etc. return zero values because they are never called on
// the bench's hot path.
//
// Methodology:
//   - b.ResetTimer() after Precompute construction so setup cost is
//     excluded.
//   - b.ReportAllocs() surfaces inner-loop allocations.
//   - Each bench uses a deterministic PRNG (rand.New(rand.NewSource))
//     so successive `-count=N` runs are comparable.
//   - The Precompute.Tick path is intentionally out of scope: the gate
//     is per-observation latency, not the periodic flush.
//   - Window size is set to one hour so the bench loop never rotates.

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

const benchSeed int64 = 0x5A9C011EC709072

// benchObservations preallocates a slice of pointer-Observations the
// inner loop indexes by `i % len(observations)`. The hot path is
// then a pure dispatch into Precompute.Observe with no
// per-iteration allocation cost contaminating ns/op.
const benchObservations = 1024

// errUnsupportedKind is returned by every bench observer when the
// caller hands an unexpected ObservationValue kind. Bench inputs
// always match the observer's expected kind so this is a defensive
// check, not a hot-path return.
var errUnsupportedKind = errors.New("bench observer: unsupported value kind")

// newBenchPrecompute constructs a Precompute wired to the supplied
// sketch factory + observer + sketch type. The window is wide
// enough (1 hour) that no observation in the bench loop ever
// rotates the active window — we want pure Observe timings.
func newBenchPrecompute(b *testing.B, sketchType precompute.SketchType, factory precompute.SketchFactory, observer precompute.SketchObserver) precompute.Precompute {
	b.Helper()
	cfg := &precompute.PrecomputeConfig{
		AggID:      1,
		SketchType: sketchType,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: time.Hour},
	}
	return precompute.New(cfg, factory, observer)
}

// --- DDSketch bench ---------------------------------------------------------

type benchDDSketch struct {
	sk *ddsketch.DDSketch
}

func (b *benchDDSketch) Snapshot() ([]byte, error)                          { return nil, nil }
func (b *benchDDSketch) ComputeDeltaAgainst(prev []byte, t uint64) ([]byte, bool, error) {
	return nil, true, nil
}
func (b *benchDDSketch) ApplyDelta(_ []byte) error      { return nil }
func (b *benchDDSketch) Merge(_ precompute.Sketch) error { return nil }
func (b *benchDDSketch) Reset()                          {}

type benchDDObserver struct{}

func (benchDDObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*benchDDSketch)
	if !ok {
		return fmt.Errorf("benchDDObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindFloat {
		return errUnsupportedKind
	}
	w.sk.Update(v.Float)
	return nil
}

// BenchmarkPrecompute_Observe_DDSketch times the float-valued observe
// path that sits underneath ddsketchprocessor's batch loop.
func BenchmarkPrecompute_Observe_DDSketch(b *testing.B) {
	factory := precompute.SketchFactory(func() precompute.Sketch {
		return &benchDDSketch{sk: ddsketch.NewDDSketch(0.01)}
	})
	p := newBenchPrecompute(b, precompute.SketchTypeDDSketch, factory, benchDDObserver{})

	rng := rand.New(rand.NewSource(benchSeed))
	obs := make([]*precompute.Observation, benchObservations)
	for i := range obs {
		obs[i] = &precompute.Observation{
			TimestampMs: 1_000,
			Metric:      "request_latency",
			Labels:      []precompute.KeyValue{{Key: "route", Value: "/api"}},
			Value:       precompute.FloatValue(rng.Float64() * 10000),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}

// --- KLL bench --------------------------------------------------------------

type benchKLLSketch struct {
	sk *kll.KLLSketch
}

func (b *benchKLLSketch) Snapshot() ([]byte, error) { return nil, nil }
func (b *benchKLLSketch) ComputeDeltaAgainst(prev []byte, t uint64) ([]byte, bool, error) {
	return nil, true, nil
}
func (b *benchKLLSketch) ApplyDelta(_ []byte) error      { return nil }
func (b *benchKLLSketch) Merge(_ precompute.Sketch) error { return nil }
func (b *benchKLLSketch) Reset()                          {}

type benchKLLObserver struct{}

func (benchKLLObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*benchKLLSketch)
	if !ok {
		return fmt.Errorf("benchKLLObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindFloat {
		return errUnsupportedKind
	}
	w.sk.Update(v.Float)
	return nil
}

// BenchmarkPrecompute_Observe_KLL exercises the float-valued observe
// path against a KLL sketch (k=256, the kllprocessor default).
func BenchmarkPrecompute_Observe_KLL(b *testing.B) {
	sk0, err := kll.NewKLLSketch(256)
	if err != nil {
		b.Fatalf("NewKLLSketch: %v", err)
	}
	_ = sk0 // sanity check that the constructor accepts our k
	factory := precompute.SketchFactory(func() precompute.Sketch {
		sk, err := kll.NewKLLSketch(256)
		if err != nil {
			panic(err)
		}
		return &benchKLLSketch{sk: sk}
	})
	p := newBenchPrecompute(b, precompute.SketchTypeKLLSketch, factory, benchKLLObserver{})

	rng := rand.New(rand.NewSource(benchSeed))
	obs := make([]*precompute.Observation, benchObservations)
	for i := range obs {
		obs[i] = &precompute.Observation{
			TimestampMs: 1_000,
			Metric:      "request_latency",
			Labels:      []precompute.KeyValue{{Key: "route", Value: "/api"}},
			Value:       precompute.FloatValue(rng.Float64() * 10000),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}

// --- HLL bench --------------------------------------------------------------

type benchHLLSketch struct {
	sk *hll.HyperLogLog
}

func (b *benchHLLSketch) Snapshot() ([]byte, error) { return nil, nil }
func (b *benchHLLSketch) ComputeDeltaAgainst(prev []byte, t uint64) ([]byte, bool, error) {
	return nil, true, nil
}
func (b *benchHLLSketch) ApplyDelta(_ []byte) error      { return nil }
func (b *benchHLLSketch) Merge(_ precompute.Sketch) error { return nil }
func (b *benchHLLSketch) Reset()                          {}

type benchHLLObserver struct{}

func (benchHLLObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*benchHLLSketch)
	if !ok {
		return fmt.Errorf("benchHLLObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindFloat {
		return errUnsupportedKind
	}
	w.sk.UpdateValue(v.Float)
	return nil
}

// BenchmarkPrecompute_Observe_HLL feeds float-valued observations
// into the HLL sketch (precision=14, sketchlib-go default).
func BenchmarkPrecompute_Observe_HLL(b *testing.B) {
	factory := precompute.SketchFactory(func() precompute.Sketch {
		return &benchHLLSketch{sk: hll.NewHyperLogLog()}
	})
	p := newBenchPrecompute(b, precompute.SketchTypeHLLSketch, factory, benchHLLObserver{})

	rng := rand.New(rand.NewSource(benchSeed))
	obs := make([]*precompute.Observation, benchObservations)
	for i := range obs {
		obs[i] = &precompute.Observation{
			TimestampMs: 1_000,
			Metric:      "user_id",
			Labels:      []precompute.KeyValue{{Key: "shard", Value: strconv.Itoa(i % 8)}},
			Value:       precompute.FloatValue(rng.Float64() * 1_000_000),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}

// --- CountSketch bench -------------------------------------------------------

type benchCountSketch struct {
	cs         *countsketch.CountSketch
	defaultKey string
}

func (b *benchCountSketch) Snapshot() ([]byte, error) { return nil, nil }
func (b *benchCountSketch) ComputeDeltaAgainst(prev []byte, t uint64) ([]byte, bool, error) {
	return nil, true, nil
}
func (b *benchCountSketch) ApplyDelta(_ []byte) error      { return nil }
func (b *benchCountSketch) Merge(_ precompute.Sketch) error { return nil }
func (b *benchCountSketch) Reset()                          {}

type benchCountSketchObserver struct{}

func (benchCountSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*benchCountSketch)
	if !ok {
		return fmt.Errorf("benchCountSketchObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindFloat {
		return errUnsupportedKind
	}
	key := w.defaultKey
	if len(v.Bytes) > 0 {
		key = string(v.Bytes)
	}
	w.cs.UpdateString(key, v.Float)
	return nil
}

// BenchmarkPrecompute_Observe_CountSketch exercises the float-valued
// observe path that drives countsketchprocessor.
func BenchmarkPrecompute_Observe_CountSketch(b *testing.B) {
	rows, cols := 5, 1024
	factory := precompute.SketchFactory(func() precompute.Sketch {
		cs, err := countsketch.NewCountSketch(rows, cols)
		if err != nil {
			panic(err)
		}
		return &benchCountSketch{cs: cs, defaultKey: "request_latency"}
	})
	p := newBenchPrecompute(b, precompute.SketchTypeCountSketch, factory, benchCountSketchObserver{})

	rng := rand.New(rand.NewSource(benchSeed))
	obs := make([]*precompute.Observation, benchObservations)
	for i := range obs {
		key := []byte("k" + strconv.Itoa(i%64))
		obs[i] = &precompute.Observation{
			TimestampMs: 1_000,
			Metric:      "request_latency",
			Labels:      []precompute.KeyValue{{Key: "route", Value: "/api"}},
			Value: precompute.ObservationValue{
				Kind:  precompute.KindFloat,
				Float: rng.Float64() * 100,
				Bytes: key,
			},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}

// --- CountMinSketch bench ----------------------------------------------------

type benchCMS struct {
	sk *cms.CountMinSketch
}

func (b *benchCMS) Snapshot() ([]byte, error) { return nil, nil }
func (b *benchCMS) ComputeDeltaAgainst(prev []byte, t uint64) ([]byte, bool, error) {
	return nil, true, nil
}
func (b *benchCMS) ApplyDelta(_ []byte) error      { return nil }
func (b *benchCMS) Merge(_ precompute.Sketch) error { return nil }
func (b *benchCMS) Reset()                          {}

type benchCMSObserver struct{}

func (benchCMSObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	w, ok := s.(*benchCMS)
	if !ok {
		return fmt.Errorf("benchCMSObserver: sketch is %T", s)
	}
	if v.Kind != precompute.KindBytes {
		return errUnsupportedKind
	}
	w.sk.InsertWithHash(common.FromBytes(v.Bytes).Hash)
	return nil
}

// BenchmarkPrecompute_Observe_CMS feeds opaque byte keys into the
// CountMinSketch — the shape that countminsketchprocessor's flow-key
// path produces.
func BenchmarkPrecompute_Observe_CMS(b *testing.B) {
	rows, cols := 5, 1024
	factory := precompute.SketchFactory(func() precompute.Sketch {
		sk, err := cms.NewCountMinSketch(rows, cols)
		if err != nil {
			panic(err)
		}
		return &benchCMS{sk: sk}
	})
	p := newBenchPrecompute(b, precompute.SketchTypeCountMinSketch, factory, benchCMSObserver{})

	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte("flow-" + strconv.Itoa(i))
	}
	obs := make([]*precompute.Observation, benchObservations)
	for i := range obs {
		obs[i] = &precompute.Observation{
			TimestampMs: 1_000,
			Metric:      "http_requests_total",
			Labels:      []precompute.KeyValue{{Key: "service.name", Value: "web"}},
			Value:       precompute.BytesValue(keys[i%len(keys)]),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}
