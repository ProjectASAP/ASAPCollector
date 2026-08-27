// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// Hot-path guard for continuous monitoring (Discipline B). The plan's hard
// constraint: with the monitor hook ENABLED but in its steady-state quiet path
// (registered, no slack granted yet → arithmetic + early return, no network),
// Observe must cost essentially the same as with the hook DISABLED. These two
// benchmarks are meant to be compared:
//
//	go test -run '^$' -bench 'BenchmarkMonitorHotPath' -benchmem ./...
//
// A large gap between Off and OnQuiet would mean the hook regressed the runtime
// for every deployment, monitored or not.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
)

// benchSumSketch is a minimal Sum sketch exposing Sum() float64 so
// monitorValue(FunctionalSum, …) can read it on the hot path.
type benchSumSketch struct{ sum float64 }

func (b *benchSumSketch) Snapshot() ([]byte, error) { return nil, nil }
func (b *benchSumSketch) ComputeDeltaAgainst(prev []byte, t uint64) ([]byte, bool, error) {
	return nil, true, nil
}
func (b *benchSumSketch) ApplyDelta(_ []byte) error       { return nil }
func (b *benchSumSketch) Merge(_ precompute.Sketch) error { return nil }
func (b *benchSumSketch) Reset()                          { b.sum = 0 }
func (b *benchSumSketch) Sum() float64                    { return b.sum }

type benchSumObserver struct{}

func (benchSumObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	if w, ok := s.(*benchSumSketch); ok && v.Kind == precompute.KindFloat {
		w.sum += v.Float
	}
	return nil
}

// nopReporter satisfies monitor.Reporter without doing any work, so the bench
// measures only the engine's hot-path arithmetic, not transport.
type nopReporter struct{}

func (nopReporter) Register(monitor.Registration) {}
func (nopReporter) Report(monitor.Report)         {}

func newSumPrecompute(monitored bool) precompute.Precompute {
	cfg := &precompute.PrecomputeConfig{
		AggID:   1,
		AggKind: precompute.AggKindSum,
		Mode:    precompute.Tumbling,
		Window:  precompute.WindowSpec{Size: time.Hour},
	}
	if monitored {
		cfg.Monitor = monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalSum,
			CoordinatorURL: "passthrough:///bench",
			Tau:            1e18, // effectively never crosses in the bench
			Epsilon:        0.01,
		}
	}
	factory := precompute.SketchFactory(func() precompute.Sketch { return &benchSumSketch{} })
	p := precompute.New(cfg, factory, benchSumObserver{})
	if monitored {
		p.SetMonitorEngine(monitor.NewEngine("bench-edge", uint64(time.Hour/time.Millisecond), nopReporter{}))
	}
	return p
}

func benchSumObs() []*precompute.Observation {
	obs := make([]*precompute.Observation, benchObservations)
	for i := range obs {
		obs[i] = &precompute.Observation{
			TimestampMs: 1_000,
			Metric:      "bytes_sent",
			Labels:      []precompute.KeyValue{{Key: "route", Value: "/api"}},
			Value:       precompute.FloatValue(1.0),
		}
	}
	return obs
}

// BenchmarkMonitorHotPath_Off: hook disabled (no monitor engine).
func BenchmarkMonitorHotPath_Off(b *testing.B) {
	p := newSumPrecompute(false)
	obs := benchSumObs()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}

// BenchmarkMonitorHotPath_OnQuiet: hook enabled, steady-state quiet path
// (registered once, no slack granted → engine returns after the slack check).
func BenchmarkMonitorHotPath_OnQuiet(b *testing.B) {
	p := newSumPrecompute(true)
	obs := benchSumObs()
	// Prime one observation so the monitor registers (lazy) before timing.
	_ = p.Observe(obs[0])
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Observe(obs[i%len(obs)])
	}
}
