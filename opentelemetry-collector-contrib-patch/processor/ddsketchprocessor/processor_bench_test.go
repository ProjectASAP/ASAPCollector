// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

// Phase 2.11 path A — shim-level testing.B benchmark for the DDSketch
// processor. Measures the cost of one ProcessMetrics call (decode →
// observe-into-Precompute) on a deterministic synthetic batch. There is
// no pre-shim equivalent: the legacy processor was not a shim, so this
// number is informational only — it captures the absolute overhead of
// the post-shim batch path so any future refactor has a baseline.
//
// Methodology:
//   - The benchmark builds a single pmetric.Metrics fixture once (1000
//     gauge data points, deterministic float values), then calls
//     ProcessMetrics(ctx, md) b.N times.
//   - The processor is constructed in window mode so ProcessMetrics
//     observes-only (no Tick + encode contamination per call); the
//     batch-mode equivalent would also include the per-call flush cost.
//   - b.ReportAllocs() surfaces inner-loop allocations.
//   - Each iteration re-uses the same md; the runtime's internal
//     window state is cumulative across iterations, which matches the
//     legacy processor's accumulateIntoWindow accumulation pattern.

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	benchDataPointsPerCall = 1000
	benchSeed              int64 = 0x5A9C011EC709072
)

// buildBenchMetrics constructs a deterministic pmetric.Metrics fixture
// with `benchDataPointsPerCall` Gauge data points across two route
// values so the runtime sees a small (2-series) per-call cardinality.
// Using a Gauge keeps the input shape identical to what the legacy
// ddsketchprocessor emits when fed by an OTel SDK metric exporter.
func buildBenchMetrics() pmetric.Metrics {
	rng := rand.New(rand.NewSource(benchSeed))
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "web")
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("request_latency")
	metric.SetUnit("ms")
	g := metric.SetEmptyGauge()
	now := pcommon.NewTimestampFromTime(time.Now())
	for i := 0; i < benchDataPointsPerCall; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(now)
		dp.SetTimestamp(now)
		dp.SetDoubleValue(rng.Float64() * 10000)
		dp.Attributes().PutStr("route", "/api/"+strconv.Itoa(i%2))
	}
	return md
}

// BenchmarkProcessor_ProcessMetrics times one ProcessMetrics invocation
// at the shim boundary. Window-mode is used so each call is a pure
// "decode + observe" loop without the periodic encode path.
func BenchmarkProcessor_ProcessMetrics(b *testing.B) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = time.Hour // never rotates inside the bench
	cfg.RelativeAccuracy = 0.01
	cfg.TransmitSketch = true
	if err := cfg.validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := buildBenchMetrics()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := proc.ProcessMetrics(ctx, md); err != nil {
			b.Fatalf("ProcessMetrics: %v", err)
		}
	}
}

// BenchmarkProcessor_ProcessBatch times one ProcessBatch invocation
// (batch mode: observe + tick + encode + merge into md). This path
// is what production Mode=batch deployments hit on every flushed
// batch; the bench captures the full synchronous cost.
func BenchmarkProcessor_ProcessBatch(b *testing.B) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.RelativeAccuracy = 0.01
	cfg.TransmitSketch = true
	if err := cfg.validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each iteration uses a fresh md because ProcessBatch may
		// mutate (graft synthesized RMs onto md). Building the
		// fixture inside the timed loop is unavoidable for batch
		// mode but reflects the real cost shape — production
		// batches arrive fresh too.
		md := buildBenchMetrics()
		if _, err := proc.ProcessBatch(ctx, md); err != nil {
			b.Fatalf("ProcessBatch: %v", err)
		}
	}
}
