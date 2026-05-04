// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

// Phase 2.11 path A — shim-level testing.B benchmark for the CountSketch
// processor. Measures the cost of one ProcessMetrics call (decode →
// observe-into-Precompute → tick + encode). There is no pre-shim
// equivalent: the legacy processor was not a shim, so this number is
// informational only — it captures the absolute overhead of the
// post-shim batch path so any future refactor has a baseline.
//
// Methodology:
//   - The benchmark builds a deterministic Gauge fixture (1000 data
//     points across multiple metric names so the CountSketch indexes
//     by metric-name as the legacy processor did).
//   - countSketchProcessor.ProcessMetrics ticks inline in batch mode;
//     the bench captures the full synchronous cost (observe + tick +
//     encode), which mirrors what production batch deployments hit.
//   - b.ReportAllocs() surfaces inner-loop allocations.

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
	benchSeed        int64 = 0x5A9C011EC709072
)

// buildBenchMetrics constructs a deterministic Gauge fixture with
// `benchDataPointsPerCall` data points spread across 8 distinct metric
// names. CountSketch's legacy hot path is `cs.UpdateString(metricName,
// value)` so distinct metric names exercise different sketch cells.
func buildBenchMetrics() pmetric.Metrics {
	rng := rand.New(rand.NewSource(benchSeed))
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "web")
	sm := rm.ScopeMetrics().AppendEmpty()
	for n := 0; n < 8; n++ {
		metric := sm.Metrics().AppendEmpty()
		metric.SetName("metric_" + strconv.Itoa(n))
		metric.SetUnit("1")
		g := metric.SetEmptyGauge()
		now := pcommon.NewTimestampFromTime(time.Now())
		for i := 0; i < benchDataPointsPerCall/8; i++ {
			dp := g.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(now)
			dp.SetTimestamp(now)
			dp.SetDoubleValue(rng.Float64() * 100)
			dp.Attributes().PutStr("route", "/api/"+strconv.Itoa(i%2))
		}
	}
	return md
}

// BenchmarkProcessor_ProcessMetrics times one ProcessMetrics invocation
// (batch mode: observe + tick + encode + merge into md).
func BenchmarkProcessor_ProcessMetrics(b *testing.B) {
	cfg := &Config{
		Mode:                 ModeBatch,
		Epsilon:              0.01,
		Delta:                0.99,
		WindowDuration:       0,
		TransmitSketch:       true,
		DropOriginal:         true,
		EnableSelfMonitoring: false,
		Encoding:             EncodingProto,
	}
	if err := cfg.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, sink)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md := buildBenchMetrics()
		if _, err := proc.ProcessMetrics(ctx, md); err != nil {
			b.Fatalf("ProcessMetrics: %v", err)
		}
	}
}
