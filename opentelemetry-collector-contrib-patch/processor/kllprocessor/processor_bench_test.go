// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllprocessor

// Phase 2.11 path A — shim-level testing.B benchmark for the KLL
// processor. Measures the cost of one ProcessMetrics call (decode →
// observe-into-Precompute → tick + encode in batch mode). There is no
// pre-shim equivalent: the legacy processor was not a shim, so this
// number is informational only — it captures the absolute overhead of
// the post-shim batch path so any future refactor has a baseline.
//
// Methodology:
//   - The benchmark builds a single pmetric.Metrics fixture once (1000
//     gauge data points, deterministic float values).
//   - kllprocessor's ProcessMetrics is an alias for ProcessBatch (the
//     shim ticks every call) — there's no observe-only public method,
//     so this bench exercises the full batch path. Tick cost is
//     bounded by the runtime's window state machine and small for a
//     2-series fixture; the dominant cost remains the per-observation
//     KLL Update.
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
// `benchDataPointsPerCall` data points spread across two route
// values, mirroring what the kllprocessor sees from a real OTel SDK.
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

// BenchmarkProcessor_ProcessBatch times one ProcessBatch invocation
// (batch mode: observe + tick + encode). This path is what production
// Mode=batch deployments hit on every flushed batch; the bench
// captures the full synchronous cost.
func BenchmarkProcessor_ProcessBatch(b *testing.B) {
	cfg := &Config{
		Mode:                 ModeBatch,
		WindowDuration:       time.Hour,
		K:                    256,
		Quantiles:            []float64{0.5, 0.99},
		TransmitSketch:       true,
		DropOriginal:         true,
		EnableSelfMonitoring: false,
	}
	if err := cfg.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md := buildBenchMetrics()
		if _, err := proc.ProcessBatch(ctx, md); err != nil {
			b.Fatalf("ProcessBatch: %v", err)
		}
	}
}
