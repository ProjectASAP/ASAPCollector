// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllprocessor

// Phase 2.11 path A — shim-level testing.B benchmark for the HLL
// processor. Measures the cost of one ProcessBatch call (decode →
// observe-into-Precompute → tick + encode). There is no pre-shim
// equivalent: the legacy processor was not a shim, so this number is
// informational only — it captures the absolute overhead of the
// post-shim batch path so any future refactor has a baseline.
//
// Methodology:
//   - The benchmark builds a deterministic Gauge fixture (1000 data
//     points spread across multiple distinct float values to exercise
//     the HLL register-update path).
//   - hllprocessor's ProcessMetrics is an alias for ProcessBatch (the
//     shim ticks every call) — there's no observe-only public method,
//     so this bench captures the full batch path.
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
// `benchDataPointsPerCall` data points whose DoubleValue is drawn from
// a wide range so the HLL register churns realistically.
func buildBenchMetrics() pmetric.Metrics {
	rng := rand.New(rand.NewSource(benchSeed))
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "web")
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("user_id")
	metric.SetUnit("1")
	g := metric.SetEmptyGauge()
	now := pcommon.NewTimestampFromTime(time.Now())
	for i := 0; i < benchDataPointsPerCall; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(now)
		dp.SetTimestamp(now)
		dp.SetDoubleValue(rng.Float64() * 1_000_000)
		dp.Attributes().PutStr("shard", strconv.Itoa(i%4))
	}
	return md
}

// BenchmarkProcessor_ProcessBatch times one ProcessBatch invocation
// (observe + tick + encode). This path is what production Mode=batch
// deployments hit on every flushed batch; the bench captures the full
// synchronous cost.
func BenchmarkProcessor_ProcessBatch(b *testing.B) {
	cfg := &Config{
		Mode:                 ModeBatch,
		WindowDuration:       time.Hour,
		TransmitSketch:       false,
		DropOriginal:         true,
		EnableSelfMonitoring: false,
		Encoding:             EncodingProto,
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
