// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

// Phase 2.11 path A — shim-level testing.B benchmark for the CountMin
// sketch processor. Measures the cost of one ProcessBatch call
// (decode → observe-into-Precompute → tick + encode). There is no
// pre-shim equivalent: the legacy processor was not a shim, so this
// number is informational only — it captures the absolute overhead of
// the post-shim batch path so any future refactor has a baseline.
//
// Methodology:
//   - The benchmark builds a deterministic Gauge fixture (1000 data
//     points across 8 distinct service.name values so the flow-key
//     hash indexes different sketch cells).
//   - cmsProcessor.ProcessBatch ticks inline; the bench captures the
//     full synchronous cost (observe + tick + encode + merge), which
//     mirrors what production batch deployments hit.
//   - b.ReportAllocs() surfaces inner-loop allocations.

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const benchDataPointsPerCall = 1000

// buildBenchMetrics constructs a deterministic Gauge fixture with
// `benchDataPointsPerCall` data points spread across 8 service.name
// label values so the CMS flow-key hash exercises distinct cells.
func buildBenchMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "web")
	sm := rm.ScopeMetrics().AppendEmpty()
	now := pcommon.NewTimestampFromTime(time.Now())
	for i := 0; i < benchDataPointsPerCall; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("http_requests_total")
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(now)
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		dp.Attributes().PutStr("service.name", "svc-"+strconv.Itoa(i%8))
	}
	return md
}

// BenchmarkProcessor_ProcessBatch times one ProcessBatch invocation
// (batch mode: observe + tick + encode + merge into md).
func BenchmarkProcessor_ProcessBatch(b *testing.B) {
	cfg := &Config{
		Mode:                 ModeBatch,
		MetricName:           "countmin_sketch",
		Rows:                 5,
		Columns:              1024,
		EnableSelfMonitoring: false,
		TransmitSketch:       true,
		DropOriginal:         false,
		WindowDuration:       0,
		Encoding:             EncodingProto,
	}
	if err := cfg.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, sink, zap.NewNop())

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
