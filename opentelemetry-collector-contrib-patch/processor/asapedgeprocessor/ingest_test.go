// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestUnsupportedTypeNeverDropped covers P0 #1: a Histogram metric configured
// (and drop_original=true) must NOT be removed from the passthrough stream nor
// silently lost — it has no warm/cold path, so the passthrough is its only
// survival. The unsupported-type counter must also tick.
func TestUnsupportedTypeNeverDropped(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		// Configure the histogram name so the drop_original RemoveIf would
		// normally remove it; the fix must exempt unsupported types.
		Metrics: []MetricFamily{{Metric: "request_latency_hist", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:    ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), cap)
	if err != nil {
		t.Fatal(err)
	}

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("request_latency_hist")
	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(3)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if len(cap.got) != 1 {
		t.Fatalf("expected histogram forwarded in passthrough, got %d batches", len(cap.got))
	}
	if got := cap.got[0].MetricCount(); got != 1 {
		t.Fatalf("expected 1 forwarded metric (the histogram), got %d", got)
	}
	if c := p.unsupportedTypeCount.Load(); c != 1 {
		t.Fatalf("unsupportedTypeCount = %d, want 1", c)
	}
}
