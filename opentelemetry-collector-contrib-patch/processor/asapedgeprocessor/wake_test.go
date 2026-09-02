// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// TestWakeSubWindowFlushesBeforeTicker proves wakeSubWindow triggers a
// sub-window emit immediately, independent of the SubWindowInterval ticker's
// own cadence — the mechanism used by per-family GOS insert-time checks.
// SubWindowInterval is set far longer than the test would
// ever run, so any output observed can only have come from the wake, not the
// ticker firing on its own.
func TestWakeSubWindowFlushesBeforeTicker(t *testing.T) {
	tru := true
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:        1,
		WindowDuration:    time.Hour,
		SubWindowInterval: 30 * time.Minute,
		DropOriginal:      true,
		Metrics: []MetricFamily{
			{Metric: "reqs", Family: FamilySum, AggregateBy: []string{"zone"}, DeltaTransmission: &tru},
		},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("reqs")
	s := m.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := s.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("zone", "z0")
	dp.SetDoubleValue(7)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}

	// Rapid double-wake must not block (buffered-1, non-blocking send).
	p.wakeSubWindow()
	p.wakeSubWindow()

	deadline := time.Now().Add(2 * time.Second)
	for len(cap.got) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if len(cap.got) == 0 {
		t.Fatal("wakeSubWindow did not produce a flush within 2s; SubWindowInterval (30m) could not have fired on its own")
	}
}
