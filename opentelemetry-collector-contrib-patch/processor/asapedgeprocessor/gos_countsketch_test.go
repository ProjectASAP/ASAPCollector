// Copyright The OpenTelemetry Authors
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

// TestGOSCountSketchInsertWakesFlush is the full end-to-end proof of the
// insert-time GOS mechanism (design-gos-unified-edge-telemetry.md §11): with
// SubWindowInterval left UNSET (no periodic sub-window ticker at all — proving
// subWindowEnabled's decoupling from SubWindowInterval) and WindowDuration set
// far longer than the test could ever run (so the window-boundary tick also
// cannot be the source), feeding enough CountSketch observations to cross the
// GOS threshold must, on its own, produce a flushed envelope — with NO manual
// wakeSubWindow() call anywhere in this test (unlike TestWakeSubWindowFlushesBeforeTicker
// in wake_test.go, which proves the transport primitive in isolation; this
// proves the real producer — UpdateStringGOS's crossing → ConsumeWakeSignal →
// windowState.wakeHook → Precompute.SetWakeHook → processor.wakeSubWindow —
// actually fires end to end).
func TestGOSCountSketchInsertWakesFlush(t *testing.T) {
	tru := true
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{
				Metric:            "events",
				Family:            FamilyCountSketch,
				DeltaTransmission: &tru,
				GosDeltaEpsilon:   0.9,
				GosSites:          1,
			},
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
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("events")
	gauge := g.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	// Repeated observations of the SAME attribute set drive that one cell
	// group's magnitude up quickly; cold start (threshold ≈ ε·‖Ĉ‖/(2k√dw)
	// starts near 0 when the sketch is empty) means this should cross well
	// before 500 inserts.
	const n = 500
	for i := 0; i < n; i++ {
		dp := gauge.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", "z0")
		dp.SetDoubleValue(1)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Millisecond)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

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
		t.Fatal("no flush observed within 2s: neither SubWindowInterval (unset) nor " +
			"WindowDuration (1h) could have produced one, so the insert-time GOS wake " +
			"path did not fire as expected")
	}
}
