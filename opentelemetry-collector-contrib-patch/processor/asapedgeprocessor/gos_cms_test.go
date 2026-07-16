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

// TestGOSCMSInsertWakesFlush is the CountMinSketch counterpart of
// gos_countsketch_test.go's TestGOSCountSketchInsertWakesFlush — the full
// end-to-end proof of the insert-time GOS mechanism
// (design-gos-unified-edge-telemetry.md §11, derivations §8.2) for CMS. With
// SubWindowInterval left UNSET (no periodic sub-window ticker at all) and
// WindowDuration set far longer than the test could ever run (so the
// window-boundary tick also cannot be the source), feeding CountMinSketch
// observations that cross the GOS threshold T=ε·N/k must, on its own,
// produce a flushed envelope — with NO manual wakeSubWindow() call anywhere
// in this test (unlike TestWakeSubWindowFlushesBeforeTicker in
// wake_test.go, which proves the transport primitive in isolation; this
// proves the real producer — InsertWithHashGOS's crossing ->
// ConsumeWakeSignal -> windowState.wakeHook -> Precompute.SetWakeHook ->
// processor.wakeSubWindow — actually fires end to end).
func TestGOSCMSInsertWakesFlush(t *testing.T) {
	tru := true
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{
				Metric:            "events",
				Family:            FamilyCountMinSketch,
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
	// group's magnitude up quickly; cold start (threshold ≈ ε·N/k starts near
	// 0 when the sketch is empty) means this crosses on the very first
	// insert in practice — n is generously sized for safety margin.
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
