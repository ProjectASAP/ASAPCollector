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

// TestGOSKLLInsertWakesFlush is the KLL counterpart of
// TestGOSCountSketchInsertWakesFlush (gos_countsketch_test.go): with
// SubWindowInterval left UNSET (no periodic sub-window ticker at all) and
// WindowDuration set far longer than the test could ever run (so the
// window-boundary tick also cannot be the source), feeding enough KLL
// observations to cross the insert-time GOS trigger (R>=epsilon*N,
// derivations doc §8.6) must, on its own, produce a flushed envelope — with
// NO manual wakeSubWindow() call anywhere in this test. This proves the real
// producer — KLLWrapper.Update's crossing check -> ConsumeWakeSignal ->
// windowState.wakeHook -> Precompute.SetWakeHook -> processor.wakeSubWindow
// -> EmitSubWindow's disjoint-segment emit-then-reset — fires end to end for
// KLL specifically (a family whose segment-mode reset the GOS trigger must
// coexist with, unlike Count-Sketch's per-cell subtractive delta).
func TestGOSKLLInsertWakesFlush(t *testing.T) {
	tru := true
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{
				Metric:            "latency",
				Family:            FamilyKLL,
				DeltaTransmission: &tru,
				GosDeltaEpsilon:   0.5,
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
	g.SetName("latency")
	gauge := g.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	// R==N after every insert (one KLL Update per observation), so
	// R>=epsilon*N (0.5*N) holds starting from the very first sample —
	// a single ConsumeMetrics batch is more than enough to cross it.
	const n = 10
	for i := 0; i < n; i++ {
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(i))
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
			"path did not fire as expected for KLL")
	}
}
