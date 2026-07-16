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

// TestGOSHLLInsertWakesFlush is the end-to-end proof of the HLL register-change
// GOS adapter (sampling-cdm-gos-derivations.md §8.7, design-gos-unified-edge-
// telemetry.md §11): with SubWindowInterval left UNSET (no periodic sub-window
// ticker) and WindowDuration set far longer than the test could ever run (so
// the window-boundary tick also cannot be the source), feeding distinct HLL
// observations that raise registers must, on its own, produce a flushed
// envelope — with NO manual wakeSubWindow() call anywhere in this test. This
// mirrors TestGOSCountSketchInsertWakesFlush but for HLL, where gos_delta_epsilon
// is REINTERPRETED as τ (a doublings count) rather than an ε budget.
//
// Because each distinct value's first write to a still-zero register is the
// "first-ever nonzero write" the design doc's (unverified) small-cardinality
// mitigation sends unconditionally, the wake fires almost immediately —
// exactly the cold-start behavior we want.
func TestGOSHLLInsertWakesFlush(t *testing.T) {
	tru := true
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{
				Metric:            "uniques",
				Family:            FamilyHLL,
				DeltaTransmission: &tru,
				// τ=1: "report a register once its linearized contribution at
				// least doubled since last sent" — plus the unconditional
				// first-nonzero-write send, which fires on the first insert.
				GosDeltaEpsilon: 1,
				GosSites:        1,
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
	g.SetName("uniques")
	gauge := g.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	// Distinct values populate distinct HLL registers; each register's first
	// nonzero write crosses unconditionally, so the wake fires well before 500.
	const n = 500
	for i := 0; i < n; i++ {
		dp := gauge.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", "z0")
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
			"WindowDuration (1h) could have produced one, so the insert-time HLL GOS " +
			"register-change wake path did not fire as expected")
	}
}
