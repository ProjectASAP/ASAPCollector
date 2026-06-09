// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// Tests for the threshold-driven sub-window delta producer: per-family
// divergence gating, first-emit-full, fixed-mode (epsilon=0) backward compat,
// and the Count-Sketch L2 divergence metric.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

func ddSubWindowCfg(eps float64) *precompute.PrecomputeConfig {
	return &precompute.PrecomputeConfig{
		AggID:             1,
		SketchType:        precompute.SketchTypeDDSketch,
		Mode:              precompute.Tumbling,
		Window:            precompute.WindowSpec{Size: time.Hour},
		DeltaTransmission: true,
		SubWindowInterval: time.Second,
		SubWindowEpsilon:  eps,
	}
}

func ddObs(ts uint64, v float64) *precompute.Observation {
	return &precompute.Observation{
		TimestampMs: ts, Metric: "lat",
		Labels: []precompute.KeyValue{{Key: "svc", Value: "a"}},
		Value:  precompute.FloatValue(v),
	}
}

func TestSubWindow_ThresholdGating_CountFamily(t *testing.T) {
	pc := precompute.New(ddSubWindowCfg(0.2),
		func() precompute.Sketch { return sketches.NewDDSketchWrapper(0.01) },
		sketches.DDSketchObserver{})
	ts := uint64(3_600_000)
	for i := 0; i < 10; i++ {
		_ = pc.Observe(ddObs(ts, float64(i+1)))
	}
	// First sub-window emit: ships FULL state (no prior base), one envelope.
	e1 := pc.EmitSubWindow(ts)
	if len(e1) != 1 || e1[0].Encoding != precompute.EncodingProtoFull {
		t.Fatalf("first emit: want 1 full envelope, got %d enc=%v", len(e1), e1[0].Encoding)
	}
	// No new observations → divergence 0 → no emit.
	if e2 := pc.EmitSubWindow(ts); len(e2) != 0 {
		t.Fatalf("stable series should not emit, got %d", len(e2))
	}
	// +1 obs (count 11): div=1 < 0.2*11=2.2 → still below threshold.
	_ = pc.Observe(ddObs(ts, 5))
	if e3 := pc.EmitSubWindow(ts); len(e3) != 0 {
		t.Fatalf("sub-threshold divergence should not emit, got %d", len(e3))
	}
	// +2 obs (count 13): div since last emit (10) = 3 >= 0.2*13=2.6 → emit DELTA.
	_ = pc.Observe(ddObs(ts, 6))
	_ = pc.Observe(ddObs(ts, 7))
	e4 := pc.EmitSubWindow(ts)
	if len(e4) != 1 || e4[0].Encoding != precompute.EncodingProtoDelta {
		t.Fatalf("crossing threshold: want 1 delta envelope, got %d", len(e4))
	}
}

func TestSubWindow_FixedMode_EmitsEveryTick(t *testing.T) {
	pc := precompute.New(ddSubWindowCfg(0.0), // epsilon=0 ⇒ #458 fixed mode
		func() precompute.Sketch { return sketches.NewDDSketchWrapper(0.01) },
		sketches.DDSketchObserver{})
	ts := uint64(3_600_000)
	for i := 0; i < 5; i++ {
		_ = pc.Observe(ddObs(ts, float64(i+1)))
	}
	if len(pc.EmitSubWindow(ts)) != 1 {
		t.Fatalf("fixed mode: first emit expected")
	}
	// Even with NO new observations, fixed mode emits every tick.
	if got := len(pc.EmitSubWindow(ts)); got != 1 {
		t.Fatalf("fixed mode should emit every tick regardless of divergence, got %d", got)
	}
}

func TestSubWindow_DisabledWithoutDelta(t *testing.T) {
	cfg := ddSubWindowCfg(0.2)
	cfg.DeltaTransmission = false // no in-window base → sub-window inert
	pc := precompute.New(cfg,
		func() precompute.Sketch { return sketches.NewDDSketchWrapper(0.01) },
		sketches.DDSketchObserver{})
	_ = pc.Observe(ddObs(3_600_000, 1))
	if got := pc.EmitSubWindow(3_600_000); got != nil {
		t.Fatalf("EmitSubWindow must be a no-op without DeltaTransmission, got %d", len(got))
	}
}

func TestCountSketchWrapper_L2Divergence(t *testing.T) {
	w, err := sketches.NewCountSketchWrapper(5, 2048)
	if err != nil {
		t.Fatalf("new count-sketch: %v", err)
	}
	for i := 0; i < 100; i++ {
		w.UpdateString("k", 1)
	}
	d0, n0 := w.L2DivergenceSinceEmit()
	if n0 <= 0 || d0 <= 0 {
		t.Fatalf("pre-emit: expect nonzero div+norm (acked is empty), got div=%v norm=%v", d0, n0)
	}
	w.MarkSubWindowEmitted()
	// Right after MarkSubWindowEmitted, divergence is ~0.
	if d, _ := w.L2DivergenceSinceEmit(); d != 0 {
		t.Fatalf("divergence right after mark should be 0, got %v", d)
	}
	// More updates → divergence grows again.
	for i := 0; i < 50; i++ {
		w.UpdateString("k", 1)
	}
	if d, _ := w.L2DivergenceSinceEmit(); d <= 0 {
		t.Fatalf("divergence should grow after new updates, got %v", d)
	}
}

// TestSubWindow_ExcludesFullStateFamilies verifies Sum/count and KLL — whose
// ComputeDeltaAgainst returns full state (not incremental) — are excluded from
// sub-window emission even with delta + sub-window configured, so the additive
// backend can't over-count (Sum) / re-merge-inflate (KLL). They emit only at
// the window boundary.
func TestSubWindow_ExcludesFullStateFamilies(t *testing.T) {
	ts := uint64(3_600_000)

	sumCfg := &precompute.PrecomputeConfig{
		AggID: 2, AggKind: precompute.AggKindSum, Mode: precompute.Tumbling,
		Window:            precompute.WindowSpec{Size: time.Hour},
		DeltaTransmission: true, SubWindowInterval: time.Second, SubWindowEpsilon: 0,
	}
	sumPC := precompute.New(sumCfg,
		func() precompute.Sketch { return sketches.NewSumWrapper() }, sketches.SumObserver{})
	_ = sumPC.Observe(ddObs(ts, 10))
	_ = sumPC.Observe(ddObs(ts, 20))
	if got := sumPC.EmitSubWindow(ts); got != nil {
		t.Fatalf("Sum must be excluded from sub-window emission (full-state ⇒ over-count), got %d envelopes", len(got))
	}

	kllCfg := &precompute.PrecomputeConfig{
		AggID: 3, SketchType: precompute.SketchTypeKLLSketch, Mode: precompute.Tumbling,
		Window:            precompute.WindowSpec{Size: time.Hour},
		DeltaTransmission: true, SubWindowInterval: time.Second, SubWindowEpsilon: 0,
	}
	kllPC := precompute.New(kllCfg,
		func() precompute.Sketch { return sketches.NewKLLWrapper(200, nil) }, sketches.KLLObserver{})
	_ = kllPC.Observe(ddObs(ts, 10))
	_ = kllPC.Observe(ddObs(ts, 20))
	if got := kllPC.EmitSubWindow(ts); got != nil {
		t.Fatalf("KLL must be excluded from sub-window emission (full-state ⇒ inflate), got %d envelopes", len(got))
	}
}
