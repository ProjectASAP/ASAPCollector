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

// TestSubWindow_ExcludesKLL verifies KLL — whose ComputeDeltaAgainst is a
// full-state merge (it cannot subtract) — is excluded from sub-window emission
// even with delta + sub-window configured, so the additive backend can't
// re-merge-inflate it. KLL emits only at the window boundary.
func TestSubWindow_ExcludesKLL(t *testing.T) {
	ts := uint64(3_600_000)
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
		t.Fatalf("KLL must be excluded from sub-window emission (full-state merge ⇒ inflate), got %d envelopes", len(got))
	}
}

// TestSubWindow_Sum_IncrementalReconstruction verifies Sum is now sub-window
// capable: its incremental {Δsum,Δcount} deltas, accumulated additively at the
// backend (which is what ApplyDelta does), reconstruct the window total — i.e.
// multiple sub-window emits + the boundary emit do NOT over-count. Also checks
// the per-window-reset: a new window's first emit is a delta from ZERO, not a
// cross-window subtraction.
func TestSubWindow_Sum_IncrementalReconstruction(t *testing.T) {
	const w1 = uint64(3_600_000)
	cfg := &precompute.PrecomputeConfig{
		AggID: 2, AggKind: precompute.AggKindSum, Mode: precompute.Tumbling,
		Window:            precompute.WindowSpec{Size: time.Second}, // 1000ms windows
		DeltaTransmission: true, SubWindowInterval: 100 * time.Millisecond, SubWindowEpsilon: 0,
	}
	pc := precompute.New(cfg,
		func() precompute.Sketch { return sketches.NewSumWrapper() }, sketches.SumObserver{})

	// Reconstruct exactly as the additive backend does: reset on window_start
	// change (PWR), then ApplyDelta every payload (full or delta — both additive).
	recon := sketches.NewSumWrapper()
	var lastStart uint64
	var haveStart bool
	apply := func(envs []*precompute.SketchEnvelope) {
		for _, e := range envs {
			if !haveStart || e.WindowStartMs != lastStart {
				recon.Reset()
				lastStart, haveStart = e.WindowStartMs, true
			}
			if err := recon.ApplyDelta(e.Payload); err != nil {
				t.Fatalf("ApplyDelta: %v", err)
			}
		}
	}

	// --- Window 1: three sub-window emits + a boundary emit, total = 175 ---
	_ = pc.Observe(ddObs(w1, 10))
	_ = pc.Observe(ddObs(w1, 20)) // sum=30
	apply(pc.EmitSubWindow(w1))   // full 30
	_ = pc.Observe(ddObs(w1, 70)) // sum=100
	apply(pc.EmitSubWindow(w1))   // Δ70
	_ = pc.Observe(ddObs(w1, 50)) // sum=150
	apply(pc.EmitSubWindow(w1))   // Δ50
	_ = pc.Observe(ddObs(w1, 25)) // sum=175
	apply(pc.Drain())             // boundary Δ25 (then PWR-resets the base to {0,0})
	if got := recon.Sum(); got != 175 {
		t.Fatalf("window 1 reconstructed sum = %v, want 175 (no over-count across sub-window + boundary emits)", got)
	}

	// --- Window 2 (later start): first emit must be a delta from ZERO ---
	const w2 = w1 + 1000 // next 1000ms window
	_ = pc.Observe(ddObs(w2, 40))
	apply(pc.EmitSubWindow(w2))
	if got := recon.Sum(); got != 40 {
		t.Fatalf("window 2 reconstructed sum = %v, want 40 (PWR: delta from zero, not cross-window subtraction)", got)
	}
}
