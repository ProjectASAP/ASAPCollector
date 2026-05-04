// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestDDSketchWrapper_ObserveSnapshotApplyDelta drives a minimal
// observe -> snapshot -> ApplyDelta cycle and verifies the wrapped
// DDSketch reproduces the observed quantile after a round-trip. This
// is a smoke test only: the parity harness in integration/parity
// covers byte-equality with the legacy emit path.
func TestDDSketchWrapper_ObserveSnapshotApplyDelta(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	for _, v := range []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		w.Update(v)
	}
	snap, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("snapshot empty")
	}

	// ApplyDelta into a fresh wrapper and check quantile parity.
	other := NewDDSketchWrapper(0.01)
	if err := other.ApplyDelta(snap); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	want := w.Quantile(0.5)
	got := other.Quantile(0.5)
	// DDSketch reports a relative-accuracy bound; allow alpha=0.01 margin.
	if rel := abs(got-want) / want; rel > 0.011 {
		t.Fatalf("Quantile mismatch: want %f, got %f (rel %f)", want, got, rel)
	}
}

// TestDDSketchWrapper_ComputeDeltaShape exercises the
// ComputeDeltaAgainst path: prev empty -> isFull=true; non-empty prev
// with sub-threshold change -> a non-empty delta payload with
// isFull=false. The actual delta-apply correctness lives in
// sketchlib-go's own tests; this test just confirms the wrapper
// dispatches into the right sketchlib API.
func TestDDSketchWrapper_ComputeDeltaShape(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	for i := 1; i <= 50; i++ {
		w.Update(float64(i))
	}
	// First call: no prev -> full snapshot.
	full, isFull, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil || !isFull || len(full) == 0 {
		t.Fatalf("first call: full=%v err=%v len=%d", isFull, err, len(full))
	}
	prev := full
	for i := 51; i <= 100; i++ {
		w.Update(float64(i))
	}
	delta, isFull, err := w.ComputeDeltaAgainst(prev, 1<<30)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if isFull {
		t.Fatal("expected sub-threshold delta, got isFull=true")
	}
	if len(delta) == 0 {
		t.Fatal("empty delta")
	}
}

// TestDDSketchObserver verifies the SketchObserver dispatches a
// KindFloat observation into the underlying wrapper and rejects
// unsupported kinds.
func TestDDSketchObserver(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	if err := (DDSketchObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat, Float: 42}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := w.Quantile(0.5); abs(got-42) > 1 {
		t.Fatalf("post-observe quantile: %f", got)
	}
	if err := (DDSketchObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindHash}); err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
