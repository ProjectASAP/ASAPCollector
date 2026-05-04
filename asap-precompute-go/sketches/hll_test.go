// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestHLLWrapper_BasicObserveSnapshot drives a basic observe ->
// snapshot cycle and verifies the cardinality estimate is non-zero
// and within the expected error margin.
func TestHLLWrapper_BasicObserveSnapshot(t *testing.T) {
	t.Parallel()
	w := NewHLLWrapper()
	for i := 0; i < 1000; i++ {
		w.UpdateValue(float64(i))
	}
	want := w.Estimate()
	if want < 800 || want > 1200 {
		t.Fatalf("baseline estimate looks wrong: %d", want)
	}
	snap, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("empty snapshot")
	}
}

// TestHLLWrapper_RegisterDeltaShape verifies ComputeDeltaAgainst
// returns a non-empty register-delta payload with isFull=false when
// fed a non-empty prior snapshot.
func TestHLLWrapper_RegisterDeltaShape(t *testing.T) {
	t.Parallel()
	w := NewHLLWrapper()
	for i := 0; i < 500; i++ {
		w.UpdateValue(float64(i))
	}
	prev, err := w.Snapshot()
	if err != nil {
		t.Fatalf("prev: %v", err)
	}
	for i := 500; i < 1000; i++ {
		w.UpdateValue(float64(i))
	}
	delta, isFull, err := w.ComputeDeltaAgainst(prev, 1<<30)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if isFull {
		t.Fatal("expected sparse register delta, got full")
	}
	if len(delta) == 0 {
		t.Fatal("empty delta payload")
	}
}

// TestHLLObserver verifies the HLL SketchObserver hot path.
func TestHLLObserver(t *testing.T) {
	t.Parallel()
	w := NewHLLWrapper()
	for i := 0; i < 100; i++ {
		err := (HLLObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat, Float: float64(i)})
		if err != nil {
			t.Fatalf("Observe[%d]: %v", i, err)
		}
	}
	if w.Estimate() == 0 {
		t.Fatal("estimate zero after 100 observes")
	}
}
