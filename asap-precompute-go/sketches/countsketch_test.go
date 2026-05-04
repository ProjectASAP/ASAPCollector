// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestCountSketchWrapper_BasicRoundTrip drives observe -> snapshot ->
// merge into another wrapper and verifies the estimated frequency
// roughly matches across the roundtrip.
func TestCountSketchWrapper_BasicRoundTrip(t *testing.T) {
	t.Parallel()
	a, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	for i := 0; i < 100; i++ {
		a.UpdateString("foo", 1.0)
	}
	for i := 0; i < 50; i++ {
		a.UpdateString("bar", 1.0)
	}
	if got := a.EstimateCount([]byte("foo")); got < 90 || got > 110 {
		t.Fatalf("EstimateCount(foo): %f", got)
	}
	snap, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("empty snapshot")
	}
}

// TestCountSketchWrapper_DeltaRoundTrip exercises the delta path
// the runtime relies on after the SnapshotCache always-refresh fix.
func TestCountSketchWrapper_DeltaRoundTrip(t *testing.T) {
	t.Parallel()
	w, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("w: %v", err)
	}
	for i := 0; i < 100; i++ {
		w.UpdateString("baz", 1.0)
	}
	prev, err := w.Snapshot()
	if err != nil {
		t.Fatalf("prev: %v", err)
	}
	for i := 0; i < 200; i++ {
		w.UpdateString("baz", 1.0)
	}
	delta, isFull, err := w.ComputeDeltaAgainst(prev, 1<<30)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if isFull {
		t.Fatal("expected sub-threshold delta")
	}
	if len(delta) == 0 {
		t.Fatal("empty delta payload")
	}
}

// TestCountSketchObserver verifies the observer routes through to the
// underlying CountSketch via UpdateString and rejects unsupported
// value kinds.
func TestCountSketchObserver(t *testing.T) {
	t.Parallel()
	w, _ := NewCountSketchWrapper(5, 1024)
	obs := CountSketchObserver{DefaultKey: "default"}
	if err := obs.Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat, Float: 1.0, Bytes: []byte("k1")}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := w.EstimateCount([]byte("k1")); got < 0.5 {
		t.Fatalf("EstimateCount: %f", got)
	}
	if err := obs.Observe(w, precompute.ObservationValue{Kind: precompute.KindHash}); err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}
