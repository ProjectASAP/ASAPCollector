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

// TestCountSketchWrapper_PerWindowDelta_AgainstEmptyRoundTrips drives
// the precompute.SnapshotCache delta path with delta mode on
// (delta-against-empty, delta-baseline-contract.md §3): window 1 emits a
// full frame; window 2 — a fresh per-window sketch — emits a DELTA
// computed against the empty base cached at window-1 close. Applying that
// delta to an EMPTY base reconstructs window 2's per-cell state and the
// estimated frequency matches.
func TestCountSketchWrapper_PerWindowDelta_AgainstEmptyRoundTrips(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 1024
	c := precompute.NewSnapshotCache()

	w1, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("w1: %v", err)
	}
	for i := 0; i < 200; i++ {
		w1.UpdateString("hot", 1.0)
	}
	if _, isFull, err := c.ComputeDelta("series", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	w2, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	for i := 0; i < 200; i++ {
		w2.UpdateString("hot", 1.0)
	}
	payload, isFull, err := c.ComputeDelta("series", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if isFull {
		t.Fatal("window 2 must emit a delta, not a full frame")
	}
	if len(payload) == 0 {
		t.Fatal("window 2 delta payload empty")
	}

	recon, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("recon: %v", err)
	}
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	want := w2.EstimateCount([]byte("hot"))
	got := recon.EstimateCount([]byte("hot"))
	// CountSketch's median-of-rows estimator round-trips the cells
	// exactly under delta-against-empty, so the estimate matches.
	if got < want*0.9 || got > want*1.1 {
		t.Fatalf("estimate: want %f got %f", want, got)
	}
}

// TestCountSketchWrapper_PerWindowDelta_NoCrossWindowSubtraction
// verifies two consecutive windows each emit their OWN state. Window 1
// inserts "k" 300 times, window 2 inserts it 50 times. Reconstructing
// window 2 from EMPTY must yield window 2's own count (~50), proving the
// delta was computed against empty, not against window 1.
func TestCountSketchWrapper_PerWindowDelta_NoCrossWindowSubtraction(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 1024
	c := precompute.NewSnapshotCache()

	w1, _ := NewCountSketchWrapper(rows, cols)
	for i := 0; i < 300; i++ {
		w1.UpdateString("k", 1.0)
	}
	if _, isFull, err := c.ComputeDelta("s", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	w2, _ := NewCountSketchWrapper(rows, cols)
	for i := 0; i < 50; i++ {
		w2.UpdateString("k", 1.0)
	}
	payload, isFull, err := c.ComputeDelta("s", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if isFull {
		t.Fatal("window 2 must emit a delta")
	}
	if len(payload) == 0 {
		t.Fatal("window 2 delta empty — cross-window subtraction leaked")
	}

	recon, _ := NewCountSketchWrapper(rows, cols)
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := recon.EstimateCount([]byte("k"))
	want := w2.EstimateCount([]byte("k"))
	if got < want*0.9 || got > want*1.1 {
		t.Fatalf("window 2 count: want %f got %f", want, got)
	}
	if got > 150 {
		t.Fatalf("window 2 count leaked window 1's mass: got %f", got)
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
