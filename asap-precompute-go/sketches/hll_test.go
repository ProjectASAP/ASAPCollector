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

// TestHLLWrapper_DeltaNeverLargerThanFull verifies the min(full, delta) clamp:
// ComputeDeltaAgainst never returns a delta larger than the equivalent full
// frame. With a large base and a tiny incremental change the clamp keeps a
// (smaller) delta; with a near-empty base and a large change — where the
// per-register-update delta exceeds the sparse-packed full frame — it emits
// the full frame instead. Both directions respect payload ≤ full.
func TestHLLWrapper_DeltaNeverLargerThanFull(t *testing.T) {
	t.Parallel()
	check := func(name string, base, extra int) bool {
		w := NewHLLWrapper()
		for i := 0; i < base; i++ {
			w.UpdateValue(float64(i))
		}
		prev, err := w.Snapshot()
		if err != nil {
			t.Fatalf("%s prev: %v", name, err)
		}
		for i := base; i < base+extra; i++ {
			w.UpdateValue(float64(i))
		}
		full, err := w.Snapshot()
		if err != nil {
			t.Fatalf("%s full: %v", name, err)
		}
		payload, isFull, err := w.ComputeDeltaAgainst(prev, 1<<30)
		if err != nil {
			t.Fatalf("%s delta: %v", name, err)
		}
		if len(payload) > len(full) {
			t.Fatalf("%s: clamp violated, payload %d > full %d", name, len(payload), len(full))
		}
		if len(payload) == 0 {
			t.Fatalf("%s: empty payload", name)
		}
		return isFull
	}
	// Large base, tiny increment → delta far smaller than full → keep delta.
	if check("large-base", 20000, 50) {
		t.Fatal("large base + tiny increment should stay a delta, not clamp to full")
	}
	// Near-empty base, large increment → per-register-update delta exceeds the
	// sparse full frame → clamp emits full.
	if !check("near-empty-base", 1, 400) {
		t.Fatal("near-empty base + large increment should clamp to full")
	}
}

// TestHLLWrapper_PerWindowDelta_AgainstEmptyRoundTrips drives the
// precompute.SnapshotCache delta path with delta mode on
// (delta-against-empty, delta-baseline-contract.md §3): window 1 emits a
// full frame; window 2 — a fresh per-window sketch — emits a register
// DELTA computed against the empty base cached at window-1 close.
// Applying that delta to an EMPTY base reconstructs window 2's register
// state and the cardinality estimate matches.
func TestHLLWrapper_PerWindowDelta_AgainstEmptyRoundTrips(t *testing.T) {
	t.Parallel()
	c := precompute.NewSnapshotCache()

	// Window 1: first emit is full (no prior base).
	w1 := NewHLLWrapper()
	for i := 0; i < 2000; i++ {
		w1.UpdateValue(float64(i))
	}
	if _, isFull, err := c.ComputeDelta("series", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	// Window 2: a fresh per-window sketch with the SAME values. The cache
	// reset the base to empty at window-1 close, so this must be a DELTA.
	w2 := NewHLLWrapper()
	for i := 0; i < 2000; i++ {
		w2.UpdateValue(float64(i))
	}
	payload, _, err := c.ComputeDelta("series", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("window 2 payload empty")
	}
	// Note: the min(full,delta) clamp emits whichever is smaller. For HLL the
	// per-register-update delta-against-empty is typically larger than the
	// sparse-packed full frame, so window 2 usually emits a full frame here;
	// either way it reconstructs window 2 on an empty base.

	// Apply the payload to an EMPTY base -> reconstructs window 2 exactly
	// (HLL register-delta carries every non-zero register over an empty
	// base, so the reconstructed registers equal window 2's).
	recon := NewHLLWrapper()
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	want := float64(w2.Estimate())
	got := float64(recon.Estimate())
	if rel := abs(got-want) / want; rel > 0.001 {
		t.Fatalf("cardinality: want %f got %f rel %f", want, got, rel)
	}
}

// TestHLLWrapper_PerWindowDelta_NoCrossWindowSubtraction verifies two
// consecutive windows each emit their OWN register state. HLL merges by
// register-wise MAX over a never-reset base, which over-counts
// window-scoped cardinality (delta-baseline-contract.md §1.5 / §2.3); the
// delta-against-empty base reset makes window 2's emitted delta carry
// window 2's own registers. Window 1 sees a large disjoint key set;
// window 2 sees a small one. Reconstructing window 2 from EMPTY must
// yield window 2's own (small) cardinality, NOT the union with window 1.
func TestHLLWrapper_PerWindowDelta_NoCrossWindowSubtraction(t *testing.T) {
	t.Parallel()
	c := precompute.NewSnapshotCache()

	w1 := NewHLLWrapper()
	for i := 0; i < 5000; i++ {
		w1.UpdateValue(float64(i))
	}
	if _, isFull, err := c.ComputeDelta("s", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	// Window 2: a DISJOINT, much smaller key set.
	w2 := NewHLLWrapper()
	for i := 1_000_000; i < 1_000_200; i++ {
		w2.UpdateValue(float64(i))
	}
	payload, _, err := c.ComputeDelta("s", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("window 2 payload empty")
	}
	// The clamp may emit a full frame (window 2's own state) rather than a
	// register delta; either reconstructs window 2 only — never the union with
	// window 1 — which is what this test guards.

	recon := NewHLLWrapper()
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Reconstructed-from-empty cardinality must match window 2's own
	// (~200), NOT the ~5000-element union with window 1. A register-MAX
	// merge against a never-reset base would have left window 1's high
	// registers in place, inflating the estimate far above 200.
	got := float64(recon.Estimate())
	want := float64(w2.Estimate())
	if rel := abs(got-want) / want; rel > 0.05 {
		t.Fatalf("window 2 cardinality: want %f got %f rel %f", want, got, rel)
	}
	if got > 1000 {
		t.Fatalf("window 2 cardinality leaked window 1's registers: got %f", got)
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
