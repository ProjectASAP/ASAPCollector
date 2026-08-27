// Copyright ProjectASAP Authors
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

// TestHLLWrapper_UpdateBytesCardinality verifies the item_label cardinality
// path: hashing distinct byte keys (e.g. user_id values) via UpdateBytes /
// KindBytes counts DISTINCT keys, and repeated keys do not inflate the
// estimate. This is the BUG-2 HLL subject — distinct label-value cardinality,
// not the numeric sample.
func TestHLLWrapper_UpdateBytesCardinality(t *testing.T) {
	t.Parallel()
	w := NewHLLWrapper()
	obs := HLLObserver{}
	// 300 distinct user_ids, each observed 5×.
	for rep := 0; rep < 5; rep++ {
		for i := 0; i < 300; i++ {
			key := []byte("u" + itoa5(i))
			if err := obs.Observe(w, precompute.BytesValue(key)); err != nil {
				t.Fatalf("Observe: %v", err)
			}
		}
	}
	est := w.Estimate()
	if est < 270 || est > 330 {
		t.Fatalf("distinct-key estimate %d, want ~300 (±10%%)", est)
	}
	// An empty key is a no-op.
	before := w.Estimate()
	if err := obs.Observe(w, precompute.BytesValue(nil)); err != nil {
		t.Fatalf("Observe(empty): %v", err)
	}
	if w.Estimate() != before {
		t.Fatalf("empty key changed estimate %d -> %d", before, w.Estimate())
	}
}

func itoa5(i int) string {
	b := []byte("00000")
	for p := 4; p >= 0 && i > 0; p-- {
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b)
}

// TestHLLWrapper_DeltaNeverLargerThanFull verifies the genuine anti-
// pessimization invariant: ComputeDeltaAgainst never returns a payload LARGER
// than the equivalent full snapshot of the same state. The wrapper enforces
// this with a min(full, delta) clamp (hll.go: "if len(payload) >= len(full)
// return full, isFull=true"), so the wire payload is always min(delta, full).
//
// Sparse-representation note (sketchlib-go #66 in-memory sparse HLL): the full
// snapshot is now representation-adaptive. A low/medium-cardinality sketch
// serializes to a small wire-sparse frame whose size grows with the number of
// non-zero registers, and only a high-cardinality sketch reaches the dense
// register array (~16.5KB). Because BOTH the full frame and the register delta
// shrink with cardinality, whether the clamp fires is cardinality-dependent —
// it does NOT fire for a small/medium increment (the delta legitimately wins),
// and it DOES fire only once the register-delta cost catches up with the dense
// full frame at high cardinality. So this test asserts the size invariant
// directly (delta <= full, always) and checks the clamp's TWO genuine regimes
// rather than hardcoding which fixed input forces a full.
//
// Measured payload vs full (bytes) on the merged sparse-HLL dependency:
//
//	base    extra   full    delta   isFull
//	20000   +50     16532   88      false   (delta wins by a mile)
//	1       +400    957     805     false   (sparse full > delta: delta still wins)
//	0       +50000  16532   16532   true    (delta caught up -> clamp to full)
func TestHLLWrapper_DeltaNeverLargerThanFull(t *testing.T) {
	t.Parallel()
	// check returns isFull and asserts the size invariant for the given case.
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
		// The core invariant: the emitted payload is never larger than the
		// equivalent full snapshot of the SAME state. Compared against the
		// ACTUAL same-state full size, never a hardcoded dense constant — so
		// it stays correct whether that full snapshot is sparse or dense.
		if len(payload) > len(full) {
			t.Fatalf("%s: pessimization — payload %d > same-state full %d", name, len(payload), len(full))
		}
		if len(payload) == 0 {
			t.Fatalf("%s: empty payload", name)
		}
		// When the clamp fires (isFull) the payload must BE the full snapshot;
		// when it does not, the payload must be a strictly smaller delta. This
		// pins both regimes without assuming which input lands in which.
		if isFull && len(payload) != len(full) {
			t.Fatalf("%s: isFull but payload %d != full %d", name, len(payload), len(full))
		}
		if !isFull && len(payload) >= len(full) {
			t.Fatalf("%s: kept a delta that is not smaller than full (%d >= %d)", name, len(payload), len(full))
		}
		return isFull
	}
	// Large established base, tiny increment → delta is tiny → keep delta.
	if check("large-base", 20000, 50) {
		t.Fatal("large base + tiny increment should stay a delta, not clamp to full")
	}
	// Near-empty base, medium increment. Under the sparse representation the
	// full snapshot is itself small (wire-sparse) but the register delta is
	// still smaller, so the delta is (correctly) KEPT — it is not a
	// pessimization. (Pre-#66 the full frame here was the dense array; the
	// delta-vs-full size relationship is what changed, not the invariant.)
	if check("near-empty-base", 1, 400) {
		t.Fatal("near-empty base + medium increment: delta is smaller than the sparse full, should be kept")
	}
	// High-cardinality from empty: the register delta grows until it reaches
	// the dense full-frame size, at which point the clamp fires and emits full.
	// This is the regime that genuinely exercises the min(full, delta) fallback.
	if !check("high-card-from-empty", 0, 50000) {
		t.Fatal("high-cardinality delta should reach the dense full size and clamp to full")
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
