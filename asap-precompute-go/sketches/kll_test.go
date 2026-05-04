// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestKLLWrapper_DeterministicSeed verifies that two KLL wrappers
// constructed with the same (k, seed) and fed the same input produce
// byte-identical Snapshots — the parity-harness invariant the OTel
// shim relies on.
func TestKLLWrapper_DeterministicSeed(t *testing.T) {
	t.Parallel()
	seed := int64(42)
	a := NewKLLWrapper(200, &seed)
	b := NewKLLWrapper(200, &seed)
	for _, v := range []float64{1, 5, 9, 4, 8, 2, 7, 6, 3, 10} {
		a.Update(v)
		b.Update(v)
	}
	sa, err := a.Snapshot()
	if err != nil {
		t.Fatalf("a.Snapshot: %v", err)
	}
	sb, err := b.Snapshot()
	if err != nil {
		t.Fatalf("b.Snapshot: %v", err)
	}
	if string(sa) != string(sb) {
		t.Fatalf("seeded snapshots differ (%d vs %d bytes)", len(sa), len(sb))
	}
}

// TestKLLWrapper_ComputeDeltaAlwaysFull confirms the KLL wrapper
// honors its design contract: KLL is not additively mergeable, so
// ComputeDeltaAgainst always returns isFull=true regardless of input.
func TestKLLWrapper_ComputeDeltaAlwaysFull(t *testing.T) {
	t.Parallel()
	seed := int64(1)
	w := NewKLLWrapper(200, &seed)
	for i := 0; i < 100; i++ {
		w.Update(float64(i))
	}
	_, isFull, err := w.ComputeDeltaAgainst([]byte("anything"), 1024)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if !isFull {
		t.Fatal("KLL ComputeDeltaAgainst must always report isFull=true")
	}
}

// TestKLLObserver verifies the KLL SketchObserver hot path.
func TestKLLObserver(t *testing.T) {
	t.Parallel()
	seed := int64(7)
	w := NewKLLWrapper(200, &seed)
	if err := (KLLObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat, Float: 99.0}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if w.Count() != 1 {
		t.Fatalf("count after one observe: %d", w.Count())
	}
}

// TestKLLWrapper_ApplyDeltaFromSnapshot confirms a Snapshot can be
// re-loaded into an empty wrapper via ApplyDelta — the inbound
// envelope merge path the runtime uses.
func TestKLLWrapper_ApplyDeltaFromSnapshot(t *testing.T) {
	t.Parallel()
	seed := int64(13)
	src := NewKLLWrapper(200, &seed)
	for i := 0; i < 200; i++ {
		src.Update(float64(i))
	}
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snap: %v", err)
	}
	dst := NewKLLWrapper(200, &seed)
	if err := dst.ApplyDelta(snap); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if dst.Count() == 0 {
		t.Fatal("dst empty after ApplyDelta")
	}
}
