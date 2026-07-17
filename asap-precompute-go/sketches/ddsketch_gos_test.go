// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"math"
	"testing"
)

// TestDDSketchWrapper_GOS_EmptyDrainReturnsNil verifies ComputeDeltaAgainst
// in GOS mode returns a nil payload (not an empty-but-valid delta) before
// anything has crossed threshold — the runtime treats nil as "nothing to
// emit" (design-gos-unified-edge-telemetry.md §11). Mirrors the CountSketch
// wrapper's equivalent.
func TestDDSketchWrapper_GOS_EmptyDrainReturnsNil(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	w.SetGosMode(0.5, 1)
	payload, isFull, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if payload != nil || isFull {
		t.Fatalf("expected (nil, false) before any crossing, got (%v, %v)", payload, isFull)
	}
}

// TestDDSketchWrapper_GOS_WakeSignalFiresOnceAndDrains drives enough inserts
// of the SAME value (so one bucket's count climbs quickly) to force at least
// one threshold crossing (cold start: T=ε·N/(k·B) starts near 0, so it fires
// quickly), and verifies: ConsumeWakeSignal reports true exactly once (armed
// by the first crossing, cleared on read, not re-armed by later crossings in
// the same batch), the drain is non-empty and never isFull, and a second
// immediate drain returns nil (dirty list actually cleared).
func TestDDSketchWrapper_GOS_WakeSignalFiresOnceAndDrains(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	w.SetGosMode(0.9, 1)

	if w.ConsumeWakeSignal() {
		t.Fatal("wake signal must not be armed before any insert")
	}

	for i := 0; i < 500; i++ {
		w.Update(100.0)
	}

	if !w.ConsumeWakeSignal() {
		t.Fatal("expected the wake signal to have fired after 500 inserts")
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("ConsumeWakeSignal must clear the flag on read (second call must be false)")
	}

	payload, isFull, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if isFull {
		t.Fatal("GOS drain must never report isFull")
	}
	if len(payload) == 0 {
		t.Fatal("expected a non-empty drained delta payload")
	}

	payload2, _, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("second ComputeDeltaAgainst: %v", err)
	}
	if payload2 != nil {
		t.Fatal("expected nil payload on the second drain (dirty list should be empty)")
	}
}

// TestDDSketchWrapper_GOS_TelescopingReconstruction is the wrapper-level
// counterpart of sketchlib-go's UpdateGOS telescoping test: repeatedly
// insert into a GOS-mode source, periodically drain via ComputeDeltaAgainst +
// ApplyDelta onto a fresh target, then apply the source's final below-
// threshold RESIDUAL (its remaining full snapshot — the crossed buckets were
// already zeroed in place, so the snapshot holds only what hasn't been sent),
// and verify the target reconstructs the same count and quantiles a reference
// sketch fed the identical inserts WITHOUT ever resetting would answer.
func TestDDSketchWrapper_GOS_TelescopingReconstruction(t *testing.T) {
	t.Parallel()
	const alpha = 0.01
	source := NewDDSketchWrapper(alpha)
	target := NewDDSketchWrapper(alpha)
	reference := NewDDSketchWrapper(alpha)
	source.SetGosMode(0.3, 1)

	values := []float64{1.0, 2.5, 10.0, 42.0, 1000.0}
	const n = 4000
	for i := 0; i < n; i++ {
		v := values[i%len(values)]
		source.Update(v)
		reference.Update(v)
		if i%50 == 49 {
			payload, _, err := source.ComputeDeltaAgainst(nil, 1<<30)
			if err != nil {
				t.Fatalf("drain at i=%d: %v", i, err)
			}
			if payload == nil {
				continue
			}
			if err := target.ApplyDelta(payload); err != nil {
				t.Fatalf("ApplyDelta at i=%d: %v", i, err)
			}
		}
	}
	// Final crossed-bucket drain.
	if payload, _, err := source.ComputeDeltaAgainst(nil, 1<<30); err != nil {
		t.Fatalf("final drain: %v", err)
	} else if payload != nil {
		if err := target.ApplyDelta(payload); err != nil {
			t.Fatalf("final ApplyDelta: %v", err)
		}
	}
	// Apply the below-threshold residual: source.sk now holds ONLY the
	// un-crossed accumulation (crossed buckets were reset to 0 in place), so
	// its full snapshot is exactly the residual to fold in — mirrors the
	// sketchlib-level test's EachBucket residual drain and a real
	// window-boundary flush.
	residual, err := source.Snapshot()
	if err != nil {
		t.Fatalf("residual snapshot: %v", err)
	}
	if err := target.ApplyDelta(residual); err != nil {
		t.Fatalf("residual ApplyDelta: %v", err)
	}

	// Total count must reconstruct exactly (bucket counts are exact — no
	// hashing collisions). LinearReadout([0]) counts every bucket (all values
	// are positive, so all lie in [0, +Inf]).
	gotN := target.LinearReadout([]float64{0})
	wantN := reference.LinearReadout([]float64{0})
	if gotN != wantN {
		t.Fatalf("telescoped total count = %v, want %v (reference, never reset/drained)", gotN, wantN)
	}

	// Quantiles must match within the relative-accuracy guarantee.
	const tol = 2 * alpha
	for _, q := range []float64{0.0, 0.25, 0.5, 0.9, 1.0} {
		got := target.Quantile(q)
		want := reference.Quantile(q)
		if want == 0 {
			continue
		}
		if re := math.Abs(got-want) / want; re > tol {
			t.Errorf("q=%v: telescoped=%v, want=%v, relErr=%v > tol=%v", q, got, want, re, tol)
		}
	}
}
