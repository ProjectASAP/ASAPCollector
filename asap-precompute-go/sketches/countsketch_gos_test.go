// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

// TestCountSketchWrapper_GOS_EmptyDrainReturnsNil verifies ComputeDeltaAgainst
// in GOS mode returns a nil payload (not an empty-but-valid delta) before
// anything has crossed threshold — the runtime treats nil as "nothing to
// emit" (design-gos-unified-edge-telemetry.md §11).
import (
	"testing"
)

func TestCountSketchWrapper_GOS_EmptyDrainReturnsNil(t *testing.T) {
	t.Parallel()
	w, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("w: %v", err)
	}
	w.SetGosMode(0.5, 1)
	payload, isFull, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if payload != nil || isFull {
		t.Fatalf("expected (nil, false) before any crossing, got (%v, %v)", payload, isFull)
	}
}

// TestCountSketchWrapper_GOS_WakeSignalFiresOnceAndDrains drives enough
// inserts to force at least one threshold crossing (cold start: the
// threshold starts near 0, so it fires quickly — design doc §11 "cold start
// is a feature"), and verifies: ConsumeWakeSignal reports true exactly once
// (armed by the first crossing, cleared on read, not re-armed by later
// crossings in the same batch), and the subsequent drain is non-empty.
func TestCountSketchWrapper_GOS_WakeSignalFiresOnceAndDrains(t *testing.T) {
	t.Parallel()
	w, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("w: %v", err)
	}
	w.SetGosMode(0.9, 1)

	if w.ConsumeWakeSignal() {
		t.Fatal("wake signal must not be armed before any insert")
	}

	for i := 0; i < 200; i++ {
		w.UpdateString("k", 1.0)
	}

	fired := w.ConsumeWakeSignal()
	if !fired {
		t.Fatal("expected the wake signal to have fired after 200 inserts")
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

	// A second immediate drain (nothing crossed since the first drain) must
	// return nil — proves the dirty list was actually cleared, not just read.
	payload2, _, err := w.ComputeDeltaAgainst(nil, 1<<30)
	if err != nil {
		t.Fatalf("second ComputeDeltaAgainst: %v", err)
	}
	if payload2 != nil {
		t.Fatal("expected nil payload on the second drain (dirty list should be empty)")
	}
}

// TestCountSketchWrapper_GOS_TelescopingReconstruction is the wrapper-level
// counterpart of sketchlib-go's UpdateStringGOS telescoping test: repeatedly
// insert, periodically drain via ComputeDeltaAgainst + ApplyDelta onto a
// fresh target, and verify the target's reconstructed estimate matches a
// reference sketch fed the identical inserts WITHOUT ever resetting.
func TestCountSketchWrapper_GOS_TelescopingReconstruction(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 1024
	source, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("source: %v", err)
	}
	target, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("target: %v", err)
	}
	reference, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	source.SetGosMode(0.3, 1)

	const n = 3000
	for i := 0; i < n; i++ {
		source.UpdateString("k", 1.0)
		reference.UpdateString("k", 1.0)
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
	// Final drain picks up anything accumulated since the last periodic one.
	if payload, _, err := source.ComputeDeltaAgainst(nil, 1<<30); err != nil {
		t.Fatalf("final drain: %v", err)
	} else if payload != nil {
		if err := target.ApplyDelta(payload); err != nil {
			t.Fatalf("final ApplyDelta: %v", err)
		}
	}

	got := target.EstimateCount([]byte("k"))
	want := reference.EstimateCount([]byte("k"))
	if got != want {
		t.Fatalf("telescoped reconstruction = %v, want %v (reference, never reset/drained)", got, want)
	}
}
