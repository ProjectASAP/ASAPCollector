// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"math"
	"testing"
)

// TestSumWrapperGOS_EmptyDrainReturnsNil confirms drainGosDelta (reached via
// ComputeDeltaAgainst once GOS mode is active) returns a nil payload when
// nothing has crossed the insert-time threshold yet — the empty-dirty-set
// case the runtime treats as "nothing to emit" (design-gos-unified-edge-
// telemetry.md §11).
func TestSumWrapperGOS_EmptyDrainReturnsNil(t *testing.T) {
	w := NewSumWrapper()
	w.SetGosMode(0.5, 4)

	payload, isFull, err := w.ComputeDeltaAgainst(nil, 0)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if isFull {
		t.Fatalf("isFull = true, want false (GOS drains are never full frames)")
	}
	if payload != nil {
		t.Fatalf("payload = %v, want nil (nothing captured before any Update)", payload)
	}
}

// TestSumWrapperGOS_WakeFiresOnceAndClears proves the wake signal arms on the
// FIRST insert-time crossing (cold start: T≈0 when sum starts at 0, so the
// very first nonzero insert always crosses — design doc §11 "Cold start is a
// feature, not a bug"), is consumed exactly once, and does NOT re-arm for a
// small follow-up insert once the running total (and therefore the
// threshold) has grown large.
func TestSumWrapperGOS_WakeFiresOnceAndClears(t *testing.T) {
	w := NewSumWrapper()
	w.SetGosMode(0.1, 2)

	if w.ConsumeWakeSignal() {
		t.Fatal("wake armed before any insert")
	}

	w.Update(50) // cold start: T=ε·|sum|/k starts at 0, so this always crosses.
	if !w.ConsumeWakeSignal() {
		t.Fatal("expected wake to be armed after the first insert (cold start)")
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("wake did not clear after being consumed once")
	}

	// A tiny follow-up insert should NOT re-cross: the threshold now scales
	// with the much larger current total (T=ε·|50.0001|/2 ≈ 2.5), while the
	// since-crossing accumulator reset to 0 after the first capture.
	w.Update(0.0001)
	if w.ConsumeWakeSignal() {
		t.Fatal("small insert unexpectedly re-armed wake; threshold should have grown with the total")
	}
}

// TestSumWrapperGOS_TelescopingReconstruction is the key correctness
// property: repeatedly Update a source wrapper, periodically "flush" it
// (mirroring the runtime's SnapshotCache — Snapshot() on the very first
// emit, ComputeDeltaAgainst thereafter) and apply every non-nil payload onto
// a fresh target via ApplyDelta (the same additive path the backend uses).
// Nothing crossed must be lost or double-counted: the target's reconstructed
// total, plus whatever the source still has queued below threshold (a
// legitimate, bounded staleness gap — not a bug, see design doc §11's error
// bound Err^cdm ≤ kT), must equal the true cumulative sum.
func TestSumWrapperGOS_TelescopingReconstruction(t *testing.T) {
	w := NewSumWrapper()
	w.SetGosMode(0.15, 5)
	target := NewSumWrapper()

	values := []float64{3, -1, 7, 12, -20, 100, 0.5, -0.5, 40, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -63, 8, 2, 2}
	var trueTotal float64
	firstEmit := true

	flush := func(i int) {
		var payload []byte
		var err error
		if firstEmit {
			payload, err = w.Snapshot()
			firstEmit = false
		} else {
			payload, _, err = w.ComputeDeltaAgainst(nil, 0)
		}
		if err != nil {
			t.Fatalf("drain at i=%d: %v", i, err)
		}
		if payload != nil {
			if aerr := target.ApplyDelta(payload); aerr != nil {
				t.Fatalf("ApplyDelta at i=%d: %v", i, aerr)
			}
		}
	}

	for i, v := range values {
		w.Update(v)
		trueTotal += v
		// Flush every 4th insert — several insert-time crossings can queue
		// up in gosReadySum/gosReadyCount between these periodic flushes,
		// exercising the "burst of many crossings between two flushes"
		// accumulation path.
		if i%4 == 3 {
			flush(i)
		}
	}
	flush(len(values)) // final flush to drain anything captured since the loop's last checkpoint

	// Anything still sitting below threshold (never crossed) is a known,
	// bounded GOS staleness gap, not data loss — account for it explicitly.
	gotTotal := target.Sum() + w.gosSinceCrossSum
	if math.Abs(gotTotal-trueTotal) > 1e-9 {
		t.Fatalf("reconstructed total mismatch: target.Sum()=%v + residual=%v = %v, want true cumulative sum %v",
			target.Sum(), w.gosSinceCrossSum, gotTotal, trueTotal)
	}
	gotCount := target.Count() + w.gosSinceCrossCount
	if gotCount != uint64(len(values)) {
		t.Fatalf("reconstructed count mismatch: target.Count()=%d + residual=%d = %d, want %d",
			target.Count(), w.gosSinceCrossCount, gotCount, len(values))
	}
}
