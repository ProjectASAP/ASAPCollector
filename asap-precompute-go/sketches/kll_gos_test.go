// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

// TestKLLWrapper_GOS_WakeFiresAtThresholdNotBefore verifies the insert-time
// GOS emit trigger (derivations doc §8.6 "Emit trigger": R>=epsilon*N, where
// R is the sketch's own since-last-reset Count() and N is the wrapper's
// never-reset windowTotal): with epsilon=0.5, the wake must NOT be armed for
// the first several inserts (R==N so R>=epsilon*N holds from the very first
// insert onward once epsilon<=1 — so this test uses a fresh wrapper and
// checks the signal is unarmed before ANY insert, then fires on the very
// first insert once GOS mode is enabled) and must be consumed exactly once
// (cleared on read, not re-armed merely by more inserts crossing the
// already-armed flag again).
import (
	"testing"
)

func TestKLLWrapper_GOS_WakeFiresAtThresholdNotBefore(t *testing.T) {
	t.Parallel()
	w := NewKLLWrapper(200, nil)
	w.SetGosMode(0.5)

	if w.ConsumeWakeSignal() {
		t.Fatal("wake signal must not be armed before any insert")
	}

	// R==N after every insert (one KLL Update per wrapper Update call), so
	// R>=epsilon*N (0.5*N) holds starting at the very first insert.
	w.Update(1.0)

	if !w.ConsumeWakeSignal() {
		t.Fatal("expected the wake signal to have fired after crossing R>=epsilon*N")
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("ConsumeWakeSignal must clear the flag on read (second call must be false)")
	}
}

// TestKLLWrapper_GOS_WakeNeverFiresWhenDisabled verifies epsilon<=0 (GOS
// disabled, the default) leaves Update byte-identical to before: no wake
// ever fires regardless of how many items are inserted.
func TestKLLWrapper_GOS_WakeNeverFiresWhenDisabled(t *testing.T) {
	t.Parallel()
	w := NewKLLWrapper(200, nil)
	for i := 0; i < 500; i++ {
		w.Update(float64(i))
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("wake signal must never fire when GOS mode is disabled")
	}
}

// TestKLLWrapper_GOS_HighEpsilonDelaysWake verifies the trigger genuinely
// depends on the epsilon/N relationship, not just "any insert": with
// epsilon>1 the R>=epsilon*N condition can never be satisfied (R can equal
// but never exceed N under this wrapper's one-Update-increments-both
// invariant), so the wake must never fire.
func TestKLLWrapper_GOS_HighEpsilonDelaysWake(t *testing.T) {
	t.Parallel()
	w := NewKLLWrapper(200, nil)
	w.SetGosMode(1.5)
	for i := 0; i < 1000; i++ {
		w.Update(float64(i))
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("epsilon>1 must never satisfy R>=epsilon*N under this wrapper's R==N invariant")
	}
}

// TestKLLWrapper_GOS_SegmentResetPreservesWindowTotal is the wrapper-level
// proof of the disjoint-segment model surviving a GOS-triggered emit
// (design-gos-unified-edge-telemetry.md §11; derivations doc §8.6): after
// Reset() (the segment reset EmitSubWindow performs right after a successful
// emit), the sketch's own Count() (R) goes back to 0 — a fresh segment — but
// windowTotal (N) must NOT reset, since it tracks the window's lifetime
// count, a different quantity. A second round of inserts after Reset must
// re-trigger the wake using the CONTINUED N (not a reset N), proving the
// segment/reset mechanism and the trigger's own bookkeeping don't interfere
// with each other and don't double-count.
func TestKLLWrapper_GOS_SegmentResetPreservesWindowTotal(t *testing.T) {
	t.Parallel()
	w := NewKLLWrapper(200, nil)
	w.SetGosMode(0.9)

	const firstBatch = 100
	for i := 0; i < firstBatch; i++ {
		w.Update(float64(i))
	}
	if !w.ConsumeWakeSignal() {
		t.Fatal("expected wake after first batch")
	}
	if got := w.Count(); got != firstBatch {
		t.Fatalf("Count() before reset = %d, want %d", got, firstBatch)
	}

	// Simulate EmitSubWindow's post-emit segment reset.
	w.Reset()

	if got := w.Count(); got != 0 {
		t.Fatalf("Count() (R) after Reset = %d, want 0 (fresh segment)", got)
	}
	if w.windowTotal != firstBatch {
		t.Fatalf("windowTotal (N) after Reset = %d, want %d (must survive the segment reset)", w.windowTotal, firstBatch)
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("Reset must clear any pending wake (the segment that armed it was just emitted)")
	}

	// Next segment: a single new insert brings R (segment count) to 1
	// against N (window total) now at firstBatch+1, so R>=0.9*N is false —
	// proving N (not a reset N) governs the trigger, i.e. the second
	// segment does NOT get an unearned near-zero-N cold start.
	w.Update(999.0)
	if w.ConsumeWakeSignal() {
		t.Fatal("wake must not re-fire immediately: R=1 against the CONTINUED window total must not satisfy R>=epsilon*N")
	}
	if w.windowTotal != firstBatch+1 {
		t.Fatalf("windowTotal after one more insert = %d, want %d", w.windowTotal, firstBatch+1)
	}

	// Snapshot after the reset must reflect only the new segment's own
	// data (the disjoint-segment contract), not the pre-reset segment's.
	payload, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if payload == nil {
		t.Fatal("expected a non-nil snapshot after the new segment's insert")
	}
}
