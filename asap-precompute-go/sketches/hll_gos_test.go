// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
)

// gosTestHashForLZ returns a hash that maps to HLL register 0 with EXACTLY
// leading-zero count lz (1..50). Derivation (mirrors sketchlib-go's
// HLL/gos_test.go::testHashIndex0SmallLZ): InsertWithHash computes
// index = top-14 bits of hash (0 when hash < 2^50) and
// w = (hash<<14)|mask, lz = LeadingZeros64(w)+1. Setting hash = 1<<(50-lz)
// puts exactly one bit at position 63-(lz-1) of w, giving LeadingZeros64(w)
// = lz-1. lz=51 (the maximum) is the all-zero-payload case (hash=0).
func gosTestHashForLZ(lz uint8) uint64 {
	if lz >= 51 {
		return 0
	}
	if lz < 1 {
		lz = 1
	}
	return uint64(1) << (50 - uint(lz))
}

// TestGosRegisterCrossed is a direct unit test of the §8.7 boxed formula
// |2^C'-2^C|>=2^τ plus the first-nonzero-write mitigation, independent of
// any wrapper/insert plumbing.
func TestGosRegisterCrossed(t *testing.T) {
	cases := []struct {
		name      string
		last, cur uint8
		tau       float64
		want      bool
	}{
		{"no change", 5, 5, 1, false},
		{"regression (should never happen, but must not crash/report)", 5, 3, 1, false},
		{"first nonzero write, huge tau -> unconditional", 0, 1, 100, true},
		{"first nonzero write, tau=0", 0, 1, 0, true},
		{"tau=1 (double): 1->2 crosses (2^2-2^1=2 >= 2^1=2)", 1, 2, 1, true},
		{"tau=1 (double): 5->6 crosses (2^6-2^5=32 >= 2)", 5, 6, 1, true},
		{"tau=2 (4x): 5->6 does NOT cross (32 < 2^2=4)... wait recompute", 5, 6, 2, true},
		{"tau=10: small bump 1->2 does not cross (diff=2 < 1024)", 1, 2, 10, false},
		{"tau=10: big enough jump crosses", 1, 12, 10, true}, // 2^12-2^1=4094 >= 1024
		{"tau<=0 degenerates to report-any-change", 5, 6, 0, true},
		{"tau<=0, negative", 5, 6, -3, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := gosRegisterCrossed(c.last, c.cur, c.tau)
			if got != c.want {
				t.Fatalf("gosRegisterCrossed(last=%d, cur=%d, tau=%v) = %v, want %v", c.last, c.cur, c.tau, got, c.want)
			}
		})
	}
}

// TestGosRegisterCrossed_Tau2Boundary checks the τ=2 (4x) boundary precisely:
// 5->6 is 2^6-2^5=32, and 2^tau=4, so 32>=4 DOES cross (the earlier informal
// derivations-doc example "5->6 doubles" undersells how big real jumps are at
// high C; the point of the formula is that it's linearized, not that a
// single-step bump barely crosses). Verify the boundary value itself: a cur
// where the exact 2^C jump equals 2^tau.
func TestGosRegisterCrossed_ExactBoundary(t *testing.T) {
	// last=0 is always a first-write (unconditional), so use last=1 to
	// exercise the general (non-mitigated) boundary: 2^cur - 2^1 == 2^tau.
	// tau=2 -> threshold=4; cur such that 2^cur-2 == 4 -> 2^cur==6, not exact
	// integer; instead pick tau=1 -> threshold=2; 2^cur-2^1==2 -> 2^cur=4 ->
	// cur=2. So (last=1,cur=2,tau=1) sits EXACTLY at the boundary and must
	// cross (>=, not >).
	if !gosRegisterCrossed(1, 2, 1) {
		t.Fatal("exact-boundary crossing (diff == threshold) must be reported (>=, not >)")
	}
	// One less: (last=1, cur=2, tau=2) -> threshold=4, diff=2 < 4 -> no cross.
	if gosRegisterCrossed(1, 2, 2) {
		t.Fatal("diff below threshold must not be reported")
	}
}

// TestHLLWrapperGOS_DisabledIsUnchanged verifies gosTau<=0 (the default)
// behaves exactly like a plain HLLWrapper: no dirty entries, no wake, normal
// cardinality accumulation.
func TestHLLWrapperGOS_DisabledIsUnchanged(t *testing.T) {
	w := NewHLLWrapper()
	for i := 0; i < 500; i++ {
		w.UpdateValue(float64(i))
	}
	if len(w.gosDirty) != 0 {
		t.Fatalf("GOS disabled but gosDirty has %d entries", len(w.gosDirty))
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("GOS disabled must never arm the wake signal")
	}
	est := w.Estimate()
	if est < 400 || est > 600 {
		t.Fatalf("estimate looks wrong with GOS disabled: %d", est)
	}
}

// TestHLLWrapperGOS_NeverResetsRegisters is the central correctness property
// of this whole mechanism: after enough inserts to produce a non-empty GOS
// delta, draining that delta (via ComputeDeltaAgainst, exactly the runtime's
// flush path) must NOT alter a single underlying register. This is the
// OPPOSITE assertion from every other GOS-converted family in this
// workstream (e.g. CountSketch's cells ARE reset to 0 on send) — HLL's
// registers are monotone (MAX-merge) and must never regress or be cleared.
func TestHLLWrapperGOS_NeverResetsRegisters(t *testing.T) {
	w := NewHLLWrapper()
	w.SetGosMode(1, 0) // tau=1: report once a register's contribution at least doubles.

	for i := 0; i < 20000; i++ {
		w.UpdateValue(float64(i))
	}
	if len(w.gosDirty) == 0 {
		t.Fatal("expected at least one GOS crossing after 20000 distinct inserts")
	}

	before := append([]uint8(nil), w.sk.RegisterSlice()...)

	payload, isFull, err := w.ComputeDeltaAgainst(nil, 0)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("expected a non-empty GOS delta payload")
	}
	if isFull {
		t.Log("note: GOS delta was clamped to a full frame (delta >= full size) — still must not reset registers")
	}

	after := w.sk.RegisterSlice()
	if len(before) != len(after) {
		t.Fatalf("register count changed: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("register[%d] changed by drain: before=%d after=%d — "+
				"a GOS drain must NEVER reset a register (the critical "+
				"correctness difference from every other GOS-converted "+
				"family in this workstream)", i, before[i], after[i])
		}
	}

	// The drained list itself is exhausted (nothing left pending)...
	if len(w.gosDirty) != 0 {
		t.Fatalf("gosDirty should be empty after drain, got %d entries", len(w.gosDirty))
	}
	// ...but a SECOND ComputeDeltaAgainst before any new insert reports
	// nothing new (not a redundant full re-send of the whole sketch).
	payload2, isFull2, err := w.ComputeDeltaAgainst(nil, 0)
	if err != nil {
		t.Fatalf("second ComputeDeltaAgainst: %v", err)
	}
	if len(payload2) != 0 || isFull2 {
		t.Fatalf("second drain with nothing new queued should be (nil,false), got (%d bytes, isFull=%v)", len(payload2), isFull2)
	}

	if !isFull {
		// When not clamped to a full frame, verify the decoded delta's values
		// match the CURRENT (still live, unreset) register values exactly.
		deltaMsg, err := hll.DeserializeRegisterDelta(payload)
		if err != nil {
			t.Fatalf("DeserializeRegisterDelta: %v", err)
		}
		if len(deltaMsg.Updates) == 0 {
			t.Fatal("decoded delta has no updates")
		}
		for _, u := range deltaMsg.Updates {
			if got := after[u.Index]; got != u.Value {
				t.Fatalf("register[%d]: reported delta value %d != live (unreset) register value %d", u.Index, u.Value, got)
			}
		}
	}
}

// TestHLLWrapperGOS_FirstNonzeroWriteMitigation verifies the design doc's
// (explicitly UNVERIFIED, small-cardinality) proposed mitigation: a
// register's first-ever nonzero write is reported UNCONDITIONALLY, even
// under a tau so large that the raw jump would never cross on its own.
func TestHLLWrapperGOS_FirstNonzeroWriteMitigation(t *testing.T) {
	w := NewHLLWrapper()
	w.SetGosMode(10, 0) // threshold 2^10=1024; a first write of C'=1 (2^1=2) would never cross unmitigated.

	hash := gosTestHashForLZ(1) // deterministic: register 0, lz=1.
	idx, _, newVal, changed := w.sk.InsertWithHashReportingChange(hash)
	if !changed {
		t.Fatal("expected the first insert to change register 0")
	}
	if newVal != 1 {
		t.Fatalf("newVal = %d, want 1 (crafted hash targets lz=1)", newVal)
	}
	w.recordGosCrossing(idx, newVal)
	if len(w.gosDirty) != 1 {
		t.Fatalf("expected the first-ever nonzero write to be reported unconditionally despite tau=10, got %d dirty entries", len(w.gosDirty))
	}
	if w.gosDirty[0].Index != uint32(idx) || w.gosDirty[0].Value != newVal {
		t.Fatalf("gosDirty[0] = %+v, want {Index:%d Value:%d}", w.gosDirty[0], idx, newVal)
	}
	// The register itself must reflect the write (recordGosCrossing never
	// touches registers directly, but the earlier InsertWithHashReportingChange
	// call already did — sanity-check it's visible).
	if got := w.sk.RegisterValue(idx); got != newVal {
		t.Fatalf("register[%d] = %d, want %d", idx, got, newVal)
	}

	// A SECOND, smaller-or-equal candidate for the SAME register must not be
	// mechanically "changed" at all (MAX semantics), so no second report.
	idx2, _, _, changed2 := w.sk.InsertWithHashReportingChange(hash)
	if changed2 {
		t.Fatalf("repeat insert of the identical hash must not mechanically change register %d again", idx2)
	}
}

// TestHLLWrapperGOS_Idempotency verifies the MAX-merge idempotency property
// downstream consumers rely on: applying an already-known delta again, or a
// delta carrying smaller/stale register values, must never DECREASE a
// target's cardinality estimate.
func TestHLLWrapperGOS_Idempotency(t *testing.T) {
	source := NewHLLWrapper()
	source.SetGosMode(1, 0)
	for i := 0; i < 5000; i++ {
		source.UpdateValue(float64(i))
	}
	payload, _, err := source.ComputeDeltaAgainst(nil, 0)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("expected a non-empty delta from 5000 distinct inserts")
	}

	target := NewHLLWrapper()
	if err := target.ApplyDelta(payload); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	est1 := target.Estimate()
	if est1 == 0 {
		t.Fatal("expected a nonzero estimate after applying the delta")
	}

	// Re-applying the SAME delta again must be a no-op (max(x,x)=x).
	if err := target.ApplyDelta(payload); err != nil {
		t.Fatalf("ApplyDelta (repeat): %v", err)
	}
	if got := target.Estimate(); got != est1 {
		t.Fatalf("re-applying an already-known delta changed the estimate: %d -> %d (MAX-merge must be idempotent)", est1, got)
	}

	// Applying a delta built from a much smaller, strictly-earlier prefix
	// (guaranteed to carry only smaller-or-equal register values on any
	// overlapping index, by HLL's monotone construction) must never DECREASE
	// the target's estimate.
	stale := NewHLLWrapper()
	stale.SetGosMode(1, 0)
	for i := 0; i < 100; i++ {
		stale.UpdateValue(float64(i))
	}
	stalePayload, _, err := stale.ComputeDeltaAgainst(nil, 0)
	if err != nil {
		t.Fatalf("stale ComputeDeltaAgainst: %v", err)
	}
	if err := target.ApplyDelta(stalePayload); err != nil {
		t.Fatalf("ApplyDelta (stale): %v", err)
	}
	if got := target.Estimate(); got < est1 {
		t.Fatalf("applying a stale/smaller delta DECREASED the estimate: %d -> %d (MAX-merge must never regress)", est1, got)
	}
}

// TestHLLWrapperGOS_WakeSignal verifies ConsumeWakeSignal arms exactly once
// per burst of crossings and clears itself on read.
func TestHLLWrapperGOS_WakeSignal(t *testing.T) {
	w := NewHLLWrapper()
	w.SetGosMode(1, 0)
	if w.ConsumeWakeSignal() {
		t.Fatal("wake signal should be unarmed on a fresh wrapper")
	}
	for i := 0; i < 100; i++ {
		w.UpdateValue(float64(i))
	}
	if !w.ConsumeWakeSignal() {
		t.Fatal("expected the wake signal to be armed after a burst of GOS crossings")
	}
	if w.ConsumeWakeSignal() {
		t.Fatal("ConsumeWakeSignal must clear the flag (a second call in a row must report false)")
	}
}

// TestHLLWrapperGOS_ResetClearsPerWindowState verifies Reset clears the
// per-window GOS bookkeeping (gosDirty/gosLastSent/gosWake — a new window's
// registers all start at 0, so "last sent" must restart at 0 too) but
// preserves gosTau (a mode CONFIGURATION, not per-window accumulation
// state), mirroring CountSketchWrapper.Reset's gosEpsilon/gosSites handling.
func TestHLLWrapperGOS_ResetClearsPerWindowState(t *testing.T) {
	w := NewHLLWrapper()
	w.SetGosMode(2, 0)
	for i := 0; i < 100; i++ {
		w.UpdateValue(float64(i))
	}
	if len(w.gosDirty) == 0 || !w.gosWake {
		t.Fatal("expected dirty entries + an armed wake before Reset")
	}
	w.Reset()
	if len(w.gosDirty) != 0 {
		t.Fatalf("Reset must clear gosDirty, got %d entries", len(w.gosDirty))
	}
	if w.gosWake {
		t.Fatal("Reset must clear gosWake")
	}
	if w.gosLastSent != nil {
		t.Fatal("Reset must clear gosLastSent (new window = registers back to 0)")
	}
	if w.gosTau != 2 {
		t.Fatalf("Reset must NOT clear gosTau (mode config, not per-window state): got %v, want 2", w.gosTau)
	}
}
