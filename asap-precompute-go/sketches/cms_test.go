// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	"github.com/ProjectASAP/sketchlib-go/common"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestCMSWrapper_BasicRoundTrip drives insert -> snapshot -> apply
// and verifies the estimated frequency survives the full-state
// round-trip.
func TestCMSWrapper_BasicRoundTrip(t *testing.T) {
	t.Parallel()
	w := NewCMSWrapper(5, 1024, false)
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, k := range keys {
		for i := 0; i < 50; i++ {
			w.InsertHash(common.FromBytes(k).Hash)
		}
	}
	if got := w.EstimateCount([]byte("a")); got < 40 || got > 60 {
		t.Fatalf("EstimateCount(a): %f", got)
	}
	snap, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	other := NewCMSWrapper(5, 1024, false)
	if err := other.ApplyDelta(snap); err != nil {
		t.Fatalf("ApplyDelta(full): %v", err)
	}
	if got := other.EstimateCount([]byte("a")); got < 40 || got > 60 {
		t.Fatalf("post-apply EstimateCount(a): %f", got)
	}
}

// TestCMSWrapper_DeltaRoundTrip exercises the cms.ComputeDelta /
// SerializeDelta / ApplyDelta cycle the runtime drives now that
// SnapshotCache is always-refresh.
func TestCMSWrapper_DeltaRoundTrip(t *testing.T) {
	t.Parallel()
	w := NewCMSWrapper(5, 1024, false)
	for i := 0; i < 50; i++ {
		w.InsertHash(common.FromBytes([]byte("x")).Hash)
	}
	prev, err := w.Snapshot()
	if err != nil {
		t.Fatalf("prev: %v", err)
	}
	for i := 0; i < 100; i++ {
		w.InsertHash(common.FromBytes([]byte("x")).Hash)
	}
	delta, isFull, err := w.ComputeDeltaAgainst(prev, 1)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if isFull {
		t.Fatal("expected sub-threshold delta")
	}
	if len(delta) == 0 {
		t.Fatal("empty delta")
	}
	rebuilt := NewCMSWrapper(5, 1024, false)
	if err := rebuilt.ApplyDelta(prev); err != nil {
		t.Fatalf("apply prev: %v", err)
	}
	if err := rebuilt.ApplyDelta(delta); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	want := w.EstimateCount([]byte("x"))
	got := rebuilt.EstimateCount([]byte("x"))
	if got < want*0.9 || got > want*1.1 {
		t.Fatalf("post-delta estimate: want %f, got %f", want, got)
	}
}

// TestCMSWrapper_MsgpackForcesFullSnapshot confirms msgpack mode
// always emits a full snapshot — delta transmission isn't supported
// for msgpack-encoded CMS state.
func TestCMSWrapper_MsgpackForcesFullSnapshot(t *testing.T) {
	t.Parallel()
	w := NewCMSWrapper(5, 1024, true)
	w.InsertHash(common.FromBytes([]byte("k")).Hash)
	_, isFull, err := w.ComputeDeltaAgainst([]byte("ignored"), 1)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if !isFull {
		t.Fatal("msgpack mode must always report isFull=true")
	}
}

// TestCMSWrapper_PerWindowDelta_AgainstEmptyRoundTrips drives the
// precompute.SnapshotCache delta path with delta mode on
// (delta-against-empty, delta-baseline-contract.md §3): the first window
// emits a full frame, and the second window — a fresh per-window sketch,
// since the runtime resets per-series state every window — emits a DELTA
// computed against the empty base the cache stored at window-1 close.
// Applying that delta to an EMPTY base reconstructs the window's full
// per-cell state and the estimated frequencies match.
func TestCMSWrapper_PerWindowDelta_AgainstEmptyRoundTrips(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 1024
	c := precompute.NewSnapshotCache()

	key := common.FromBytes([]byte("hot")).Hash

	// Window 1: first emit is full (no prior base).
	w1 := NewCMSWrapper(rows, cols, false)
	for i := 0; i < 200; i++ {
		w1.InsertHash(key)
	}
	if _, isFull, err := c.ComputeDelta("series", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	// Window 2: a fresh per-window sketch. The cache reset the base to
	// empty at window-1 close, so this emit must be a DELTA.
	w2 := NewCMSWrapper(rows, cols, false)
	for i := 0; i < 200; i++ {
		w2.InsertHash(key)
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

	// Apply the delta to an EMPTY base -> reconstructs window 2.
	recon := NewCMSWrapper(rows, cols, false)
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	want := w2.EstimateCount([]byte("hot"))
	got := recon.EstimateCount([]byte("hot"))
	if got < want*0.9 || got > want*1.1 {
		t.Fatalf("estimate: want %f got %f", want, got)
	}
}

// TestCMSWrapper_PerWindowDelta_NoCrossWindowSubtraction verifies two
// consecutive windows each emit their OWN state — window 2's delta is
// NOT diffed against window 1. Window 1 inserts the key 300 times (high
// count), window 2 the SAME key 50 times. If the cache diffed window 2
// against window 1, the per-cell delta would be negative and a
// threshold>0 would drop the cells, under-transmitting window 2; under
// delta-against-empty the delta reconstructs window 2's own count from
// an empty base.
func TestCMSWrapper_PerWindowDelta_NoCrossWindowSubtraction(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 1024
	c := precompute.NewSnapshotCache()
	key := common.FromBytes([]byte("k")).Hash

	w1 := NewCMSWrapper(rows, cols, false)
	for i := 0; i < 300; i++ {
		w1.InsertHash(key)
	}
	if _, isFull, err := c.ComputeDelta("s", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	w2 := NewCMSWrapper(rows, cols, false)
	for i := 0; i < 50; i++ {
		w2.InsertHash(key)
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

	recon := NewCMSWrapper(rows, cols, false)
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Reconstructed-from-empty count must match window 2's own count
	// (~50), NOT window 1's (~300) and NOT a saturated 0.
	got := recon.EstimateCount([]byte("k"))
	want := w2.EstimateCount([]byte("k"))
	if got < want*0.9 || got > want*1.1 {
		t.Fatalf("window 2 count: want %f got %f", want, got)
	}
	if got > 100 {
		t.Fatalf("window 2 count leaked window 1's mass: got %f", got)
	}
}

// TestCMSWrapper_MsgpackOptsOutOfPerWindowDelta confirms msgpack mode
// returns a nil empty-base so the SnapshotCache keeps legacy
// always-refresh (msgpack cannot carry deltas), while proto mode opts in.
func TestCMSWrapper_MsgpackOptsOutOfPerWindowDelta(t *testing.T) {
	t.Parallel()
	mp := NewCMSWrapper(5, 1024, true)
	base, err := mp.DeltaAgainstEmptyBase()
	if err != nil {
		t.Fatalf("msgpack DeltaAgainstEmptyBase: %v", err)
	}
	if len(base) != 0 {
		t.Fatalf("msgpack mode must opt out (nil base), got %d bytes", len(base))
	}
	pr := NewCMSWrapper(5, 1024, false)
	base, err = pr.DeltaAgainstEmptyBase()
	if err != nil {
		t.Fatalf("proto DeltaAgainstEmptyBase: %v", err)
	}
	if len(base) == 0 {
		t.Fatal("proto mode must opt in (non-empty empty-base envelope)")
	}
}

// TestCMSObserver verifies the observer rejects non-bytes kinds and
// hashes byte input the same way the legacy processor does.
func TestCMSObserver(t *testing.T) {
	t.Parallel()
	w := NewCMSWrapper(5, 1024, false)
	if err := (CMSObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat}); err == nil {
		t.Fatal("expected error for KindFloat")
	}
	if err := (CMSObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindBytes, Bytes: []byte("z")}); err != nil {
		t.Fatalf("Observe(KindBytes): %v", err)
	}
	if got := w.EstimateCount([]byte("z")); got < 0.5 {
		t.Fatalf("post-observe estimate: %f", got)
	}
}

// TestCMSObserver_RowSampled verifies a RowSampled observation routes
// through ApplyAdmittedOccurrence (not InsertHash), applying the SDK's
// admission bitmask verbatim.
func TestCMSObserver_RowSampled(t *testing.T) {
	t.Parallel()
	w := NewCMSWrapper(5, 1024, false)
	allRows := uint64(0b11111)
	for i := 0; i < 500; i++ {
		v := precompute.ObservationValue{
			Kind: precompute.KindBytes, Bytes: []byte("hot"),
			RowSampled: true, AdmittedRows: allRows, SampleP: 1.0,
		}
		if err := (CMSObserver{}).Observe(w, v); err != nil {
			t.Fatalf("Observe(RowSampled): %v", err)
		}
	}
	if got := w.EstimateCount([]byte("hot")); got < 490 || got > 510 {
		t.Fatalf("estimate = %v, want ~500 (all rows admitted at p=1)", got)
	}

	w2 := NewCMSWrapper(5, 1024, false)
	zero := precompute.ObservationValue{
		Kind: precompute.KindBytes, Bytes: []byte("x"),
		RowSampled: true, AdmittedRows: 0, SampleP: 0.3,
	}
	if err := (CMSObserver{}).Observe(w2, zero); err != nil {
		t.Fatalf("Observe(RowSampled, zero mask): %v", err)
	}
	if got := w2.EstimateCount([]byte("x")); got != 0 {
		t.Fatalf("zero admittedRows must be a no-op, got estimate %v", got)
	}
}
