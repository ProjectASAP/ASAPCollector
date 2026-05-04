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
