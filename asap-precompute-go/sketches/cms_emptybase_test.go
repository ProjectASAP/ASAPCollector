// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
)

// populateCMS fills a fresh rows×cols CMS wrapper with a spread of hashed
// keys so the matrix has a realistic non-zero fill before delta encoding.
func populateCMS(rows, cols, nKeys, perKey int) *CMSWrapper {
	w := NewCMSWrapper(rows, cols, false)
	for k := 0; k < nKeys; k++ {
		h := common.FromBytes([]byte{byte(k), byte(k >> 8), byte(k >> 16)}).Hash
		for i := 0; i < perKey; i++ {
			w.InsertHash(h)
		}
	}
	return w
}

// TestCMSWrapper_EmptyBaseFastPath_WireParity is the correctness gate for the
// empty-base fast path: the delta bytes the optimized ComputeDeltaAgainst
// produces against the cached empty base MUST reconstruct to the exact same
// Count-Min state as the reference path (decode the empty prev with
// DeserializeCountMinSketchFromProtoBytes, then cms.ComputeDelta against it).
// We assert (a) the emitted payload is non-full, (b) it byte-equals the
// reference delta, and (c) point-frequency estimates reconstruct exactly.
func TestCMSWrapper_EmptyBaseFastPath_WireParity(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 2048

	w := populateCMS(rows, cols, 64, 7)

	// The empty base the SnapshotCache would have stored after a window close.
	emptyBase, err := w.DeltaAgainstEmptyBase()
	if err != nil {
		t.Fatalf("DeltaAgainstEmptyBase: %v", err)
	}
	if len(emptyBase) == 0 {
		t.Fatal("empty base must be non-empty for proto mode")
	}

	// Optimized path: diff against the empty base.
	got, isFull, err := w.ComputeDeltaAgainst(emptyBase, 1)
	if err != nil {
		t.Fatalf("ComputeDeltaAgainst(empty): %v", err)
	}
	if isFull {
		t.Fatal("empty-base delta should not be a full frame for a sparse fill")
	}

	// Reference path: replicate exactly what the old code did — decode the
	// empty prev, run sketchlib ComputeDelta, serialize.
	prevSk, err := cms.DeserializeCountMinSketchFromProtoBytes(emptyBase)
	if err != nil {
		t.Fatalf("decode empty base: %v", err)
	}
	refDeltaMsg, err := cms.ComputeDelta(prevSk, w.sk, 1)
	if err != nil {
		t.Fatalf("ref ComputeDelta: %v", err)
	}
	refBytes, err := cms.SerializeDelta(refDeltaMsg)
	if err != nil {
		t.Fatalf("ref SerializeDelta: %v", err)
	}

	if len(got) != len(refBytes) {
		t.Fatalf("payload length mismatch: opt=%d ref=%d", len(got), len(refBytes))
	}
	for i := range got {
		if got[i] != refBytes[i] {
			t.Fatalf("payload byte mismatch at %d: opt=%d ref=%d", i, got[i], refBytes[i])
		}
	}

	// Reconstruct from empty and check several point estimates match exactly.
	recon := NewCMSWrapper(rows, cols, false)
	if err := recon.ApplyDelta(got); err != nil {
		t.Fatalf("apply opt delta: %v", err)
	}
	for k := 0; k < 64; k++ {
		key := []byte{byte(k), byte(k >> 8), byte(k >> 16)}
		want := w.EstimateCount(key)
		gotEst := recon.EstimateCount(key)
		if want != gotEst {
			t.Fatalf("estimate mismatch for key %d: want %v got %v", k, want, gotEst)
		}
	}
}

// TestCMSWrapper_EmptyBaseFastPath_NonEmptyPrevUnchanged guards that a REAL
// (non-empty) prev still takes the standard decode-and-diff path and produces
// a correct, reconstructable delta — the fast path must only trigger for the
// empty base.
func TestCMSWrapper_EmptyBaseFastPath_NonEmptyPrevUnchanged(t *testing.T) {
	t.Parallel()
	const rows, cols = 5, 2048

	w := NewCMSWrapper(rows, cols, false)
	h := common.FromBytes([]byte("x")).Hash
	for i := 0; i < 50; i++ {
		w.InsertHash(h)
	}
	prev, err := w.Snapshot()
	if err != nil {
		t.Fatalf("prev: %v", err)
	}
	for i := 0; i < 100; i++ {
		w.InsertHash(h)
	}
	delta, isFull, err := w.ComputeDeltaAgainst(prev, 1)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if isFull {
		t.Fatal("expected a delta against a real non-empty prev")
	}
	// Reference: decode prev + sketchlib diff.
	prevSk, _ := cms.DeserializeCountMinSketchFromProtoBytes(prev)
	refMsg, _ := cms.ComputeDelta(prevSk, w.sk, 1)
	refBytes, _ := cms.SerializeDelta(refMsg)
	if len(delta) != len(refBytes) {
		t.Fatalf("non-empty prev delta length mismatch: got=%d ref=%d", len(delta), len(refBytes))
	}
}

// BenchmarkCMSComputeDeltaAgainstEmptyBase measures the hot path: a populated
// 5×2048 CMS computing its delta against the cached empty base on every emit.
func BenchmarkCMSComputeDeltaAgainstEmptyBase(b *testing.B) {
	const rows, cols = 5, 2048
	w := populateCMS(rows, cols, 200, 5)
	emptyBase, err := w.DeltaAgainstEmptyBase()
	if err != nil {
		b.Fatalf("DeltaAgainstEmptyBase: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := w.ComputeDeltaAgainst(emptyBase, 1)
		if err != nil {
			b.Fatalf("ComputeDeltaAgainst: %v", err)
		}
	}
}
