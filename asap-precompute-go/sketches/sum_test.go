// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"encoding/binary"
	"math"
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func TestSumWrapper_RoundTripMergeReset(t *testing.T) {
	w := NewSumWrapper()
	for _, v := range []float64{1, 2, 3, 4} {
		w.Update(v)
	}
	if w.Sum() != 10 || w.Count() != 4 {
		t.Fatalf("after Update: sum=%v count=%v, want 10/4", w.Sum(), w.Count())
	}

	// Snapshot emits the fixed 16-byte {sum,count} payload; it must decode
	// back to the same values.
	b, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(b) != 16 {
		t.Fatalf("Snapshot len = %d, want 16", len(b))
	}
	gotSum := math.Float64frombits(binary.LittleEndian.Uint64(b[0:8]))
	gotCount := binary.LittleEndian.Uint64(b[8:16])
	if gotSum != 10 || gotCount != 4 {
		t.Fatalf("decoded payload sum=%v count=%v, want 10/4", gotSum, gotCount)
	}

	// ApplyDelta folds the snapshot into a fresh wrapper (additive load).
	w2 := NewSumWrapper()
	if err := w2.ApplyDelta(b); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if w2.Sum() != 10 || w2.Count() != 4 {
		t.Fatalf("after ApplyDelta: sum=%v count=%v, want 10/4", w2.Sum(), w2.Count())
	}

	// Merge is associative add.
	if err := w2.Merge(w); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if w2.Sum() != 20 || w2.Count() != 8 {
		t.Fatalf("after Merge: sum=%v count=%v, want 20/8", w2.Sum(), w2.Count())
	}

	// Reset zeros; an empty window emits a nil payload.
	w2.Reset()
	if w2.Sum() != 0 || w2.Count() != 0 {
		t.Fatalf("after Reset: sum=%v count=%v, want 0/0", w2.Sum(), w2.Count())
	}
	if empty, _ := w2.Snapshot(); empty != nil {
		t.Fatalf("empty Snapshot = %v, want nil", empty)
	}

	// Observer routes a KindFloat observation via Update.
	w3 := NewSumWrapper()
	if err := (SumObserver{}).Observe(w3, precompute.FloatValue(5)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if w3.Sum() != 5 || w3.Count() != 1 {
		t.Fatalf("after Observe: sum=%v count=%v, want 5/1", w3.Sum(), w3.Count())
	}
}
