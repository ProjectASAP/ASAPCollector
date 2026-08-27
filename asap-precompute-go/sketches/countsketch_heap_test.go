// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	"github.com/ProjectASAP/sketchlib-go/wire/asapmsgpack"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestCountSketchWithHeapWrapper_SnapshotIsBackendReadable drives the
// heap-bearing wrapper through the observer (the keyed-observe path that
// feeds the Space-Saving tracker) and asserts Snapshot() emits the
// heap-bearing msgpack the backend reads, with a NON-EMPTY heap (the
// promotion gate) ranking the heavy hitter first.
func TestCountSketchWithHeapWrapper_SnapshotIsBackendReadable(t *testing.T) {
	w, err := NewCountSketchWithHeapWrapper(5, 1024, 20)
	if err != nil {
		t.Fatalf("new heap wrapper: %v", err)
	}
	obs := CountSketchObserver{DefaultKey: "fallback"}
	feed := func(key string, n int) {
		for i := 0; i < n; i++ {
			if err := obs.Observe(w, precompute.ObservationValue{
				Kind:  precompute.KindFloat,
				Float: 1.0,
				Bytes: []byte(key),
			}); err != nil {
				t.Fatalf("observe: %v", err)
			}
		}
	}
	feed("/checkout", 100)
	feed("/cart", 40)
	feed("/home", 5)

	b, err := w.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	rows, cols, _, heap, heapSize, err := asapmsgpack.UnmarshalCountSketchWithHeap(b)
	if err != nil {
		t.Fatalf("decode (must match backend from_msgpack framing): %v", err)
	}
	if rows != 5 || cols != 1024 || heapSize != 20 {
		t.Fatalf("dims rows=%d cols=%d heapSize=%d", rows, cols, heapSize)
	}
	if len(heap) == 0 {
		t.Fatal("heap empty — backend would NOT promote to CountSketchWithHeap")
	}
	if heap[0].Key != "/checkout" {
		t.Fatalf("heaviest item should rank first, got %q", heap[0].Key)
	}
}

// TestCountSketchWrapper_DefaultSnapshotUnchanged confirms the non-heap
// wrapper still emits the proto full state (byte-parity / back-compat).
func TestCountSketchWrapper_DefaultSnapshotUnchanged(t *testing.T) {
	w, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	w.UpdateString("x", 1)
	b, err := w.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// A proto full state must NOT decode as the heap-bearing msgpack.
	if _, _, _, _, _, err := asapmsgpack.UnmarshalCountSketchWithHeap(b); err == nil {
		t.Fatal("default proto snapshot unexpectedly decoded as heap msgpack")
	}
}
