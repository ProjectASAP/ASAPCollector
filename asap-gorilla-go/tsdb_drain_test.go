package gorilla

import (
	"context"
	"testing"
	"time"
)

// TestDrainQueueSemantics exercises the earliest-pending drain-queue rewrite of
// drainWatermarkLocked: a series is drained only when its earliest pending
// timestamp is watermark-safe (maxObserved - reorderGrace), out-of-order points
// within grace are appended, points at/behind a series' last written timestamp
// are dropped, Finalize flushes everything, and series are independent.
//
// maxObserved is global across series; the watermark is maxObserved-grace.
func TestDrainQueueSemantics(t *testing.T) {
	b, err := NewStreamingTSDBBlockBuilder(StreamingTSDBOptions{
		ReorderGrace:    2 * time.Second,
		SamplesPerChunk: 120,
	})
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}

	add := func(series string, ms int64, v float64) {
		if err := b.AddSample(TSDBSample{
			MetricName: "m",
			Attributes: map[string]string{"s": series},
			Timestamp:  time.UnixMilli(ms),
			Value:      v,
		}); err != nil {
			t.Fatalf("add %s@%d: %v", series, ms, err)
		}
	}

	add("a", 1000, 1) // wm=-1000: pending, not drained
	add("b", 1000, 1) // wm=-1000: pending, not drained
	add("a", 5000, 5) // wm=3000: drains a@1000 and b@1000 (lastTs=1000 each)
	add("a", 2000, 2) // out-of-order within grace: 2000>lastTs(1000) → appended
	add("a", 1500, 9) // out-of-order behind lastTs(2000): dropped
	add("b", 4000, 4) // wm=3000: 4000>wm → stays pending

	// Before finalize: appended a@{1000,2000} + b@{1000} = 3; dropped a@1500 = 1.
	if b.numSamples != 3 {
		t.Errorf("pre-finalize numSamples = %d, want 3", b.numSamples)
	}
	if b.numDropped != 1 {
		t.Errorf("pre-finalize numDropped = %d, want 1", b.numDropped)
	}
	if got := b.ActiveSeries(); got != 2 {
		t.Errorf("ActiveSeries = %d, want 2", got)
	}

	art, err := b.Finalize(context.Background())
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	// Finalize flushes the still-pending a@5000 and b@4000.
	// Total appended: a@{1000,2000,5000}=3 + b@{1000,4000}=2 = 5; dropped=1.
	if art.NumSamples != 5 {
		t.Errorf("artifact NumSamples = %d, want 5", art.NumSamples)
	}
	if art.NumOOODropped != 1 {
		t.Errorf("artifact NumOOODropped = %d, want 1", art.NumOOODropped)
	}
}

// TestDrainQueueInOrderFlushesAll: the common in-order case — every sample
// eventually appended, none dropped, drain queue stays bounded (one entry per
// series with pending, not one per sample).
func TestDrainQueueInOrderFlushesAll(t *testing.T) {
	b, err := NewStreamingTSDBBlockBuilder(StreamingTSDBOptions{
		ReorderGrace:    time.Second,
		SamplesPerChunk: 120,
	})
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}
	const nSeries, nPer = 5, 50
	for i := 0; i < nPer; i++ {
		for s := 0; s < nSeries; s++ {
			ts := int64(10000 + i*100) // strictly increasing, in order
			if err := b.AddSample(TSDBSample{
				MetricName: "m",
				Attributes: map[string]string{"s": string(rune('a' + s))},
				Timestamp:  time.UnixMilli(ts),
				Value:      float64(i),
			}); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
	}
	// drain queue should be O(series), not O(samples): well under nSeries*nPer.
	if qlen := b.drainQueue.Len(); qlen > nSeries*4 {
		t.Errorf("drainQueue grew to %d (expected ~O(series)=%d) — not bounded", qlen, nSeries)
	}
	art, err := b.Finalize(context.Background())
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if want := uint64(nSeries * nPer); art.NumSamples != want {
		t.Errorf("NumSamples = %d, want %d (all in-order samples flushed)", art.NumSamples, want)
	}
	if art.NumOOODropped != 0 {
		t.Errorf("NumOOODropped = %d, want 0", art.NumOOODropped)
	}
}
