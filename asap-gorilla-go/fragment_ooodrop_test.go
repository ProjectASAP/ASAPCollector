package gorilla

import (
	"context"
	"testing"
	"time"
)

// TestFragmentOOODropCountIsPerFragmentDelta proves the OOO-drop counter is
// shipped as a PER-FRAGMENT DELTA (drops since the last flush), not the running
// cumulative total. The bug it guards: stamping every flushed fragment with the
// encoder's running cumulative e.dropped made FragmentBlockFinalizer.AddFragment
// (which SUMS OOODropCount over every fragment) over-count quadratically. With
// the delta fix, the finalizer's summation must equal the true number of dropped
// samples across many fragments.
func TestFragmentOOODropCountIsPerFragmentDelta(t *testing.T) {
	const (
		grace           = 2000 // reorder grace ms
		samplesPerChunk = 10   // small chunks so many fragments flush
	)
	enc := NewStreamingFragmentEncoder(StreamingFragmentOptions{
		ReorderGrace:    grace * time.Millisecond,
		SamplesPerChunk: samplesPerChunk,
		IdleEvict:       -1, // disable eviction; not under test here
		Source:          "agent-ooo",
	})

	add := func(series string, ms int64, v float64) {
		if err := enc.AddSample(TSDBSample{
			MetricName: "m",
			Attributes: map[string]string{"s": series},
			Timestamp:  time.UnixMilli(ms),
			Value:      v,
		}); err != nil {
			t.Fatalf("add %s@%d: %v", series, ms, err)
		}
	}

	// Drive a long in-order stream per series so many chunks flush (>1 fragment
	// per series), interleaving deliberate out-of-order points that land BEHIND
	// the series' already-written lastTs and are therefore dropped. We count the
	// true number of drops independently.
	const series = 3
	const points = 400
	wantDropped := uint64(0)
	ts := int64(100000)
	for i := 0; i < points; i++ {
		ts += 1000
		for s := 0; s < series; s++ {
			name := string(rune('a' + s))
			add(name, ts, float64(i))
			// Every 7th step, after the watermark has advanced past it, inject a
			// far-behind point that will be dropped on drain (t <= lastTs).
			if i > 0 && i%7 == 0 {
				add(name, ts-5*grace, -1) // far behind watermark and lastTs -> drop
				wantDropped++
			}
		}
	}

	// Collect fragments as they drain during ingest plus the final forced flush.
	var allFrags []Fragment
	frags, err := enc.Drain(false)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	allFrags = append(allFrags, frags...)
	frags, err = enc.Drain(true)
	if err != nil {
		t.Fatalf("final drain: %v", err)
	}
	allFrags = append(allFrags, frags...)

	if len(allFrags) < 2 {
		t.Fatalf("expected many fragments to exercise the summation, got %d", len(allFrags))
	}
	if enc.DroppedSamples() != wantDropped {
		t.Fatalf("encoder DroppedSamples = %d, want %d", enc.DroppedSamples(), wantDropped)
	}

	// The sum of the per-fragment OOODropCount deltas must reconstruct the true
	// total exactly (the bug made this sum balloon to ~drops * fragments).
	var sumDeltas uint64
	for _, f := range allFrags {
		sumDeltas += f.OOODropCount
	}
	if sumDeltas != wantDropped {
		t.Errorf("sum of per-fragment OOODropCount = %d, want %d (cumulative-stamp bug over-counts)", sumDeltas, wantDropped)
	}

	// End-to-end: feed every fragment into the finalizer and assert the block's
	// NumOOODropped equals the true drop count. NumOOODropped also folds in any
	// chunk-overlap drops the finalizer itself detects; here fragments are
	// per-series time-disjoint chunk runs so the finalizer adds none, leaving the
	// summed encoder deltas as the whole total.
	fin, err := NewFragmentBlockFinalizer(FragmentBlockOptions{})
	if err != nil {
		t.Fatalf("new finalizer: %v", err)
	}
	for _, f := range allFrags {
		if err := fin.AddFragment(f); err != nil {
			t.Fatalf("add fragment: %v", err)
		}
	}
	art, err := fin.Finalize(context.Background())
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if art == nil {
		t.Fatalf("finalize returned nil artifact")
	}
	if art.NumOOODropped != wantDropped {
		t.Errorf("block NumOOODropped = %d, want %d", art.NumOOODropped, wantDropped)
	}
}
