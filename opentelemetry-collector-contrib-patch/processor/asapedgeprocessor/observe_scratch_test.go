// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.uber.org/zap"
)

// TestObserveScratchPreservesCounts verifies the P1-3 scratch reuse does not
// change observe() behavior: the same sample stream yields the same recorded
// per-attribute-set frequency as before (CountSketch keyed path exercises
// kvScratch + attrKeyScratch + obsScratch).
func TestObserveScratchPreservesCounts(t *testing.T) {
	fam := &MetricFamily{Metric: "events", Family: FamilyCountSketch}
	sa, ok := newSketchAggregator("events", fam, sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	const n = 40
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	for i := 0; i < n; i++ {
		// Re-create the map each iter (as the real ingest path does) so the
		// scratch reuse is the only thing carrying state across samples.
		am := map[string]string{"zone": "z0", "method": "GET"}
		sa.observe(am, float64(i), base+uint64(i), false, 0, 0)
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("observe errored: %v", sa.lastObserveErr)
	}

	envs := sa.pc.Drain()
	rows, cols := csmDims(fam)
	rebuilt, err := sketches.NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatal(err)
	}
	got := false
	for _, env := range envs {
		if len(env.Payload) == 0 {
			continue
		}
		if err := rebuilt.ApplyDelta(env.Payload); err != nil {
			t.Fatalf("ApplyDelta: %v", err)
		}
		got = true
	}
	if !got {
		t.Fatal("no envelope emitted")
	}
	attrKey := []byte(precompute.AttributesKey([]precompute.KeyValue{
		{Key: "method", Value: "GET"}, {Key: "zone", Value: "z0"},
	}, nil))
	if est := rebuilt.EstimateCount(attrKey); est < float64(n)-5 {
		t.Fatalf("EstimateCount=%v, want ~%d (scratch reuse changed counts?)", est, n)
	}
}

// TestObserveScratchReusesBuffers proves the P1-3 scratch reuse: the per-shard
// kvScratch, attrKeyScratch, and obsScratch contribute ZERO allocations per
// sample at steady state. The only per-sample allocations that remain are the
// two string-returning precompute helpers (AttributesKey + SeriesKeyFor), which
// the processor cannot avoid without a new zero-alloc precompute API. The test
// asserts observe() allocates no more than those two helpers do on their own —
// i.e. the scratch buffers added nothing.
func TestObserveScratchReusesBuffers(t *testing.T) {
	fam := &MetricFamily{Metric: "events", Family: FamilyCountMinSketch}
	sa, ok := newSketchAggregator("events", fam, sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	am := map[string]string{"zone": "z0", "method": "GET"}
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	// Warm up so the scratch buffers reach steady-state capacity.
	for i := 0; i < 100; i++ {
		sa.observe(am, 1, base+uint64(i), false, 0, 0)
	}

	// Irreducible baseline: everything observe() must do that is NOT in our
	// control — the two string-returning precompute helpers (AttributesKey for
	// the hashed key bytes, SeriesKeyFor for the keyed entry) plus the
	// precompute-internal ObserveKeyed cost (latency-defer / observer dispatch,
	// inside the precompute module). The scratch reuse cannot remove any of
	// these.
	kv := []precompute.KeyValue{{Key: "method", Value: "GET"}, {Key: "zone", Value: "z0"}}
	probeObs := &precompute.Observation{TimestampMs: base, Labels: kv,
		Value: precompute.BytesValue([]byte(precompute.AttributesKey(kv, nil)))}
	probeKey := sa.pcfg.SeriesKeyFor(probeObs)
	baseline := testing.AllocsPerRun(500, func() {
		_ = precompute.AttributesKey(kv, nil)
		_ = sa.pcfg.SeriesKeyFor(probeObs)
		_ = sa.pc.ObserveKeyed(probeKey, probeObs)
	})

	got := testing.AllocsPerRun(500, func() {
		sa.observe(am, 1, base, false, 0, 0)
	})
	t.Logf("observe allocs/op = %v, irreducible baseline = %v", got, baseline)

	// observe() must allocate NO MORE than the irreducible baseline: the kv
	// slice, the attr-key []byte, and the Observation struct are all reused from
	// per-shard scratch (each would otherwise add an alloc on top of the
	// baseline). If the scratch were not reused, got would exceed baseline.
	if got > baseline {
		t.Fatalf("observe allocs/op = %v exceeds irreducible baseline %v; scratch buffers are not being reused", got, baseline)
	}
}
