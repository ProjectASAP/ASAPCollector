// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.uber.org/zap"
)

// TestValidateCountSketchDims rejects dimensions that overflow the 64-bit
// row-hash budget (P0-1a) and accepts the defaults.
func TestValidateCountSketchDims(t *testing.T) {
	cases := []struct {
		name      string
		rows      int
		cols      int
		wantError bool
	}{
		{"defaults (5x2048)", 0, 0, false},    // csmDims -> 5 rows * 11 bits = 55 <= 64
		{"5 x 2048 explicit", 5, 2048, false}, // 5 * 11 = 55
		{"6 x 2048 overflows", 6, 2048, true}, // 6 * 11 = 66 > 64
		{"8 x 256 fits", 8, 256, false},       // 8 * 8 = 64
		{"9 x 256 overflows", 9, 256, true},   // 9 * 8 = 72 > 64
		{"non-pow2 cols rejected", 5, 1000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				ShardCount:     1,
				WindowDuration: time.Hour,
				Metrics: []MetricFamily{
					{Metric: "events", Family: FamilyCountSketch, Rows: tc.rows, Cols: tc.cols},
				},
				Cold: ColdConfig{Enabled: false},
			}
			err := cfg.Validate()
			if tc.wantError && err == nil {
				t.Fatalf("rows=%d cols=%d: want validation error, got nil", tc.rows, tc.cols)
			}
			if !tc.wantError && err != nil {
				t.Fatalf("rows=%d cols=%d: want no error, got %v", tc.rows, tc.cols, err)
			}
		})
	}
}

// TestNewSketchAggregatorSkipsBadCountSketchDims verifies P0-1(b): when the
// CountSketch wrapper constructor would reject the dimensions, the factory no
// longer discards the error and wires a nil-backed wrapper; instead
// newSketchAggregator returns (nil, false) so the family is skipped.
func TestNewSketchAggregatorSkipsBadCountSketchDims(t *testing.T) {
	// 6 rows * 11 bits (2048) = 66 > 64: the wrapper constructor errors.
	fam := &MetricFamily{Metric: "events", Family: FamilyCountSketch, Rows: 6, Cols: 2048}
	sa, ok := newSketchAggregator("events", fam, sketchOpts{window: time.Hour}, zap.NewNop())
	if ok || sa != nil {
		t.Fatalf("want (nil,false) for over-budget CountSketch dims, got (%v,%v)", sa, ok)
	}

	// emit_heap variant with the same bad dims is also skipped.
	famHeap := &MetricFamily{Metric: "events", Family: FamilyCountSketch, Rows: 6, Cols: 2048, EmitHeap: true, HeapSize: 100}
	saH, okH := newSketchAggregator("events", famHeap, sketchOpts{window: time.Hour}, zap.NewNop())
	if okH || saH != nil {
		t.Fatalf("want (nil,false) for over-budget heap CountSketch dims, got (%v,%v)", saH, okH)
	}
}

// TestCountSketchWrapperNilGuards verifies P0-1(c): a wrapper whose backing
// sketch is nil (the discarded-error scenario) does not panic on UpdateString,
// Snapshot, or Reset.
func TestCountSketchWrapperNilGuards(t *testing.T) {
	var w sketches.CountSketchWrapper // zero value: cs == nil
	// None of these may panic.
	w.UpdateString("k", 1)
	if b, err := w.Snapshot(); b != nil || err != nil {
		t.Fatalf("nil-backed Snapshot: want (nil,nil), got (%v,%v)", b, err)
	}
	w.Reset()
}

// TestWarmAllowedLatenessDefaultsToWindow verifies P1-1: the warm window's
// AllowedLateness is the dedicated WarmAllowedLateness (default WindowDuration),
// NOT the cold reorder grace.
func TestWarmAllowedLatenessDefaultsToWindow(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: 60 * time.Second,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		// Cold enabled with a small reorder grace — the OLD bug threaded this
		// 2s grace into the warm window.
		Cold: ColdConfig{Enabled: true, ReorderGrace: 2 * time.Second, ShipEndpoint: "http://x"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.WarmAllowedLateness != 60*time.Second {
		t.Fatalf("WarmAllowedLateness default: want 60s (= window), got %v", cfg.WarmAllowedLateness)
	}

	sa, ok := newSketchAggregator("lat", &cfg.Metrics[0],
		sketchOpts{window: cfg.WindowDuration, allowedLateness: cfg.WarmAllowedLateness}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	if got := sa.pcfg.Window.AllowedLateness; got != 60*time.Second {
		t.Fatalf("warm WindowSpec.AllowedLateness: want 60s, got %v (still coupled to cold grace?)", got)
	}
}

// TestWarmWindowAdmitsProcessingDelayedSample verifies the behavioral payoff of
// P1-1: a sample whose event timestamp is older than the window's aligned start
// by more than the (tiny) cold reorder grace but still within the
// WindowDuration is ADMITTED, not dropped as late.
func TestWarmWindowAdmitsProcessingDelayedSample(t *testing.T) {
	const window = 60 * time.Second
	// allowedLateness = full window (the new default).
	sa, ok := newSketchAggregator("lat",
		&MetricFamily{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01},
		sketchOpts{window: window, allowedLateness: window}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}

	// Initialize the window with a sample near a window boundary so the aligned
	// start is well-defined, then feed an earlier-but-in-window sample.
	base := uint64(time.Unix(1700000040, 0).UnixMilli()) // 40s into a 60s-aligned window
	am := map[string]string{"zone": "z0"}
	sa.observe(am, 1, base, false, 0, 0)
	// A sample 30s earlier in event-time: older than a 2s grace, but inside the
	// 60s window. Must be admitted (no observe drop).
	sa.observe(am, 2, base-30_000, false, 0, 0)
	if sa.lastObserveErr != nil {
		t.Fatalf("in-window-but-delayed sample dropped: %v", sa.lastObserveErr)
	}
	if got := sa.droppedSamples.Load(); got != 0 {
		t.Fatalf("droppedSamples: want 0, got %d (sample dropped as late)", got)
	}
}

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

// TestSketchEncodeDropCounterWired verifies P0-2 wiring: the processor's
// encode-drop counter is threaded into each sketch aggregator and a normal
// successful flush leaves it at zero (the counter only increments on an Encode
// failure, which is otherwise silent).
func TestSketchEncodeDropCounterWired(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := testSettings()
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	sh := p.shards[0]
	sa := sh.sketchAggs["lat"]
	if sa == nil {
		t.Fatal("no sketch aggregator wired")
	}
	if sa.procEncodeDropCount != &p.sketchEncodeDropCount {
		t.Fatal("procEncodeDropCount not wired to processor counter")
	}
	if got := p.sketchEncodeDropCount.Load(); got != 0 {
		t.Fatalf("encode-drop counter: want 0 before any flush, got %d", got)
	}
}
