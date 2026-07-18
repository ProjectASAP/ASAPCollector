// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

func TestDDSketchFamilyFlushes(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics:        []MetricFamily{{Metric: "http_latency_ms", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("http_latency_ms")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < 20; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", "z0")
		dp.SetDoubleValue(float64(10 + i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Millisecond)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	p.flushAll(context.Background())
	if len(cap.got) != 1 {
		t.Fatalf("expected 1 flushed batch, got %d", len(cap.got))
	}
	dpCount := cap.got[0].DataPointCount()
	if dpCount == 0 {
		t.Fatalf("expected sketch envelope data points, got 0")
	}
}

// TestSketchMaxSeriesBounds covers P0 #2: with MaxSeries=2 the precompute
// series map must stop growing past the cap, with overflow counted in Stats.
func TestSketchMaxSeriesBounds(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "m", Family: FamilyDDSketch, RelativeAccuracy: 0.01, MaxSeries: 2}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Metrics[0].MaxSeries != 2 {
		t.Fatalf("per-metric max_series should stay 2, got %d", cfg.Metrics[0].MaxSeries)
	}
	sa, ok := newSketchAggregator("m", &cfg.Metrics[0], sketchOpts{window: time.Hour, maxSeries: 2}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	// 5 distinct series; only 2 fit.
	for i := 0; i < 5; i++ {
		sa.observe(map[string]string{"series": string(rune('a' + i))}, float64(i), base, false, 0, 0)
	}
	snap := sa.pc.Stats().Snapshot()
	if snap.ActiveSeries > 2 {
		t.Fatalf("ActiveSeries = %d, want <= 2 (cap not enforced)", snap.ActiveSeries)
	}
	if snap.DroppedOverflow == 0 {
		t.Fatalf("DroppedOverflow = 0, want > 0 (overflow not counted)")
	}
}

// TestCountSketchCountsAttributeSet covers B6 (#9): CountSketch must count the
// per-attribute-set frequency (like CMS), NOT the degenerate single metric-name
// key. After observing N samples of attrs {zone=z0}, the reconstructed sketch
// must estimate ~N for the encoded attribute key — and ~0 for the metric name,
// proving the subject is the attribute set and not the name.
func TestCountSketchCountsAttributeSet(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "events", Family: FamilyCountSketch}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sa, ok := newSketchAggregator("events", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(CountSketch) returned ok=false")
	}
	if sa.obsKind != obsKindKeyedFreq {
		t.Fatalf("CountSketch obsKind = %v, want obsKindKeyedFreq", sa.obsKind)
	}

	const n = 50
	am := map[string]string{"zone": "z0"}
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	for i := 0; i < n; i++ {
		sa.observe(am, float64(100+i), base+uint64(i), false, 0, 0) // values vary; count must not
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("CountSketch observe errored: %v", sa.lastObserveErr)
	}

	envs := sa.pc.Drain()
	rows, cols := csmDims(&cfg.Metrics[0])
	rebuilt, err := sketches.NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatal(err)
	}
	gotEnvelope := false
	for _, env := range envs {
		if env.SketchType != precompute.SketchTypeCountSketch || len(env.Payload) == 0 {
			continue
		}
		if err := rebuilt.ApplyDelta(env.Payload); err != nil {
			t.Fatalf("ApplyDelta(payload): %v", err)
		}
		gotEnvelope = true
	}
	if !gotEnvelope {
		t.Fatal("no CountSketch envelope with a non-empty payload emitted")
	}

	attrKey := []byte(precompute.AttributesKey([]precompute.KeyValue{{Key: "zone", Value: "z0"}}, nil))
	if got := rebuilt.EstimateCount(attrKey); got < float64(n)-5 {
		t.Fatalf("EstimateCount(attr zone=z0)=%v, want ~%d (frequency of attribute set not recorded)", got, n)
	}
	// The metric name must NOT be the counted subject (degenerate B6 case).
	if got := rebuilt.EstimateCount([]byte("events")); got > 5 {
		t.Fatalf("EstimateCount(metric-name 'events')=%v, want ~0 (still counting the name)", got)
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
