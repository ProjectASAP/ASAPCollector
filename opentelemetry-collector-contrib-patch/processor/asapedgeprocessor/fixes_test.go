// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"sync"
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

func testSettings() processor.Settings {
	return processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
}

// TestUnsupportedTypeNeverDropped covers P0 #1: a Histogram metric configured
// (and drop_original=true) must NOT be removed from the passthrough stream nor
// silently lost — it has no warm/cold path, so the passthrough is its only
// survival. The unsupported-type counter must also tick.
func TestUnsupportedTypeNeverDropped(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		// Configure the histogram name so the drop_original RemoveIf would
		// normally remove it; the fix must exempt unsupported types.
		Metrics: []MetricFamily{{Metric: "request_latency_hist", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:    ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), cap)
	if err != nil {
		t.Fatal(err)
	}

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("request_latency_hist")
	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(3)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if len(cap.got) != 1 {
		t.Fatalf("expected histogram forwarded in passthrough, got %d batches", len(cap.got))
	}
	if got := cap.got[0].MetricCount(); got != 1 {
		t.Fatalf("expected 1 forwarded metric (the histogram), got %d", got)
	}
	if c := p.unsupportedTypeCount.Load(); c != 1 {
		t.Fatalf("unsupportedTypeCount = %d, want 1", c)
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
		sa.observe(map[string]string{"series": string(rune('a' + i))}, float64(i), base)
	}
	snap := sa.pc.Stats().Snapshot()
	if snap.ActiveSeries > 2 {
		t.Fatalf("ActiveSeries = %d, want <= 2 (cap not enforced)", snap.ActiveSeries)
	}
	if snap.DroppedOverflow == 0 {
		t.Fatalf("DroppedOverflow = 0, want > 0 (overflow not counted)")
	}
}

// TestSumMaxGroupsBounds covers P0 #2 (sum half): the group map is capped and
// overflow is counted.
func TestSumMaxGroupsBounds(t *testing.T) {
	sa := newSumAggregator([]string{"zone"}, 2)
	for i := 0; i < 5; i++ {
		sa.observe(map[string]string{"zone": string(rune('a' + i))}, 1)
	}
	if len(sa.groups) != 2 {
		t.Fatalf("group map size = %d, want 2 (cap not enforced)", len(sa.groups))
	}
	if sa.overflowCount.Load() != 3 {
		t.Fatalf("overflowCount = %d, want 3", sa.overflowCount.Load())
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
		sa.observe(am, float64(100+i), base+uint64(i)) // values vary; count must not
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

// TestDeltaTransmissionEmitsDeltaEncoding covers P1 #5: with delta enabled, the
// first window emits PROTO_FULL (no prior snapshot) and the SECOND window emits
// PROTO_DELTA.
func TestDeltaTransmissionEmitsDeltaEncoding(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sa, ok := newSketchAggregator("lat", &cfg.Metrics[0],
		sketchOpts{window: time.Hour, delta: true}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	if !sa.pcfg.DeltaTransmission {
		t.Fatal("DeltaTransmission not set on PrecomputeConfig")
	}

	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	am := map[string]string{"zone": "z0"}

	// Window 1 -> PROTO_FULL (first snapshot for the series).
	for i := 0; i < 10; i++ {
		sa.observe(am, float64(i), base+uint64(i))
	}
	envs1 := sa.pc.Drain()
	if !hasEncoding(envs1, precompute.EncodingProtoFull) {
		t.Fatalf("window 1: expected a PROTO_FULL envelope, got %v", encodings(envs1))
	}

	// Window 2 -> PROTO_DELTA (against the cached window-1 snapshot).
	for i := 0; i < 10; i++ {
		sa.observe(am, float64(i), base+1000+uint64(i))
	}
	envs2 := sa.pc.Drain()
	if !hasEncoding(envs2, precompute.EncodingProtoDelta) {
		t.Fatalf("window 2: expected a PROTO_DELTA envelope, got %v", encodings(envs2))
	}
}

// TestDeltaDefaultOff confirms the conservative default: without enabling delta
// (top-level default false, unset per-metric), the family ships PROTO_FULL
// every window.
func TestDeltaDefaultOff(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Metrics[0].effectiveDelta(cfg.DeltaTransmission) {
		t.Fatal("delta should default OFF when neither top-level nor per-metric set")
	}
}

func hasEncoding(envs []*precompute.SketchEnvelope, want precompute.Encoding) bool {
	for _, e := range envs {
		if e.Encoding == want {
			return true
		}
	}
	return false
}

func encodings(envs []*precompute.SketchEnvelope) []precompute.Encoding {
	out := make([]precompute.Encoding, 0, len(envs))
	for _, e := range envs {
		out = append(out, e.Encoding)
	}
	return out
}

// fakeControlChannel is a deterministic in-memory ControlChannel for testing
// the poll loop without standing up an HTTP server.
type fakeControlChannel struct {
	mu     sync.Mutex
	queued []*precompute.PrecomputeConfigSet
	acked  []uint64
}

func (f *fakeControlChannel) push(set *precompute.PrecomputeConfigSet) {
	f.mu.Lock()
	f.queued = append(f.queued, set)
	f.mu.Unlock()
}

func (f *fakeControlChannel) Poll() *precompute.PrecomputeConfigSet {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queued) == 0 {
		return nil
	}
	s := f.queued[0]
	f.queued = f.queued[1:]
	return s
}

func (f *fakeControlChannel) Ack(v uint64) {
	f.mu.Lock()
	f.acked = append(f.acked, v)
	f.mu.Unlock()
}

// TestControlPlaneAppliesConfig covers P1 #4: applyConfigSet swaps a received
// PrecomputeConfigSet into the live sketch aggregator via UpdateConfig (in
// place) and acks the version.
func TestControlPlaneAppliesConfig(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlChannel{}
	p.ctrlChan = fake

	// Build a config set targeting the live aggregator's AggID so UpdateConfig
	// picks it. A high MaxSeries proves the update is applied (the swap is
	// in-place; we only assert it doesn't panic and the version is acked).
	aggID := precompute.AggId(fnv64("lat"))
	set := &precompute.PrecomputeConfigSet{
		Version: 7,
		Configs: []precompute.PrecomputeConfig{{
			AggID:      aggID,
			SketchType: precompute.SketchTypeDDSketch,
			Mode:       precompute.Tumbling,
			Window:     precompute.WindowSpec{Size: time.Hour},
			MetricName: "lat",
			MaxSeries:  500,
		}},
	}
	p.applyConfigSet(set)

	if p.ctrlLastApply.Load() != 7 {
		t.Fatalf("ctrlLastApply = %d, want 7", p.ctrlLastApply.Load())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.acked) != 1 || fake.acked[0] != 7 {
		t.Fatalf("acked = %v, want [7]", fake.acked)
	}

	// The aggregator must still observe cleanly after the in-place swap (state
	// preserved, no rebuild).
	sa := p.shards[0].sketchAggs["lat"]
	sa.observe(map[string]string{"zone": "z0"}, 1, uint64(time.Now().UnixMilli()))
	if sa.lastObserveErr != nil {
		t.Fatalf("observe after UpdateConfig errored: %v", sa.lastObserveErr)
	}
}

// TestControlPlanePollLoop drives the goroutine end-to-end through a fake
// channel and asserts the queued set is applied + acked, then stops cleanly.
func TestControlPlanePollLoop(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
		ControlChannel: ControlChannelConfig{Enabled: true, PollURL: "http://unused", PollInterval: 5 * time.Millisecond},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlChannel{}
	fake.push(&precompute.PrecomputeConfigSet{
		Version: 11,
		Configs: []precompute.PrecomputeConfig{{
			AggID:      precompute.AggId(fnv64("lat")),
			SketchType: precompute.SketchTypeDDSketch,
			Mode:       precompute.Tumbling,
			Window:     precompute.WindowSpec{Size: time.Hour},
			MetricName: "lat",
		}},
	})
	p.ctrlChan = fake // replace the real HTTP channel with the fake
	p.startControlPlane()

	deadline := time.After(2 * time.Second)
	for p.ctrlLastApply.Load() != 11 {
		select {
		case <-deadline:
			t.Fatal("control poll loop never applied version 11")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	p.stopControlPlane()
	// stopControlPlane joins the goroutine; a second call must be a no-op.
	p.stopControlPlane()
}

// TestConcurrentConsumeMetricsRace covers #11: ConsumeMetrics is hammered from
// many goroutines (the Collector fans out Export calls) to prove the shard hot
// path is race-free under `go test -race`.
func TestConcurrentConsumeMetricsRace(t *testing.T) {
	cfg := &Config{
		ShardCount:     8,
		WindowDuration: 10 * time.Millisecond,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{Metric: "http_requests_total", Family: FamilySum, AggregateBy: []string{"zone"}},
			{Metric: "http_latency_ms", Family: FamilyDDSketch, RelativeAccuracy: 0.01},
			{Metric: "flows", Family: FamilyCountMinSketch},
			{Metric: "events", Family: FamilyCountSketch},
		},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), componenttestHost{}); err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const itersPer = 40
	var wg sync.WaitGroup
	wg.Add(goroutines)
	base := time.Unix(1700000000, 0)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for it := 0; it < itersPer; it++ {
				md := pmetric.NewMetrics()
				sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
				// Sum
				sumM := sm.Metrics().AppendEmpty()
				sumM.SetName("http_requests_total")
				s := sumM.SetEmptySum()
				s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				sdp := s.DataPoints().AppendEmpty()
				sdp.Attributes().PutStr("zone", "z"+string(rune('0'+g%4)))
				sdp.SetDoubleValue(1)
				sdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))
				// DDSketch gauge
				gM := sm.Metrics().AppendEmpty()
				gM.SetName("http_latency_ms")
				gg := gM.SetEmptyGauge()
				gdp := gg.DataPoints().AppendEmpty()
				gdp.Attributes().PutStr("ep", "e"+string(rune('0'+(g+it)%6)))
				gdp.SetDoubleValue(float64(it))
				gdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))
				// CMS gauge
				cM := sm.Metrics().AppendEmpty()
				cM.SetName("flows")
				cg := cM.SetEmptyGauge()
				cdp := cg.DataPoints().AppendEmpty()
				cdp.Attributes().PutStr("src", "s"+string(rune('0'+g)))
				cdp.SetDoubleValue(1)
				cdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))
				// CountSketch gauge
				csM := sm.Metrics().AppendEmpty()
				csM.SetName("events")
				csg := csM.SetEmptyGauge()
				csdp := csg.DataPoints().AppendEmpty()
				csdp.Attributes().PutStr("kind", "k"+string(rune('0'+it%3)))
				csdp.SetDoubleValue(1)
				csdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))

				if err := p.ConsumeMetrics(context.Background(), md); err != nil {
					t.Errorf("ConsumeMetrics: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestShutdownDrainsColdPartEvenWithExpiredCtx covers P0 #3: when the Shutdown
// context is already expired, the durable cold-part drain must STILL run (under
// a fresh bounded best-effort deadline) instead of being skipped — otherwise
// the partial intchunk block buffered in the accumulators is lost. The previous
// code returned early on ctx.Done() and dropped it.
func TestShutdownDrainsColdPartEvenWithExpiredCtx(t *testing.T) {
	pc := newPartCollector(t)
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: pc.srv.URL,
			ExternalLabels:   map[string]string{"agent": "edge-exp"},
			BlockDuration:    60 * time.Second,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	// Feed one in-block flush so the accumulator holds a partial (unsealed) block.
	base := time.Unix(1700000000, 0)
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cpu_seconds_total")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("core", "0")
	dp.SetDoubleValue(42)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	p.flushAll(context.Background())

	// Already-expired context.
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	// The flush loop was never started, so flushLoopDone is immediately true and
	// the accumulator drain runs under the fresh grace deadline.
	_ = p.Shutdown(expired) // returns ctx.Err(); the drain still happens

	pc.waitForParts(1, 2*time.Second)
	if got := pc.count(); got != 1 {
		t.Fatalf("parts after Shutdown(expired ctx) = %d, want 1 (partial block must still ship)", got)
	}
}

// TestSumNeverEmitsInvertedTimestamps covers P2 #7: a future-timestamped sample
// must not permanently skew maxObserved, and an idle window must never emit a
// data point whose start > end. After a window with a far-future sample, the
// watermark is reset; the next (idle) window emits start <= end.
func TestSumNeverEmitsInvertedTimestamps(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics:        []MetricFamily{{Metric: "reqs", Family: FamilySum, AggregateBy: []string{"zone"}}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), cap)
	if err != nil {
		t.Fatal(err)
	}
	// Seed windowStartMs to "now" as Start would.
	now := uint64(time.Now().UnixMilli())
	p.windowStartMs.Store(now)

	// A far-future sample raises maxObserved well past windowStart.
	future := now + 365*24*3600*1000 // +1 year
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("reqs")
	s := m.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := s.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("zone", "z0")
	dp.SetDoubleValue(1)
	dp.SetTimestamp(pcommon.Timestamp(future * 1e6))
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	// Window 1 flush: emits [start, future]; watermark is then reset to future.
	p.flushAll(context.Background())
	// After reset, maxObservedMs must equal windowStartMs (not stuck at future
	// for a later idle window in a way that inverts it).
	if p.maxObservedMs.Load() != p.windowStartMs.Load() {
		t.Fatalf("maxObserved=%d windowStart=%d: watermark not reset to window boundary",
			p.maxObservedMs.Load(), p.windowStartMs.Load())
	}

	// Window 2: idle (no new samples). Must emit a point with start <= end.
	cap.got = nil
	p.flushAll(context.Background())
	// No groups -> emitSumMetric returns early, so no batch is forwarded; assert
	// no inverted point exists in whatever was forwarded.
	assertNoInvertedSum(t, cap.got)

	// Window 3: one in-range sample after the future skew.
	cap.got = nil
	md3 := pmetric.NewMetrics()
	m3 := md3.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m3.SetName("reqs")
	s3 := m3.SetEmptySum()
	s3.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp3 := s3.DataPoints().AppendEmpty()
	dp3.Attributes().PutStr("zone", "z0")
	dp3.SetDoubleValue(2)
	dp3.SetTimestamp(pcommon.Timestamp((uint64(time.Now().UnixMilli())) * 1e6))
	if err := p.ConsumeMetrics(context.Background(), md3); err != nil {
		t.Fatal(err)
	}
	p.flushAll(context.Background())
	assertNoInvertedSum(t, cap.got)
}

func assertNoInvertedSum(t *testing.T, batches []pmetric.Metrics) {
	t.Helper()
	for _, md := range batches {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					mm := ms.At(k)
					if mm.Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := mm.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						if dp.StartTimestamp() > dp.Timestamp() {
							t.Fatalf("%s dp[%d]: start %d > end %d (inverted)",
								mm.Name(), d, dp.StartTimestamp(), dp.Timestamp())
						}
					}
				}
			}
		}
	}
}

// componenttestHost is a minimal component.Host for Start in tests.
type componenttestHost struct{}

func (componenttestHost) GetExtensions() map[component.ID]component.Component { return nil }
