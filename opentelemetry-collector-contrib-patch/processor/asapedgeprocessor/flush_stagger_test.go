package asapedgeprocessor

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// newStaggerProc builds a processor with cold disabled and a sum + ddsketch
// family for the staggered-flush tests.
func newStaggerProc(t *testing.T, cap *capMetrics, shards int) *asapEdgeProcessor {
	t.Helper()
	cfg := &Config{
		ShardCount:     shards,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{Metric: "http_requests_total", Family: FamilySum, AggregateBy: []string{"zone"}},
			{Metric: "http_latency_ms", Family: FamilyDDSketch, RelativeAccuracy: 0.01},
		},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// feedZoneRequests feeds one delta-Sum data point per (zone) and one latency
// gauge sample per zone, so series spread across shards.
func feedZoneRequests(t *testing.T, p *asapEdgeProcessor, zones []string, val float64) {
	t.Helper()
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	req := sm.Metrics().AppendEmpty()
	req.SetName("http_requests_total")
	s := req.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	lat := sm.Metrics().AppendEmpty()
	lat.SetName("http_latency_ms")
	g := lat.SetEmptyGauge()
	now := pcommon.Timestamp(uint64(time.Now().UnixMilli()) * 1e6)
	for _, z := range zones {
		dp := s.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", z)
		dp.SetDoubleValue(val)
		dp.SetTimestamp(now)

		ldp := g.DataPoints().AppendEmpty()
		ldp.Attributes().PutStr("zone", z)
		ldp.SetDoubleValue(val)
		ldp.SetTimestamp(now)
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
}

// sumByZone collects the merged per-zone delta sum out of all captured batches.
func sumByZone(cap *capMetrics) map[string]float64 {
	got := map[string]float64{}
	for _, b := range cap.got {
		rms := b.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					mm := ms.At(k)
					if mm.Name() != "http_requests_total" || mm.Type() != pmetric.MetricTypeSum {
						continue
					}
					dps := mm.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						z, _ := dp.Attributes().Get("zone")
						got[z.AsString()] += dp.DoubleValue()
					}
				}
			}
		}
	}
	return got
}

// countSketchDPs counts the sketch envelope data points (every metric that is
// NOT the http_requests_total sum) across all captured batches.
func countSketchDPs(cap *capMetrics) int {
	n := 0
	for _, b := range cap.got {
		rms := b.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					mm := ms.At(k)
					if mm.Name() == "http_requests_total" {
						continue
					}
					n += metricDPCount(mm)
				}
			}
		}
	}
	return n
}

// metricDPCount returns a metric's data-point count regardless of its type
// (sketch envelopes may be emitted as Sum/Gauge/etc.).
func metricDPCount(mm pmetric.Metric) int {
	switch mm.Type() {
	case pmetric.MetricTypeSum:
		return mm.Sum().DataPoints().Len()
	case pmetric.MetricTypeGauge:
		return mm.Gauge().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return mm.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return mm.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return mm.Summary().DataPoints().Len()
	default:
		return 0
	}
}

// TestStaggeredEnabled asserts the round-robin cadence is active for the
// default multi-shard config and disabled in the single-flush fallback cases.
func TestStaggeredEnabled(t *testing.T) {
	cap := &capMetrics{}
	p4 := newStaggerProc(t, cap, 4)
	if !p4.staggered() {
		t.Fatal("4-shard, hour-window processor should be staggered")
	}

	// ShardCount <= 1 => single-flush fallback.
	p1 := newStaggerProc(t, cap, 1)
	if p1.staggered() {
		t.Fatal("1-shard processor must use the single-flush fallback")
	}

	// WindowDuration <= 0 => single-flush fallback (set after Validate so the
	// default isn't applied).
	p0 := newStaggerProc(t, cap, 4)
	p0.cfg.WindowDuration = 0
	if p0.staggered() {
		t.Fatal("non-positive window must use the single-flush fallback")
	}
}

// TestStaggeredLoopShipsPerShardOverTime runs the REAL flushLoop with a short
// window and cold shipping enabled, feeding many distinct cold series so they
// spread across shards. It asserts the staggered ticker ships in MULTIPLE
// separate POSTs (one shard per WindowDuration/ShardCount tick) rather than one
// big batch, and that Shutdown's final drain flushes the remaining shards.
func TestStaggeredLoopShipsPerShardOverTime(t *testing.T) {
	var (
		mu    sync.Mutex
		posts int
		// distinct cold series observed across all POSTs (proves all shards
		// eventually flush).
		series = map[string]struct{}{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := gunzipReq(t, r)
		frags, err := gorilla.DecodeFragmentBatch(raw)
		if err != nil {
			t.Errorf("decode batch: %v", err)
		}
		mu.Lock()
		posts++
		for _, f := range frags {
			series[f.MetricName+"|"+f.Attributes["core"]] = struct{}{}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	const shards = 4
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     shards,
		WindowDuration: 400 * time.Millisecond, // tick = 100ms per shard
		DropOriginal:   true,
		Cold:           ColdConfig{Enabled: true, ShipEndpoint: srv.URL, SpoolDir: testSpoolDir(t)},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	if !p.staggered() {
		t.Fatal("expected staggered cadence")
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	// Feed 16 distinct cold series (core=0..15) so several land in each shard.
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cpu_seconds_total")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for c := 0; c < 16; c++ {
		for i := 0; i < 4; i++ {
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("core", string(rune('a'+c)))
			dp.SetDoubleValue(float64(i))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(c*4+i) * time.Second)))
		}
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}

	// Let ~one full window elapse so each shard ticks once (4 ticks @ 100ms).
	time.Sleep(550 * time.Millisecond)

	// Shutdown does the final drain of any not-yet-ticked shards.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Poll briefly for the async worker to deliver all POSTs.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		ns := len(series)
		mu.Unlock()
		if ns >= 16 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	// Staggered: each shard ships in its own tick, so we expect MORE than one
	// POST (the old single-flush would emit exactly one per window).
	if posts < 2 {
		t.Fatalf("staggered loop produced %d POSTs; expected multiple (one per shard tick)", posts)
	}
	// All 16 distinct series must eventually be shipped (no shard lost).
	if len(series) != 16 {
		t.Fatalf("shipped %d distinct cold series, want 16 (all shards must flush)", len(series))
	}
}

func gunzipReq(t *testing.T, r *http.Request) []byte {
	t.Helper()
	if r.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", r.Header.Get("Content-Encoding"))
	}
	gr, err := gzip.NewReader(r.Body)
	if err != nil {
		t.Errorf("gzip reader: %v", err)
		return nil
	}
	defer gr.Close()
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Errorf("gunzip: %v", err)
		return nil
	}
	return out
}
