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

// TestStaggeredRoundCoversAllShardsAndSum drives one full round-robin window
// cycle the way flushLoop does (flush shard k%N each tick; unified sum on the
// last shard of the round) and asserts:
//   - every shard's sketches are flushed exactly once over the round, and
//   - the per-zone delta sum total equals exactly what one flushAll emits
//     (output unchanged — the staggering only changes WHEN, not the totals).
func TestStaggeredRoundCoversAllShardsAndSum(t *testing.T) {
	const shards = 4
	zones := []string{"z0", "z1", "z2", "z3", "z4", "z5", "z6", "z7"}

	// --- reference: a single flushAll over the same input ---
	refCap := &capMetrics{}
	ref := newStaggerProc(t, refCap, shards)
	feedZoneRequests(t, ref, zones, 5)
	ref.flushAll(context.Background())
	wantSum := sumByZone(refCap)
	wantSketchDPs := countSketchDPs(refCap)
	if len(wantSum) != len(zones) {
		t.Fatalf("reference flush: got %d zones, want %d", len(wantSum), len(zones))
	}

	// --- staggered: one full round of N shard ticks ---
	stCap := &capMetrics{}
	st := newStaggerProc(t, stCap, shards)
	feedZoneRequests(t, st, zones, 5)
	n := len(st.shards)
	for tick := 0; tick < n; tick++ {
		shardIdx := tick % n
		st.flushShardWarmCold(context.Background(), shardIdx)
		if shardIdx == n-1 {
			st.flushSum(context.Background())
		}
	}

	gotSum := sumByZone(stCap)
	if len(gotSum) != len(wantSum) {
		t.Fatalf("staggered sum zones: got %v want %v", gotSum, wantSum)
	}
	for z, v := range wantSum {
		if gotSum[z] != v {
			t.Fatalf("zone %s: staggered sum %v != reference %v (full got=%v)", z, gotSum[z], v, gotSum)
		}
	}

	// Sketches: every series was observed once; over the full round each shard
	// flushes once, so the total sketch dp count matches the single flushAll.
	if gotSketchDPs := countSketchDPs(stCap); gotSketchDPs != wantSketchDPs {
		t.Fatalf("staggered sketch dps = %d, want %d (one flush per shard per round)", gotSketchDPs, wantSketchDPs)
	}
}

// TestStaggeredSumEmittedOncePerWindow asserts the unified sum is emitted on the
// last shard tick of the round (window-aligned), not on every shard tick — so
// the sum output cadence is one batch per WindowDuration, unchanged from before.
func TestStaggeredSumEmittedOncePerWindow(t *testing.T) {
	const shards = 4
	cap := &capMetrics{}
	p := newStaggerProc(t, cap, shards)
	feedZoneRequests(t, p, []string{"z0", "z1", "z2", "z3"}, 2)

	sumBatches := func() int {
		n := 0
		for _, b := range cap.got {
			rms := b.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				sms := rms.At(i).ScopeMetrics()
				for j := 0; j < sms.Len(); j++ {
					ms := sms.At(j).Metrics()
					for k := 0; k < ms.Len(); k++ {
						if ms.At(k).Name() == "http_requests_total" {
							n++
						}
					}
				}
			}
		}
		return n
	}

	// First N-1 shard ticks: only sketches, no sum metric.
	for tick := 0; tick < shards-1; tick++ {
		p.flushShardWarmCold(context.Background(), tick)
	}
	if got := sumBatches(); got != 0 {
		t.Fatalf("sum emitted before the round completed: got %d sum metrics, want 0", got)
	}

	// Last shard tick of the round + the unified sum flush.
	p.flushShardWarmCold(context.Background(), shards-1)
	p.flushSum(context.Background())
	if got := sumBatches(); got != 1 {
		t.Fatalf("sum should be emitted exactly once per window, got %d", got)
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
