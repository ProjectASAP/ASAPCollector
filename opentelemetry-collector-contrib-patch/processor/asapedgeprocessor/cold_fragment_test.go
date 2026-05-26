package asapedgeprocessor

import (
	"bytes"
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

// TestColdAndSumCoexist runs the fused processor with cold ENABLED (no ship
// endpoint => drain-only, no network) and a Sum family, feeding number data
// points once. It verifies the single decode pass drives BOTH the cold
// fragment encoder and the sum aggregator without error, and that flush drains
// XOR-chunk fragments for both distinct series.
func TestColdAndSumCoexist(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics:        []MetricFamily{{Metric: "http_requests_total", Family: FamilySum, AggregateBy: []string{"zone"}}},
		Cold:           ColdConfig{Enabled: true}, // no ShipEndpoint => drain only
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	s := m.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	base := time.Unix(1700000000, 0)
	for i := 0; i < 4; i++ {
		for _, z := range []string{"z0", "z1"} {
			dp := s.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("zone", z)
			dp.Attributes().PutStr("method", "GET")
			dp.SetDoubleValue(float64(i + 1))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
		}
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	// Force-drain each shard's cold encoder; collect distinct series across
	// shards (zone×method: z0/GET, z1/GET => 2 series, each one fragment).
	series := map[string]struct{}{}
	totalFrags := 0
	for _, sh := range p.shards {
		if sh.cold == nil {
			continue
		}
		frags, derr := sh.cold.Drain(true)
		if derr != nil {
			t.Fatalf("cold drain: %v", derr)
		}
		for _, f := range frags {
			totalFrags++
			series[f.MetricName+"|"+f.Attributes["zone"]] = struct{}{}
			if f.Encoding != "xor" || len(f.Data) == 0 {
				t.Fatalf("fragment for %v has bad encoding/data", f.Attributes)
			}
		}
	}
	if len(series) != 2 {
		t.Fatalf("cold series across shards = %d, want 2", len(series))
	}
	if totalFrags < 2 {
		t.Fatalf("cold fragments = %d, want >= 2", totalFrags)
	}
}

// TestColdFlushShipsFragments wires the processor to an httptest.Server, runs
// a window, and asserts flushAll ships gzip'd ASAPFRG1 fragments that decode
// back to the ingested series.
func TestColdFlushShipsFragments(t *testing.T) {
	var (
		mu   sync.Mutex
		body []byte
		hdr  http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		body = raw
		hdr = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold:           ColdConfig{Enabled: true, ShipEndpoint: srv.URL, ExternalLabels: map[string]string{"agent": "edge-7"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	// Start launches the async ship worker so flushAll's enqueued batch is
	// actually POSTed (the ship is now decoupled from flushAll).
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cpu_seconds_total")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < 10; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("core", "0")
		dp.SetDoubleValue(float64(i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	p.flushAll(context.Background())

	// The ship is async; poll briefly for the worker to deliver the POST.
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(body)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(body) == 0 {
		t.Fatal("shipper POSTed no body")
	}
	if got := hdr.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream", got)
	}
	if got := hdr.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	frags, err := gorilla.DecodeFragmentBatch(gunzip(t, body))
	if err != nil {
		t.Fatalf("DecodeFragmentBatch: %v", err)
	}
	if len(frags) == 0 {
		t.Fatal("decoded 0 fragments, want >= 1")
	}
	for _, f := range frags {
		if f.MetricName != "cpu_seconds_total" {
			t.Fatalf("fragment metric = %q, want cpu_seconds_total", f.MetricName)
		}
		if f.Source != "edge-7" {
			t.Fatalf("fragment source = %q, want edge-7 (from external_labels)", f.Source)
		}
		if f.Count == 0 || len(f.Data) == 0 {
			t.Fatalf("fragment has no samples/data: %+v", f)
		}
	}
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	gr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gr.Close()
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return out
}
