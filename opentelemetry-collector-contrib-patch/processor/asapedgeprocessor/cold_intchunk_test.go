// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// makeFragments runs samples through a StreamingFragmentEncoder and drains it,
// producing the SAME XOR fragments the cold tier would ship — the input the
// intchunk cold-part path re-encodes.
func makeFragments(t *testing.T, source string, samples []gorilla.TSDBSample) []gorilla.Fragment {
	t.Helper()
	enc := gorilla.NewStreamingFragmentEncoder(gorilla.StreamingFragmentOptions{Source: source})
	for _, s := range samples {
		if err := enc.AddSample(s); err != nil {
			t.Fatalf("AddSample: %v", err)
		}
	}
	frags, err := enc.Drain(true)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(frags) == 0 {
		t.Fatal("drained 0 fragments")
	}
	return frags
}

// TestColdPartRoundTrip is the end-to-end loop proof: the agent-side cold-part
// builder turns drained XOR fragments into a coldpart.Part, and coldpart.OpenPart
// + Series() round-trips the samples BIT-EXACTLY, with absolute-ms block bounds.
func TestColdPartRoundTrip(t *testing.T) {
	base := time.Unix(1700000000, 0)
	// Two series of one metric; values chosen so the XOR->intchunk re-encode is
	// exercised across distinct attributes.
	var in []gorilla.TSDBSample
	for i := 0; i < 8; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		in = append(in,
			gorilla.TSDBSample{MetricName: "cpu_seconds_total", Attributes: map[string]string{"core": "0"}, Timestamp: ts, Value: float64(i)},
			gorilla.TSDBSample{MetricName: "cpu_seconds_total", Attributes: map[string]string{"core": "1"}, Timestamp: ts, Value: float64(100 + i)},
		)
	}
	frags := makeFragments(t, "edge-7", in)

	ext := map[string]string{"agent": "edge-7"}
	s := newColdPartShipper("http://unused", ext)
	body, err := s.buildPart(frags)
	if err != nil {
		t.Fatalf("buildPart: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("buildPart produced no bytes")
	}

	part, err := coldpart.OpenPart(body)
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	if part.NumSeries() != 2 {
		t.Fatalf("NumSeries = %d, want 2", part.NumSeries())
	}

	// Block bounds must be absolute ms = [first sample, last sample].
	wantStart := base.UnixMilli()
	wantEnd := base.Add(7 * time.Second).UnixMilli()
	if part.BlockStartMs != wantStart || part.BlockEndMs != wantEnd {
		t.Fatalf("block range = [%d,%d], want [%d,%d] (absolute ms)",
			part.BlockStartMs, part.BlockEndMs, wantStart, wantEnd)
	}

	got, err := part.Series(nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Series returned %d, want 2", len(got))
	}

	// Reconstruct the expected per-series samples (absolute ms) and compare
	// bit-exactly, including the external-label-built identity.
	want := map[string][]coldpart.Sample{}
	for _, sm := range in {
		ls := gorilla.FragmentLabels(sm.MetricName, sm.Attributes, ext)
		want[ls.String()] = append(want[ls.String()], coldpart.Sample{T: sm.Timestamp.UnixMilli(), V: sm.Value})
	}
	for _, sd := range got {
		exp, ok := want[sd.Labels.String()]
		if !ok {
			t.Fatalf("unexpected series %s", sd.Labels.String())
		}
		// The external label must be present (built via FragmentLabels).
		if sd.Labels.Get("agent") != "edge-7" {
			t.Fatalf("series %s missing agent=edge-7 external label", sd.Labels.String())
		}
		if len(sd.Samples) != len(exp) {
			t.Fatalf("series %s: %d samples, want %d", sd.Labels.String(), len(sd.Samples), len(exp))
		}
		for i := range exp {
			if sd.Samples[i].T != exp[i].T || math.Float64bits(sd.Samples[i].V) != math.Float64bits(exp[i].V) {
				t.Fatalf("series %s sample %d: got (%d,%v) want (%d,%v)",
					sd.Labels.String(), i, sd.Samples[i].T, sd.Samples[i].V, exp[i].T, exp[i].V)
			}
		}
	}
}

// TestColdPartShipPOST proves the wire: shipEncoded POSTs the serialized part to
// an in-process handler that validates it exactly as the merger's
// /ingest/coldpart does (OpenPart, no decode), and the round-tripped part still
// matches the ingested series.
func TestColdPartShipPOST(t *testing.T) {
	var (
		mu       sync.Mutex
		received []byte
		ctype    string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		// Mirror the merger's HandlePut validation: OpenPart (header/version/crc,
		// no chunk-body decode) before accepting.
		if _, err := coldpart.OpenPart(raw); err != nil {
			http.Error(w, "validate part: "+err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = raw
		ctype = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	base := time.Unix(1700000000, 0)
	in := []gorilla.TSDBSample{
		{MetricName: "http_requests_total", Attributes: map[string]string{"code": "200"}, Timestamp: base, Value: 1},
		{MetricName: "http_requests_total", Attributes: map[string]string{"code": "200"}, Timestamp: base.Add(time.Second), Value: 2},
		{MetricName: "http_requests_total", Attributes: map[string]string{"code": "200"}, Timestamp: base.Add(2 * time.Second), Value: 3},
	}
	frags := makeFragments(t, "", in)

	s := newColdPartShipper(srv.URL, nil)
	if err := s.ship(context.Background(), frags); err != nil {
		t.Fatalf("ship: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("handler received no part")
	}
	if ctype != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream", ctype)
	}
	part, err := coldpart.OpenPart(received)
	if err != nil {
		t.Fatalf("OpenPart(received): %v", err)
	}
	got, _ := part.Series([]*labels.Matcher{}, math.MinInt64, math.MaxInt64)
	if len(got) != 1 {
		t.Fatalf("Series returned %d, want 1", len(got))
	}
	if got[0].Samples[0].T != base.UnixMilli() || got[0].Samples[0].V != 1 {
		t.Fatalf("first sample = (%d,%v), want (%d,1)", got[0].Samples[0].T, got[0].Samples[0].V, base.UnixMilli())
	}
}

// TestColdFormatDefaultOff verifies the flag is opt-in: with cold.format unset
// the processor keeps the gorilla-fragment path (no cold-part shipper wired),
// and with format=intchunk the cold-part shipper is wired and the fragment
// worker is bypassed.
func TestColdFormatDefaultOff(t *testing.T) {
	mk := func(format ColdFormat, coldEndpoint string) *asapEdgeProcessor {
		cfg := &Config{
			ShardCount:     1,
			WindowDuration: time.Hour,
			DropOriginal:   true,
			Cold:           ColdConfig{Enabled: true, ShipEndpoint: "http://merger/ingest", Format: format, ColdPartEndpoint: coldEndpoint},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate(%q): %v", format, err)
		}
		set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
		p, err := newProcessor(cfg, set, &capMetrics{})
		if err != nil {
			t.Fatalf("newProcessor(%q): %v", format, err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}

	// Default (unset) => fragment format, NO cold-part shipper.
	def := mk("", "")
	if def.coldFormat != ColdFormatFragment {
		t.Fatalf("default coldFormat = %q, want fragment", def.coldFormat)
	}
	if def.coldPartShip != nil {
		t.Fatal("default config wired a cold-part shipper; the path must be opt-in")
	}

	// Opt-in intchunk => cold-part shipper wired.
	on := mk(ColdFormatIntchunk, "http://merger/ingest/coldpart")
	if on.coldFormat != ColdFormatIntchunk {
		t.Fatalf("opt-in coldFormat = %q, want intchunk", on.coldFormat)
	}
	if on.coldPartShip == nil || on.coldPartShip.endpoint != "http://merger/ingest/coldpart" {
		t.Fatal("intchunk config did not wire the cold-part shipper to coldpart_endpoint")
	}

	// intchunk without coldpart_endpoint must fail validation.
	bad := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Cold:           ColdConfig{Enabled: true, Format: ColdFormatIntchunk},
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("intchunk format with no coldpart_endpoint: want validation error, got nil")
	}
}

// TestConsumeThenColdPartShip wires the full processor with the intchunk format,
// runs a ConsumeMetrics pass, then flushAll, and asserts the merger handler
// receives a valid part carrying the ingested series — proving the opt-in path
// is exercised by the real flush, not just the helper.
func TestConsumeThenColdPartShip(t *testing.T) {
	var (
		mu   sync.Mutex
		body []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if _, err := coldpart.OpenPart(raw); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		body = raw
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: srv.URL,
			ExternalLabels:   map[string]string{"agent": "edge-9"},
			// The 6 samples span 5s; a 1ms block window makes the single flushAll
			// reach the block boundary and seal the part immediately (rather than
			// buffering until Shutdown), keeping this a flush-path proof.
			BlockDuration: time.Millisecond,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("mem_used_bytes")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < 6; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("host", "h1")
		dp.SetDoubleValue(float64(i * 1024))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	p.flushAll(context.Background())

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
		t.Fatal("merger handler received no cold part")
	}
	part, err := coldpart.OpenPart(body)
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	got, _ := part.Series(nil, math.MinInt64, math.MaxInt64)
	if len(got) != 1 {
		t.Fatalf("Series returned %d, want 1", len(got))
	}
	if got[0].Labels.Get("__name__") != "mem_used_bytes" || got[0].Labels.Get("agent") != "edge-9" {
		t.Fatalf("series labels = %s, want __name__=mem_used_bytes agent=edge-9", got[0].Labels.String())
	}
	if len(got[0].Samples) != 6 {
		t.Fatalf("got %d samples, want 6", len(got[0].Samples))
	}
}

// partCollector is an in-process /ingest/coldpart endpoint that records EVERY
// posted part (validating each as the merger would), so a test can assert how
// many parts were emitted and what each one carried.
type partCollector struct {
	srv  *httptest.Server
	mu   sync.Mutex
	bods [][]byte
}

func newPartCollector(t *testing.T) *partCollector {
	t.Helper()
	pc := &partCollector{}
	pc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if _, err := coldpart.OpenPart(raw); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pc.mu.Lock()
		pc.bods = append(pc.bods, raw)
		pc.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(pc.srv.Close)
	return pc
}

func (pc *partCollector) count() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.bods)
}

// waitForParts polls until at least n parts have been received or the deadline
// passes (the cold-part POST is async on the seal path).
func (pc *partCollector) waitForParts(n int, d time.Duration) {
	deadline := time.Now().Add(d)
	for pc.count() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// TestColdPartAccumulatesAcrossFlushes is the granularity proof: with the
// intchunk cold-part format, feeding MULTIPLE flushes whose samples all fall
// inside one BlockDuration must emit exactly ONE part covering the union of the
// flushes (Σ samples/series), NOT one part per flush. The fixed per-part index +
// symbol-table overhead is what made the old per-flush part ~3x larger than
// gorilla; this accumulation amortizes it away.
func TestColdPartAccumulatesAcrossFlushes(t *testing.T) {
	pc := newPartCollector(t)

	cfg := &Config{
		ShardCount:     1, // single shard => flushAll is the per-tick unit
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: pc.srv.URL,
			ExternalLabels:   map[string]string{"agent": "edge-acc"},
			// A 60s block: the three flushes below all land inside one block, so
			// none should seal a part on flush — only the Shutdown drain does.
			BlockDuration: 60 * time.Second,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}

	base := time.Unix(1700000000, 0)
	// Three flushes, each adding 4 samples per series (2 series) at distinct,
	// in-block timestamps. flush f covers seconds [10f, 10f+3] — all within 60s.
	const flushes, perFlush = 3, 4
	feed := func(flush int) {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("cpu_seconds_total")
		g := m.SetEmptyGauge()
		for i := 0; i < perFlush; i++ {
			ts := base.Add(time.Duration(flush*10+i) * time.Second)
			for _, core := range []string{"0", "1"} {
				dp := g.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("core", core)
				dp.SetDoubleValue(float64(flush*100 + i))
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
			}
		}
		if err := p.ConsumeMetrics(context.Background(), md); err != nil {
			t.Fatalf("ConsumeMetrics flush %d: %v", flush, err)
		}
		p.flushAll(context.Background())
	}
	for f := 0; f < flushes; f++ {
		feed(f)
	}

	// No flush crossed the 60s block boundary, so NO part should be POSTed yet —
	// the old per-flush behavior would have emitted one part per flush here.
	pc.waitForParts(1, 200*time.Millisecond)
	if got := pc.count(); got != 0 {
		t.Fatalf("parts emitted before block boundary = %d, want 0 (must not ship one part per flush)", got)
	}

	// Shutdown force-seals the partial block: exactly ONE part for the union.
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	pc.waitForParts(1, time.Second)
	if got := pc.count(); got != 1 {
		t.Fatalf("parts after Shutdown = %d, want exactly 1 (one part per block, covering the union)", got)
	}

	pc.mu.Lock()
	body := pc.bods[0]
	pc.mu.Unlock()
	part, err := coldpart.OpenPart(body)
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	if part.NumSeries() != 2 {
		t.Fatalf("NumSeries = %d, want 2", part.NumSeries())
	}
	// Block bounds must span min..max sample T across ALL flushes (absolute ms):
	// first sample of flush 0 .. last sample of flush 2.
	wantStart := base.UnixMilli()
	wantEnd := base.Add(time.Duration((flushes-1)*10+(perFlush-1)) * time.Second).UnixMilli()
	if part.BlockStartMs != wantStart || part.BlockEndMs != wantEnd {
		t.Fatalf("block range = [%d,%d], want [%d,%d] (union of all flushes)",
			part.BlockStartMs, part.BlockEndMs, wantStart, wantEnd)
	}
	got, err := part.Series(nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	// Each series must carry Σ of the per-flush samples (flushes*perFlush), proving
	// the buffer accumulated rather than shipping per-flush fragments separately.
	wantPerSeries := flushes * perFlush
	for _, sd := range got {
		if sd.Labels.Get("agent") != "edge-acc" {
			t.Fatalf("series %s missing agent external label", sd.Labels.String())
		}
		if len(sd.Samples) != wantPerSeries {
			t.Fatalf("series %s: %d samples, want %d (Σ of %d flushes x %d)",
				sd.Labels.String(), len(sd.Samples), wantPerSeries, flushes, perFlush)
		}
	}
}

// TestColdPartSealsAtBlockBoundary proves the in-flight seal: when accumulated
// samples cross BlockDuration during normal flushes, a part is sealed + POSTed
// WITHOUT waiting for Shutdown.
func TestColdPartSealsAtBlockBoundary(t *testing.T) {
	pc := newPartCollector(t)

	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: pc.srv.URL,
			BlockDuration:    10 * time.Second, // small block: a 12s span crosses it
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	base := time.Unix(1700000000, 0)
	// Two flushes: the first spans [0,4]s (under the 10s block, no seal); the
	// second extends to 12s, so the buffered span (12s) crosses BlockDuration and
	// seals one part on that flush.
	feed := func(secs []int) {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("io_bytes_total")
		g := m.SetEmptyGauge()
		for _, s := range secs {
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("dev", "sda")
			dp.SetDoubleValue(float64(s))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(s) * time.Second)))
		}
		if err := p.ConsumeMetrics(context.Background(), md); err != nil {
			t.Fatalf("ConsumeMetrics: %v", err)
		}
		p.flushAll(context.Background())
	}
	feed([]int{0, 2, 4})
	pc.waitForParts(1, 200*time.Millisecond)
	if got := pc.count(); got != 0 {
		t.Fatalf("parts after sub-block flush = %d, want 0", got)
	}
	feed([]int{8, 12})
	pc.waitForParts(1, time.Second)
	if got := pc.count(); got != 1 {
		t.Fatalf("parts after boundary-crossing flush = %d, want 1", got)
	}

	pc.mu.Lock()
	body := pc.bods[0]
	pc.mu.Unlock()
	part, err := coldpart.OpenPart(body)
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	got, _ := part.Series(nil, math.MinInt64, math.MaxInt64)
	if len(got) != 1 || len(got[0].Samples) != 5 {
		t.Fatalf("sealed part series/samples = %d/%v, want 1 series of 5 samples",
			len(got), func() int {
				if len(got) > 0 {
					return len(got[0].Samples)
				}
				return 0
			}())
	}
}
