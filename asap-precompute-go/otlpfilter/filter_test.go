package otlpfilter

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// testBaseNanos is an arbitrary fixed wall-clock origin so runs are
// reproducible; datapoint i is stamped base + i·1ms so every datapoint has a
// distinct occurrence id (occ = nanos/1e6).
const testBaseNanos = uint64(1_700_000_000_000_000_000)

// buildGaugeMetric returns a pmetric.Metrics with a single ResourceMetrics /
// ScopeMetrics holding one Gauge metric named `name` with `n` NumberDataPoints.
// Each datapoint carries a unique "idx" attribute, an int value, and a unique
// millisecond timestamp so the consistent decision varies per datapoint.
func buildGaugeMetric(name string, n int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	for i := 0; i < n; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutInt("idx", int64(i))
		dp.Attributes().PutStr("label", "val")
		dp.SetIntValue(int64(i))
		dp.SetTimestamp(pcommon.Timestamp(testBaseNanos + uint64(i)*uint64(time.Millisecond)))
	}
	return md
}

func marshal(t *testing.T, md pmetric.Metrics) []byte {
	t.Helper()
	b, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func unmarshal(t *testing.T, b []byte) pmetric.Metrics {
	t.Helper()
	md, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
	if err != nil {
		t.Fatalf("unmarshal filtered bytes (validity check failed): %v", err)
	}
	return md
}

// gaugeDataPointCount returns the number of NumberDataPoints in the first
// (and only) gauge metric named `name`, or -1 if not found.
func gaugeDataPointCount(md pmetric.Metrics, name string) int {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		sms := md.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() == name && m.Type() == pmetric.MetricTypeGauge {
					return m.Gauge().DataPoints().Len()
				}
			}
		}
	}
	return -1
}

// TestColdMetricPassthrough: a metric absent from the params map keeps all of
// its datapoints and the filtered bytes are byte-identical to the input.
func TestColdMetricPassthrough(t *testing.T) {
	s := NewSampleState() // empty params map => nothing sampled
	const n = 100
	in := marshal(t, buildGaugeMetric("cold.metric", n))

	out := s.FilterRequest(in)

	if string(out) != string(in) {
		t.Fatalf("cold metric not byte-identical: in=%d out=%d bytes", len(in), len(out))
	}
	md := unmarshal(t, out)
	if got := gaugeDataPointCount(md, "cold.metric"); got != n {
		t.Fatalf("cold metric lost datapoints: got %d want %d", got, n)
	}
}

// TestWarmMetricThinned: a warm metric at p=0.25, rows=1 keeps roughly p*N
// datapoints, drops a strictly positive number, the result re-unmarshals, and
// surviving datapoints retain their attributes/values exactly.
func TestWarmMetricThinned(t *testing.T) {
	const (
		name = "warm.metric"
		n    = 1000
		p    = 0.25
	)
	s := NewSampleState()
	s.SetP(map[string]float64{name: p})

	in := marshal(t, buildGaugeMetric(name, n))
	out := s.FilterRequest(in)
	md := unmarshal(t, out)

	kept := gaugeDataPointCount(md, name)
	if kept < 0 {
		t.Fatalf("warm metric vanished entirely")
	}
	dropped := n - kept
	if dropped <= 0 {
		t.Fatalf("expected some datapoints dropped, kept=%d of %d", kept, n)
	}
	// kept ~= p*N. Tolerance: 5 sigma of Binomial(N,p) plus slack.
	expected := p * n
	sigma := math.Sqrt(n * p * (1 - p))
	tol := 5*sigma + 5
	if math.Abs(float64(kept)-expected) > tol {
		t.Fatalf("kept=%d not within %.1f of expected %.1f (p=%.2f, N=%d)",
			kept, tol, expected, p, n)
	}
	t.Logf("warm p=%.2f N=%d: kept=%d (expected ~%.0f, dropped=%d)", p, n, kept, expected, dropped)

	// Survivors must be intact: each kept dp has both attributes and an int
	// value matching its idx.
	g := firstGauge(t, md, name)
	for i := 0; i < g.DataPoints().Len(); i++ {
		dp := g.DataPoints().At(i)
		idxVal, ok := dp.Attributes().Get("idx")
		if !ok {
			t.Fatalf("survivor missing 'idx' attribute")
		}
		lbl, ok := dp.Attributes().Get("label")
		if !ok || lbl.Str() != "val" {
			t.Fatalf("survivor missing/incorrect 'label' attribute")
		}
		if dp.IntValue() != idxVal.Int() {
			t.Fatalf("survivor value %d != idx %d (corrupted)", dp.IntValue(), idxVal.Int())
		}
	}
}

// TestPerRowFanoutKeepRate: with rows=d the keep probability is 1-(1-p)^d (a
// datapoint survives iff ANY of the d per-row decisions admits — the R(x)=∅
// wire-drop fast path of design §3.1).
func TestPerRowFanoutKeepRate(t *testing.T) {
	const (
		name = "warm.perrow"
		n    = 2000
		p    = 0.25
		d    = 4
	)
	s := NewSampleState()
	s.SetParams(map[string]SampleParams{name: {P: p, Rows: d}})

	in := marshal(t, buildGaugeMetric(name, n))
	out := s.FilterRequest(in)
	kept := gaugeDataPointCount(unmarshal(t, out), name)

	pKeep := 1 - math.Pow(1-p, d) // ≈ 0.684 for p=.25, d=4
	sigma := math.Sqrt(n * pKeep * (1 - pKeep))
	if math.Abs(float64(kept)-pKeep*n) > 5*sigma+5 {
		t.Fatalf("kept=%d, want ≈ %.0f (1-(1-p)^d = %.3f)", kept, pKeep*n, pKeep)
	}
	t.Logf("per-row d=%d p=%.2f: kept=%d of %d (expected ~%.0f)", d, p, kept, n, pKeep*n)
}

// TestConsistentDecisionsMatchSurvivors: survivors are EXACTLY the datapoints
// for which the shared stateless decision (common.ConsistentAdmit with the
// canonical seed and occ = time-ms) admits at least one row — the property
// that lets the collector wrapper re-derive the identical decision set.
func TestConsistentDecisionsMatchSurvivors(t *testing.T) {
	const (
		name = "warm.agree"
		n    = 1000
		p    = 0.2
		d    = 3
	)
	s := NewSampleState()
	s.SetParams(map[string]SampleParams{name: {P: p, Rows: d}})

	in := marshal(t, buildGaugeMetric(name, n))
	out := s.FilterRequest(in)
	md := unmarshal(t, out)

	// Recompute the expected survivor idx-set from shared inputs only.
	seed := common.SeedForMetric(name)
	expect := map[int64]bool{}
	for i := 0; i < n; i++ {
		occ := (testBaseNanos + uint64(i)*uint64(time.Millisecond)) / 1_000_000
		for r := 0; r < d; r++ {
			if common.ConsistentAdmit(seed, occ, r, p) {
				expect[int64(i)] = true
				break
			}
		}
	}

	g := firstGauge(t, md, name)
	if g.DataPoints().Len() != len(expect) {
		t.Fatalf("survivors=%d != recomputed admits=%d", g.DataPoints().Len(), len(expect))
	}
	for i := 0; i < g.DataPoints().Len(); i++ {
		idx, _ := g.DataPoints().At(i).Attributes().Get("idx")
		if !expect[idx.Int()] {
			t.Fatalf("survivor idx=%d was not in the recomputed admit set", idx.Int())
		}
	}
	t.Logf("filter survivors == stateless recomputation (%d of %d kept)", len(expect), n)
}

// TestZeroTimestampPassthrough: datapoints with no timestamp are kept
// (fail-open — no wire identity to decide on); the wrapper samples them via
// its occurrence-counter fallback instead, exactly once end-to-end.
func TestZeroTimestampPassthrough(t *testing.T) {
	const (
		name = "warm.nots"
		n    = 200
	)
	s := NewSampleState()
	s.SetParams(map[string]SampleParams{name: {P: 0.1, Rows: 2}})

	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName(name)
	gg := g.SetEmptyGauge()
	for i := 0; i < n; i++ {
		dp := gg.DataPoints().AppendEmpty()
		dp.SetIntValue(int64(i)) // no SetTimestamp → time_unix_nano = 0
	}
	out := s.FilterRequest(marshal(t, md))
	if kept := gaugeDataPointCount(unmarshal(t, out), name); kept != n {
		t.Fatalf("zero-timestamp datapoints must pass through: kept=%d of %d", kept, n)
	}
}

// TestMixedBatch: one warm + one cold metric in the same request. The cold
// metric is fully preserved; the warm metric is thinned.
func TestMixedBatch(t *testing.T) {
	const (
		warmName = "warm.mixed"
		coldName = "cold.mixed"
		n        = 1000
		p        = 0.3
	)
	s := NewSampleState()
	s.SetP(map[string]float64{warmName: p})

	// Build one request containing both metrics in the same ScopeMetrics.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	for _, nm := range []string{warmName, coldName} {
		m := sm.Metrics().AppendEmpty()
		m.SetName(nm)
		g := m.SetEmptyGauge()
		for i := 0; i < n; i++ {
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutInt("idx", int64(i))
			dp.SetIntValue(int64(i))
			dp.SetTimestamp(pcommon.Timestamp(testBaseNanos + uint64(i)*uint64(time.Millisecond)))
		}
	}
	in := marshal(t, md)
	out := s.FilterRequest(in)
	res := unmarshal(t, out)

	if got := gaugeDataPointCount(res, coldName); got != n {
		t.Fatalf("cold metric in mixed batch thinned: got %d want %d", got, n)
	}
	warmKept := gaugeDataPointCount(res, warmName)
	if warmKept >= n || warmKept <= 0 {
		t.Fatalf("warm metric not thinned: kept=%d of %d", warmKept, n)
	}
	t.Logf("mixed: cold kept=%d (=N), warm kept=%d of %d (p=%.2f)", n, warmKept, n, p)
}

// TestValidityMultiResource: a request with multiple ResourceMetrics /
// ScopeMetrics, warm + cold mixed across them, re-unmarshals without error.
func TestValidityMultiResource(t *testing.T) {
	const p = 0.5
	s := NewSampleState()
	s.SetP(map[string]float64{"warm.a": p, "warm.b": p})

	md := pmetric.NewMetrics()
	for r := 0; r < 3; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("rid", fmt.Sprintf("r%d", r))
		for sc := 0; sc < 2; sc++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName(fmt.Sprintf("scope%d", sc))
			for _, nm := range []string{"warm.a", "cold.x", "warm.b"} {
				m := sm.Metrics().AppendEmpty()
				m.SetName(nm)
				g := m.SetEmptyGauge()
				for i := 0; i < 50; i++ {
					dp := g.DataPoints().AppendEmpty()
					dp.SetIntValue(int64(i))
					dp.SetTimestamp(pcommon.Timestamp(testBaseNanos + uint64(i)*uint64(time.Millisecond)))
				}
			}
		}
	}
	in := marshal(t, md)
	out := s.FilterRequest(in)
	_ = unmarshal(t, out) // must not error
	t.Logf("multi-resource validity OK: in=%d out=%d bytes", len(in), len(out))
}

// firstGauge returns the Gauge of the first metric named `name`.
func firstGauge(t *testing.T, md pmetric.Metrics, name string) pmetric.Gauge {
	t.Helper()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		sms := md.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() == name && m.Type() == pmetric.MetricTypeGauge {
					return m.Gauge()
				}
			}
		}
	}
	t.Fatalf("gauge %q not found", name)
	return pmetric.Gauge{}
}
