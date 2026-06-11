package otlpfilter

import (
	"fmt"
	"math"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// buildGaugeMetric returns a pmetric.Metrics with a single ResourceMetrics /
// ScopeMetrics holding one Gauge metric named `name` with `n` NumberDataPoints.
// Each datapoint carries a unique "idx" attribute and an int value so we can
// verify survivors are intact and identify which were kept.
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

// TestColdMetricPassthrough: a metric absent from the p-map keeps all of its
// datapoints and the filtered bytes are byte-identical to the input.
func TestColdMetricPassthrough(t *testing.T) {
	s := NewSampleState() // empty p-map => nothing sampled
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

// TestWarmMetricThinned: a warm metric at p=0.25 keeps roughly p*N datapoints
// (with N large), drops a strictly positive number, the result re-unmarshals,
// and surviving datapoints retain their attributes/values exactly.
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

// TestSamplerAdmitsMatchSurvivors: the number of geometric admissions the
// sampler grants equals the number of surviving datapoints, demonstrating that
// dropped datapoints are exactly those the sampler rejected (and thus never
// materialized).
func TestSamplerAdmitsMatchSurvivors(t *testing.T) {
	const (
		name = "warm.count"
		n    = 1000
		p    = 0.2
	)
	// Reference sampler: replay the SAME deterministic sequence the filter will
	// use (same p, same name-derived seed) and count admissions.
	s := NewSampleState()
	s.SetP(map[string]float64{name: p})
	ms := s.samplerFor(name, p)
	expectAdmits := 0
	for i := 0; i < n; i++ {
		if ms.gs.Admit() {
			expectAdmits++
		}
	}

	// Fresh state (fresh sampler, same seed) for the actual filter run.
	s2 := NewSampleState()
	s2.SetP(map[string]float64{name: p})
	in := marshal(t, buildGaugeMetric(name, n))
	out := s2.FilterRequest(in)
	md := unmarshal(t, out)
	kept := gaugeDataPointCount(md, name)

	if kept != expectAdmits {
		t.Fatalf("survivors=%d != sampler admits=%d (drops not aligned with sampler)", kept, expectAdmits)
	}
	t.Logf("sampler admits=%d == survivors=%d (dropped %d never materialized)", expectAdmits, kept, n-kept)
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
