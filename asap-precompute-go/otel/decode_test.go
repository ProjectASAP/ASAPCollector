package otel

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func nsTimestamp(t time.Time) pcommon.Timestamp {
	return pcommon.Timestamp(t.UnixNano())
}

func TestDecode_GaugeProducesFloatObservation(t *testing.T) {
	t.Parallel()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "web")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.requests")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("method", "GET")
	dp.SetTimestamp(nsTimestamp(time.UnixMilli(2_000)))
	dp.SetDoubleValue(3.14)

	obs, err := Decode(md, &AdapterConfig{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("len: want 1, got %d", len(obs))
	}
	o := obs[0]
	if o.Value.Kind != precompute.KindFloat {
		t.Errorf("kind: want Float, got %v", o.Value.Kind)
	}
	if o.Value.Float != 3.14 {
		t.Errorf("float: want 3.14, got %v", o.Value.Float)
	}
	if o.Metric != "http.requests" {
		t.Errorf("metric: %q", o.Metric)
	}
	if o.TimestampMs != 2_000 {
		t.Errorf("ts: %d", o.TimestampMs)
	}
	if len(o.ResourceLabels) != 1 || o.ResourceLabels[0].Value != "web" {
		t.Errorf("resource labels: %+v", o.ResourceLabels)
	}
	if len(o.Labels) != 1 || o.Labels[0].Value != "GET" {
		t.Errorf("labels: %+v", o.Labels)
	}
}

func TestDecode_GaugeReadAsInt(t *testing.T) {
	t.Parallel()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("counter")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetIntValue(42)

	obs, err := Decode(md, &AdapterConfig{ReadAsInt: true})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(obs) != 1 || obs[0].Value.Float != 42 {
		t.Fatalf("want 42, got %+v", obs)
	}
}

func TestRoundTrip_SumAggEnvelope(t *testing.T) {
	t.Parallel()
	// The fixed 16-byte Sum payload SumWrapper produces (and the backend
	// cross-language golden): float64 sum (LE) || uint64 count (LE),
	// here sum=100, count=4.
	payload := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
		0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	in := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		AggKind:       precompute.AggKindSum,
		MetricName:    "google_cluster_2019_cpu_rate",
		Labels:        []precompute.KeyValue{{Key: "zone", Value: "z1"}},
		WindowStartMs: 1_000,
		WindowEndMs:   2_000,
		Encoding:      precompute.EncodingProtoFull,
		Payload:       payload,
	}
	md, err := Encode([]*precompute.SketchEnvelope{in}, &AdapterConfig{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Type() != pmetric.MetricTypeSumAgg {
		t.Fatalf("encoded metric type: want SumAgg, got %v", m.Type())
	}
	dp := m.SumAgg().DataPoints().At(0)
	if string(dp.Sketch()) != string(payload) {
		t.Errorf("encoded sketch payload mismatch")
	}
	if dp.Encoding() != pmetric.SumAggEncodingProto {
		t.Errorf("encoded encoding: %v", dp.Encoding())
	}

	obs, err := Decode(md, &AdapterConfig{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("obs len: want 1, got %d", len(obs))
	}
	env := obs[0].Value.Envelope
	if env == nil || env.EffectiveAggKind() != precompute.AggKindSum {
		t.Fatalf("decoded agg kind: %+v", env)
	}
	if string(env.Payload) != string(payload) {
		t.Errorf("payload round-trip mismatch: want %x got %x", payload, env.Payload)
	}
	if env.WindowStartMs != 1_000 || env.WindowEndMs != 2_000 {
		t.Errorf("window round-trip: [%d,%d)", env.WindowStartMs, env.WindowEndMs)
	}
}

func TestDecode_DDSketchProducesEnvelope(t *testing.T) {
	t.Parallel()
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	dd := m.SetEmptyDDSketch()
	dp := dd.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("path", "/users")
	dp.SetStartTimestamp(nsTimestamp(time.UnixMilli(1_000)))
	dp.SetTimestamp(nsTimestamp(time.UnixMilli(2_000)))
	dp.SetSketch(payload)
	dp.SetEncoding(pmetric.DDSketchEncodingProto)

	obs, err := Decode(md, &AdapterConfig{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("len: want 1, got %d", len(obs))
	}
	o := obs[0]
	if o.Value.Kind != precompute.KindEnvelope {
		t.Fatalf("kind: want Envelope, got %v", o.Value.Kind)
	}
	env := o.Value.Envelope
	if env == nil {
		t.Fatal("envelope: nil")
	}
	if env.SketchType != precompute.SketchTypeDDSketch {
		t.Errorf("sketch type: %v", env.SketchType)
	}
	if string(env.Payload) != string(payload) {
		t.Errorf("payload: want %x, got %x", payload, env.Payload)
	}
	if env.Encoding != precompute.EncodingProtoFull {
		t.Errorf("encoding: want PROTO_FULL, got %v", env.Encoding)
	}
	if env.WindowStartMs != 1_000 || env.WindowEndMs != 2_000 {
		t.Errorf("window: [%d,%d)", env.WindowStartMs, env.WindowEndMs)
	}
	if len(env.ResourceLabels) != 1 || env.ResourceLabels[0].Value != "api" {
		t.Errorf("resource labels: %+v", env.ResourceLabels)
	}
}

func TestDecode_ResourceLabelsPropagatedAcrossDataPoints(t *testing.T) {
	t.Parallel()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("region", "us-east")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("c")
	g := m.SetEmptyGauge()
	for i := 0; i < 3; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(i))
	}

	obs, err := Decode(md, &AdapterConfig{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(obs) != 3 {
		t.Fatalf("len: want 3, got %d", len(obs))
	}
	for i, o := range obs {
		if len(o.ResourceLabels) != 1 || o.ResourceLabels[0].Value != "us-east" {
			t.Errorf("idx %d: %+v", i, o.ResourceLabels)
		}
	}
}

func TestDecode_EmptyMetricsReturnsNil(t *testing.T) {
	t.Parallel()
	md := pmetric.NewMetrics()
	obs, err := Decode(md, &AdapterConfig{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs != nil {
		t.Errorf("want nil, got %+v", obs)
	}
}
