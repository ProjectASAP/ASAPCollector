package otel

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func TestEncode_OneEnvelope_OneResourceMetrics(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{
			SchemaVersion:  1,
			SketchType:     precompute.SketchTypeDDSketch,
			AggID:          1,
			ResourceLabels: []precompute.KeyValue{{Key: "service.name", Value: "web"}},
			Labels:         []precompute.KeyValue{{Key: "method", Value: "GET"}},
			WindowStartMs:  1_000,
			WindowEndMs:    2_000,
			Encoding:       precompute.EncodingProtoFull,
			Payload:        []byte{1, 2, 3},
		},
	}
	md, err := Encode(envs, &AdapterConfig{MetricSuffix: "_ddsketch"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if md.ResourceMetrics().Len() != 1 {
		t.Fatalf("rm: want 1, got %d", md.ResourceMetrics().Len())
	}
	rm := md.ResourceMetrics().At(0)
	if v, ok := rm.Resource().Attributes().Get("service.name"); !ok || v.AsString() != "web" {
		t.Errorf("resource attr: %v ok=%v", v, ok)
	}
	sm := rm.ScopeMetrics().At(0)
	if sm.Metrics().Len() != 1 {
		t.Fatalf("metrics: want 1, got %d", sm.Metrics().Len())
	}
	m := sm.Metrics().At(0)
	if m.Type() != pmetric.MetricTypeDDSketch {
		t.Errorf("type: %v", m.Type())
	}
	dp := m.DDSketch().DataPoints().At(0)
	if string(dp.Sketch()) != string([]byte{1, 2, 3}) {
		t.Errorf("payload: %x", dp.Sketch())
	}
	if dp.Encoding() != pmetric.DDSketchEncodingProto {
		t.Errorf("encoding: %v", dp.Encoding())
	}
}

func TestEncode_GroupsByResourceLabels(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{
			SketchType:     precompute.SketchTypeDDSketch,
			ResourceLabels: []precompute.KeyValue{{Key: "service.name", Value: "web"}},
			Labels:         []precompute.KeyValue{{Key: "method", Value: "GET"}},
			Payload:        []byte{1},
			Encoding:       precompute.EncodingProtoFull,
		},
		{
			SketchType:     precompute.SketchTypeDDSketch,
			ResourceLabels: []precompute.KeyValue{{Key: "service.name", Value: "web"}},
			Labels:         []precompute.KeyValue{{Key: "method", Value: "POST"}},
			Payload:        []byte{2},
			Encoding:       precompute.EncodingProtoFull,
		},
		{
			SketchType:     precompute.SketchTypeDDSketch,
			ResourceLabels: []precompute.KeyValue{{Key: "service.name", Value: "api"}},
			Labels:         []precompute.KeyValue{{Key: "method", Value: "GET"}},
			Payload:        []byte{3},
			Encoding:       precompute.EncodingProtoFull,
		},
	}
	md, err := Encode(envs, &AdapterConfig{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if md.ResourceMetrics().Len() != 2 {
		t.Fatalf("rm count: want 2, got %d", md.ResourceMetrics().Len())
	}
	// Each ResourceMetrics has its own metrics list — first group has
	// 2 metrics, second has 1. Order follows insertion order.
	web := md.ResourceMetrics().At(0)
	if web.ScopeMetrics().At(0).Metrics().Len() != 2 {
		t.Errorf("web group: want 2 metrics, got %d", web.ScopeMetrics().At(0).Metrics().Len())
	}
	api := md.ResourceMetrics().At(1)
	if api.ScopeMetrics().At(0).Metrics().Len() != 1 {
		t.Errorf("api group: want 1 metric, got %d", api.ScopeMetrics().At(0).Metrics().Len())
	}
}

func TestEncode_MetricSuffixAppended(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{
			SketchType: precompute.SketchTypeDDSketch,
			Labels: []precompute.KeyValue{
				{Key: "_asap_metric_name", Value: "http.duration"},
			},
			Payload:  []byte{1},
			Encoding: precompute.EncodingProtoFull,
		},
	}
	md, err := Encode(envs, &AdapterConfig{MetricSuffix: "_ddsketch"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Name() != "http.duration_ddsketch" {
		t.Errorf("name: %q", m.Name())
	}
}

func TestEncode_MetricNameOverrides(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{
			SketchType: precompute.SketchTypeDDSketch,
			Labels: []precompute.KeyValue{
				{Key: "_asap_metric_name", Value: "ignored"},
			},
			Payload:  []byte{1},
			Encoding: precompute.EncodingProtoFull,
		},
	}
	md, err := Encode(envs, &AdapterConfig{MetricName: "explicit_override", MetricSuffix: "_x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if m.Name() != "explicit_override" {
		t.Errorf("name: %q", m.Name())
	}
}

func TestEncode_DefaultScopeName(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{SketchType: precompute.SketchTypeDDSketch, Payload: []byte{1}, Encoding: precompute.EncodingProtoFull},
	}
	md, err := Encode(envs, &AdapterConfig{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Scope().Name()
	if got != "asap_precompute" {
		t.Errorf("scope name: %q", got)
	}
}

func TestEncode_EmptyReturnsEmptyMetrics(t *testing.T) {
	t.Parallel()
	md, err := Encode(nil, &AdapterConfig{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if md.ResourceMetrics().Len() != 0 {
		t.Errorf("rm: want 0, got %d", md.ResourceMetrics().Len())
	}
}

func TestEncode_DeltaEncodingPreserved(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{
			SketchType: precompute.SketchTypeDDSketch,
			Payload:    []byte{1},
			Encoding:   precompute.EncodingProtoDelta,
		},
	}
	md, err := Encode(envs, &AdapterConfig{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).DDSketch().DataPoints().At(0)
	if dp.Encoding() != pmetric.DDSketchEncodingProtoDelta {
		t.Errorf("want PROTO_DELTA, got %v", dp.Encoding())
	}
}

func TestAdapter_DecodeWrongTypeRejected(t *testing.T) {
	t.Parallel()
	a := New(&AdapterConfig{}, nil)
	if _, err := a.Decode("not a metrics"); err == nil {
		t.Fatal("want error on wrong type")
	}
}
