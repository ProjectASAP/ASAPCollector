package telegraf

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	telegrafmetric "github.com/influxdata/telegraf/metric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func TestEncode_SingleEnvelope_HappyPath(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		AggID:         7,
		Labels: []precompute.KeyValue{
			{Key: "method", Value: "POST"},
		},
		WindowStartMs: 1_000,
		WindowEndMs:   2_000,
		Encoding:      precompute.EncodingProtoFull,
		Payload:       []byte{0xde, 0xad, 0xbe, 0xef},
		MetricName:    "http.duration",
		Count:         5,
	}

	metrics, err := Encode([]*precompute.SketchEnvelope{env}, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("want 1 metric, got %d", len(metrics))
	}
	m := metrics[0]
	if m.Name() != "http.duration" {
		t.Errorf("name: %q", m.Name())
	}
	tag, ok := m.GetTag("method")
	if !ok || tag != "POST" {
		t.Errorf("tag method: ok=%v val=%q", ok, tag)
	}
	if m.Time().UnixMilli() != 2_000 {
		t.Errorf("time: %d", m.Time().UnixMilli())
	}

	// Field must be present and base64-decodable to the original env.
	raw, ok := m.GetField(DefaultEnvelopeField)
	if !ok {
		t.Fatalf("envelope field missing")
	}
	b64, ok := raw.(string)
	if !ok {
		t.Fatalf("envelope field not string: %T", raw)
	}
	jsonBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	got := &precompute.SketchEnvelope{}
	if err := json.Unmarshal(jsonBytes, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.SketchType != env.SketchType {
		t.Errorf("sketch type: %v", got.SketchType)
	}
	if string(got.Payload) != string(env.Payload) {
		t.Errorf("payload mismatch")
	}
}

func TestEncode_FallbackMetricName(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeKLLSketch,
		// MetricName intentionally empty.
		WindowEndMs: 1_000,
		Payload:     []byte{0x01},
	}
	cfg := DefaultAdapterConfig()
	cfg.OutputMetricName = "asap_kll_default"

	metrics, err := Encode([]*precompute.SketchEnvelope{env}, cfg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if metrics[0].Name() != "asap_kll_default" {
		t.Errorf("name: %q", metrics[0].Name())
	}
}

func TestEncode_ResourceLabelsFlattenIntoTags(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeHLLSketch,
		ResourceLabels: []precompute.KeyValue{
			{Key: "host", Value: "web-01"},
			{Key: "region", Value: "us-east-1"},
		},
		Labels: []precompute.KeyValue{
			{Key: "service", Value: "checkout"},
		},
		WindowEndMs: 1_000,
		MetricName:  "x",
		Payload:     []byte{0x01},
	}
	metrics, err := Encode([]*precompute.SketchEnvelope{env}, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	m := metrics[0]
	for _, tk := range []string{"host", "region", "service"} {
		if _, ok := m.GetTag(tk); !ok {
			t.Errorf("tag %q missing", tk)
		}
	}
}

func TestEncode_LabelOverridesResourceLabelOnCollision(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		ResourceLabels: []precompute.KeyValue{
			{Key: "host", Value: "resource-host"},
		},
		Labels: []precompute.KeyValue{
			{Key: "host", Value: "label-host"}, // wins
		},
		WindowEndMs: 1_000,
		MetricName:  "x",
		Payload:     []byte{0x01},
	}
	metrics, err := Encode([]*precompute.SketchEnvelope{env}, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	val, _ := metrics[0].GetTag("host")
	if val != "label-host" {
		t.Errorf("collision: want label-host, got %q", val)
	}
}

func TestEncode_NilEnvelope_ReturnsError(t *testing.T) {
	t.Parallel()
	envs := []*precompute.SketchEnvelope{
		{SchemaVersion: 1, SketchType: precompute.SketchTypeDDSketch, MetricName: "ok", Payload: []byte{1}},
		nil,
	}
	_, err := Encode(envs, DefaultAdapterConfig())
	if err == nil {
		t.Fatalf("want error for nil envelope")
	}
	if !strings.Contains(err.Error(), "envelope[1]") {
		t.Errorf("error: %v", err)
	}
}

func TestEncode_EmptySliceReturnsNilNoError(t *testing.T) {
	t.Parallel()
	metrics, err := Encode(nil, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if metrics != nil {
		t.Errorf("want nil slice, got %d", len(metrics))
	}
}

func TestEncode_EmptyEnvelopeField_IsConfigError(t *testing.T) {
	t.Parallel()
	cfg := &AdapterConfig{
		ValueField:       "value",
		EnvelopeField:    "",
		OutputMetricName: "x",
	}
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		MetricName:    "x",
		Payload:       []byte{0x01},
		WindowEndMs:   1_000,
	}
	_, err := Encode([]*precompute.SketchEnvelope{env}, cfg)
	if err == nil {
		t.Fatalf("want config error")
	}
}

func TestEncode_NoTimestamp_UsesNow(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		MetricName:    "x",
		Payload:       []byte{0x01},
		// WindowStartMs and WindowEndMs both zero.
	}
	before := time.Now().Add(-time.Second)
	metrics, err := Encode([]*precompute.SketchEnvelope{env}, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	after := time.Now().Add(time.Second)
	got := metrics[0].Time()
	if got.Before(before) || got.After(after) {
		t.Errorf("now timestamp out of range: %v", got)
	}
}

func TestEncode_UsesWindowStartIfEndZero(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		MetricName:    "x",
		WindowStartMs: 9_000,
		Payload:       []byte{0x01},
	}
	// telegraf's metric.New keeps fields and tags trivially; we only
	// rely on the time stamping behavior here.
	metrics, err := Encode([]*precompute.SketchEnvelope{env}, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if metrics[0].Time().UnixMilli() != 9_000 {
		t.Errorf("expected fallback to WindowStartMs, got %d", metrics[0].Time().UnixMilli())
	}
}

// Sanity check that telegrafmetric.New handles the encode output's
// shape (string field) without coercion surprises.
func TestEncode_FieldIsPlainString(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		MetricName:    "x",
		WindowEndMs:   1,
		Payload:       []byte{0x00, 0x01, 0x02},
	}
	metrics, _ := Encode([]*precompute.SketchEnvelope{env}, DefaultAdapterConfig())
	raw, _ := metrics[0].GetField(DefaultEnvelopeField)
	if _, ok := raw.(string); !ok {
		t.Errorf("field type: %T", raw)
	}
	// Just confirm telegrafmetric.New is sane.
	_ = telegrafmetric.New
}
