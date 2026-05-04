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

func TestDecode_FloatValue_HappyPath(t *testing.T) {
	t.Parallel()

	ts := time.UnixMilli(1_700_000_000_000).UTC()
	m := telegrafmetric.New(
		"http_requests",
		map[string]string{"method": "GET", "host": "web-01"},
		map[string]interface{}{"value": 3.14},
		ts,
	)

	obs, err := Decode(m, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs == nil {
		t.Fatalf("nil observation")
	}
	if obs.Metric != "http_requests" {
		t.Errorf("metric: %q", obs.Metric)
	}
	if obs.TimestampMs != 1_700_000_000_000 {
		t.Errorf("ts: %d", obs.TimestampMs)
	}
	if obs.Value.Kind != precompute.KindFloat {
		t.Errorf("kind: %v", obs.Value.Kind)
	}
	if obs.Value.Float != 3.14 {
		t.Errorf("float: %v", obs.Value.Float)
	}
	if obs.ResourceLabels != nil {
		t.Errorf("resource labels not nil for telegraf: %+v", obs.ResourceLabels)
	}
	if len(obs.Labels) != 2 {
		t.Fatalf("labels len: %d", len(obs.Labels))
	}
	// Labels must be sorted by key for stable series-key hashing.
	if obs.Labels[0].Key != "host" || obs.Labels[1].Key != "method" {
		t.Errorf("labels not sorted: %+v", obs.Labels)
	}
}

func TestDecode_IntValueCastsToFloat(t *testing.T) {
	t.Parallel()
	m := telegrafmetric.New(
		"counter",
		nil,
		map[string]interface{}{"value": int64(42)},
		time.UnixMilli(1_000),
	)
	obs, err := Decode(m, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs.Value.Kind != precompute.KindFloat || obs.Value.Float != 42.0 {
		t.Errorf("want 42.0, got %+v", obs.Value)
	}
}

func TestDecode_CustomValueField(t *testing.T) {
	t.Parallel()
	cfg := DefaultAdapterConfig()
	cfg.ValueField = "duration_ms"
	m := telegrafmetric.New(
		"req",
		nil,
		map[string]interface{}{"duration_ms": 125.5, "value": 999.0},
		time.UnixMilli(2_000),
	)
	obs, err := Decode(m, cfg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs.Value.Float != 125.5 {
		t.Errorf("want custom field 125.5, got %v", obs.Value.Float)
	}
}

func TestDecode_MissingValueField_ReturnsError(t *testing.T) {
	t.Parallel()
	m := telegrafmetric.New(
		"sparse",
		nil,
		map[string]interface{}{"other_field": 1.0},
		time.UnixMilli(3_000),
	)
	_, err := Decode(m, DefaultAdapterConfig())
	if err == nil {
		t.Fatalf("want error, got nil")
	}
	if !strings.Contains(err.Error(), "missing value field") {
		t.Errorf("error text: %v", err)
	}
}

func TestDecode_NonNumericValueField_ReturnsError(t *testing.T) {
	t.Parallel()
	m := telegrafmetric.New(
		"weird",
		nil,
		map[string]interface{}{"value": "not a number"},
		time.UnixMilli(4_000),
	)
	_, err := Decode(m, DefaultAdapterConfig())
	if err == nil {
		t.Fatalf("want error, got nil")
	}
	if !strings.Contains(err.Error(), "expected numeric") {
		t.Errorf("error text: %v", err)
	}
}

func TestDecode_NilMetric_ReturnsError(t *testing.T) {
	t.Parallel()
	_, err := Decode(nil, DefaultAdapterConfig())
	if err == nil {
		t.Fatalf("want error, got nil")
	}
}

func TestDecode_EnvelopeShortcut(t *testing.T) {
	t.Parallel()
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		AggID:         42,
		Labels: []precompute.KeyValue{
			{Key: "service", Value: "checkout"},
		},
		WindowStartMs: 100,
		WindowEndMs:   200,
		Encoding:      precompute.EncodingProtoFull,
		Payload:       []byte{0x01, 0x02, 0x03, 0xff, 0x7f},
		MetricName:    "http_request_duration_ms",
		Count:         17,
	}
	jsonBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(jsonBytes)

	m := telegrafmetric.New(
		"http_request_duration_ms",
		map[string]string{"service": "checkout"},
		map[string]interface{}{
			"_asap_envelope_b64": b64,
			"value":              999.0, // ignored when envelope path taken
		},
		time.UnixMilli(5_000),
	)

	obs, err := Decode(m, DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs.Value.Kind != precompute.KindEnvelope {
		t.Fatalf("kind: %v", obs.Value.Kind)
	}
	if obs.Value.Envelope == nil {
		t.Fatalf("envelope nil")
	}
	got := obs.Value.Envelope
	if got.SketchType != precompute.SketchTypeDDSketch {
		t.Errorf("sketch type: %v", got.SketchType)
	}
	if got.AggID != 42 {
		t.Errorf("agg id: %v", got.AggID)
	}
	if got.MetricName != "http_request_duration_ms" {
		t.Errorf("metric name: %q", got.MetricName)
	}
	if string(got.Payload) != string(env.Payload) {
		t.Errorf("payload bytes mismatch: got %x want %x", got.Payload, env.Payload)
	}
}

func TestDecode_MalformedEnvelope_ReturnsError(t *testing.T) {
	t.Parallel()
	m := telegrafmetric.New(
		"x",
		nil,
		map[string]interface{}{"_asap_envelope_b64": "not valid base64!!!"},
		time.UnixMilli(6_000),
	)
	_, err := Decode(m, DefaultAdapterConfig())
	if err == nil {
		t.Fatalf("want error for malformed envelope")
	}
	if !strings.Contains(err.Error(), "envelope field") {
		t.Errorf("error text: %v", err)
	}
}

func TestDecode_EmptyEnvelopeFieldDisablesShortcut(t *testing.T) {
	t.Parallel()
	cfg := &AdapterConfig{
		ValueField:    "value",
		EnvelopeField: "", // explicit-empty disables envelope path
	}
	m := telegrafmetric.New(
		"scalar_only",
		nil,
		map[string]interface{}{
			"value":              7.0,
			"_asap_envelope_b64": "ignored-because-disabled",
		},
		time.UnixMilli(7_000),
	)
	obs, err := Decode(m, cfg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs.Value.Kind != precompute.KindFloat || obs.Value.Float != 7.0 {
		t.Errorf("want scalar 7.0, got %+v", obs.Value)
	}
}

func TestDecode_NilConfigUsesDefaults(t *testing.T) {
	t.Parallel()
	m := telegrafmetric.New(
		"x",
		nil,
		map[string]interface{}{"value": 1.5},
		time.UnixMilli(1),
	)
	obs, err := Decode(m, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs.Value.Float != 1.5 {
		t.Errorf("got %v", obs.Value.Float)
	}
}
