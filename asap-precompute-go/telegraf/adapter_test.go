package telegraf

import (
	"testing"
	"time"

	telegrafmetric "github.com/influxdata/telegraf/metric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func TestAdapter_DecodeWrapsPackageHelper(t *testing.T) {
	t.Parallel()
	a := NewAdapter(DefaultAdapterConfig())
	m := telegrafmetric.New(
		"x",
		map[string]string{"k": "v"},
		map[string]interface{}{"value": 2.5},
		time.UnixMilli(1_234),
	)
	obs, err := a.Decode(m)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if obs.Value.Float != 2.5 {
		t.Errorf("float: %v", obs.Value.Float)
	}
	if obs.TimestampMs != 1_234 {
		t.Errorf("ts: %d", obs.TimestampMs)
	}
}

func TestAdapter_EncodeWrapsPackageHelper(t *testing.T) {
	t.Parallel()
	a := NewAdapter(DefaultAdapterConfig())
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		MetricName:    "x",
		WindowEndMs:   1,
		Payload:       []byte{0x01},
	}
	metrics, err := a.Encode([]*precompute.SketchEnvelope{env})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("len: %d", len(metrics))
	}
	if metrics[0].Name() != "x" {
		t.Errorf("name: %q", metrics[0].Name())
	}
}

func TestAdapter_NewAdapterStoresConfig(t *testing.T) {
	t.Parallel()
	cfg := &AdapterConfig{
		ValueField:       "v",
		EnvelopeField:    "_e",
		OutputMetricName: "om",
	}
	a := NewAdapter(cfg)
	if a.Config() != cfg {
		t.Errorf("config not preserved")
	}
}

func TestAdapter_Roundtrip_FloatPath(t *testing.T) {
	t.Parallel()
	// Build a synthetic envelope, Encode → telegraf.Metric, Decode the
	// result back, and confirm we recover the same envelope. This is
	// the round-trip test the codec spec calls for: it gates the
	// encode/decode inverse property the upstream multi-hop case
	// (edge sketchtelegraf → gateway sketchtelegraf) depends on.
	a := NewAdapter(DefaultAdapterConfig())
	env := &precompute.SketchEnvelope{
		SchemaVersion: 1,
		SketchType:    precompute.SketchTypeDDSketch,
		AggID:         99,
		Labels: []precompute.KeyValue{
			{Key: "method", Value: "GET"},
		},
		WindowStartMs: 1_000,
		WindowEndMs:   2_000,
		Encoding:      precompute.EncodingProtoFull,
		Payload:       []byte{0x10, 0x20, 0x30, 0x40},
		MetricName:    "rtt_ms",
		Count:         12,
	}

	metrics, err := a.Encode([]*precompute.SketchEnvelope{env})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("metrics len: %d", len(metrics))
	}
	obs, err := a.Decode(metrics[0])
	if err != nil {
		t.Fatalf("decode after encode: %v", err)
	}
	if obs.Value.Kind != precompute.KindEnvelope {
		t.Fatalf("kind: %v", obs.Value.Kind)
	}
	got := obs.Value.Envelope
	if got.SchemaVersion != env.SchemaVersion {
		t.Errorf("schema version: %v vs %v", got.SchemaVersion, env.SchemaVersion)
	}
	if got.SketchType != env.SketchType {
		t.Errorf("sketch type: %v vs %v", got.SketchType, env.SketchType)
	}
	if got.AggID != env.AggID {
		t.Errorf("agg id: %v vs %v", got.AggID, env.AggID)
	}
	if got.MetricName != env.MetricName {
		t.Errorf("metric name: %q vs %q", got.MetricName, env.MetricName)
	}
	if got.Count != env.Count {
		t.Errorf("count: %v vs %v", got.Count, env.Count)
	}
	if got.WindowStartMs != env.WindowStartMs || got.WindowEndMs != env.WindowEndMs {
		t.Errorf("window: %v..%v vs %v..%v",
			got.WindowStartMs, got.WindowEndMs, env.WindowStartMs, env.WindowEndMs)
	}
	if got.Encoding != env.Encoding {
		t.Errorf("encoding: %v vs %v", got.Encoding, env.Encoding)
	}
	if string(got.Payload) != string(env.Payload) {
		t.Errorf("payload: %x vs %x", got.Payload, env.Payload)
	}
	if len(got.Labels) != len(env.Labels) {
		t.Errorf("labels len: %d vs %d", len(got.Labels), len(env.Labels))
	} else {
		for i := range got.Labels {
			if got.Labels[i] != env.Labels[i] {
				t.Errorf("label[%d]: %+v vs %+v", i, got.Labels[i], env.Labels[i])
			}
		}
	}
	// Outer Observation must also reflect the round-trip metric name
	// (obs.Metric is set from telegraf.Metric.Name(), which Encode
	// stamps from env.MetricName).
	if obs.Metric != env.MetricName {
		t.Errorf("obs metric: %q vs %q", obs.Metric, env.MetricName)
	}
}
