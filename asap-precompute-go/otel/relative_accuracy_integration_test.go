package otel_test

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	oteladapter "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Exercises the REAL DDSketch wrapper through observe -> Drain -> Encode and
// asserts the relative_accuracy (alpha) survives onto the emitted
// pmetric.DDSketch container. Regression guard for the fused-edge bug where
// the wire frame shipped relative_accuracy=0.0 (degenerate sketch -> backend
// quantile queries capability-miss to archive).
func TestDDSketchRelativeAccuracyReachesWire(t *testing.T) {
	const alpha = 0.02
	cfg := &precompute.PrecomputeConfig{
		AggID:      1,
		SketchType: precompute.SketchTypeDDSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: 10 * time.Second},
		MetricName: "m",
	}
	factory := func() precompute.Sketch { return sketches.NewDDSketchWrapper(alpha) }
	p := precompute.New(cfg, factory, sketches.DDSketchObserver{})
	for i := 0; i < 5; i++ {
		if err := p.Observe(&precompute.Observation{
			TimestampMs: uint64(1000 * (i + 1)),
			Metric:      "m",
			Labels:      []precompute.KeyValue{{Key: "k", Value: "v"}},
			Value:       precompute.FloatValue(float64(i + 1)),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}
	envs := p.Tick(10_000)
	if len(envs) == 0 {
		t.Fatal("no envelopes drained")
	}
	if envs[0].RelativeAccuracy != alpha {
		t.Fatalf("envelope.RelativeAccuracy: want %v, got %v", alpha, envs[0].RelativeAccuracy)
	}

	md, err := oteladapter.Encode(envs, &oteladapter.AdapterConfig{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got float64 = -1
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Type() == pmetric.MetricTypeDDSketch {
					got = ms.At(k).DDSketch().RelativeAccuracy()
				}
			}
		}
	}
	if got != alpha {
		t.Fatalf("in-memory pmetric.DDSketch.RelativeAccuracy: want %v, got %v", alpha, got)
	}

	// Decisive: marshal to OTLP proto bytes and back (what the gRPC exporter
	// actually ships). If the pdata marshaler drops field 3, relative_accuracy
	// is lost on the wire even though it was set in memory.
	b, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	md2, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var wire float64 = -1
	rms2 := md2.ResourceMetrics()
	for i := 0; i < rms2.Len(); i++ {
		sms := rms2.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Type() == pmetric.MetricTypeDDSketch {
					wire = ms.At(k).DDSketch().RelativeAccuracy()
				}
			}
		}
	}
	if wire != alpha {
		t.Fatalf("AFTER proto round-trip DDSketch.RelativeAccuracy: want %v, got %v (pdata marshaler drops it)", alpha, wire)
	}
}
