package asapedgeprocessor

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	oteladapter "github.com/ProjectASAP/asap-precompute-go/otel"
)

func TestSum_SubWindowEmit_EncodesAsSumAgg(t *testing.T) {
	tru := true
	fam := &MetricFamily{Metric: "x", Family: FamilySum, AggregateBy: []string{"zone"}, DeltaTransmission: &tru}
	opts := sketchOpts{window: 30 * time.Second, delta: true, subWindowInterval: 3 * time.Second, subWindowEpsilon: 0}
	sa, ok := newSketchAggregator("x", fam, opts, nil)
	if !ok {
		t.Fatal("newSketchAggregator ok=false")
	}
	obs := func(v float64) {
		_ = sa.pc.Observe(&precompute.Observation{
			TimestampMs: 3_600_000, Metric: "x",
			Labels: []precompute.KeyValue{{Key: "zone", Value: "z0"}},
			Value:  precompute.FloatValue(v),
		})
	}
	obs(10)
	obs(20)
	envs := sa.pc.EmitSubWindow(3_600_000)
	t.Logf("EmitSubWindow returned %d envelopes", len(envs))
	if len(envs) == 0 {
		t.Fatal("Sum EmitSubWindow returned 0 envelopes in processor context")
	}
	for i, e := range envs {
		t.Logf("env[%d] encoding=%v payloadLen=%d", i, e.Encoding, len(e.Payload))
	}
	out, err := oteladapter.Encode(envs, sa.enc)
	if err != nil {
		t.Fatalf("oteladapter.Encode FAILED for Sum sub-window envelope: %v", err)
	}
	t.Logf("encoded resourceMetrics=%d", out.ResourceMetrics().Len())
	if out.ResourceMetrics().Len() == 0 {
		t.Fatal("Encode produced EMPTY output for Sum sub-window emit — this is why the collector dropped it")
	}
}
