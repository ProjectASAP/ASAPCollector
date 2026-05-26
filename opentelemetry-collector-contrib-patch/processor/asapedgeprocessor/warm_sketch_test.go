package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

func TestDDSketchFamilyFlushes(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics:        []MetricFamily{{Metric: "http_latency_ms", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("http_latency_ms")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < 20; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", "z0")
		dp.SetDoubleValue(float64(10 + i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Millisecond)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	p.flushAll(context.Background())
	if len(cap.got) != 1 {
		t.Fatalf("expected 1 flushed batch, got %d", len(cap.got))
	}
	dpCount := cap.got[0].DataPointCount()
	if dpCount == 0 {
		t.Fatalf("expected sketch envelope data points, got 0")
	}
}
