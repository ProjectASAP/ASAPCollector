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

// TestAllSketchFamiliesFlush runs each sketch family through the fused
// processor (gauge in → ObserveKeyed → Drain → Encode out) and asserts the
// family constructs, observes, and emits without panic.
func TestAllSketchFamiliesFlush(t *testing.T) {
	for _, fam := range []FamilyKind{FamilyDDSketch, FamilyKLL, FamilyHLL, FamilyCountSketch, FamilyCountMinSketch} {
		t.Run(string(fam), func(t *testing.T) {
			cap := &capMetrics{}
			cfg := &Config{
				ShardCount:     2,
				WindowDuration: time.Hour,
				DropOriginal:   true,
				Metrics:        []MetricFamily{{Metric: "m", Family: fam, RelativeAccuracy: 0.01}},
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
			m.SetName("m")
			g := m.SetEmptyGauge()
			base := time.Unix(1700000000, 0)
			for i := 0; i < 30; i++ {
				dp := g.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("zone", "z0")
				dp.SetDoubleValue(float64(i % 7))
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Millisecond)))
			}
			if err := p.ConsumeMetrics(context.Background(), md); err != nil {
				t.Fatalf("ConsumeMetrics: %v", err)
			}
			p.flushAll(context.Background())
			if len(cap.got) != 1 {
				t.Fatalf("family %s: expected 1 flushed batch, got %d", fam, len(cap.got))
			}
			if cap.got[0].DataPointCount() == 0 {
				t.Fatalf("family %s: expected sketch envelope data points, got 0", fam)
			}
		})
	}
}
