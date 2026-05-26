package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type capMetrics struct{ got []pmetric.Metrics }

func (c *capMetrics) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *capMetrics) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	c.got = append(c.got, md)
	return nil
}

// TestSumPathMergesByZone feeds a delta-Sum counter with series spread across
// zones (and methods), sharded across N shards, and verifies the flushed
// output is one delta Sum metric with per-zone merged sums — i.e. the
// asap-native equivalent of metricstransform aggregate_labels label_set:[zone].
func TestSumPathMergesByZone(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     4,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics:        []MetricFamily{{Metric: "http_requests_total", Family: FamilySum, AggregateBy: []string{"zone"}}},
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
	m.SetName("http_requests_total")
	s := m.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	now := pcommon.Timestamp(uint64(time.Now().UnixMilli()) * 1e6)
	add := func(zone, method string, v float64) {
		dp := s.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", zone)
		dp.Attributes().PutStr("method", method)
		dp.SetDoubleValue(v)
		dp.SetTimestamp(now)
	}
	// distinct series (zone×method), summed per zone: z0=1+2=3, z1=4+8=12, z2=16
	add("z0", "GET", 1)
	add("z0", "POST", 2)
	add("z1", "GET", 4)
	add("z1", "POST", 8)
	add("z2", "GET", 16)

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if len(cap.got) != 0 {
		t.Fatalf("drop_original: expected no passthrough, got %d batches", len(cap.got))
	}

	p.flushAll(context.Background())
	if len(cap.got) != 1 {
		t.Fatalf("expected 1 flushed batch, got %d", len(cap.got))
	}
	got := map[string]float64{}
	var temporality pmetric.AggregationTemporality
	rms := cap.got[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				mm := ms.At(k)
				if mm.Name() != "http_requests_total" || mm.Type() != pmetric.MetricTypeSum {
					continue
				}
				temporality = mm.Sum().AggregationTemporality()
				dps := mm.Sum().DataPoints()
				for d := 0; d < dps.Len(); d++ {
					dp := dps.At(d)
					z, _ := dp.Attributes().Get("zone")
					if dp.Attributes().Len() != 1 {
						t.Fatalf("expected only [zone] on output dp, got %d attrs", dp.Attributes().Len())
					}
					got[z.AsString()] += dp.DoubleValue()
				}
			}
		}
	}
	if temporality != pmetric.AggregationTemporalityDelta {
		t.Fatalf("expected delta temporality, got %v", temporality)
	}
	want := map[string]float64{"z0": 3, "z1": 12, "z2": 16}
	if len(got) != len(want) {
		t.Fatalf("zone count: got %v want %v", got, want)
	}
	for z, v := range want {
		if got[z] != v {
			t.Fatalf("zone %s: got %v want %v (full=%v)", z, got[z], v, got)
		}
	}
}
