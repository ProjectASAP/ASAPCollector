// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// wsTotalDPs counts the data points across every warm-emitted metric in a
// captured batch, covering the first-class SumAgg aggregate AND the
// modified-OTLP sketch variants the edge emits (DDSketch / KLLSketch /
// HLLSketch / CountSketch / CountMinSketch) in addition to plain Sum/Gauge. It
// lets a test assert PerSeries fans out to N points while WholeStream collapses
// to exactly 1.
func wsTotalDPs(md pmetric.Metrics) int {
	n := 0
	forEachMetric(md, func(m pmetric.Metric) {
		switch m.Type() {
		case pmetric.MetricTypeSum:
			n += m.Sum().DataPoints().Len()
		case pmetric.MetricTypeGauge:
			n += m.Gauge().DataPoints().Len()
		case pmetric.MetricTypeSumAgg:
			n += m.SumAgg().DataPoints().Len()
		case pmetric.MetricTypeDDSketch:
			n += m.DDSketch().DataPoints().Len()
		case pmetric.MetricTypeKLLSketch:
			n += m.KLLSketch().DataPoints().Len()
		case pmetric.MetricTypeHLLSketch:
			n += m.HLLSketch().DataPoints().Len()
		case pmetric.MetricTypeCountSketch:
			n += m.CountSketch().DataPoints().Len()
		case pmetric.MetricTypeCountMinSketch:
			n += m.CountMinSketch().DataPoints().Len()
		}
	})
	return n
}

// wsMetricNamed returns the first emitted metric with the given name.
func wsMetricNamed(md pmetric.Metrics, name string) (pmetric.Metric, bool) {
	var found pmetric.Metric
	ok := false
	forEachMetric(md, func(m pmetric.Metric) {
		if !ok && m.Name() == name {
			found, ok = m, true
		}
	})
	return found, ok
}

// TestWholeStreamSumGrandTotal: PerSeries Sum over [zone] emits one dp per zone;
// WholeStream Sum collapses to ONE dp carrying the grand total across all zones.
// (runItemLabelMetric uses ShardCount=1, so a whole-stream metric collapses to
// exactly one envelope.)
func TestWholeStreamSumGrandTotal(t *testing.T) {
	emit := func(g pmetric.Gauge, base time.Time) {
		add := func(zone string, val float64, n int) {
			for i := 0; i < n; i++ {
				dp := g.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("zone", zone)
				dp.SetDoubleValue(val)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
			}
		}
		add("z1", 2, 3) // 6
		add("z2", 5, 2) // 10
	}

	// PerSeries baseline: two zones => two data points.
	ps := runItemLabelMetric(t, &MetricFamily{Metric: "req", Family: FamilySum, Tier: TierWarm, AggregateBy: []string{"zone"}}, emit)
	if got := wsTotalDPs(ps); got != 2 {
		t.Fatalf("PerSeries Sum: want 2 dps (per zone), got %d", got)
	}

	// WholeStream: ONE data point, grand total = 6 + 10 = 16.
	ws := runItemLabelMetric(t, &MetricFamily{Metric: "req", Family: FamilySum, Tier: TierWarm, AggregateBy: []string{"zone"}, Mode: "whole_stream"}, emit)
	if got := wsTotalDPs(ws); got != 1 {
		t.Fatalf("WholeStream Sum: want 1 dp (collapsed), got %d", got)
	}
	// Sum is a first-class aggregate, emitted as the SumAgg modified-OTLP type
	// (not a plain Sum metric) since #468.
	m, ok := wsMetricNamed(ws, "req")
	if !ok || m.Type() != pmetric.MetricTypeSumAgg {
		t.Fatalf("WholeStream Sum metric missing/wrong type (got %v)", func() any {
			if ok {
				return m.Type()
			}
			return "absent"
		}())
	}
	dp := m.SumAgg().DataPoints().At(0)
	// The Sum value rides inside the SumAgg sketch payload (16-byte
	// little-endian {sum,count}); decode it via the SumWrapper, mirroring how
	// item_label_test decodes the HLL/CMS frames.
	w := sketches.NewSumWrapper()
	if err := w.ApplyDelta(dp.Sketch()); err != nil {
		t.Fatalf("decode SumAgg payload: %v", err)
	}
	if w.Sum() != 16 {
		t.Errorf("WholeStream grand total: want 16, got %v", w.Sum())
	}
	if _, has := dp.Attributes().Get("zone"); has {
		t.Errorf("WholeStream Sum dp should have collapsed the zone grouping label")
	}
}

// TestWholeStreamDDSketchPooled: PerSeries DDSketch over [zone] emits one sketch
// per zone; WholeStream pools every series' values into a single sketch ⇒ one dp.
func TestWholeStreamDDSketchPooled(t *testing.T) {
	emit := func(g pmetric.Gauge, base time.Time) {
		for _, z := range []string{"z1", "z2", "z3"} {
			for v := 1; v <= 10; v++ {
				dp := g.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("zone", z)
				dp.SetDoubleValue(float64(v))
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
			}
		}
	}

	ps := runItemLabelMetric(t, &MetricFamily{Metric: "lat", Family: FamilyDDSketch, Tier: TierWarm, AggregateBy: []string{"zone"}, RelativeAccuracy: 0.01}, emit)
	if got := wsTotalDPs(ps); got != 3 {
		t.Fatalf("PerSeries DDSketch: want 3 dps (per zone), got %d", got)
	}

	ws := runItemLabelMetric(t, &MetricFamily{Metric: "lat", Family: FamilyDDSketch, Tier: TierWarm, AggregateBy: []string{"zone"}, RelativeAccuracy: 0.01, Mode: "whole_stream"}, emit)
	if got := wsTotalDPs(ws); got != 1 {
		t.Fatalf("WholeStream DDSketch: want 1 pooled dp, got %d", got)
	}
}

// TestWholeStreamHLLDistinct: WholeStream HLL over an item dimension counts
// DISTINCT items across the whole stream in a single sketch (one dp), with the
// item_label projected out and the grouping label collapsed; PerSeries keeps
// one HLL per zone.
func TestWholeStreamHLLDistinct(t *testing.T) {
	emit := func(g pmetric.Gauge, base time.Time) {
		for _, z := range []string{"z1", "z2", "z3"} {
			for _, u := range []string{"alice", "bob", "alice"} {
				dp := g.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("zone", z)
				dp.Attributes().PutStr("user_id", u)
				dp.SetDoubleValue(1)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
			}
		}
	}

	// PerSeries (per zone) => one HLL dp per zone.
	ps := runItemLabelMetric(t, &MetricFamily{Metric: "users", Family: FamilyHLL, Tier: TierWarm, AggregateBy: []string{"zone"}, ItemLabel: "user_id"}, emit)
	if got := wsTotalDPs(ps); got != 3 {
		t.Fatalf("PerSeries HLL: want 3 dps (per zone), got %d", got)
	}

	// WholeStream => ONE HLL dp, item_label projected out and zone collapsed.
	ws := runItemLabelMetric(t, &MetricFamily{Metric: "users", Family: FamilyHLL, Tier: TierWarm, AggregateBy: []string{"zone"}, ItemLabel: "user_id", Mode: "whole_stream"}, emit)
	if got := wsTotalDPs(ws); got != 1 {
		t.Fatalf("WholeStream HLL: want 1 dp (collapsed), got %d", got)
	}
	checked := false
	forEachMetric(ws, func(m pmetric.Metric) {
		if m.Type() != pmetric.MetricTypeHLLSketch {
			return
		}
		dp := m.HLLSketch().DataPoints().At(0)
		checked = true
		if _, has := dp.Attributes().Get("user_id"); has {
			t.Errorf("user_id (item) must be projected out")
		}
		if _, has := dp.Attributes().Get("zone"); has {
			t.Errorf("WholeStream must collapse the zone grouping label")
		}
	})
	if !checked {
		t.Fatalf("no HLLSketch metric emitted")
	}
}

// TestWholeStreamModeValidation: a bad mode string is rejected at config
// Validate; the canonical modes pass.
func TestWholeStreamModeValidation(t *testing.T) {
	bad := &Config{
		ShardCount: 1, WindowDuration: time.Hour,
		Metrics: []MetricFamily{{Metric: "m", Family: FamilySum, Mode: "bogus"}},
	}
	if err := bad.Validate(); err == nil {
		t.Fatalf("expected validation error for mode=bogus")
	}
	for _, mode := range []string{"", "per_series", "whole_stream"} {
		c := &Config{
			ShardCount: 1, WindowDuration: time.Hour,
			Metrics: []MetricFamily{{Metric: "m", Family: FamilySum, Mode: mode}},
		}
		if err := c.Validate(); err != nil {
			t.Errorf("mode=%q should validate, got %v", mode, err)
		}
	}
}
