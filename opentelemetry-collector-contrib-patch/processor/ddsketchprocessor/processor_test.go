package ddsketchprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestProcessorAddsDDSketchMetric(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	proc := newProcessor(cfg, zap.NewNop())

	metrics := buildNumberMetrics()
	out, err := proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	rm := out.ResourceMetrics()
	require.Equal(t, 1, rm.Len())
	sm := rm.At(0).ScopeMetrics()
	require.Equal(t, 1, sm.Len())
	metricsSlice := sm.At(0).Metrics()
	require.Equal(t, 2, metricsSlice.Len())

	original := metricsSlice.At(0)
	sketch := metricsSlice.At(1)
	require.Equal(t, original.Name()+cfg.MetricSuffix, sketch.Name())
	require.Equal(t, pmetric.MetricTypeSummary, sketch.Type())

	dps := sketch.Summary().DataPoints()
	require.Equal(t, 1, dps.Len())
	dp := dps.At(0)
	require.Equal(t, uint64(2), dp.Count())
	require.NotZero(t, dp.Sum())
	require.Equal(t, len(cfg.Quantiles), dp.QuantileValues().Len())
}

func buildNumberMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("request_latency")
	metric.SetUnit("ms")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dps := sum.DataPoints()

	dp1 := dps.AppendEmpty()
	dp1.SetStartTimestamp(pcommon.Timestamp(1))
	dp1.SetTimestamp(pcommon.Timestamp(2))
	dp1.SetDoubleValue(25.5)
	dp1.Attributes().PutStr("route", "/api")

	dp2 := dps.AppendEmpty()
	dp2.SetStartTimestamp(pcommon.Timestamp(1))
	dp2.SetTimestamp(pcommon.Timestamp(3))
	dp2.SetDoubleValue(50.25)
	dp2.Attributes().PutStr("route", "/api")

	return metrics
}
