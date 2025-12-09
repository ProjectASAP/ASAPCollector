package sketchcountminprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestProcessMetrics_AppendsSketch(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.TopK = 3
	cfg.DropOriginal = true
	cfg.TagKeys = []string{"method", "status"}

	p, err := newProcessor(cfg, zap.NewNop())
	require.NoError(t, err)

	in := buildTestMetrics()
	out, err := p.processMetrics(context.Background(), in)
	require.NoError(t, err)

	// DropOriginal should remove the source metric, leaving only the sketch metric.
	require.Equal(t, 1, out.ResourceMetrics().Len())
	sm := out.ResourceMetrics().At(0).ScopeMetrics()
	require.Equal(t, 1, sm.Len())
	metric := sm.At(0).Metrics().At(0)
	require.Equal(t, cfg.Measurement, metric.Name())
	dps := metric.Gauge().DataPoints()
	require.Equal(t, 2, dps.Len()) // one per tag key (method,status)

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		attrs := dp.Attributes()
		val, ok := attrs.Get("source_measurement")
		require.True(t, ok)
		require.Equal(t, "requests_total", val.Str())
		require.Greater(t, dp.DoubleValue(), 0.0)
		_, hasCountMin := attrs.Get("countmin")
		_, hasRows := attrs.Get("rows")
		_, hasCols := attrs.Get("columns")
		require.True(t, hasCountMin)
		require.True(t, hasRows)
		require.True(t, hasCols)
	}
}

func buildTestMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("tenant", "acme")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests_total")
	dps := m.SetEmptyGauge().DataPoints()

	dp1 := dps.AppendEmpty()
	dp1.Attributes().PutStr("method", "GET")
	dp1.Attributes().PutStr("status", "200")
	dp1.Attributes().PutStr("region", "us-east")
	dp1.SetDoubleValue(5)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	dp2 := dps.AppendEmpty()
	dp2.Attributes().PutStr("method", "POST")
	dp2.Attributes().PutStr("status", "500")
	dp2.Attributes().PutStr("region", "us-east")
	dp2.SetDoubleValue(2)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return metrics
}
