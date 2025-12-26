package countminsketchprocessor

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
	// 1. Setup Configuration
	cfg := createDefaultConfig().(*Config)
	cfg.MetricName = "custom_cms_metric"
	cfg.Rows = 5
	cfg.Columns = 100
	cfg.GroupBy = []string{"region"} // We will group by 'region'

	// 2. Instantiate Processor
	p := newProcessor(cfg, zap.NewNop())

	// 3. Build Input Data
	// This helper creates data with 'region' attributes on the DataPoints
	in := buildTestMetricsWithDataPointAttributes()

	// 4. Execute Processor
	out, err := p.processMetrics(context.Background(), in)
	require.NoError(t, err)

	// 5. Assertions
	totalSketches := 0

	// Loop through all outputs to count generated sketches
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		rm := out.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)

				// Identify our sketch metric
				if m.Name() == cfg.MetricName {
					// Count how many data points (sketches) were created
					totalSketches += m.Gauge().DataPoints().Len()

					// Validate format
					dp := m.Gauge().DataPoints().At(0)
					val, ok := dp.Attributes().Get("sketch_payload")
					require.True(t, ok, "sketch_payload attribute must exist")
					require.Equal(t, pcommon.ValueTypeBytes, val.Type())
				}
			}
		}
	}

	// EXPECTATION: 2 Sketches.
	// 1 for region="us-east", 1 for region="us-west".
	require.Equal(t, 2, totalSketches, "Should produce 2 sketches for 2 different regions")
}

// Helper: Puts attributes directly on DataPoints
func buildTestMetricsWithDataPointAttributes() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptySum().SetIsMonotonic(true)

	dps := m.Sum().DataPoints()

	// Data Point 1 (Region: us-east)
	dp1 := dps.AppendEmpty()
	dp1.Attributes().PutStr("method", "GET")
	dp1.Attributes().PutStr("region", "us-east") // <--- Attribute on DataPoint
	dp1.SetIntValue(10)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Data Point 2 (Region: us-west)
	dp2 := dps.AppendEmpty()
	dp2.Attributes().PutStr("method", "POST")
	dp2.Attributes().PutStr("region", "us-west") // <--- Attribute on DataPoint
	dp2.SetIntValue(5)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return metrics
}
