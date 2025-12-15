package countsketchprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestProcessorPassThrough(t *testing.T) {
	// Setup
	cfg := createDefaultConfig().(*Config)
	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	// Start the processor
	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create generic metrics
	metrics := buildTestMetrics()

	// Process metrics
	out, err := proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Shutdown
	err = proc.Shutdown(context.Background())
	require.NoError(t, err)

	// Verify the original metrics were passed through to the next consumer
	assert.Equal(t, metrics, out)
	assert.Equal(t, 1, len(next.AllMetrics()))
}

func TestProcessorFlushLogic(t *testing.T) {
	// Setup with a very short window for testing flush
	cfg := &Config{
		Epsilon:    0.1,
		Delta:      0.9,
		WindowSize: 100 * time.Millisecond,
	}
	next := new(consumertest.MetricsSink)
	proc := newProcessor(zap.NewNop(), cfg, next)

	// Start
	err := proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Send some data to populate sketches
	metrics := buildTestMetrics()
	_, err = proc.processMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Wait for a window flush (window is 100ms)
	time.Sleep(200 * time.Millisecond)

	// Shutdown
	err = proc.Shutdown(context.Background())
	require.NoError(t, err)
}

func buildTestMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("host.name", "host-A")
	
	sm := rm.ScopeMetrics().AppendEmpty()
	
	// Add a Gauge
	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("system.cpu.usage")
	gauge.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(45.5)

	// Add a Sum
	sumMetric := sm.Metrics().AppendEmpty()
	sumMetric.SetName("http.requests.total")
	sumMetric.SetEmptySum().DataPoints().AppendEmpty().SetIntValue(100)

	// Add a Histogram
	histMetric := sm.Metrics().AppendEmpty()
	histMetric.SetName("http.response.time")
	hDp := histMetric.SetEmptyHistogram().DataPoints().AppendEmpty()
	hDp.SetCount(5)
	hDp.SetStartTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
	hDp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

	return metrics
}