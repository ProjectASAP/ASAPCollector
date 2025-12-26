package countminsketchprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

//
// ─────────────────────────────────────────────────────────────
// Test sink (mock downstream consumer)
// ─────────────────────────────────────────────────────────────
//

type testMetricsSink struct {
	received []pmetric.Metrics
}

func newTestMetricsSink() *testMetricsSink {
	return &testMetricsSink{}
}

func (s *testMetricsSink) ConsumeMetrics(
	ctx context.Context,
	md pmetric.Metrics,
) error {
	s.received = append(s.received, md)
	return nil
}

func (s *testMetricsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *testMetricsSink) Count() int {
	return len(s.received)
}

//
// ─────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────
//

func TestWindowedCountMinSketchProcessor_AppendsSketch(t *testing.T) {
	// 1. Configuration
	cfg := createDefaultConfig().(*Config)
	cfg.MetricName = "custom_cms_metric"
	cfg.Rows = 5
	cfg.Columns = 100
	cfg.DropOriginal = true
	cfg.WindowInterval = time.Second

	logger := zap.NewNop()
	sink := newTestMetricsSink()

	// 2. Processor
	proc := newProcessor(cfg, sink, logger)

	err := proc.Start(context.Background(), nil)
	require.NoError(t, err)
	defer proc.Shutdown(context.Background())

	// 3. Input metrics
	input := buildTestMetrics()

	// 4. Ingest
	_, err = proc.ConsumeMetrics(context.Background(), input)
	require.NoError(t, err)

	// 5. Close window manually
	proc.emitWindowAndReset()

	// 6. Assertions
	require.Equal(t, 1, sink.Count())

	md := sink.received[0]
	require.Equal(t, 1, md.ResourceMetrics().Len())

	sm := md.ResourceMetrics().At(0).ScopeMetrics()
	require.Equal(t, 1, sm.Len())

	metric := sm.At(0).Metrics().At(0)
	require.Equal(t, cfg.MetricName, metric.Name())
	require.Equal(t, "1", metric.Unit())

	dps := metric.Gauge().DataPoints()
	require.Equal(t, 2, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		attrs := dp.Attributes()

		// sketch_payload
		payload, ok := attrs.Get("sketch_payload")
		require.True(t, ok)
		require.Equal(t, pcommon.ValueTypeBytes, payload.Type())
		require.NotEmpty(t, payload.Bytes().AsRaw())

		// rows / cols
		rows, ok := attrs.Get("rows")
		require.True(t, ok)
		require.Equal(t, int64(cfg.Rows), rows.Int())

		cols, ok := attrs.Get("cols")
		require.True(t, ok)
		require.Equal(t, int64(cfg.Columns), cols.Int())

		// aggregation key
		_, ok = attrs.Get("aggregation_key")
		require.True(t, ok)
	}
}

func TestWindowedCountMinSketchProcessor_WindowReset(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = true
	cfg.WindowInterval = time.Second

	logger := zap.NewNop()
	sink := newTestMetricsSink()
	proc := newProcessor(cfg, sink, logger)

	err := proc.Start(context.Background(), nil)
	require.NoError(t, err)
	defer proc.Shutdown(context.Background())

	// First window
	input := buildTestMetrics()
	proc.ConsumeMetrics(context.Background(), input)
	proc.emitWindowAndReset()

	// Second window (no new data)
	proc.emitWindowAndReset()

	// Only one emission expected
	require.Equal(t, 1, sink.Count())
}

func TestWindowedCountMinSketchProcessor_DropOriginal(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = true

	logger := zap.NewNop()
	sink := newTestMetricsSink()
	proc := newProcessor(cfg, sink, logger)

	input := buildTestMetrics()

	out, err := proc.ConsumeMetrics(context.Background(), input)
	require.NoError(t, err)

	require.Equal(t, 0, out.ResourceMetrics().Len())
}

//
// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────
//

func buildTestMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptySum().SetIsMonotonic(true)

	dps := m.Sum().DataPoints()

	// Data point 1
	dp1 := dps.AppendEmpty()
	dp1.Attributes().PutStr("method", "GET")
	dp1.Attributes().PutStr("status", "200")
	dp1.SetIntValue(10)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Data point 2
	dp2 := dps.AppendEmpty()
	dp2.Attributes().PutStr("method", "POST")
	dp2.Attributes().PutStr("status", "500")
	dp2.SetIntValue(5)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return metrics
}
