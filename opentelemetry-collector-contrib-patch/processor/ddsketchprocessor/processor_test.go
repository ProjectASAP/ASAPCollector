package ddsketchprocessor

import (
	"context"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestProcessorAddsDDSketchMetric(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// Use a minimal processor instance that only exercises batch aggregation.
	proc := &ddsketchProcessor{
		cfg:    cfg,
		logger: zap.NewNop(),
	}

	metrics := buildDDSketchMetrics(t)
	out, err := proc.processBatch(context.Background(), metrics)
	require.NoError(t, err)

	rm := out.ResourceMetrics()
	require.Equal(t, 1, rm.Len())
	sm := rm.At(0).ScopeMetrics()
	require.Equal(t, 1, sm.Len())
	metricsSlice := sm.At(0).Metrics()
	require.Equal(t, 2, metricsSlice.Len())

	original := metricsSlice.At(0)
	sketchMetric := metricsSlice.At(1)
	require.Equal(t, original.Name()+cfg.MetricSuffix, sketchMetric.Name())
	require.Equal(t, pmetric.MetricTypeDDSketch, sketchMetric.Type())

	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	dp := dps.At(0)
	require.Equal(t, uint64(12), dp.Count())
	require.Equal(t, pmetric.DDSketchEncodingProto, dp.Encoding())
	require.NotEmpty(t, dp.Sketch())

	merged := decodeSketch(t, dp.Sketch())
	require.InEpsilon(t, 12, merged.GetCount(), 1e-9)
}

func TestBatchModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.EmitDDSketch = false
	cfg.MetricSuffix = "_quantile"
	cfg.Quantiles = []float64{0.5}

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("latency")
	metric.SetUnit("ms")
	g := metric.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(1)
	dp.SetTimestamp(2)
	dp.SetDoubleValue(10)

	err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	rms := out[0].ResourceMetrics()
	require.Equal(t, 1, rms.Len())
	sms := rms.At(0).ScopeMetrics()
	require.Equal(t, 1, sms.Len())
	ms := sms.At(0).Metrics()
	require.Equal(t, 2, ms.Len())

	// Second metric should be the quantile output.
	outMetric := ms.At(1)
	assert.Equal(t, "latency_quantile", outMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, outMetric.Type())
}

func TestWindowModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.EmitDDSketch = false
	cfg.MetricSuffix = "_quantile"
	cfg.Quantiles = []float64{0.5}

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("latency")
	metric.SetUnit("ms")
	g := metric.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(1)
	dp.SetTimestamp(2)
	dp.SetDoubleValue(10)

	// Window mode should not forward immediately.
	err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Len(t, sink.AllMetrics(), 0)

	// Force a flush and verify output.
	err = proc.flushWindow(context.Background())
	require.NoError(t, err)

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	rms := out[0].ResourceMetrics()
	require.Equal(t, 1, rms.Len())
	sms := rms.At(0).ScopeMetrics()
	require.Equal(t, 1, sms.Len())
	ms := sms.At(0).Metrics()
	require.Equal(t, 1, ms.Len())
	outMetric := ms.At(0)
	assert.Equal(t, "latency_quantile", outMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, outMetric.Type())
}

func TestWindowModeDDSketchInputMultipleBatches(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.EmitDDSketch = true
	cfg.MetricSuffix = "_ddsketch"

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	// First batch
	md1 := buildDDSketchMetrics(t)
	err := proc.ConsumeMetrics(context.Background(), md1)
	require.NoError(t, err)

	// Second batch with the same attributes
	md2 := buildDDSketchMetrics(t)
	err = proc.ConsumeMetrics(context.Background(), md2)
	require.NoError(t, err)

	// Flush and verify that sketches from both batches were merged.
	err = proc.flushWindow(context.Background())
	require.NoError(t, err)

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	rms := out[0].ResourceMetrics()
	require.Equal(t, 1, rms.Len())
	sms := rms.At(0).ScopeMetrics()
	require.Equal(t, 1, sms.Len())
	ms := sms.At(0).Metrics()
	require.Equal(t, 1, ms.Len())

	sketchMetric := ms.At(0)
	assert.Equal(t, "request_latency_ddsketch", sketchMetric.Name())
	require.Equal(t, pmetric.MetricTypeDDSketch, sketchMetric.Type())

	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	dp := dps.At(0)
	merged := decodeSketch(t, dp.Sketch())

	// Original test used total count of 12; with two batches it should be ~24.
	require.InEpsilon(t, 24, merged.GetCount(), 1e-9)
}

func buildDDSketchMetrics(t *testing.T) pmetric.Metrics {
	t.Helper()
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("request_latency")
	metric.SetUnit("ms")
	dd := metric.SetEmptyDDSketch()
	dd.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dps := dd.DataPoints()

	dp1 := dps.AppendEmpty()
	dp1.SetStartTimestamp(pcommon.Timestamp(1))
	dp1.SetTimestamp(pcommon.Timestamp(2))
	dp1.SetCount(4)
	dp1.SetEncoding(pmetric.DDSketchEncodingProto)
	dp1.Attributes().PutStr("route", "/api")
	setSketchPayload(t, dp1, []float64{25.5, 26.5})

	dp2 := dps.AppendEmpty()
	dp2.SetStartTimestamp(pcommon.Timestamp(1))
	dp2.SetTimestamp(pcommon.Timestamp(3))
	dp2.SetCount(8)
	dp2.SetEncoding(pmetric.DDSketchEncodingProto)
	dp2.Attributes().PutStr("route", "/api")
	setSketchPayload(t, dp2, []float64{50.25, 75.0})

	return metrics
}

func setSketchPayload(t *testing.T, dp pmetric.DDSketchDataPoint, values []float64) {
	sk, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)
	for _, v := range values {
		require.NoError(t, sk.Add(v))
	}
	bytes, err := proto.Marshal(sk.ToProto())
	require.NoError(t, err)
	dp.SetSketch(bytes)
}

func decodeSketch(t *testing.T, payload []byte) *ddsketch.DDSketch {
	t.Helper()
	var pb sketchpb.DDSketch
	require.NoError(t, proto.Unmarshal(payload, &pb))
	result, err := ddsketch.FromProto(&pb)
	require.NoError(t, err)
	return result
}
