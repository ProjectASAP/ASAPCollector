package ddsketchprocessor

import (
	"context"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestProcessorAddsDDSketchMetric(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	proc := newProcessor(cfg, zap.NewNop())

	metrics := buildDDSketchMetrics(t)
	out, err := proc.processMetrics(context.Background(), metrics)
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
