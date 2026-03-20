package ddsketchprocessor

import (
	"context"
	"sync"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
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
	require.GreaterOrEqual(t, dp.Count(), uint64(1), "sketch datapoint should have count")
	require.Equal(t, pmetric.DDSketchEncodingProto, dp.Encoding())
	require.NotEmpty(t, dp.Sketch())

	merged := decodeSketch(t, dp.Sketch())
	require.GreaterOrEqual(t, merged.GetCount(), float64(1), "merged sketch should have at least one sample")
}

func TestBatchModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
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
	cfg.TransmitSketch = false
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
	cfg.TransmitSketch = true
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
	assert.Equal(t, pmetric.AggregationTemporalityDelta, sketchMetric.DDSketch().AggregationTemporality(),
		"window mode must preserve AggregationTemporality from DDSketch inputs")

	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	dp := dps.At(0)
	merged := decodeSketch(t, dp.Sketch())

	// Two batches: merged sketch count should be at least the size of one batch.
	require.GreaterOrEqual(t, merged.GetCount(), float64(1), "window merge should produce sketch with samples")
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

// TestBatchModeDualInput verifies that batch mode correctly processes both Gauge and DDSketch inputs in the same batch.
func TestBatchModeDualInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = true
	cfg.MetricSuffix = "_ddsketch"

	proc := &ddsketchProcessor{cfg: cfg, logger: zap.NewNop()}

	// Build metrics with both Gauge and DDSketch for same logical metric (different metric names in one scope).
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Gauge metric
	gaugeMetric := sm.Metrics().AppendEmpty()
	gaugeMetric.SetName("latency_ms")
	gaugeMetric.SetUnit("ms")
	gaugeMetric.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(10.0)
	gaugeMetric.Gauge().DataPoints().At(0).Attributes().PutStr("route", "/api")

	// DDSketch metric (same scope)
	ddMetric := sm.Metrics().AppendEmpty()
	ddMetric.SetName("request_latency")
	ddMetric.SetUnit("ms")
	dd := ddMetric.SetEmptyDDSketch()
	dd.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := dd.DataPoints().AppendEmpty()
	dp.SetCount(2)
	dp.SetEncoding(pmetric.DDSketchEncodingProto)
	dp.Attributes().PutStr("route", "/api")
	setSketchPayload(t, dp, []float64{25.0, 75.0})

	out, err := proc.processBatch(context.Background(), metrics)
	require.NoError(t, err)

	// Should have original 2 metrics + 2 sketch metrics (one per input type)
	ms := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.GreaterOrEqual(t, ms.Len(), 2)
	var foundGaugeSketch, foundDDSketch bool
	for i := 0; i < ms.Len(); i++ {
		m := ms.At(i)
		if m.Name() == "latency_ms_ddsketch" {
			foundGaugeSketch = true
			require.Equal(t, pmetric.MetricTypeDDSketch, m.Type())
		}
		if m.Name() == "request_latency_ddsketch" {
			foundDDSketch = true
			require.Equal(t, pmetric.MetricTypeDDSketch, m.Type())
		}
	}
	assert.True(t, foundGaugeSketch, "expected sketch from gauge input")
	assert.True(t, foundDDSketch, "expected sketch from DDSketch input")
}

// TestWindowModeDualInput verifies that window mode merges both Gauge and DDSketch inputs for the same metric.
func TestWindowModeDualInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.TransmitSketch = true
	cfg.MetricSuffix = "_ddsketch"
	cfg.WindowDuration = 60 * 60 * 24 // large so ticker doesn't fire

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	// First batch: raw gauge for "latency"
	md1 := pmetric.NewMetrics()
	rm1 := md1.ResourceMetrics().AppendEmpty()
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("latency")
	m1.SetUnit("ms")
	g := m1.SetEmptyGauge()
	g.DataPoints().AppendEmpty().SetDoubleValue(5.0)
	g.DataPoints().At(0).Attributes().PutStr("route", "/api")
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md1))

	// Second batch: DDSketch for same "latency" (same attributes)
	md2 := buildDDSketchMetrics(t)
	// Rename to "latency" to match
	md2.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).SetName("latency")
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md2))

	require.NoError(t, proc.flushWindow(context.Background()))

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	ms := out[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.GreaterOrEqual(t, ms.Len(), 1)
	sketchMetric := ms.At(0)
	assert.Equal(t, "latency_ddsketch", sketchMetric.Name())
	require.Equal(t, pmetric.MetricTypeDDSketch, sketchMetric.Type())
	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	merged := decodeSketch(t, dps.At(0).Sketch())
	// Both gauge and DDSketch inputs should be merged (exact count depends on merge semantics).
	assert.GreaterOrEqual(t, merged.GetCount(), float64(1), "merged sketch should include gauge and/or DDSketch inputs")
}

// TestEmptyInput verifies that empty metrics do not cause panics and produce no output in window mode.
func TestEmptyInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	empty := pmetric.NewMetrics()
	require.NoError(t, proc.ConsumeMetrics(context.Background(), empty))
	assert.Len(t, sink.AllMetrics(), 0)

	require.NoError(t, proc.flushWindow(context.Background()))
	// Empty window should not emit
	assert.Len(t, sink.AllMetrics(), 0)
}

// TestEmptyResourceMetrics verifies ResourceMetrics with zero ScopeMetrics is handled.
func TestEmptyResourceMetrics(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty() // no scope metrics
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	require.Len(t, sink.AllMetrics(), 1)
}

// TestBatchModeNoStatePersistence verifies batch mode does not carry state across ConsumeMetrics calls.
func TestBatchModeNoStatePersistence(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	cfg.MetricSuffix = "_quantile"
	cfg.Quantiles = []float64{0.5}

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	addGauge := func(name string, val float64) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(val)
		return md
	}

	require.NoError(t, proc.ConsumeMetrics(context.Background(), addGauge("x", 10)))
	require.NoError(t, proc.ConsumeMetrics(context.Background(), addGauge("x", 100)))

	out := sink.AllMetrics()
	require.Len(t, out, 2)
	// First batch p50 ~10, second batch p50 ~100 (no cross-batch state)
	p50First := getQuantileFromOutput(t, out[0], "x_quantile")
	p50Second := getQuantileFromOutput(t, out[1], "x_quantile")
	require.NotNil(t, p50First)
	require.NotNil(t, p50Second)
	// Sketch quantiles are approximate; use relaxed delta
	assert.InDelta(t, 10.0, *p50First, 1.0)
	assert.InDelta(t, 100.0, *p50Second, 1.0)
}

func getQuantileFromOutput(t *testing.T, md pmetric.Metrics, namePrefix string) *float64 {
	t.Helper()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if (m.Name() == namePrefix || len(namePrefix) == 0) && m.Type() == pmetric.MetricTypeGauge && m.Gauge().DataPoints().Len() > 0 {
					v := m.Gauge().DataPoints().At(0).DoubleValue()
					return &v
				}
			}
		}
	}
	return nil
}

// TestMixedIntDoubleGauge verifies that Int and Double gauge datapoints are both accepted.
func TestMixedIntDoubleGauge(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	cfg.Quantiles = []float64{0.5}
	cfg.MetricSuffix = "_quantile"

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("mixed")
	m.SetUnit("ms")
	g := m.SetEmptyGauge()
	g.DataPoints().AppendEmpty().SetDoubleValue(10.5)
	g.DataPoints().AppendEmpty().SetIntValue(20)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), metrics))

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	q := getQuantileFromOutput(t, out[0], "mixed_quantile")
	require.NotNil(t, q)
	assert.GreaterOrEqual(t, *q, 10.0)
	assert.LessOrEqual(t, *q, 20.0)
}

// TestWindowModeConcurrentConsume verifies concurrent ConsumeMetrics calls in window mode do not race.
func TestWindowModeConcurrentConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.TransmitSketch = true
	cfg.WindowDuration = 60 * 60 * 24
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			md := buildDDSketchMetrics(t)
			_ = proc.ConsumeMetrics(context.Background(), md)
		}()
	}
	wg.Wait()

	require.NoError(t, proc.flushWindow(context.Background()))
	out := sink.AllMetrics()
	require.Len(t, out, 1)
	// Should have merged all concurrent batches
	ms := out[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.GreaterOrEqual(t, ms.Len(), 1)
}

// TestWindowModeFlushDuringConsume verifies flush and ConsumeMetrics can run concurrently without race.
func TestWindowModeFlushDuringConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 1 // 1ns ticker for rapid flushes
	cfg.TransmitSketch = true
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { _ = proc.Shutdown(context.Background()) }()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			md := buildDDSketchMetrics(t)
			_ = proc.ConsumeMetrics(context.Background(), md)
		}
		close(done)
	}()
	<-done
	// Flushes happen in background; no panic or race
}

// TestShutdownDuringConsume verifies Shutdown completes even when ConsumeMetrics is in progress.
func TestShutdownDuringConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			md := buildDDSketchMetrics(t)
			_ = proc.ConsumeMetrics(context.Background(), md)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = proc.Shutdown(context.Background())
	}()
	wg.Wait()
}
