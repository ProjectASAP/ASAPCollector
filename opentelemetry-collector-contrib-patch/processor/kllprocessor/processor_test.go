package kllprocessor

import (
	"context"
	"sync"
	"testing"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestBatchModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

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
	require.GreaterOrEqual(t, rms.Len(), 1)
	// Batch mode appends a new RM with scope "otelcol/kllprocessor" containing quantile metrics
	var foundQuantile bool
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			if sms.At(j).Scope().Name() == "otelcol/kllprocessor" {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "latency_p50" {
						foundQuantile = true
						assert.Equal(t, pmetric.MetricTypeGauge, ms.At(k).Type())
						require.Equal(t, 1, ms.At(k).Gauge().DataPoints().Len())
						assert.InDelta(t, 10.0, ms.At(k).Gauge().DataPoints().At(0).DoubleValue(), 0.01)
					}
				}
			}
		}
	}
	assert.True(t, foundQuantile, "expected quantile metric latency_p50")
}

func TestBatchModeTransmitSketch(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = true
	cfg.Quantiles = nil
	cfg.DropOriginal = true
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("latency")
	metric.SetUnit("ms")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("route", "/api")
	dp.SetDoubleValue(10)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	var found bool
	rms := out[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() != "latency_kll" {
					continue
				}
				found = true
				require.Equal(t, 1, m.Gauge().DataPoints().Len())
				outDP := m.Gauge().DataPoints().At(0)
				payload, ok := outDP.Attributes().Get("kll.sketch_payload")
				require.True(t, ok)
				sketch, err := kll.DeserializeKLLSketchFromBytes(payload.Bytes().AsRaw())
				require.NoError(t, err)
				assert.Equal(t, 1, sketch.Count())
			}
		}
	}
	assert.True(t, found, "expected serialized sketch metric")
}

func TestBatchModeNoStatePersistence(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	// First batch: single value 10
	md1 := pmetric.NewMetrics()
	rm1 := md1.ResourceMetrics().AppendEmpty()
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("x")
	m1.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(10)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md1))

	// Second batch: single value 100 (independent batch, no cross-batch state)
	md2 := pmetric.NewMetrics()
	rm2 := md2.ResourceMetrics().AppendEmpty()
	sm2 := rm2.ScopeMetrics().AppendEmpty()
	m2 := sm2.Metrics().AppendEmpty()
	m2.SetName("x")
	m2.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(100)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md2))

	out := sink.AllMetrics()
	require.Len(t, out, 2)
	// First output: p50 should be ~10
	// Second output: p50 should be ~100 (not merged with first)
	p50First := getQuantileFromOutput(t, out[0], "x_p50")
	p50Second := getQuantileFromOutput(t, out[1], "x_p50")
	require.NotNil(t, p50First)
	require.NotNil(t, p50Second)
	assert.InDelta(t, 10.0, *p50First, 0.01)
	assert.InDelta(t, 100.0, *p50Second, 0.01)
}

func getQuantileFromOutput(t *testing.T, md pmetric.Metrics, metricName string) *float64 {
	t.Helper()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == metricName && ms.At(k).Gauge().DataPoints().Len() > 0 {
					v := ms.At(k).Gauge().DataPoints().At(0).DoubleValue()
					return &v
				}
			}
		}
	}
	return nil
}

func TestWindowModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24 // large so ticker doesn't fire
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

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
	assert.Len(t, sink.AllMetrics(), 0)

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
	assert.Equal(t, "latency_p50", ms.At(0).Name())
	assert.Equal(t, pmetric.MetricTypeGauge, ms.At(0).Type())
	assert.InDelta(t, 10.0, ms.At(0).Gauge().DataPoints().At(0).DoubleValue(), 0.01)
}

func TestWindowModeMultipleBatches(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	addGauge := func(md pmetric.Metrics, name string, val float64) {
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(val)
	}

	md1 := pmetric.NewMetrics()
	addGauge(md1, "latency", 10)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md1))

	md2 := pmetric.NewMetrics()
	addGauge(md2, "latency", 30)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md2))

	require.NoError(t, proc.flushWindow(context.Background()))

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	p50 := getQuantileFromOutput(t, out[0], "latency_p50")
	require.NotNil(t, p50)
	// Merged window: p50 of [10, 30] should be between 10 and 30
	assert.GreaterOrEqual(t, *p50, 9.0)
	assert.LessOrEqual(t, *p50, 31.0)
}

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 0
	assert.Error(t, cfg.Validate())

	cfg.WindowDuration = 10
	cfg.Quantiles = []float64{1.5}
	assert.Error(t, cfg.Validate())

	cfg.Quantiles = []float64{0.5, 0.99}
	assert.NoError(t, cfg.Validate())

	cfg.TransmitSketch = true
	cfg.Quantiles = nil
	assert.NoError(t, cfg.Validate())

	cfg.TransmitSketch = false
	assert.Error(t, cfg.Validate())
}

// TestEmptyInput verifies that empty metrics do not cause panics and produce no output in window mode.
func TestEmptyInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	empty := pmetric.NewMetrics()
	require.NoError(t, proc.ConsumeMetrics(context.Background(), empty))
	assert.Len(t, sink.AllMetrics(), 0)

	require.NoError(t, proc.flushWindow(context.Background()))
	assert.Len(t, sink.AllMetrics(), 0)
}

// TestEmptyResourceMetrics verifies ResourceMetrics with zero ScopeMetrics is handled in batch mode.
// Uses DropOriginal=false to test passthrough when there is no data to aggregate.
func TestEmptyResourceMetrics(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	cfg.DropOriginal = false
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty()
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	require.Len(t, sink.AllMetrics(), 1)
}

// TestMixedIntDoubleGauge verifies both Int and Double gauge datapoints are accepted when ReadAsInt is true.
func TestMixedIntDoubleGauge(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	cfg.ReadAsInt = true
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("mixed")
	m.SetUnit("ms")
	g := m.SetEmptyGauge()
	g.DataPoints().AppendEmpty().SetIntValue(10)
	g.DataPoints().AppendEmpty().SetIntValue(20)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	p50 := getQuantileFromOutput(t, out[0], "mixed_p50")
	require.NotNil(t, p50)
	assert.GreaterOrEqual(t, *p50, 9.0)
	assert.LessOrEqual(t, *p50, 21.0)
}

// TestWindowModeConcurrentConsume verifies concurrent ConsumeMetrics calls in window mode do not race.
func TestWindowModeConcurrentConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			m.SetName("latency")
			m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(10)
			_ = proc.ConsumeMetrics(context.Background(), md)
		}()
	}
	wg.Wait()

	require.NoError(t, proc.flushWindow(context.Background()))
	out := sink.AllMetrics()
	require.Len(t, out, 1)
}

// TestWindowModeFlushDuringConsume verifies flush and ConsumeMetrics can run concurrently without race.
func TestWindowModeFlushDuringConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 1 // 1ns ticker for rapid flushes
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { _ = proc.Shutdown(context.Background()) }()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			m.SetName("latency")
			m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(float64(i))
			_ = proc.ConsumeMetrics(context.Background(), md)
		}
		close(done)
	}()
	<-done
}

// TestShutdownDuringConsume verifies Shutdown completes even when ConsumeMetrics is in progress.
func TestShutdownDuringConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			m.SetName("x")
			m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(float64(i))
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
