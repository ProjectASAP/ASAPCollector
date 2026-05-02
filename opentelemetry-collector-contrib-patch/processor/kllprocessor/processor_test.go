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

	// Post-refactor: the TransmitSketch path now emits a typed
	// `KLLSketch` metric with the sketch bytes in the `Sketch`
	// field and `KLLSketchEncodingProto` in the `Encoding` field
	// — the modified-OTLP shape ASAPQuery-backend's decoder
	// consumes.
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
				require.Equal(t, pmetric.MetricTypeKLLSketch, m.Type())
				found = true
				require.Equal(t, 1, m.KLLSketch().DataPoints().Len())
				outDP := m.KLLSketch().DataPoints().At(0)
				require.NotEmpty(t, outDP.Sketch(), "sketch bytes must be populated")
				require.Equal(t, pmetric.KLLSketchEncodingProto, outDP.Encoding())
				// Emit path uses proto.Marshal(SerializePortable());
				// matching deserializer is DeserializeKLLSketchFromProtoBytes.
				sketch, err := kll.DeserializeKLLSketchFromProtoBytes(outDP.Sketch())
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

	// Window mode forwards input through (PR #211); synthesized output
	// arrives only after flushWindow. After ConsumeMetrics the sink
	// has the forwarded input only.
	err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Len(t, sink.AllMetrics(), 1)

	err = proc.flushWindow(context.Background())
	require.NoError(t, err)

	out := sink.AllMetrics()
	require.Len(t, out, 2)
	// Second push is the flushed synthesized output.
	synthesized := out[1]
	rms := synthesized.ResourceMetrics()
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

	// Window mode forwards inputs through (PR #211): 2 ConsumeMetrics +
	// 1 flushWindow synthesized output = 3 sink entries. Scan all
	// entries for the synthesized "latency_p50" metric.
	out := sink.AllMetrics()
	require.Len(t, out, 3)
	var p50 *float64
	for _, md := range out {
		if v := getQuantileFromOutput(t, md, "latency_p50"); v != nil {
			p50 = v
			break
		}
	}
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
	// Window mode forwards inputs through (PR #211); even an empty input
	// is forwarded so chained processors observe the original payload.
	assert.Len(t, sink.AllMetrics(), 1)

	require.NoError(t, proc.flushWindow(context.Background()))
	// Empty window: flush emits nothing, so sink length is unchanged.
	assert.Len(t, sink.AllMetrics(), 1)
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
	// Window mode forwards inputs through (PR #211): 10 ConsumeMetrics +
	// 1 flushWindow synthesized output = 11 sink entries. Concurrent
	// ordering is non-deterministic; locate the synthesized
	// "latency_p50" metric by name across all entries.
	out := sink.AllMetrics()
	require.Len(t, out, 11)
	var found bool
	for _, md := range out {
		if v := getQuantileFromOutput(t, md, "latency_p50"); v != nil {
			found = true
			break
		}
	}
	require.True(t, found, "expected synthesized latency_p50 metric in sink")
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

// TestAggregateByCollapsesSeries verifies that data points sharing the same aggregate_by
// label values are merged into a single sketch, and the output carries only those labels.
func TestAggregateByCollapsesSeries(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	cfg.AggregateBy = []string{"region"}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	m.SetUnit("ms")
	g := m.SetEmptyGauge()

	// Two data points: same region, different server → should collapse into one sketch.
	dp1 := g.DataPoints().AppendEmpty()
	dp1.Attributes().PutStr("region", "us-east")
	dp1.Attributes().PutStr("server", "a")
	dp1.SetDoubleValue(10)

	dp2 := g.DataPoints().AppendEmpty()
	dp2.Attributes().PutStr("region", "us-east")
	dp2.Attributes().PutStr("server", "b")
	dp2.SetDoubleValue(20)

	// Third data point: different region → separate sketch.
	dp3 := g.DataPoints().AppendEmpty()
	dp3.Attributes().PutStr("region", "eu-west")
	dp3.Attributes().PutStr("server", "c")
	dp3.SetDoubleValue(100)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	// Collect all latency_p50 data points.
	var p50DPs []pmetric.NumberDataPoint
	rms := out[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		for j := 0; j < rms.At(i).ScopeMetrics().Len(); j++ {
			ms := rms.At(i).ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == "latency_p50" {
					dps := ms.At(k).Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						p50DPs = append(p50DPs, dps.At(l))
					}
				}
			}
		}
	}

	// Two groups: us-east (merged from dp1+dp2) and eu-west (dp3 only).
	require.Len(t, p50DPs, 2)

	for _, dp := range p50DPs {
		// Output attributes must only contain "region", not "server".
		_, hasServer := dp.Attributes().Get("server")
		assert.False(t, hasServer, "output should not carry 'server' label")
		region, ok := dp.Attributes().Get("region")
		require.True(t, ok, "output must carry 'region' label")
		switch region.AsString() {
		case "us-east":
			// p50 of [10, 20] ≈ 15
			assert.InDelta(t, 15.0, dp.DoubleValue(), 5.0)
		case "eu-west":
			assert.InDelta(t, 100.0, dp.DoubleValue(), 1.0)
		default:
			t.Fatalf("unexpected region %q", region.AsString())
		}
	}
}

// TestLabelMatchersFilterGauge verifies that only data points matching ALL label matchers
// are included; non-matching data points are silently dropped.
func TestLabelMatchersFilterGauge(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	cfg.LabelMatchers = []LabelMatcher{{Key: "env", Value: "prod"}}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	m.SetUnit("ms")
	g := m.SetEmptyGauge()

	dpProd := g.DataPoints().AppendEmpty()
	dpProd.Attributes().PutStr("env", "prod")
	dpProd.SetDoubleValue(10)

	dpStaging := g.DataPoints().AppendEmpty()
	dpStaging.Attributes().PutStr("env", "staging")
	dpStaging.SetDoubleValue(9999) // should be filtered out

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	p50 := getQuantileFromOutput(t, out[0], "latency_p50")
	require.NotNil(t, p50)
	// Only the prod data point (10) was included.
	assert.InDelta(t, 10.0, *p50, 0.5)
}

// TestAggregateByWithLabelMatchersWindowMode verifies cross-series aggregation with
// label filtering in window mode (SDK-sends-sketches path via pre-aggregated input).
func TestAggregateByWithLabelMatchersWindowMode(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 24 * 60 * 60 // large: no auto-flush
	cfg.Quantiles = []float64{0.5}
	cfg.AggregateBy = []string{"region"}
	cfg.LabelMatchers = []LabelMatcher{{Key: "env", Value: "prod"}}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	addDP := func(md pmetric.Metrics, region, env string, val float64) {
		var sm pmetric.ScopeMetrics
		if md.ResourceMetrics().Len() == 0 {
			sm = md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		} else {
			sm = md.ResourceMetrics().At(0).ScopeMetrics().At(0)
		}
		var metric pmetric.Metric
		if sm.Metrics().Len() == 0 {
			metric = sm.Metrics().AppendEmpty()
			metric.SetName("latency")
			metric.SetEmptyGauge()
		} else {
			metric = sm.Metrics().At(0)
		}
		dp := metric.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("region", region)
		dp.Attributes().PutStr("env", env)
		dp.SetDoubleValue(val)
	}

	md := pmetric.NewMetrics()
	addDP(md, "us-east", "prod", 10)
	addDP(md, "us-east", "prod", 30)
	addDP(md, "us-east", "staging", 9999) // filtered out by label_matchers
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	require.NoError(t, proc.flushWindow(context.Background()))

	// Window mode forwards input through (PR #211): 1 ConsumeMetrics +
	// 1 flushWindow synthesized output = 2 sink entries. Scan all
	// entries for the synthesized "latency_p50" metric.
	out := sink.AllMetrics()
	require.Len(t, out, 2)

	var p50 *float64
	for _, md := range out {
		if v := getQuantileFromOutput(t, md, "latency_p50"); v != nil {
			p50 = v
			break
		}
	}
	require.NotNil(t, p50)
	// Only prod data points [10, 30] included; p50 ≈ 20.
	assert.GreaterOrEqual(t, *p50, 9.0)
	assert.LessOrEqual(t, *p50, 31.0)

	// Verify output attribute is only "region" (env is not in aggregate_by).
	for _, md := range out {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			for j := 0; j < rms.At(i).ScopeMetrics().Len(); j++ {
				ms := rms.At(i).ScopeMetrics().At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "latency_p50" {
						dp := ms.At(k).Gauge().DataPoints().At(0)
						_, hasEnv := dp.Attributes().Get("env")
						assert.False(t, hasEnv, "output should not carry 'env' label")
						_, hasRegion := dp.Attributes().Get("region")
						assert.True(t, hasRegion, "output must carry 'region' label")
					}
				}
			}
		}
	}
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

// TestRoundTripIngestProtoSketch proves that an inbound KLLSketch payload
// produced by this processor's emit path (proto-encoded SketchEnvelope via
// `serializeKLLSketch`) is accepted by the same processor's ingest path.
// Pre-fix the ingest path called `kll.DeserializeKLLSketchFromBytes` (gob),
// which silently rejected the proto bytes that production traffic carries;
// this test would have caught that asymmetry.
func TestRoundTripIngestProtoSketch(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = true
	cfg.Quantiles = nil
	cfg.DropOriginal = true
	require.NoError(t, cfg.Validate())

	// Build a source KLL sketch with a known multiset and serialize it
	// using the exact emit-side path (proto-encoded SketchEnvelope).
	src := newKLLSketch(cfg)
	require.NotNil(t, src)
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, v := range values {
		src.Update(v)
	}
	payload, err := serializeKLLSketch(src)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	// Sanity: the matching proto deserializer round-trips the bytes.
	roundTripped, err := kll.DeserializeKLLSketchFromProtoBytes(payload)
	require.NoError(t, err)
	require.Equal(t, src.Count(), roundTripped.Count())

	// Feed the proto-encoded sketch back through the processor as a typed
	// KLLSketch input. The processor must (a) accept it on ingest, (b)
	// merge it into a fresh batch sketch, and (c) emit a typed
	// KLLSketchDataPoint whose payload decodes to the same Count.
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("latency")
	metric.SetUnit("ms")
	in := metric.SetEmptyKLLSketch().DataPoints().AppendEmpty()
	in.SetCount(uint64(src.Count()))
	in.SetSketch(payload)
	in.SetEncoding(pmetric.KLLSketchEncodingProto)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	var outDP pmetric.KLLSketchDataPoint
	var found bool
	rms := out[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Type() != pmetric.MetricTypeKLLSketch || m.Name() != "latency_kll" {
					continue
				}
				require.Equal(t, 1, m.KLLSketch().DataPoints().Len())
				outDP = m.KLLSketch().DataPoints().At(0)
				found = true
			}
		}
	}
	require.True(t, found, "expected emitted KLLSketch metric latency_kll")
	require.NotEmpty(t, outDP.Sketch())
	require.Equal(t, pmetric.KLLSketchEncodingProto, outDP.Encoding())

	mergedOut, err := kll.DeserializeKLLSketchFromProtoBytes(outDP.Sketch())
	require.NoError(t, err)
	// Merging the inbound sketch into a fresh empty batch sketch yields a
	// sketch with the same item count as the source.
	assert.Equal(t, src.Count(), mergedOut.Count())
}
