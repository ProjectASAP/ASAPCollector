package ddsketchprocessor

import (
	"context"
	"sync"
	"testing"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
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
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	// Build via newProcessor so the cross-window sketchPool (#418) is
	// initialized; ProcessBatch's getOrCreate draws sketches from that
	// pool and panics on a manually-zero-valued processor.
	proc := newProcessor(cfg, zap.NewNop(), new(consumertest.MetricsSink))

	metrics := buildDDSketchMetrics(t)
	out, err := proc.ProcessBatch(context.Background(), metrics)
	require.NoError(t, err)

	rm := out.ResourceMetrics()
	require.Equal(t, 1, rm.Len())
	sm := rm.At(0).ScopeMetrics()
	require.Equal(t, 1, sm.Len())
	metricsSlice := sm.At(0).Metrics()
	require.Equal(t, 2, metricsSlice.Len())

	original := metricsSlice.At(0)
	sketchMetric := metricsSlice.At(1)
	require.Equal(t, original.Name(), sketchMetric.Name())
	require.Equal(t, pmetric.MetricTypeDDSketch, sketchMetric.Type())

	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	dp := dps.At(0)
	require.Equal(t, pmetric.DDSketchEncodingProto, dp.Encoding())
	require.NotEmpty(t, dp.Sketch())

	// Refactor-2026-05: per-DP Count was removed from DDSketchDataPoint;
	// the count is now recoverable from the serialized sketch payload
	// (GetCount sums the bucket store), which is what the backend uses.
	merged := decodeSketch(t, dp.Sketch())
	require.GreaterOrEqual(t, merged.GetCount(), uint64(1), "merged sketch should have at least one sample")
}

func TestBatchModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
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

	// Second metric should be the quantile output. Refactor-2026-05/#382:
	// the metric name is PRESERVED from the input ("latency"); the sketch
	// encoding is carried by the pdata variant, not a "_quantile" suffix.
	outMetric := ms.At(1)
	assert.Equal(t, "latency", outMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, outMetric.Type())
}

func TestWindowModeGaugeInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.TransmitSketch = false
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

	// Window mode forwards input through (PR #211); synthesized output
	// arrives only after flushWindow. After ConsumeMetrics the sink
	// has the forwarded input only.
	err := proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Len(t, sink.AllMetrics(), 1)

	// Force a flush and verify output.
	err = proc.FlushWindow(context.Background())
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
	outMetric := ms.At(0)
	// Refactor-2026-05/#382: output keeps the input metric name "latency".
	assert.Equal(t, "latency", outMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, outMetric.Type())
}

func TestWindowModeDDSketchInputMultipleBatches(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.TransmitSketch = true

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
	err = proc.FlushWindow(context.Background())
	require.NoError(t, err)

	// Window mode forwards inputs through (PR #211): 2 ConsumeMetrics +
	// 1 flushWindow synthesized output = 3 sink entries. The synthesized
	// output is the last one.
	out := sink.AllMetrics()
	require.Len(t, out, 3)
	synthesized := out[2]
	rms := synthesized.ResourceMetrics()
	require.Equal(t, 1, rms.Len())
	sms := rms.At(0).ScopeMetrics()
	require.Equal(t, 1, sms.Len())
	ms := sms.At(0).Metrics()
	require.Equal(t, 1, ms.Len())

	sketchMetric := ms.At(0)
	// Refactor-2026-05/#382: name preserved from input ("request_latency");
	// the DDSketch encoding is carried by the pdata variant tag.
	assert.Equal(t, "request_latency", sketchMetric.Name())
	require.Equal(t, pmetric.MetricTypeDDSketch, sketchMetric.Type())
	assert.Equal(t, pmetric.AggregationTemporalityDelta, sketchMetric.DDSketch().AggregationTemporality(),
		"window mode must preserve AggregationTemporality from DDSketch inputs")

	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	dp := dps.At(0)
	merged := decodeSketch(t, dp.Sketch())

	// Two batches: merged sketch count should be at least the size of one batch.
	require.GreaterOrEqual(t, merged.GetCount(), uint64(1), "window merge should produce sketch with samples")
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

	// Refactor-2026-05: per-DP Count was removed from DDSketchDataPoint;
	// the count is carried inside the serialized sketch payload that
	// setSketchPayload writes (here 2 samples per DP).
	dp1 := dps.AppendEmpty()
	dp1.SetStartTimestamp(pcommon.Timestamp(1))
	dp1.SetTimestamp(pcommon.Timestamp(2))
	dp1.SetEncoding(pmetric.DDSketchEncodingProto)
	dp1.Attributes().PutStr("route", "/api")
	setSketchPayload(t, dp1, []float64{25.5, 26.5})

	dp2 := dps.AppendEmpty()
	dp2.SetStartTimestamp(pcommon.Timestamp(1))
	dp2.SetTimestamp(pcommon.Timestamp(3))
	dp2.SetEncoding(pmetric.DDSketchEncodingProto)
	dp2.Attributes().PutStr("route", "/api")
	setSketchPayload(t, dp2, []float64{50.25, 75.0})

	return metrics
}

// setSketchPayload builds a sketchlib-go DDSketch with the given
// values, serializes it as a bare DDSketchState proto (the
// envelope-fallback path in production decodeDDSketchDataPoint),
// and writes the bytes onto the data point. Mirrors the agent
// emit path's wire format byte-for-byte.
func setSketchPayload(t *testing.T, dp pmetric.DDSketchDataPoint, values []float64) {
	t.Helper()
	sk := ddsketch.NewDDSketch(0.01)
	for _, v := range values {
		sk.Update(v)
	}
	bytes, err := sk.SerializeStateProtoBytes()
	require.NoError(t, err)
	dp.SetSketch(bytes)
}

// decodeSketch unmarshals the processor's emitted sketch payload
// back into a sketchlib-go *DDSketch we can inspect. The processor
// emits via SerializePortable (envelope-wrapped); fall back to
// bare DDSketchState for fixtures that skip the envelope.
func decodeSketch(t *testing.T, payload []byte) *ddsketch.DDSketch {
	t.Helper()
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(payload, &env); err == nil {
		if state := env.GetDdsketch(); state != nil {
			result, err := ddsketch.NewFromState(state)
			require.NoError(t, err)
			return result
		}
	}
	result, err := ddsketch.NewFromStateProtoBytes(payload)
	require.NoError(t, err)
	return result
}

// TestBatchModeDualInput verifies that batch mode correctly processes both Gauge and DDSketch inputs in the same batch.
func TestBatchModeDualInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = true

	// newProcessor initializes the sketchPool that ProcessBatch's
	// getOrCreate factory draws from (#418).
	proc := newProcessor(cfg, zap.NewNop(), new(consumertest.MetricsSink))

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
	// Refactor-2026-05: per-DP Count removed; count lives in the payload.
	dp.SetEncoding(pmetric.DDSketchEncodingProto)
	dp.Attributes().PutStr("route", "/api")
	setSketchPayload(t, dp, []float64{25.0, 75.0})

	out, err := proc.ProcessBatch(context.Background(), metrics)
	require.NoError(t, err)

	// Should have original 2 metrics + 2 sketch metrics (one per input type).
	// Refactor-2026-05/#382: synthesized sketch metrics PRESERVE the input
	// metric name (no "_ddsketch" suffix); the sketch encoding is carried by
	// the pdata variant tag. We therefore identify the synthesized outputs by
	// (name, DDSketch type): the gauge input "latency_ms" gains a DDSketch
	// metric of the same name, and the already-DDSketch "request_latency"
	// appears as a DDSketch metric (input + synthesized share the name+type).
	ms := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.GreaterOrEqual(t, ms.Len(), 2)
	var foundGaugeSketch, foundDDSketch bool
	for i := 0; i < ms.Len(); i++ {
		m := ms.At(i)
		if m.Name() == "latency_ms" && m.Type() == pmetric.MetricTypeDDSketch {
			foundGaugeSketch = true
		}
		if m.Name() == "request_latency" && m.Type() == pmetric.MetricTypeDDSketch {
			foundDDSketch = true
		}
	}
	assert.True(t, foundGaugeSketch, "expected DDSketch output from gauge input 'latency_ms'")
	assert.True(t, foundDDSketch, "expected DDSketch output from DDSketch input 'request_latency'")
}

// TestWindowModeDualInput verifies that window mode merges both Gauge and DDSketch inputs for the same metric.
func TestWindowModeDualInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.TransmitSketch = true
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

	require.NoError(t, proc.FlushWindow(context.Background()))

	// Window mode forwards inputs through (PR #211): 2 ConsumeMetrics +
	// 1 flushWindow synthesized output = 3 sink entries. Refactor-2026-05/#382:
	// the synthesized sketch PRESERVES the input name "latency" (no "_ddsketch"
	// suffix), so it is indistinguishable from the forwarded DDSketch input by
	// name alone. The synthesized output is the 3rd (FlushWindow) sink entry.
	out := sink.AllMetrics()
	require.Len(t, out, 3)
	synthesized := out[2]
	var sketchMetric pmetric.Metric
	var found bool
	rms := synthesized.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() == "latency" && m.Type() == pmetric.MetricTypeDDSketch {
					sketchMetric = m
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected synthesized 'latency' DDSketch metric in flushed output")
	dps := sketchMetric.DDSketch().DataPoints()
	require.Equal(t, 1, dps.Len())
	merged := decodeSketch(t, dps.At(0).Sketch())
	// Both gauge and DDSketch inputs should be merged (exact count depends on merge semantics).
	assert.GreaterOrEqual(t, merged.GetCount(), uint64(1), "merged sketch should include gauge and/or DDSketch inputs")
}

// TestEmptyInput verifies that empty metrics do not cause panics and produce no output in window mode.
func TestEmptyInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	empty := pmetric.NewMetrics()
	require.NoError(t, proc.ConsumeMetrics(context.Background(), empty))
	// Window mode forwards inputs through (PR #211); even an empty input
	// is forwarded so chained processors observe the original payload.
	assert.Len(t, sink.AllMetrics(), 1)

	require.NoError(t, proc.FlushWindow(context.Background()))
	// Empty window: flush emits nothing, so sink length is unchanged.
	assert.Len(t, sink.AllMetrics(), 1)
}

// TestEmptyResourceMetrics verifies ResourceMetrics with zero ScopeMetrics is handled.
func TestEmptyResourceMetrics(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
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
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
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
	// First batch p50 ~10, second batch p50 ~100 (no cross-batch state).
	// Refactor-2026-05/#382: output keeps the input metric name "x".
	p50First := getQuantileFromOutput(t, out[0], "x")
	p50Second := getQuantileFromOutput(t, out[1], "x")
	require.NotNil(t, p50First)
	require.NotNil(t, p50Second)
	// Sketch quantiles are approximate; use relaxed delta
	assert.InDelta(t, 10.0, *p50First, 1.0)
	assert.InDelta(t, 100.0, *p50Second, 1.0)
}

// getQuantileFromOutput returns the value of the synthesized quantile
// data point for the (preserved) metric name. Refactor-2026-05/#382:
// the quantile output now keeps the input metric name, so when
// DropOriginal=false the forwarded raw gauge shares that name. The
// synthesized quantile DP is the one carrying the "ddsketch.quantile"
// attribute (appendQuantileMetrics stamps it), so prefer that to avoid
// returning the raw input value.
func getQuantileFromOutput(t *testing.T, md pmetric.Metrics, name string) *float64 {
	t.Helper()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if (m.Name() == name || len(name) == 0) && m.Type() == pmetric.MetricTypeGauge {
					dps := m.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						if _, ok := dp.Attributes().Get("ddsketch.quantile"); ok {
							v := dp.DoubleValue()
							return &v
						}
					}
				}
			}
		}
	}
	return nil
}

// collectQuantileDPs gathers all synthesized quantile data points for the
// (preserved) metric name across md. Refactor-2026-05/#382: the quantile
// output keeps the input metric name, so the synthesized DPs are the gauge
// DPs carrying the "ddsketch.quantile" attribute (distinguishing them from a
// forwarded raw gauge of the same name when DropOriginal=false).
func collectQuantileDPs(md pmetric.Metrics, name string) []pmetric.NumberDataPoint {
	var out []pmetric.NumberDataPoint
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() != name || m.Type() != pmetric.MetricTypeGauge {
					continue
				}
				dps := m.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					if _, ok := dp.Attributes().Get("ddsketch.quantile"); ok {
						out = append(out, dp)
					}
				}
			}
		}
	}
	return out
}

// TestMixedIntDoubleGauge verifies that Int and Double gauge datapoints are both accepted.
func TestMixedIntDoubleGauge(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	cfg.Quantiles = []float64{0.5}

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
	q := getQuantileFromOutput(t, out[0], "mixed")
	require.NotNil(t, q)
	assert.GreaterOrEqual(t, *q, 10.0)
	assert.LessOrEqual(t, *q, 20.0)
}

// TestWindowModeConcurrentConsume verifies concurrent ConsumeMetrics calls in window mode do not race.
func TestWindowModeConcurrentConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
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

	require.NoError(t, proc.FlushWindow(context.Background()))
	// Window mode forwards inputs through (PR #211): 10 ConsumeMetrics +
	// 1 flushWindow synthesized output = 11 sink entries. Concurrent
	// ordering is non-deterministic, so locate the synthesized sketch
	// metric by name rather than by position.
	out := sink.AllMetrics()
	require.Len(t, out, 11)
	// Refactor-2026-05/#382: the synthesized sketch PRESERVES the input name
	// "request_latency" (no "_ddsketch" suffix), so it shares name+type with
	// the forwarded DDSketch inputs. FlushWindow runs after wg.Wait(), so the
	// synthesized output is deterministically the LAST sink entry.
	synthesized := out[10]
	var found bool
	rms := synthesized.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() == "request_latency" && m.Type() == pmetric.MetricTypeDDSketch {
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected synthesized 'request_latency' DDSketch metric in flushed output")
}

// TestWindowModeFlushDuringConsume verifies flush and ConsumeMetrics can run concurrently without race.
func TestWindowModeFlushDuringConsume(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
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
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
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

// TestDDAggregateByCollapsesSeries verifies that gauge data points sharing the same
// aggregate_by label values are merged into one sketch, and output carries only those labels.
func TestDDAggregateByCollapsesSeries(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	cfg.Quantiles = []float64{0.5}
	cfg.AggregateBy = []string{"region"}
	require.NoError(t, cfg.validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	m.SetUnit("ms")
	g := m.SetEmptyGauge()

	// Two data points: same region, different server → collapsed into one sketch.
	dp1 := g.DataPoints().AppendEmpty()
	dp1.Attributes().PutStr("region", "us-east")
	dp1.Attributes().PutStr("server", "a")
	dp1.SetDoubleValue(10)

	dp2 := g.DataPoints().AppendEmpty()
	dp2.Attributes().PutStr("region", "us-east")
	dp2.Attributes().PutStr("server", "b")
	dp2.SetDoubleValue(20)

	// Different region → separate sketch.
	dp3 := g.DataPoints().AppendEmpty()
	dp3.Attributes().PutStr("region", "eu-west")
	dp3.Attributes().PutStr("server", "c")
	dp3.SetDoubleValue(100)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	// Collect synthesized quantile data points. Refactor-2026-05/#382: the
	// quantile output PRESERVES the input metric name "latency" (the raw
	// input gauge is also named "latency" since DropOriginal=false), so the
	// synthesized DPs are identified by the "ddsketch.quantile" attribute that
	// appendQuantileMetrics stamps.
	outDPs := collectQuantileDPs(out[0], "latency")

	// One p50 per group (us-east, eu-west).
	require.Len(t, outDPs, 2)

	for _, dp := range outDPs {
		// Output must carry only "region" (not "server").
		_, hasServer := dp.Attributes().Get("server")
		assert.False(t, hasServer, "output should not carry 'server' label")
		region, ok := dp.Attributes().Get("region")
		require.True(t, ok)
		switch region.AsString() {
		case "us-east":
			// p50 of [10, 20]: DDSketch returns ~10 (rank 1 of 2); accept [9, 21].
			assert.GreaterOrEqual(t, dp.DoubleValue(), 9.0)
			assert.LessOrEqual(t, dp.DoubleValue(), 21.0)
		case "eu-west":
			assert.InDelta(t, 100.0, dp.DoubleValue(), 2.0)
		default:
			t.Fatalf("unexpected region %q", region.AsString())
		}
	}
}

// TestDDLabelMatchersFilterGauge verifies that only gauge data points matching all
// label matchers are included in the sketch.
func TestDDLabelMatchersFilterGauge(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	cfg.Quantiles = []float64{0.5}
	cfg.LabelMatchers = []LabelMatcher{{Key: "env", Value: "prod"}}
	require.NoError(t, cfg.validate())

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
	dpStaging.SetDoubleValue(9999) // must be filtered out

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	// Find the synthesized quantile output. Refactor-2026-05/#382: name is
	// preserved ("latency"); the synthesized DP carries the "ddsketch.quantile"
	// attribute (used by getQuantileFromOutput to skip the forwarded raw gauge).
	p50 := getQuantileFromOutput(t, out[0], "latency")
	require.NotNil(t, p50, "expected quantile output")
	// Only the prod data point (10) was included.
	assert.InDelta(t, 10.0, *p50, 1.0)
}

// TestDDAggregateByWindowModeDDSketchInput verifies cross-series aggregation in window
// mode with pre-aggregated DDSketch data points (mode 1: SDK sends sketches).
func TestDDAggregateByWindowModeDDSketchInput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 24 * 60 * 60 * 1e9 // large: no auto-flush
	cfg.TransmitSketch = false
	cfg.Quantiles = []float64{0.5}
	cfg.AggregateBy = []string{"region"}
	require.NoError(t, cfg.validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	// Build two DDSketch data points with the same region → should merge.
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	m.SetUnit("ms")
	dd := m.SetEmptyDDSketch()
	dd.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	// Refactor-2026-05: per-DP Count removed; counts live in the payloads.
	dp1 := dd.DataPoints().AppendEmpty()
	dp1.Attributes().PutStr("region", "us-east")
	dp1.Attributes().PutStr("server", "a")
	dp1.SetStartTimestamp(pcommon.Timestamp(1))
	dp1.SetTimestamp(pcommon.Timestamp(2))
	dp1.SetEncoding(pmetric.DDSketchEncodingProto)
	setSketchPayload(t, dp1, []float64{10})

	dp2 := dd.DataPoints().AppendEmpty()
	dp2.Attributes().PutStr("region", "us-east")
	dp2.Attributes().PutStr("server", "b")
	dp2.SetStartTimestamp(pcommon.Timestamp(1))
	dp2.SetTimestamp(pcommon.Timestamp(2))
	dp2.SetEncoding(pmetric.DDSketchEncodingProto)
	setSketchPayload(t, dp2, []float64{20})

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	require.NoError(t, proc.FlushWindow(context.Background()))

	// Window mode forwards input through (PR #211): 1 ConsumeMetrics +
	// 1 flushWindow synthesized output = 2 sink entries. Refactor-2026-05/#382:
	// the synthesized quantile output PRESERVES the input name "latency"; it is
	// the Gauge DP carrying the "ddsketch.quantile" attribute (the forwarded
	// raw input is DDSketch-typed, so collectQuantileDPs ignores it).
	out := sink.AllMetrics()
	require.Len(t, out, 2)

	var outDPs []pmetric.NumberDataPoint
	for _, md := range out {
		outDPs = append(outDPs, collectQuantileDPs(md, "latency")...)
	}

	// Both dp1 and dp2 share region=us-east → one output data point.
	require.Len(t, outDPs, 1)
	_, hasServer := outDPs[0].Attributes().Get("server")
	assert.False(t, hasServer, "output should not carry 'server' label")
	region, ok := outDPs[0].Attributes().Get("region")
	require.True(t, ok)
	assert.Equal(t, "us-east", region.AsString())
	// p50 of merged [10, 20]: DDSketch returns ~10 (rank 1 of 2); accept [9, 21].
	assert.GreaterOrEqual(t, outDPs[0].DoubleValue(), 9.0)
	assert.LessOrEqual(t, outDPs[0].DoubleValue(), 21.0)
}

// TestDropOriginalDefault is the bandwidth-FAIL guard. With the
// default config (DropOriginal=true) the processor MUST emit the
// sketch summary as a REPLACEMENT for the raw input metric, not as
// an additional metric appended to the input. This prevents the
// agent from shipping raw + sketch on the wire (the ① bandwidth FAIL
// root cause).
func TestDropOriginalDefault(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.True(t, cfg.DropOriginal, "default DropOriginal must be true (bandwidth fix)")
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	cfg.Quantiles = []float64{0.5}

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("http_requests_total_latency_ms")
	g := metric.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(2))
	dp.SetDoubleValue(42)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)
	ms := out[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	// EXACTLY ONE metric on the wire: the sketch summary, NOT the raw input.
	// Refactor-2026-05/#382: the summary now PRESERVES the input metric name
	// ("http_requests_total_latency_ms") rather than appending "_quantile",
	// so we distinguish the summary from a passed-through raw gauge by the
	// synthesized "ddsketch.quantile" attribute on its data point. The raw
	// input DP (value 42, no quantile attr) must not be on the wire.
	require.Equal(t, 1, ms.Len(), "DropOriginal=true must emit sketch ONLY (no raw)")
	summary := ms.At(0)
	assert.Equal(t, "http_requests_total_latency_ms", summary.Name())
	require.Equal(t, pmetric.MetricTypeGauge, summary.Type())
	require.Equal(t, 1, summary.Gauge().DataPoints().Len())
	_, isQuantile := summary.Gauge().DataPoints().At(0).Attributes().Get("ddsketch.quantile")
	assert.True(t, isQuantile,
		"the single outbound metric must be the synthesized quantile summary, not the raw input gauge")
}

// TestDropOriginalDefaultWindowMode verifies window-mode also drops
// the raw md from ConsumeMetrics' forwarded output when
// DropOriginal=true (the ticker goroutine's FlushWindow remains the
// sole source of sketch output downstream).
func TestDropOriginalDefaultWindowMode(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.True(t, cfg.DropOriginal)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24 // disable ticker
	cfg.TransmitSketch = true

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := buildDDSketchMetrics(t)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	// Window-mode + DropOriginal=true: raw md MUST NOT be forwarded.
	require.Empty(t, sink.AllMetrics(), "window mode with DropOriginal=true must not forward raw md")

	// FlushWindow still emits the sketch.
	require.NoError(t, proc.FlushWindow(context.Background()))
	out := sink.AllMetrics()
	require.Len(t, out, 1)
	ms := out[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 1, ms.Len())
	// Refactor-2026-05/#382: name preserved from input ("request_latency");
	// the sketch encoding is carried by the DDSketch pdata variant tag.
	assert.Equal(t, "request_latency", ms.At(0).Name())
	assert.Equal(t, pmetric.MetricTypeDDSketch, ms.At(0).Type())
}
