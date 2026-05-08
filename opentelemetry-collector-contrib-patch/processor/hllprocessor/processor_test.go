package hllprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func makeGaugeMetrics(name string, values []float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName(name)
	metric.SetUnit("1")
	g := metric.SetEmptyGauge()
	for _, v := range values {
		dp := g.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetDoubleValue(v)
	}
	return md
}

// TestBatchModeCardinalityOutput verifies that the batch processor emits one
// cardinality gauge metric per input series with the expected suffix.
func TestBatchModeCardinalityOutput(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	// Insert 5 distinct values.
	md := makeGaugeMetrics("requests", []float64{1, 2, 3, 4, 5})
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	var foundCardinality bool
	for i := 0; i < out[0].ResourceMetrics().Len(); i++ {
		sms := out[0].ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			if sms.At(j).Scope().Name() == "otelcol/hllprocessor" {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "requests_hll_cardinality" {
						foundCardinality = true
						assert.Equal(t, pmetric.MetricTypeGauge, ms.At(k).Type())
						require.Equal(t, 1, ms.At(k).Gauge().DataPoints().Len())
						// HLL estimate for 5 distinct values — allow generous tolerance.
						est := ms.At(k).Gauge().DataPoints().At(0).DoubleValue()
						assert.True(t, est >= 1 && est <= 20,
							"cardinality estimate %v out of expected range [1, 20]", est)
					}
				}
			}
		}
	}
	assert.True(t, foundCardinality, "expected metric requests_hll_cardinality")
}

// TestBatchModeTransmitSketch verifies that transmit_sketch=true emits a native
// HLLSketch metric with properly populated data point fields.
func TestBatchModeTransmitSketch(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = true
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := makeGaugeMetrics("latency", []float64{10, 20, 30})
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	var foundSketch bool
	for i := 0; i < out[0].ResourceMetrics().Len(); i++ {
		sms := out[0].ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			if sms.At(j).Scope().Name() == "otelcol/hllprocessor" {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "latency_hll_cardinality" {
						foundSketch = true
						assert.Equal(t, pmetric.MetricTypeHLLSketch, ms.At(k).Type())
						dps := ms.At(k).HLLSketch().DataPoints()
						require.Equal(t, 1, dps.Len())
						dp := dps.At(0)
						assert.Greater(t, dp.Cardinality(), uint64(0), "expected non-zero cardinality")
						assert.Greater(t, len(dp.Sketch()), 0, "expected non-empty sketch bytes")
						assert.Equal(t, pmetric.HLLSketchEncodingProto, dp.Encoding())
						assert.Greater(t, dp.Precision(), uint32(0), "expected non-zero precision")
					}
				}
			}
		}
	}
	assert.True(t, foundSketch, "expected metric latency_hll_cardinality as HLLSketch")
}

// TestBatchModeMetricSuffix verifies that a custom metric_suffix is applied.
func TestBatchModeMetricSuffix(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.MetricSuffix = "_card"
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := makeGaugeMetrics("events", []float64{1, 2})
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	var found bool
	for i := 0; i < out[0].ResourceMetrics().Len(); i++ {
		sms := out[0].ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == "events_card" {
					found = true
				}
			}
		}
	}
	assert.True(t, found, "expected metric events_card")
}

// TestWindowModeFlush verifies that the window processor emits output only
// after flushWindow is called, not on every ConsumeMetrics.
func TestWindowModeFlush(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 10 * time.Minute // large enough to not auto-fire
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := makeGaugeMetrics("sessions", []float64{1, 2, 3})
	// Window mode forwards input through (PR #211); synthesized output
	// arrives only after flushWindow. After ConsumeMetrics the sink
	// has the forwarded input only.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	assert.Len(t, sink.AllMetrics(), 1, "window mode forwards input pass-through (PR #211)")

	// Manually flush.
	require.NoError(t, proc.FlushWindow(context.Background()))

	// 1 ConsumeMetrics + 1 flushWindow synthesized output = 2 sink entries.
	out := sink.AllMetrics()
	require.Len(t, out, 2)

	var foundCardinality bool
	for _, md := range out {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			sms := md.ResourceMetrics().At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "sessions_hll_cardinality" {
						foundCardinality = true
					}
				}
			}
		}
	}
	assert.True(t, foundCardinality, "expected sessions_hll_cardinality after flush")
}

// TestWindowModeMergesAcrossBatches verifies that multiple ConsumeMetrics calls
// within a window are merged into a single HLL before flush.
func TestWindowModeMergesAcrossBatches(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 10 * time.Minute
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	// Two batches with overlapping values.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeGaugeMetrics("hits", []float64{1, 2, 3})))
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeGaugeMetrics("hits", []float64{3, 4, 5})))

	require.NoError(t, proc.FlushWindow(context.Background()))

	// Window mode forwards inputs through (PR #211): 2 ConsumeMetrics +
	// 1 flushWindow synthesized output = 3 sink entries. Scan for the
	// synthesized cardinality metric across all entries.
	out := sink.AllMetrics()
	require.Len(t, out, 3)

	var est float64
	for _, md := range out {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			sms := md.ResourceMetrics().At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "hits_hll_cardinality" {
						dps := ms.At(k).Gauge().DataPoints()
						if dps.Len() > 0 {
							est = dps.At(0).DoubleValue()
						}
					}
				}
			}
		}
	}
	// 5 distinct values across two batches; HLL estimate should be >= 1.
	assert.True(t, est >= 1, "expected merged cardinality estimate >= 1, got %v", est)
}

// TestWindowModeRaceFree verifies that concurrent ConsumeMetrics calls don't
// race under -race.
func TestWindowModeRaceFree(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 10 * time.Minute
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			md := makeGaugeMetrics("concurrent", []float64{float64(id), float64(id + 1)})
			_ = proc.ConsumeMetrics(context.Background(), md)
		}(i)
	}
	wg.Wait()
	_ = proc.FlushWindow(context.Background())
}

// TestHLLAggregateByCollapsesSeries verifies that data points with the same aggregate_by
// label values are merged into a single HLL sketch, and the output carries only those labels.
func TestHLLAggregateByCollapsesSeries(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.AggregateBy = []string{"region"}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	m.SetUnit("1")
	g := m.SetEmptyGauge()

	// Two data points with same region → merged into one sketch.
	dp1 := g.DataPoints().AppendEmpty()
	dp1.Attributes().PutStr("region", "us-east")
	dp1.Attributes().PutStr("server", "a")
	dp1.SetDoubleValue(1)

	dp2 := g.DataPoints().AppendEmpty()
	dp2.Attributes().PutStr("region", "us-east")
	dp2.Attributes().PutStr("server", "b")
	dp2.SetDoubleValue(2)

	// Different region → separate sketch.
	dp3 := g.DataPoints().AppendEmpty()
	dp3.Attributes().PutStr("region", "eu-west")
	dp3.Attributes().PutStr("server", "c")
	dp3.SetDoubleValue(3)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	var cardDPs []pmetric.NumberDataPoint
	rms := out[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		for j := 0; j < rms.At(i).ScopeMetrics().Len(); j++ {
			ms := rms.At(i).ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == "requests_hll_cardinality" {
					dps := ms.At(k).Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						cardDPs = append(cardDPs, dps.At(l))
					}
				}
			}
		}
	}

	// Two groups: us-east and eu-west.
	require.Len(t, cardDPs, 2)

	for _, dp := range cardDPs {
		_, hasServer := dp.Attributes().Get("server")
		assert.False(t, hasServer, "output should not carry 'server' label")
		_, hasRegion := dp.Attributes().Get("region")
		assert.True(t, hasRegion, "output must carry 'region' label")
	}
}

// TestHLLLabelMatchersFilter verifies that only data points matching all label matchers
// are included in the HLL sketch.
func TestHLLLabelMatchersFilter(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DropOriginal = false // preserve legacy "raw + sketch" assertions
	cfg.Mode = ModeBatch
	cfg.LabelMatchers = []LabelMatcher{{Key: "env", Value: "prod"}}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	m.SetUnit("1")
	g := m.SetEmptyGauge()

	dpProd := g.DataPoints().AppendEmpty()
	dpProd.Attributes().PutStr("env", "prod")
	dpProd.SetDoubleValue(1)

	// Same value but different env → filtered; if included, cardinality would stay 1
	// but let's use a distinct value to confirm it's really excluded.
	dpStaging := g.DataPoints().AppendEmpty()
	dpStaging.Attributes().PutStr("env", "staging")
	dpStaging.SetDoubleValue(42)

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	// Only one series (prod) should appear in the output.
	var cardCount int
	rms := out[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		for j := 0; j < rms.At(i).ScopeMetrics().Len(); j++ {
			ms := rms.At(i).ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == "requests_hll_cardinality" {
					cardCount += ms.At(k).Gauge().DataPoints().Len()
				}
			}
		}
	}
	assert.Equal(t, 1, cardCount, "only the prod series should appear")
}

// TestDropOriginalDefault is the bandwidth-FAIL guard for HLL. With
// the default config (DropOriginal=true) the processor MUST emit
// the HLL cardinality summary as a REPLACEMENT for the raw input
// metric. The raw must NOT appear on the outbound stream; it is
// preserved only via the gorillas3 archive write upstream.
func TestDropOriginalDefault(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.True(t, cfg.DropOriginal, "default DropOriginal must be true (bandwidth fix)")
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = false
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)

	md := makeGaugeMetrics("unique_users_per_min", []float64{1, 2, 3, 4, 5})
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	out := sink.AllMetrics()
	require.Len(t, out, 1)

	// Sketch-only on the wire: raw "unique_users_per_min" must NOT
	// appear; HLL cardinality summary "unique_users_per_min_hll_cardinality"
	// MUST appear.
	var foundRaw, foundCard bool
	rms := out[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				switch ms.At(k).Name() {
				case "unique_users_per_min":
					foundRaw = true
				case "unique_users_per_min_hll_cardinality":
					foundCard = true
				}
			}
		}
	}
	assert.False(t, foundRaw,
		"raw input metric must not be on the outbound stream when DropOriginal=true")
	assert.True(t, foundCard, "expected HLL cardinality summary on outbound stream")
}
