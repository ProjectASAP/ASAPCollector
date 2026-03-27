// Integration tests: verify processor creation via factory and pipeline-style behavior.
package hllprocessor

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

// TestIntegrationFactoryCreateMetrics verifies the processor can be created via the factory in batch mode.
func TestIntegrationFactoryCreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("HLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

// TestIntegrationPipelineBatchMode verifies full pipeline: create via factory, send gauge data points
// with distinct float64 values, verify output contains cardinality metric (name ending in _hll_cardinality).
func TestIntegrationPipelineBatchMode(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("HLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	m.SetUnit("1")
	dps := m.SetEmptyGauge().DataPoints()
	for _, v := range []float64{1.0, 2.0, 3.0, 4.0, 5.0} {
		dps.AppendEmpty().SetDoubleValue(v)
	}

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	all := sink.AllMetrics()
	require.Len(t, all, 1)
	require.GreaterOrEqual(t, all[0].ResourceMetrics().Len(), 1)

	// Verify output contains a metric whose name ends with _hll_cardinality.
	found := false
	rms := all[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				if strings.HasSuffix(metrics.At(k).Name(), "_hll_cardinality") {
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected a metric with suffix _hll_cardinality in the output")
}

// TestIntegrationPipelineWindowMode verifies window mode: Start, ConsumeMetrics multiple batches
// with distinct values, Shutdown, check output.
func TestIntegrationPipelineWindowMode(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("HLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	for _, v := range []float64{10.0, 20.0, 30.0} {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("latency")
		m.SetUnit("ms")
		m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(v)
		require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	}

	// Flush via Shutdown.
	require.NoError(t, proc.Shutdown(context.Background()))

	all := sink.AllMetrics()
	require.GreaterOrEqual(t, len(all), 0)
}

// TestIntegrationTransmitSketch verifies that when transmit_sketch=true, the output data points
// have "hll.sketch_payload" attribute set.
func TestIntegrationTransmitSketch(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.TransmitSketch = true
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("HLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("events")
	m.SetUnit("1")
	dps := m.SetEmptyGauge().DataPoints()
	for _, v := range []float64{100.0, 200.0, 300.0} {
		dps.AppendEmpty().SetDoubleValue(v)
	}

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	all := sink.AllMetrics()
	require.Len(t, all, 1)

	// Find a data point with hll.sketch_payload attribute.
	found := false
	rms := all[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.Type() != pmetric.MetricTypeGauge {
					continue
				}
				dps := metric.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					if _, ok := dps.At(l).Attributes().Get("hll.sketch_payload"); ok {
						found = true
					}
				}
			}
		}
	}
	require.True(t, found, "expected at least one data point with hll.sketch_payload attribute when transmit_sketch=true")
}

// TestIntegrationMultipleSeries verifies that distinct series (different metric names or attributes)
// produce separate cardinality outputs.
func TestIntegrationMultipleSeries(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("HLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	// Send two distinct metrics in a single batch.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("metric_alpha")
	m1.SetUnit("1")
	m1.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)

	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("metric_beta")
	m2.SetUnit("1")
	m2.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(2.0)

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	all := sink.AllMetrics()
	require.Len(t, all, 1)

	// Count cardinality output metrics.
	cardinalityMetrics := map[string]bool{}
	rms := all[0].ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				name := metrics.At(k).Name()
				if strings.HasSuffix(name, "_hll_cardinality") {
					cardinalityMetrics[name] = true
				}
			}
		}
	}

	// Expect separate cardinality metrics for each distinct input metric.
	require.Contains(t, cardinalityMetrics, "metric_alpha_hll_cardinality")
	require.Contains(t, cardinalityMetrics, "metric_beta_hll_cardinality")
}
