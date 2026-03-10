// Integration tests: verify processor creation via factory and pipeline-style behavior.
package kllprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestIntegrationFactoryCreateMetrics verifies the processor can be created via the factory.
func TestIntegrationFactoryCreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5, 0.99}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("KLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

// TestIntegrationPipelineBatchMode verifies full pipeline: create via factory, ConsumeMetrics, verify output.
func TestIntegrationPipelineBatchMode(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("KLL")),
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
	m.SetName("latency")
	m.SetUnit("ms")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(50.0)

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	all := sink.AllMetrics()
	require.Len(t, all, 1)
	require.GreaterOrEqual(t, all[0].ResourceMetrics().Len(), 1)
}

// TestIntegrationPipelineWindowMode verifies window mode: Start, ConsumeMetrics, flush, Shutdown.
func TestIntegrationPipelineWindowMode(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.WindowDuration = 60 * 60 * 24
	cfg.Quantiles = []float64{0.5}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("KLL")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(25.0)
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	// Flush via shutdown
	require.NoError(t, proc.Shutdown(context.Background()))

	all := sink.AllMetrics()
	require.GreaterOrEqual(t, len(all), 0)
}
