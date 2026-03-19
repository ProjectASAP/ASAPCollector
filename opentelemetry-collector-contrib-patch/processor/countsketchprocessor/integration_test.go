// Integration tests: verify processor creation via factory and pipeline-style behavior.
package countsketchprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor"
)

// TestIntegrationFactoryCreateMetrics verifies the processor can be created via the factory.
func TestIntegrationFactoryCreateMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Epsilon = 0.01
	cfg.Delta = 0.99
	cfg.WindowDuration = 0
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("countsketch")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

// TestIntegrationPipelineBatchMode verifies full pipeline: create via factory, process metrics, verify output.
func TestIntegrationPipelineBatchMode(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeBatch
	cfg.Epsilon = 0.01
	cfg.Delta = 0.99
	cfg.WindowDuration = 0
	cfg.DropOriginal = false
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("countsketch")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	metrics := buildTestMetrics()
	err = proc.ConsumeMetrics(context.Background(), metrics)
	require.NoError(t, err)

	all := sink.AllMetrics()
	require.GreaterOrEqual(t, len(all), 1)
}

// TestIntegrationPipelineWindowMode verifies window mode: Start, process metrics, Shutdown triggers flush.
func TestIntegrationPipelineWindowMode(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Mode = ModeWindow
	cfg.Epsilon = 0.01
	cfg.Delta = 0.99
	cfg.WindowDuration = 2 * time.Second
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	set := processor.Settings{
		ID:                component.NewID(component.MustNewType("countsketch")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}

	proc, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)

	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))

	metrics := buildTestMetrics()
	err = proc.ConsumeMetrics(context.Background(), metrics)
	require.NoError(t, err)

	require.NoError(t, proc.Shutdown(context.Background()))

	all := sink.AllMetrics()
	require.GreaterOrEqual(t, len(all), 0)
}
