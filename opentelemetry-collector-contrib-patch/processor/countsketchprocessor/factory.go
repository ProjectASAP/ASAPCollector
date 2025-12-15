package countsketchprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var typeStr = component.MustNewType("countsketch")

func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,

		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Epsilon: 0.01,
		Delta: 0.99,
		WindowSize: 5 * time.Second,
	}
}

func createMetricsProcessor(
	ctx context.Context, 
	set processor.Settings, 
	cfg component.Config, 
	next consumer.Metrics,
) (processor.Metrics, error) {
	proc := newProcessor(set.Logger, cfg.(*Config), next)

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		proc.processMetrics, 
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}