package kllprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func createDefaultConfig() component.Config {
	return &Config {
		K: 256,
		Quantiles: []float64{0.5, 0.99},
		WriteSeen: false,
		DropOriginal: true,
		ReadAsInt: false,
	}
}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("KLL"), createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelAlpha),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	oCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsed is not of type *kllprocessor.Config")
	}

	if err := oCfg.Validate(); err != nil {
		return nil, err
	}

	proc := newProcessor(oCfg, set.Logger)

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		proc.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
	)
}

