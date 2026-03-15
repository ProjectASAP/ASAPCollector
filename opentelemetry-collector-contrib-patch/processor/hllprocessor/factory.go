package hllprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func createDefaultConfig() component.Config {
	return &Config{
		Mode:           ModeBatch,
		WindowDuration: 60 * time.Second,
		TransmitSketch: false,
		DropOriginal:   true,
		MetricSuffix:   "",
	}
}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("HLL"), createDefaultConfig,
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
		return nil, fmt.Errorf("configuration parsed is not of type *hllprocessor.Config")
	}
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}
	_ = ctx
	return newProcessor(oCfg, set.Logger, next), nil
}
