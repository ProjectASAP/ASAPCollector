// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

// NewFactory creates a new CountMin processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("countmin"),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MetricName:     "countmin_sketch",
		Rows:           5,
		Columns:        1024, // Power of two required by new lib
		DropOriginal:   false,
		GroupBy:        []string{},
		WindowInterval: 10 * time.Second,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	oCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsed is not of type *countminsketchprocessor.Config")
	}

	if err := oCfg.Validate(); err != nil {
		return nil, err
	}

	// Pass 'next' to the constructor manually as requested
	proc := newProcessor(oCfg, next, set.Logger)

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		proc.ConsumeMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
	)
}
