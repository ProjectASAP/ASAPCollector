// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

// NewFactory creates a new CountMin processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("countmin"), // Ensure Type is defined or use string "countmin"
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MetricName:   "countmin_sketch",
		Rows:         5,
		Columns:      1000,
		Seed:         1,
		DropOriginal: false,
		GroupBy:      []string{},
		// Default window interval if you added that field to Config
		// WindowInterval: 10 * time.Second,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics, // <--- This 'next' is what we need to pass
) (processor.Metrics, error) {
	oCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsed is not of type *countminsketchprocessor.Config")
	}

	if err := oCfg.Validate(); err != nil {
		return nil, err
	}

	// --- FIX IS HERE ---
	// We now pass 'next' to the constructor
	proc := newProcessor(oCfg, next, set.Logger)

	// Since we are handling the "Consumer" logic manually inside proc.ConsumeMetrics
	// (swallowing some data, emitting other data via next), we often
	// don't strictly need processorhelper.NewMetrics wrapping it if we implement
	// the full consumer.Metrics interface ourselves.
	//
	// However, to keep using processorhelper for observability/lifecycle (Start/Shutdown),
	// we can pass our custom ConsumeMetrics.

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		proc.ConsumeMetrics, // Use the new ConsumeMetrics method we wrote
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.Start),       // Register Start for the Ticker
		processorhelper.WithShutdown(proc.Shutdown), // Register Shutdown
	)
}
