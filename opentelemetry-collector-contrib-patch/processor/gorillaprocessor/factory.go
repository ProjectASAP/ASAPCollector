// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

// NewFactory creates a new Gorilla processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		WindowInterval: 10 * time.Minute,
		MaxObjectBytes: 0,
		DropOriginal:   false,
		S3: S3Config{
			MultipartThreshold: 8 * 1024 * 1024,
			MultipartPartBytes: 8 * 1024 * 1024,
			MaxRetries:         3,
			RetryBackoff:       time.Second,
			UploadTimeout:      30 * time.Second,
		},
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
		return nil, fmt.Errorf("configuration is not *gorillaprocessor.Config")
	}
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}

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
