// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketchcountminprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/sketchmetricsprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var typeStr = component.MustNewType("sketchmetrics")

var capabilities = consumer.Capabilities{MutatesData: true}

// NewFactory returns a new factory for the sketchmetrics processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Measurement:  "countmin",
		Rows:         3,
		Columns:      4096,
		Seed:         0x9e3779b185ebca87,
		TopK:         20,
		DropOriginal: false,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("sketchmetrics: invalid config type %T", cfg)
	}

	p, err := newProcessor(config, set.Logger)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		p.processMetrics,
		processorhelper.WithCapabilities(capabilities),
	)
}
