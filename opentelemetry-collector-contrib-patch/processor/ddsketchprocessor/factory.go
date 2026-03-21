// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

// NewFactory creates a new DDSketch processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, MetricsStability),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	pcfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration supplied")
	}
	if err := pcfg.validate(); err != nil {
		return nil, err
	}

	_ = ctx // currently unused
	proc := newProcessor(pcfg, set.Logger, next)
	if pcfg.EnableSelfMonitoring {
		proc.enableSelfMonitoring(set.TelemetrySettings, set.ID.String())
	}
	return proc, nil
}
