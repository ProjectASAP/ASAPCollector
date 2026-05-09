// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

// NewFactory creates the gorillas3 processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		WindowInterval: 60 * time.Second,
		MaxObjectBytes: 0,
		Role:           ProcessorRoleGatewayRaw,
		DeliveryMode:   DeliveryModeDurableRaw,
		Tenant:         "default",
		PrefixTemplate: defaultPrefixTemplate,
		UseSSL:         false,
		Region:         "us-east-1",
		DropOriginal:   true,
		MaxRetries:     3,
		RetryBackoff:   time.Second,
		UploadTimeout:  30 * time.Second,
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
		return nil, fmt.Errorf("configuration is not *gorillas3processor.Config")
	}
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}
	proc := newProcessor(oCfg, next, set.Logger, nil)
	proc.monitor = newMonitor(set.TelemetrySettings, set.ID.String(), proc.activeSeries, set.Logger)
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
