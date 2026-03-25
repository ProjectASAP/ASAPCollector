// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:       "http://localhost:9000/serf",
		Compression:    "xor",
		MaxDiff:        1e-3,
		AdjustDigit:    0,
		WindowInterval: 10 * time.Second,
		MaxObjectBytes: 0,
	}
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return newExporter(c, set), nil
}
