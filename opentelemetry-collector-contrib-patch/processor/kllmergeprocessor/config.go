// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllmergeprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config configures the KLL sketch merge processor.
// This processor runs on the gateway side and accumulates per-series KLL
// sketches from `pmetric.MetricTypeKLLSketch` data points produced by the
// agent-tier `kllprocessor`. The accumulator is keyed by the data point's
// attribute set; downstream consumers read merged state via
// `GetAccumulator(key)`.
type Config struct {
	// MetricName is the metric name to watch for KLL sketch payloads.
	// Defaults to "kll_sketch" if empty. Producers (kllprocessor) emit
	// names like "<base>_kll" by default; pipelines should override
	// MetricName here to match the chosen agent-side suffix.
	MetricName string `mapstructure:"metric_name"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	return nil
}
