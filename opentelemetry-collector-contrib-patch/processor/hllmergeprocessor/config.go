// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllmergeprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config configures the HyperLogLog merge processor.
// This processor runs on the gateway side and accumulates per-series
// HyperLogLog state from `pmetric.MetricTypeHLLSketch` data points
// produced by the agent-tier `hllprocessor`. The accumulator is keyed
// by the data point's attribute set; downstream consumers read merged
// state via `GetAccumulator(key)`.
type Config struct {
	// MetricName is the metric name to watch for HLL sketch payloads.
	// Defaults to "hll_sketch" if empty. Producers (hllprocessor)
	// emit names like "<base>_hll_cardinality" by default; pipelines
	// should override MetricName to match the chosen agent-side suffix.
	MetricName string `mapstructure:"metric_name"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	return nil
}
