// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchmergeprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config configures the DDSketch merge processor.
// This processor runs on the gateway side and accumulates per-series
// DDSketch state from `pmetric.MetricTypeDDSketch` data points produced
// by the agent-tier `ddsketchprocessor`. The accumulator is keyed by
// the data point's attribute set; downstream consumers read merged
// state via `GetAccumulator(key)`.
type Config struct {
	// MetricName is the metric name to watch for DDSketch payloads.
	// Defaults to "ddsketch" if empty. Producers (ddsketchprocessor)
	// emit names like "<base>_ddsketch" by default; pipelines should
	// override MetricName to match the chosen agent-side suffix.
	MetricName string `mapstructure:"metric_name"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	return nil
}
