// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchmergeprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config configures the CountSketch merge processor.
// This processor runs on the receiver / aggregator side and reconstructs
// full CS sketches from a stream of proto_full or proto_delta payloads
// produced by countsketchprocessor with DeltaTransmission=true.
type Config struct {
	// MetricName filters which metrics to process.
	// Defaults to "countsketch_partition" if empty.
	MetricName string `mapstructure:"metric_name"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	return nil
}
