// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchmergeprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config configures the CountMinSketch merge processor.
// This processor runs on the receiver / aggregator side and reconstructs
// full CMS sketches from a stream of proto_full or proto_delta payloads
// produced by countminsketchprocessor with DeltaTransmission=true.
type Config struct {
	// MetricName is the metric name to watch for sketch payloads.
	// Defaults to "countmin_sketch" if empty.
	MetricName string `mapstructure:"metric_name"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	return nil
}
