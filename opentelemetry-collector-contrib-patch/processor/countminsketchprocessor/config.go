// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the countmin processor.
type Config struct {
	// MetricName is the name of the output metric containing the sketch.
	// THIS IS REQUIRED so that factory.go and processor.go do not error.
	MetricName string `mapstructure:"metric_name"`

	// GroupBy defines attributes used to split sketches.
	GroupBy []string `mapstructure:"group_by"`

	// Count-Min sketch parameters.
	Rows    int   `mapstructure:"rows"`
	Columns int   `mapstructure:"columns"`
	Seed    int64 `mapstructure:"seed"`

	DropOriginal bool `mapstructure:"drop_original"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.MetricName == "" {
		return fmt.Errorf("metric_name must be specified")
	}
	if c.Rows <= 0 || c.Columns <= 0 {
		return fmt.Errorf("rows and columns must be positive")
	}
	return nil
}
