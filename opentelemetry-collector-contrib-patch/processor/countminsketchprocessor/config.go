// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	MetricName string   `mapstructure:"metric_name"`
	GroupBy    []string `mapstructure:"group_by"`

	// CMS Parameters
	Rows    int `mapstructure:"rows"`
	Columns int `mapstructure:"columns"`

	DropOriginal bool `mapstructure:"drop_original"`

	// The time window to accumulate data before emitting a sketch
	WindowInterval time.Duration `mapstructure:"window_interval"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.MetricName == "" {
		return fmt.Errorf("metric_name must be specified")
	}
	if c.Rows <= 0 || c.Columns <= 0 {
		return fmt.Errorf("rows and columns must be positive")
	}
	// Default to 10s
	if c.WindowInterval <= 0 {
		c.WindowInterval = 10 * time.Second
	}
	return nil
}
