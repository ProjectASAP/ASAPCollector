// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its CountMinSketch output.
// - "batch": per-batch summary flush (no background window ticker)
// - "window": tumbling window flush driven by WindowInterval.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

type Config struct {
	// Mode controls when this processor flushes CountMinSketch output.
	//   - "batch": per-batch aggregation/flush
	//   - "window": accumulate across a tumbling time window before flushing
	Mode InputMode `mapstructure:"mode"`

	MetricName string   `mapstructure:"metric_name"`
	GroupBy    []string `mapstructure:"group_by"`

	// CMS Parameters
	Rows    int `mapstructure:"rows"`
	Columns int `mapstructure:"columns"`

	TransmitSketch bool `mapstructure:"transmit_sketch"`
	DropOriginal   bool `mapstructure:"drop_original"`

	// The time window to accumulate data before emitting a sketch (window mode only).
	WindowInterval time.Duration `mapstructure:"window_interval"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	// Default to window mode for backwards compatibility.
	switch c.Mode {
	case "":
		c.Mode = ModeWindow
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", c.Mode, ModeBatch, ModeWindow)
	}

	if c.MetricName == "" {
		return fmt.Errorf("metric_name must be specified")
	}
	if c.Rows <= 0 || c.Columns <= 0 {
		return fmt.Errorf("rows and columns must be positive")
	}

	// WindowInterval is only relevant in window mode.
	if c.Mode == ModeWindow {
		if c.WindowInterval <= 0 {
			// Default to 10s for backwards compatibility.
			c.WindowInterval = 10 * time.Second
		}
		if c.WindowInterval < 1*time.Second {
			return fmt.Errorf("window_interval is too small: %s (minimum is 1s)", c.WindowInterval)
		}
	}

	return nil
}
