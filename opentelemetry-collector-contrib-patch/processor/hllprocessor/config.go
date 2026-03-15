package hllprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its HLL output.
// - "batch": per-batch flush (one cardinality estimate per batch)
// - "window": tumbling window flush over a configurable duration.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

// Config configures the HLL cardinality-estimation processor.
// The processor ingests Gauge metrics and outputs the estimated cardinality
// of distinct float64 values seen per series using HyperLogLog (precision=14).
type Config struct {
	Mode           InputMode     `mapstructure:"mode"`
	WindowDuration time.Duration `mapstructure:"window_duration"`
	// TransmitSketch embeds the serialized HLL registers in a gauge attribute
	// instead of emitting only the cardinality estimate.
	TransmitSketch bool   `mapstructure:"transmit_sketch"`
	DropOriginal   bool   `mapstructure:"drop_original"`
	MetricSuffix   string `mapstructure:"metric_suffix"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	switch c.Mode {
	case "":
		c.Mode = ModeBatch
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", c.Mode, ModeBatch, ModeWindow)
	}
	if c.Mode == ModeWindow && c.WindowDuration <= 0 {
		return fmt.Errorf("window_duration must be > 0 in window mode, got %v", c.WindowDuration)
	}
	return nil
}
