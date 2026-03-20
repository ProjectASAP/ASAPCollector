package kllprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its KLL output.
// - "batch": per-batch flush (no tumbling window)
// - "window": tumbling window flush over a configurable duration.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

type Config struct {
	Mode           InputMode     `mapstructure:"mode"`
	WindowDuration time.Duration `mapstructure:"window_duration"`
	K              int           `mapstructure:"k"`
	Quantiles      []float64     `mapstructure:"quantiles"`
	TransmitSketch bool          `mapstructure:"transmit_sketch"`
	// DeltaTransmission must not be set to true for KLL: KLL uses random
	// compaction and is not additively mergeable. Validate() returns an error
	// if this is set.
	DeltaTransmission bool `mapstructure:"delta_transmission"`
	WriteSeen      bool          `mapstructure:"write_seen"`
	DropOriginal   bool          `mapstructure:"drop_original"`
	ReadAsInt      bool          `mapstructure:"is_int"` // gauge has separate int and double fields, we default to double
	MetricSuffix   string        `mapstructure:"metric_suffix"`

	suffixes map[float64]string // suffix to attach to output quantiles, e.g. _p50, _p99, ...
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
	if c.K < 2 {
		return fmt.Errorf("invalid argument: k must be >= 2 (k=%d)", c.K)
	}
	if !c.TransmitSketch && len(c.Quantiles) == 0 {
		return fmt.Errorf("at least one quantile must be configured when transmit_sketch=false")
	}

	// KLL uses random compaction so sketches are not linearly mergeable.
	// Delta transmission (sparse additive encoding) is not defined for KLL.
	if c.DeltaTransmission {
		return fmt.Errorf("delta_transmission is not supported for KLL sketches: KLL uses random compaction and is not additively mergeable")
	}
	c.suffixes = make(map[float64]string)
	for _, q := range c.Quantiles {
		if q < 0 || q > 1 {
			return fmt.Errorf("invalid argument: quantiles must be in [0, 1] (q=%f)", q)
		}
		c.suffixes[q] = fmt.Sprintf("_p%d", int(q*100))
	}
	return nil
}
