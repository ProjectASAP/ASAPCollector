package countsketchprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its CountSketch output.
// - "batch": per-batch summary flush (no background window ticker)
// - "window": tumbling window flush driven by WindowSize.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

type Config struct {
	// Mode controls when this processor flushes CountSketch output.
	//   - "batch": per-batch aggregation/flush
	//   - "window": accumulate across a tumbling time window before flushing
	Mode InputMode `mapstructure:"mode"`

	// Epsilon: The acceptable error rate (e.g., 0.01 for 1% error).
	// Lower epsilon = Larger sketch = More memory.
	Epsilon float64 `mapstructure:"epsilon"`

	// Delta: The probability of failure (e.g., 0.05 for 95% confidence).
	// Lower delta = More hash functions = More CPU.
	Delta float64 `mapstructure:"delta"`

	// WindowSize is the time duration for each sketch window (e.g. "10s", "1m").
	// Used only when Mode = "window".
	WindowSize time.Duration `mapstructure:"window_size"`

	// TransmitSketch reserves the shared sketch-output toggle used by the other
	// sketch processors. CountSketch currently continues to emit metric-form
	// summaries in both modes because the underlying library does not expose a
	// serializable OTLP payload in this code path.
	TransmitSketch bool `mapstructure:"transmit_sketch"`

	// DropOriginal controls whether to drop original metrics and only emit sketches.
	// When true, original metrics are not forwarded, only sketch outputs are emitted.
	DropOriginal bool `mapstructure:"drop_original"`
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

	if c.Epsilon <= 0 || c.Epsilon >= 1 {
		return fmt.Errorf("epsilon must be between 0 and 1 (exclusive), got %f", c.Epsilon)
	}

	if c.Delta <= 0 || c.Delta >= 1 {
		return fmt.Errorf("delta must be between 0 and 1 (exclusive), got %f", c.Delta)
	}

	// WindowSize validation applies only in window mode.
	if c.Mode == ModeWindow {
		if c.WindowSize <= 0 {
			return fmt.Errorf("window_size must be positive: %s", c.WindowSize)
		}
		// Prevents users from setting minute values like "1ms".
		if c.WindowSize < 1*time.Second {
			return fmt.Errorf("window_size is too small: %s (minimum is 1s)", c.WindowSize)
		}
	}

	return nil
}
