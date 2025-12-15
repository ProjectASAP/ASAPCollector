package countsketchprocessor

import (
	"fmt"
    "time"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	// Epsilon: The acceptable error rate (e.g., 0.01 for 1% error).
	// Lower epsilon = Larger sketch = More memory.
	Epsilon float64 `mapstructure:"epsilon"`

	// Delta: The probability of failure (e.g., 0.05 for 95% confidence).
	// Lower delta = More hash functions = More CPU.
	Delta float64 `mapstructure:"delta"`

	// WindowSize is the time duration for each sketch window (e.g. "10s", "1m")
	WindowSize time.Duration `mapstructure:"window_size"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.Epsilon <= 0 || c.Epsilon >= 1 {
		return fmt.Errorf("epsilon must be between 0 and 1 (exclusive), got %f", c.Epsilon)
	}

	if c.Delta <= 0 || c.Delta >= 1 {
		return fmt.Errorf("delta must be between 0 and 1 (exclusive), got %f", c.Delta)
	}

	if c.WindowSize <= 0 {
        return fmt.Errorf("window_size must be positive: %s", c.WindowSize)
    }

    // Prevents users from setting minute values like "1ms"
    if c.WindowSize < 1 * time.Second {
        return fmt.Errorf("window_size is too small: %s (minimum is 1s)", c.WindowSize)
    }

	return nil
}