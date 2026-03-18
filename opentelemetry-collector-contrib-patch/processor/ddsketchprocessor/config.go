// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its DDSketch output.
// - "batch": per-batch flush (no tumbling window)
// - "window": tumbling window flush over a configurable duration.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

// Config holds processor configuration.
type Config struct {
	// Mode controls when this processor flushes DDSketch output.
	//   - "batch": per-batch aggregation/flush
	//   - "window": accumulate across a tumbling time window before flushing
	Mode InputMode `mapstructure:"mode"`
	// WindowDuration controls how often sketches are flushed when mode = "window".
	// Ignored in "batch" mode.
	WindowDuration time.Duration `mapstructure:"window_duration"`
	// RelativeAccuracy configures the DDSketch relative accuracy.
	RelativeAccuracy float64 `mapstructure:"relative_accuracy"`
	// Quantiles controls which quantiles are exported from the DDSketch.
	Quantiles []float64 `mapstructure:"quantiles"`
	// MetricSuffix is appended to the original metric name for generated sketches.
	MetricSuffix string `mapstructure:"metric_suffix"`
	// TransmitSketch controls whether merged sketches are output as DDSketch payloads (true)
	// or converted into gauge metrics at the configured quantiles (false).
	TransmitSketch bool `mapstructure:"transmit_sketch"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	return &Config{
		Mode:             ModeBatch,
		WindowDuration:   60 * time.Second,
		RelativeAccuracy: 0.01,
		Quantiles:        []float64{0.5, 0.9, 0.99},
		MetricSuffix:     "_ddsketch",
		TransmitSketch:   true,
	}
}

func (cfg *Config) validate() error {
	switch cfg.Mode {
	case "":
		// Preserve backwards compatibility if mode is omitted.
		cfg.Mode = ModeBatch
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", cfg.Mode, ModeBatch, ModeWindow)
	}

	if cfg.Mode == ModeWindow {
		if cfg.WindowDuration <= 0 {
			return fmt.Errorf("window_duration must be > 0 in window mode, got %v", cfg.WindowDuration)
		}
	}

	if cfg.RelativeAccuracy <= 0 || cfg.RelativeAccuracy >= 1 {
		return fmt.Errorf("relative_accuracy must be within (0,1), got %v", cfg.RelativeAccuracy)
	}
	if !cfg.TransmitSketch {
		if len(cfg.Quantiles) == 0 {
			return fmt.Errorf("at least one quantile must be configured")
		}
	}
	for _, q := range cfg.Quantiles {
		if q < 0 || q > 1 {
			return fmt.Errorf("quantiles must be within [0,1], got %v", q)
		}
	}
	return nil
}
