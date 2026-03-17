// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

// Config holds processor configuration.
type Config struct {
	// RelativeAccuracy configures the DDSketch relative accuracy.
	RelativeAccuracy float64 `mapstructure:"relative_accuracy"`
	// Quantiles controls which quantiles are exported from the DDSketch.
	Quantiles []float64 `mapstructure:"quantiles"`
	// MetricSuffix is appended to the original metric name for generated sketches.
	MetricSuffix string `mapstructure:"metric_suffix"`
	// EmitDDSketch controls whether merged sketches are output as DDSketch payloads (true)
	// or converted into gauge metrics at the configured quantiles (false).
	EmitDDSketch bool `mapstructure:"emit_ddsketch"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	return &Config{
		RelativeAccuracy: 0.01,
		Quantiles:        []float64{0.5, 0.9, 0.99},
		MetricSuffix:     "_ddsketch",
		EmitDDSketch:     true,
	}
}

func (cfg *Config) validate() error {
	if cfg.RelativeAccuracy <= 0 || cfg.RelativeAccuracy >= 1 {
		return fmt.Errorf("relative_accuracy must be within (0,1), got %v", cfg.RelativeAccuracy)
	}
	if !cfg.EmitDDSketch {
		if len(cfg.Quantiles) == 0 {
			return fmt.Errorf("at least one quantile must be configured")
		}
		for _, q := range cfg.Quantiles {
			if q < 0 || q > 1 {
				return fmt.Errorf("quantiles must be within [0,1], got %v", q)
			}
		}
	}
	return nil
}
