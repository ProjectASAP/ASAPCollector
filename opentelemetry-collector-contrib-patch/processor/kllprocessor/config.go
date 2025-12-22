package kllprocessor

import (
	"fmt"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	K int `mapstructure:"k"`
	Quantiles []float64 `mapstructure:"quantiles"`
	WriteSeen bool `mapstructure:"write_seen"`
	DropOriginal bool `mapstructure:"drop_original"`
	ReadAsInt bool `mapstructure:"is_int"` // gauge has separate int and double fields, we default to double

	suffixes map[float64]string // suffix to attach to output quantiles, e.g.. _p50, _p99, ...
}

var _ component.Config = (*Config)(nil)
func (c *Config) Validate() error {
	if c.K < 2 { return fmt.Errorf("Invalid Argument. k must be >= 2 (k=%d)", c.K); }

	c.suffixes = make(map[float64]string);
	for _, q := range c.Quantiles {
		if q < 0 || q > 1 { return fmt.Errorf("Invalid Argument. Quantiles must be in [0, 1] (q=%f)", q); }
		c.suffixes[q] = fmt.Sprintf("_p%d", int(q * 100));
	}

	return nil
}