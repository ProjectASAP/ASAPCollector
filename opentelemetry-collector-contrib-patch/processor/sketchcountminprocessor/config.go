// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketchcountminprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/sketchmetricsprocessor"

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the sketchmetrics processor.
//
// It is intentionally similar to the Telegraf countmin aggregator:
//
// [[aggregators.countmin]]
//
//	measurement = "countmin"
//	tag_keys    = ["machineid", "tenant"]
//	group_by    = ["scrape_url"]
//	rows        = 3
//	columns     = 4096
//	seed        = 11400714819323198485
//	top_k       = 20
type Config struct {
	// Name of the metric to emit (equivalent to Telegraf Measurement).
	Measurement string `mapstructure:"measurement"`

	// TagKeys explicitly specify which tag keys are used as the "by(...)" dimensions
	// for top-k. If empty, all tags except GroupBy will be used.
	TagKeys []string `mapstructure:"tag_keys"`

	// GroupBy defines tags that partition the population into sub-groups.
	// For each unique combination of group_by tags we maintain a separate set of sketches.
	GroupBy []string `mapstructure:"group_by"`

	// Count-Min sketch parameters.
	Rows    int    `mapstructure:"rows"`
	Columns int    `mapstructure:"columns"`
	Seed    uint64 `mapstructure:"seed"`
	TopK    int    `mapstructure:"top_k"`

	// If true, original input metrics are dropped and only sketch metrics are forwarded.
	DropOriginal bool `mapstructure:"drop_original"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid.
func (c *Config) Validate() error {
	if c.TopK < 0 {
		return fmt.Errorf("sketchmetrics: top_k must be >= 0")
	}
	if c.Rows < 0 || c.Columns < 0 {
		return fmt.Errorf("sketchmetrics: rows/columns must be >= 0")
	}
	return nil
}
