// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfexporter

import (
	"errors"
	"time"
)

// Config defines configuration for the Serf HTTP exporter.
type Config struct {
	// Endpoint is the HTTP URL of the serfreceiver backend, e.g. "http://localhost:9000/serf".
	Endpoint string `mapstructure:"endpoint"`

	// Compression selects the Serf compression algorithm: "xor" (default) or "qt".
	Compression string `mapstructure:"compression"`

	// MaxDiff is the maximum allowed absolute error for lossy compression.
	MaxDiff float64 `mapstructure:"max_diff"`

	// AdjustDigit is the adjust-digit parameter for SerfXOR (0 disables).
	AdjustDigit int64 `mapstructure:"adjust_digit"`

	// WindowInterval is the tumbling-window flush interval (e.g. 10s).
	WindowInterval time.Duration `mapstructure:"window_interval"`

	// MaxObjectBytes caps the size of each compressed SERF1 object (0 = unlimited).
	MaxObjectBytes int64 `mapstructure:"max_object_bytes"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("serfhttp exporter: endpoint must be set")
	}
	if c.Compression == "" {
		c.Compression = "xor"
	}
	if c.Compression != "xor" && c.Compression != "qt" {
		return errors.New("serfhttp exporter: compression must be 'xor' or 'qt'")
	}
	if c.WindowInterval <= 0 {
		c.WindowInterval = 10 * time.Second
	}
	return nil
}
