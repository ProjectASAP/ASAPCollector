// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfreceiver

import "errors"

// Config defines configuration for the Serf HTTP receiver.
type Config struct {
	// Endpoint is the address the HTTP server listens on, e.g. "0.0.0.0:9000".
	Endpoint string `mapstructure:"endpoint"`

	// Compression selects the Serf decompression algorithm: "xor" (default) or "qt".
	// Must match what the serfhttp exporter uses.
	Compression string `mapstructure:"compression"`

	// MaxDiff is the max_diff parameter used during compression (needed for Qt decode).
	MaxDiff float64 `mapstructure:"max_diff"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("serfhttp receiver: endpoint must be set")
	}
	if c.Compression == "" {
		c.Compression = "xor"
	}
	if c.Compression != "xor" && c.Compression != "qt" {
		return errors.New("serfhttp receiver: compression must be 'xor' or 'qt'")
	}
	return nil
}
