// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// S3Config holds the Amazon S3 upload settings.
type S3Config struct {
	Bucket      string `mapstructure:"bucket"`
	Region      string `mapstructure:"region"`
	Prefix      string `mapstructure:"prefix"`
	SSEKMSKeyID string `mapstructure:"sse_kms_key_id"`
	ObjectName  string `mapstructure:"object_name"`

	MultipartThreshold int64         `mapstructure:"multipart_threshold"`
	MultipartPartBytes int64         `mapstructure:"multipart_part_bytes"`
	MaxRetries         int           `mapstructure:"max_retries"`
	RetryBackoff       time.Duration `mapstructure:"retry_backoff"`
	UploadTimeout      time.Duration `mapstructure:"upload_timeout"`
}

// Config holds the serfprocessor configuration.
type Config struct {
	// WindowInterval is the tumbling window duration for accumulating data points
	// before compressing and flushing.
	WindowInterval time.Duration `mapstructure:"window_interval"`

	// MaxObjectBytes caps the size of a single SERF1 binary object.
	// When exceeded, series are split across multiple objects. 0 = unlimited.
	MaxObjectBytes int64 `mapstructure:"max_object_bytes"`

	// DropOriginal controls whether incoming metrics are forwarded downstream.
	DropOriginal bool `mapstructure:"drop_original"`

	// Compression selects the Serf encoding algorithm:
	//   "xor" (default) — SerfXOR: FindAppLong + Gorilla-style XOR bit encoding.
	//   "qt"            — SerfQt: quantization delta + ZigZag/Elias-Gamma coding.
	Compression string `mapstructure:"compression"`

	// MaxDiff is the maximum allowed absolute error (used by both xor and qt modes).
	// For xor: widens the approximation search window.
	// For qt: sets the quantization step size (step = 2·maxDiff).
	// A value of 0.0 in xor mode disables approximation (lossless, equivalent to Gorilla).
	MaxDiff float64 `mapstructure:"max_diff"`

	// AdjustDigit is an integer offset added to each float64 value before XOR
	// encoding to align decimal digits and improve compression. 0 = no adjustment.
	// Only used in xor mode.
	AdjustDigit int64 `mapstructure:"adjust_digit"`

	// S3 holds S3 upload configuration.
	S3 S3Config `mapstructure:"s3"`

	// LocalDir, when set, writes compressed blocks to the local filesystem.
	// Can be used alone or together with S3.
	LocalDir string `mapstructure:"local_dir"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.WindowInterval <= 0 {
		c.WindowInterval = 10 * time.Minute
	}
	if c.Compression == "" {
		c.Compression = "xor"
	}
	if c.Compression != "xor" && c.Compression != "qt" {
		return fmt.Errorf("compression must be \"xor\" or \"qt\", got %q", c.Compression)
	}
	if c.S3.Bucket == "" && c.LocalDir == "" {
		return fmt.Errorf("at least one of s3.bucket or local_dir must be configured")
	}
	if c.S3.Bucket != "" && c.S3.Region == "" {
		return fmt.Errorf("s3.region is required when s3.bucket is set")
	}
	if c.S3.MultipartThreshold <= 0 {
		c.S3.MultipartThreshold = 8 * 1024 * 1024
	}
	if c.S3.MultipartPartBytes <= 0 {
		c.S3.MultipartPartBytes = 8 * 1024 * 1024
	}
	if c.S3.MultipartPartBytes < 5*1024*1024 {
		c.S3.MultipartPartBytes = 5 * 1024 * 1024
	}
	if c.S3.MaxRetries <= 0 {
		c.S3.MaxRetries = 3
	}
	if c.S3.RetryBackoff <= 0 {
		c.S3.RetryBackoff = time.Second
	}
	if c.S3.UploadTimeout <= 0 {
		c.S3.UploadTimeout = 30 * time.Second
	}
	return nil
}
