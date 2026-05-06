// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// defaultPrefixTemplate is the canonical S3 key prefix layout used by
// the cold-store reader. Hour-bucketed for inexpensive scan + compaction.
//
// Tokens (case-sensitive):
//
//	{tenant}  — Config.Tenant (defaults to "default")
//	{metric}  — metric name from the OTel pdata Metric.Name
//	{YYYY}    — UTC year, 4 digits
//	{MM}      — UTC month, 2 digits
//	{DD}      — UTC day, 2 digits
//	{HH}      — UTC hour, 2 digits (24h)
const defaultPrefixTemplate = "{tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/"

// Config is the gorillas3processor configuration. Fields mirror the
// Telegraf-side `gorilla_s3` output plugin where applicable so the two
// can share infra docs.
type Config struct {
	// WindowInterval is the tumbling window duration. On each tick the
	// processor encodes the buffered series, PUTs the chunk(s) to S3
	// and updates the hour-bucket index.json. Default: 60s.
	WindowInterval time.Duration `mapstructure:"window_interval"`

	// MaxObjectBytes caps a single chunk's size. When exceeded series
	// are split across multiple objects in the same window. 0 = unlimited.
	MaxObjectBytes int64 `mapstructure:"max_object_bytes"`

	// Tenant is substituted into the prefix template's {tenant} token.
	Tenant string `mapstructure:"tenant"`

	// Endpoint is the S3 endpoint URL. Leave empty to use AWS-default
	// resolution (us-east-1 etc). Set to e.g. http://minio:9000 for MinIO.
	Endpoint string `mapstructure:"endpoint"`

	// Bucket is the destination bucket. Must exist; the processor does
	// not create buckets.
	Bucket string `mapstructure:"bucket"`

	// PrefixTemplate is the S3 key prefix template — see defaultPrefixTemplate.
	PrefixTemplate string `mapstructure:"prefix_template"`

	// AccessKeyID / SecretAccessKey override the AWS-default credential
	// chain. Both must be set together (or neither).
	AccessKeyID     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`

	// UseSSL toggles https vs http on the endpoint. Default false (MinIO local).
	UseSSL bool `mapstructure:"use_ssl"`

	// Region is the AWS region; required by the SDK even for MinIO. Default us-east-1.
	Region string `mapstructure:"region"`

	// DropOriginal controls whether incoming metrics are forwarded to the
	// next consumer. Default true — the agent doesn't OTLP-forward the
	// metric further when it has been written to cold-store.
	DropOriginal bool `mapstructure:"drop_original"`

	// Retry / timeout knobs for PutObject.
	MaxRetries    int           `mapstructure:"max_retries"`
	RetryBackoff  time.Duration `mapstructure:"retry_backoff"`
	UploadTimeout time.Duration `mapstructure:"upload_timeout"`

	// LocalSpoolDir, when set, is a fallback directory the processor writes
	// chunks to when S3 PutObject fails. Empty => no spool, surface the error.
	// Phase 2 keeps this empty by default; operators opt in as needed.
	LocalSpoolDir string `mapstructure:"local_spool_dir"`
}

var _ component.Config = (*Config)(nil)

// Validate normalizes the config and returns an error if it is unusable.
func (c *Config) Validate() error {
	if c.WindowInterval <= 0 {
		c.WindowInterval = 60 * time.Second
	}
	if c.PrefixTemplate == "" {
		c.PrefixTemplate = defaultPrefixTemplate
	}
	if c.Tenant == "" {
		c.Tenant = "default"
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = time.Second
	}
	if c.UploadTimeout <= 0 {
		c.UploadTimeout = 30 * time.Second
	}
	if c.Bucket == "" {
		return fmt.Errorf("gorillas3: bucket must be set")
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") {
		return fmt.Errorf("gorillas3: access_key_id and secret_access_key must both be set or both empty")
	}
	return nil
}
