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

// BlockFormat selects the on-disk layout the processor emits to S3.
// Only "prometheus_tsdb" is supported: edge writes Prometheus TSDB block files,
// S3/MinIO stores them, and Thanos store-gateway/query reads them.
type BlockFormat string

const (
	BlockFormatPrometheusTSDB BlockFormat = "prometheus_tsdb"
)

// ProcessorRole decides where this collector runs in the split pipeline.
type ProcessorRole string

const (
	// ProcessorRoleGatewayRaw consumes raw metrics and builds TSDB blocks. Use
	// this after a durable raw transport such as Kafka.
	ProcessorRoleGatewayRaw ProcessorRole = "gateway_raw"
	// ProcessorRoleAgent encodes raw metrics into compact fragment metrics. Use
	// this on low-resource edge collectors for best-effort or durable-fragment
	// delivery.
	ProcessorRoleAgent ProcessorRole = "agent"
	// ProcessorRoleGatewayFragment consumes fragment metrics and finalizes TSDB
	// blocks. Use this after OTel/OTAP/Telegraf best-effort transport or after
	// Kafka carrying fragments.
	ProcessorRoleGatewayFragment ProcessorRole = "gateway_fragment"
)

// DeliveryMode documents the transport reliability envelope around the role.
type DeliveryMode string

const (
	DeliveryModeBestEffort      DeliveryMode = "best_effort"
	DeliveryModeDurableRaw      DeliveryMode = "durable_raw"
	DeliveryModeDurableFragment DeliveryMode = "durable_fragment"
)

// Config is the gorillas3processor configuration. Fields mirror the
// Telegraf-side `gorilla_s3` output plugin where applicable so the two
// can share infra docs.
type Config struct {
	// Role selects the processor responsibility. Default gateway_raw preserves
	// the existing "raw metrics in, TSDB block out" behavior and is also the
	// recommended role after Kafka durable raw transport.
	Role ProcessorRole `mapstructure:"role"`

	// DeliveryMode is explicit operator documentation for the pipeline contract.
	// The processor does not create Kafka topics; durable modes are achieved by
	// placing Kafka before gateway_raw or between agent and gateway_fragment.
	DeliveryMode DeliveryMode `mapstructure:"delivery_mode"`

	// WindowInterval is the tumbling window duration. On each tick the
	// processor drains its current raw builder or fragment finalizer.
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

	// SourceID identifies the edge source in fragment metadata.
	SourceID string `mapstructure:"source_id"`

	// FragmentSamplesPerChunk controls how many samples the edge encoder packs
	// into one XOR fragment before emitting it downstream.
	FragmentSamplesPerChunk int `mapstructure:"fragment_samples_per_chunk"`

	// BlockFormat selects the cold-store on-disk layout. Only
	// "prometheus_tsdb" is supported; empty defaults to that value.
	BlockFormat BlockFormat `mapstructure:"block_format"`

	// TSDBBucket is the destination bucket for Prometheus TSDB blocks. When
	// empty, gateway roles fall back to Bucket.
	TSDBBucket string `mapstructure:"tsdb_bucket"`

	// TSDBBlockDuration is the tumbling window over which samples
	// are batched into a single Prometheus block. Default 60s,
	// aligning with the controller's per-window plan cadence.
	// Prometheus' BlockWriter uses this as the block-size hint
	// (the Head's chunk range); samples whose timestamps fall
	// outside this range still flush, but the block-writer's
	// internal compactor will split / merge accordingly.
	TSDBBlockDuration time.Duration `mapstructure:"tsdb_block_duration"`

	// TSDBExternalLabels are added to every series in the emitted
	// Prometheus block. Typical use: `{cluster: foo, replica: a}`.
	// Step 2.3 is responsible for matching backend-side queries.
	TSDBExternalLabels map[string]string `mapstructure:"tsdb_external_labels"`

	// TSDBReorderGrace is the event-time lateness bound for per-series
	// out-of-order samples. Samples are streamed into XOR chunks once they are
	// older than max_observed_timestamp - tsdb_reorder_grace. Later samples
	// behind already-written data are dropped and counted.
	TSDBReorderGrace time.Duration `mapstructure:"tsdb_reorder_grace"`
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
	switch c.Role {
	case "":
		c.Role = ProcessorRoleGatewayRaw
	case ProcessorRoleGatewayRaw, ProcessorRoleAgent, ProcessorRoleGatewayFragment:
		// ok
	default:
		return fmt.Errorf("gorillas3: invalid role %q (want gateway_raw|agent|gateway_fragment)", c.Role)
	}
	switch c.DeliveryMode {
	case "":
		switch c.Role {
		case ProcessorRoleAgent, ProcessorRoleGatewayFragment:
			c.DeliveryMode = DeliveryModeBestEffort
		default:
			c.DeliveryMode = DeliveryModeDurableRaw
		}
	case DeliveryModeBestEffort, DeliveryModeDurableRaw, DeliveryModeDurableFragment:
		// ok
	default:
		return fmt.Errorf("gorillas3: invalid delivery_mode %q (want best_effort|durable_raw|durable_fragment)", c.DeliveryMode)
	}
	if c.Role != ProcessorRoleAgent && c.Bucket == "" {
		return fmt.Errorf("gorillas3: bucket must be set")
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") {
		return fmt.Errorf("gorillas3: access_key_id and secret_access_key must both be set or both empty")
	}
	switch c.BlockFormat {
	case "":
		c.BlockFormat = BlockFormatPrometheusTSDB
	case BlockFormatPrometheusTSDB:
		// ok
	default:
		return fmt.Errorf("gorillas3: invalid block_format %q (only prometheus_tsdb is supported)", c.BlockFormat)
	}
	if c.TSDBBlockDuration <= 0 {
		c.TSDBBlockDuration = c.WindowInterval
	}
	if c.Role != ProcessorRoleAgent && c.TSDBBucket == "" {
		// Default to Bucket, but warn-by-validate is impractical
		// here; operators get a clean defaults.
		c.TSDBBucket = c.Bucket
	}
	if c.TSDBReorderGrace < 0 {
		return fmt.Errorf("gorillas3: tsdb_reorder_grace must be >= 0")
	}
	if c.TSDBReorderGrace == 0 {
		c.TSDBReorderGrace = 2 * time.Second
	}
	if c.FragmentSamplesPerChunk < 0 {
		return fmt.Errorf("gorillas3: fragment_samples_per_chunk must be >= 0")
	}
	return nil
}

// EmitTSDB reports whether the processor should write a Prometheus
// TSDB block for this flush.
func (c *Config) EmitTSDB() bool {
	return c.BlockFormat == BlockFormatPrometheusTSDB
}
