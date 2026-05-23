// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// FamilyKind selects the warm aggregator applied to a metric.
type FamilyKind string

const (
	// FamilySum groups by the metric's AggregateBy labels and sums the
	// (delta) values, emitting a Sum metric — the asap-native replacement
	// for contrib metricstransform's aggregate_labels/sum.
	FamilySum FamilyKind = "sum"
	// FamilyDDSketch / KLL produce quantile sketches.
	FamilyDDSketch FamilyKind = "ddsketch"
	FamilyKLL      FamilyKind = "kll"
	// FamilyHLL produces a cardinality sketch.
	FamilyHLL FamilyKind = "hll"
	// FamilyCountSketch / CountMinSketch produce frequency sketches.
	FamilyCountSketch    FamilyKind = "countsketch"
	FamilyCountMinSketch FamilyKind = "countminsketch"
)

// MetricFamily configures the warm aggregation for one metric name. A
// metric not listed here is cold-archived only (no warm aggregation).
type MetricFamily struct {
	// Metric is the metric name this entry matches (OTLP Metric.Name).
	Metric string `mapstructure:"metric"`
	// Family selects the warm aggregator.
	Family FamilyKind `mapstructure:"family"`
	// AggregateBy is the grouping label set. For Sum this is the
	// collapse key (e.g. [zone] => one summed series per zone). For the
	// sketches, empty means per-series (group by full attribute set).
	AggregateBy []string `mapstructure:"aggregate_by"`

	// RelativeAccuracy is the DDSketch alpha (0,1). Default 0.01.
	RelativeAccuracy float64 `mapstructure:"relative_accuracy"`
	// K is the KLL sketch parameter (>=2). Default 200.
	K int `mapstructure:"k"`
	// Rows / Cols size the CountSketch / CountMinSketch matrix. Defaults
	// 5 x 2048 (mirror the standalone processors' typical dimensions).
	Rows int `mapstructure:"rows"`
	Cols int `mapstructure:"cols"`
	// HLL takes no sizing knob (fixed precision in the wrapper).
}

// ColdConfig configures the per-shard Gorilla cold archive. Each shard
// encodes samples into compact Gorilla-XOR chunk fragments and, on flush,
// SHIPS the batch (shared ASAPFRG1 binary frame, gzipped) to a downstream
// merger/backend ingest endpoint, which builds the TSDB block + index and cuts
// the 1h window block to S3. The edge does no index build / S3 PUT. Disabled
// when Enabled is false (warm-only edge aggregation).
type ColdConfig struct {
	// Enabled turns the per-shard cold archive on. Default true.
	Enabled bool `mapstructure:"enabled"`

	// ShipEndpoint is the merger/backend /ingest URL the per-window XOR-chunk
	// fragment batch is POSTed to (e.g. http://merger:9099/ingest, or the
	// backend gorilla storage-engine ingest once the merger moves
	// server-side). This is the primary delivery path — it keeps the index
	// build + S3 PUTs off the edge. Empty => drain-only (no shipping).
	ShipEndpoint string `mapstructure:"ship_endpoint"`

	// BlockDuration is the per-emit (head) block tumbling window. Default =
	// WindowDuration. The merger/backend cuts these into the larger (e.g.
	// 1h) window block downstream.
	BlockDuration time.Duration `mapstructure:"block_duration"`
	// ReorderGrace is the per-series out-of-order lateness bound.
	ReorderGrace time.Duration `mapstructure:"reorder_grace"`
	// ExternalLabels are stamped onto every cold-archived series.
	ExternalLabels map[string]string `mapstructure:"external_labels"`

	// --- legacy S3-direct fallback (used only when ShipEndpoint is empty) ---
	// Endpoint/TSDBBucket/etc. write blocks straight to S3/MinIO from the
	// edge. Retained for back-compat; the shipping path above is preferred.
	Endpoint        string `mapstructure:"endpoint"`
	TSDBBucket      string `mapstructure:"tsdb_bucket"`
	Tenant          string `mapstructure:"tenant"`
	Region          string `mapstructure:"region"`
	AccessKeyID     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	UseSSL          bool   `mapstructure:"use_ssl"`
}

// Config is the asap_edge processor configuration.
type Config struct {
	// ShardCount is the number of key-hash ingestion shards. Each shard
	// owns its own lock + cold builder + warm aggregators, so concurrent
	// OTLP Export goroutines on distinct series ingest in parallel.
	// Default 4. Must be >= 1.
	ShardCount int `mapstructure:"shard_count"`

	// WindowDuration is the warm-tier (sum/sketch) flush cadence.
	WindowDuration time.Duration `mapstructure:"window_duration"`

	// Metrics maps metric names to their warm aggregation family.
	Metrics []MetricFamily `mapstructure:"metrics"`

	// Cold configures the per-shard cold archive tier.
	Cold ColdConfig `mapstructure:"cold"`

	// DropOriginal drops the raw metric from the outbound stream after
	// cold-archiving + warm-aggregating it (the asap default — the warm
	// envelopes + cold blocks carry the data downstream). Default true.
	DropOriginal bool `mapstructure:"drop_original"`
}

var _ component.Config = (*Config)(nil)

func (k FamilyKind) valid() bool {
	switch k {
	case FamilySum, FamilyDDSketch, FamilyKLL, FamilyHLL, FamilyCountSketch, FamilyCountMinSketch:
		return true
	}
	return false
}

// Validate normalizes defaults and rejects an unusable config.
func (c *Config) Validate() error {
	if c.ShardCount < 0 {
		return fmt.Errorf("asap_edge: shard_count must be >= 0 (0/unset => default)")
	}
	if c.ShardCount == 0 {
		c.ShardCount = 4
	}
	if c.WindowDuration <= 0 {
		c.WindowDuration = 60 * time.Second
	}
	seen := make(map[string]struct{}, len(c.Metrics))
	for i := range c.Metrics {
		m := &c.Metrics[i]
		if m.Metric == "" {
			return fmt.Errorf("asap_edge: metrics[%d].metric must be set", i)
		}
		if _, dup := seen[m.Metric]; dup {
			return fmt.Errorf("asap_edge: duplicate metric %q in metrics", m.Metric)
		}
		seen[m.Metric] = struct{}{}
		if !m.Family.valid() {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): invalid family %q", i, m.Metric, m.Family)
		}
		if m.Family == FamilyDDSketch && m.RelativeAccuracy == 0 {
			m.RelativeAccuracy = 0.01
		}
		if m.RelativeAccuracy < 0 || m.RelativeAccuracy >= 1 {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): relative_accuracy must be in [0,1)", i, m.Metric)
		}
	}
	// Cold defaults (only meaningful when enabled).
	if c.Cold.Enabled {
		// tsdb_bucket is only needed for the legacy S3-direct fallback
		// (Endpoint set, no ShipEndpoint). The ship path + build-only mode
		// don't need a bucket.
		if c.Cold.ShipEndpoint == "" && c.Cold.Endpoint != "" && c.Cold.TSDBBucket == "" {
			return fmt.Errorf("asap_edge: cold.tsdb_bucket must be set for S3-direct cold (endpoint set, no ship_endpoint)")
		}
		if c.Cold.Tenant == "" {
			c.Cold.Tenant = "default"
		}
		if c.Cold.Region == "" {
			c.Cold.Region = "us-east-1"
		}
		if c.Cold.BlockDuration <= 0 {
			c.Cold.BlockDuration = c.WindowDuration
		}
		if c.Cold.ReorderGrace < 0 {
			return fmt.Errorf("asap_edge: cold.reorder_grace must be >= 0")
		}
		if c.Cold.ReorderGrace == 0 {
			c.Cold.ReorderGrace = 2 * time.Second
		}
	}
	return nil
}

// familyFor returns the configured family for a metric name, or ("", false)
// if the metric has no warm aggregation (cold-only).
func (c *Config) familyFor(metric string) (*MetricFamily, bool) {
	for i := range c.Metrics {
		if c.Metrics[i].Metric == metric {
			return &c.Metrics[i], true
		}
	}
	return nil, false
}
