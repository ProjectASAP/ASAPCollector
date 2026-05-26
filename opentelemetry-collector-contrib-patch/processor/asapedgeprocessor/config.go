// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"fmt"
	"os"
	"path/filepath"
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

// Tier selects which storage tiers a metric flows into.
type Tier string

const (
	// TierWarm builds the warm sketch/aggregation ONLY; the metric's raw
	// samples are NOT added to the cold gorilla fragment stream. Use for
	// high-cardinality sketch-only metrics whose per-series cold archive would
	// balloon the gorilla encoder.
	TierWarm Tier = "warm"
	// TierBoth builds the warm sketch/agg AND the cold gorilla archive. This is
	// the default (today's behavior) when tier is omitted.
	TierBoth Tier = "both"
	// TierCold cold-archives the raw series ONLY; no warm sketch/agg is built.
	TierCold Tier = "cold"
)

// normalized returns the effective tier, mapping the empty/unset value to the
// default TierBoth (today's behavior).
func (t Tier) normalized() Tier {
	if t == "" {
		return TierBoth
	}
	return t
}

func (t Tier) valid() bool {
	switch t {
	case "", TierWarm, TierBoth, TierCold:
		return true
	}
	return false
}

// ColdFormat selects the cold archive wire format the edge ships.
type ColdFormat string

const (
	// ColdFormatFragment ships the gorilla-XOR ASAPFRG1 fragment batch to
	// ShipEndpoint. This is the default (empty maps here) and today's behavior.
	ColdFormatFragment ColdFormat = "fragment"
	// ColdFormatIntchunk ships a lossless intchunk coldpart.Part to
	// ColdPartEndpoint (the merger's /ingest/coldpart). Opt-in.
	ColdFormatIntchunk ColdFormat = "intchunk"
)

// normalized maps the empty/unset value to the default ColdFormatFragment.
func (f ColdFormat) normalized() ColdFormat {
	if f == "" {
		return ColdFormatFragment
	}
	return f
}

func (f ColdFormat) valid() bool {
	switch f {
	case "", ColdFormatFragment, ColdFormatIntchunk:
		return true
	}
	return false
}

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

	// Tier selects which storage tiers this metric flows into: "warm" (build
	// the warm sketch/agg only, skip the cold gorilla archive), "cold"
	// (cold-archive the raw series only, no warm sketch/agg), or "both" (warm
	// AND cold — the default when omitted, today's behavior). Marking a
	// high-cardinality sketch-only metric "warm" keeps the cold tier from
	// archiving it per-series (which balloons the gorilla encoder).
	Tier Tier `mapstructure:"tier"`

	// RelativeAccuracy is the DDSketch alpha (0,1). Default 0.01.
	RelativeAccuracy float64 `mapstructure:"relative_accuracy"`
	// K is the KLL sketch parameter (>=2). Default 200.
	K int `mapstructure:"k"`
	// Rows / Cols size the CountSketch / CountMinSketch matrix. Defaults
	// 5 x 2048 (mirror the standalone processors' typical dimensions).
	Rows int `mapstructure:"rows"`
	Cols int `mapstructure:"cols"`
	// HLL takes no sizing knob (fixed precision in the wrapper).

	// SampleP is the per-metric warm-sketch sampling probability in (0,1].
	// The control plane sets it from a metric's workload spec (only when
	// sample_p < 1). 0/unset is treated as 1.0 — sampling disabled — so the
	// emitted wire bytes are byte-identical to the pre-sampling build. A value
	// <1 thins updates to the sampling-aware families (HLL / CountMinSketch)
	// to ~p; sketchlib-go stamps p on the SketchEnvelope so the backend rescales
	// by 1/p at query time. Families without sampling support (DDSketch / KLL /
	// CountSketch) ignore it. Mirrors the standalone hll/countminsketch
	// processors' sample_p (this is the fused asap_edge equivalent).
	SampleP float64 `mapstructure:"sample_p"`
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

	// Format selects the cold archive wire format. Default (empty or
	// ColdFormatFragment) is the gorilla-XOR ASAPFRG1 fragment batch POSTed to
	// ShipEndpoint — today's behavior, unchanged. ColdFormatIntchunk turns on
	// the parallel intchunk cold-part path: the same drained samples are
	// re-encoded as a lossless intchunk coldpart.Part and POSTed to
	// ColdPartEndpoint (the merger's /ingest/coldpart). This is opt-in; an
	// operator typically wires it from an env var, e.g.
	// `cold.format: ${env:ASAP_COLD_FORMAT}` with ASAP_COLD_FORMAT=intchunk.
	Format ColdFormat `mapstructure:"format"`
	// ColdPartEndpoint is the merger's POST /ingest/coldpart URL the serialized
	// intchunk cold part is shipped to when Format is intchunk (e.g.
	// http://merger:9099/ingest/coldpart). Required when Format is intchunk;
	// ignored for the fragment format.
	ColdPartEndpoint string `mapstructure:"coldpart_endpoint"`

	// BlockDuration is the per-emit (head) block tumbling window. Default =
	// WindowDuration. The merger/backend cuts these into the larger (e.g.
	// 1h) window block downstream.
	BlockDuration time.Duration `mapstructure:"block_duration"`
	// ReorderGrace is the per-series out-of-order lateness bound.
	ReorderGrace time.Duration `mapstructure:"reorder_grace"`
	// ExternalLabels are stamped onto every cold-archived series.
	ExternalLabels map[string]string `mapstructure:"external_labels"`

	// --- durable async ship (decouples flush from the network) ---
	// The flush goroutine hands each drained fragment batch to a background
	// ship worker over a buffered channel, so a slow/failing merger never
	// blocks the next window flush. On ship failure the encoded batch is
	// written to SpoolDir and re-shipped by a periodic retry loop (durable
	// across restarts), bounded by SpoolMaxBytes (oldest dropped when over).

	// SpoolDir is the directory the worker persists failed (gzipped ASAPFRG1)
	// batches to. Empty => default <os.TempDir()>/asap-edge-spool. The spool
	// is only used when ShipEndpoint is set (build-only mode never spools).
	SpoolDir string `mapstructure:"spool_dir"`
	// SpoolMaxBytes bounds the total spool size. When a new batch would exceed
	// it, the oldest spooled batches are dropped (logged as a permanent
	// cold-archive hole) so disk can't grow unbounded. Default 256 MiB.
	// <=0 => unbounded (not recommended).
	SpoolMaxBytes int64 `mapstructure:"spool_max_bytes"`
	// ShipQueueDepth is the buffered ship-channel depth between flushAll and
	// the worker. When full, flushAll spools the batch directly (still
	// non-blocking). Default 64.
	ShipQueueDepth int `mapstructure:"ship_queue_depth"`
	// SpoolRetryInterval is how often the worker re-ships spooled batches.
	// Default 30s.
	SpoolRetryInterval time.Duration `mapstructure:"spool_retry_interval"`

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

// warmEligible reports whether this metric should build its warm sketch/agg
// (tier ∈ {warm, both}).
func (m *MetricFamily) warmEligible() bool {
	t := m.Tier.normalized()
	return t == TierWarm || t == TierBoth
}

// coldEligible reports whether this metric's raw series should be added to the
// cold gorilla fragment stream (tier ∈ {both, cold}).
func (m *MetricFamily) coldEligible() bool {
	t := m.Tier.normalized()
	return t == TierBoth || t == TierCold
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
		if !m.Tier.valid() {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): invalid tier %q (want warm|both|cold or empty)", i, m.Metric, m.Tier)
		}
		if m.Family == FamilyDDSketch && m.RelativeAccuracy == 0 {
			m.RelativeAccuracy = 0.01
		}
		if m.RelativeAccuracy < 0 || m.RelativeAccuracy >= 1 {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): relative_accuracy must be in [0,1)", i, m.Metric)
		}
		// SampleP: 0/unset normalises to 1.0 (sampling disabled — the safe
		// default that keeps wire bytes byte-identical to the pre-sampling
		// build). Reject out-of-range values rather than silently clamping, so
		// a control-plane typo surfaces at agent boot instead of producing a
		// mis-scaled sketch. Mirrors the standalone hll/countminsketch
		// processors' (0,1] check.
		if m.SampleP == 0 {
			m.SampleP = 1.0
		}
		if m.SampleP < 0 || m.SampleP > 1.0 {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): sample_p must be in (0,1] (got %v)", i, m.Metric, m.SampleP)
		}
	}
	// Cold defaults (only meaningful when enabled).
	if c.Cold.Enabled {
		if !c.Cold.Format.valid() {
			return fmt.Errorf("asap_edge: cold.format %q invalid (want fragment|intchunk or empty)", c.Cold.Format)
		}
		// The intchunk cold-part path POSTs a serialized Part to a dedicated
		// /ingest/coldpart endpoint; it cannot reuse the fragment ShipEndpoint
		// (different wire contract), so it must be set explicitly.
		if c.Cold.Format.normalized() == ColdFormatIntchunk && c.Cold.ColdPartEndpoint == "" {
			return fmt.Errorf("asap_edge: cold.coldpart_endpoint must be set when cold.format is intchunk")
		}
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
		// Durable async-ship defaults (only relevant when a ship endpoint is
		// configured; build-only mode never touches the spool).
		if c.Cold.SpoolDir == "" {
			c.Cold.SpoolDir = filepath.Join(os.TempDir(), "asap-edge-spool")
		}
		if c.Cold.SpoolMaxBytes == 0 {
			c.Cold.SpoolMaxBytes = 256 << 20 // 256 MiB
		}
		if c.Cold.SpoolMaxBytes < 0 {
			return fmt.Errorf("asap_edge: cold.spool_max_bytes must be >= 0 (0 => default, <0 invalid)")
		}
		if c.Cold.ShipQueueDepth <= 0 {
			c.Cold.ShipQueueDepth = 64
		}
		if c.Cold.SpoolRetryInterval <= 0 {
			c.Cold.SpoolRetryInterval = 30 * time.Second
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
