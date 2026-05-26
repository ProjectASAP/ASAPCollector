// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (k FamilyKind) valid() bool {
	switch k {
	case FamilySum, FamilyDDSketch, FamilyKLL, FamilyHLL, FamilyCountSketch, FamilyCountMinSketch:
		return true
	}
	return false
}

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

// Validate normalizes defaults and rejects an unusable config.
func (c *Config) Validate() error {
	if c.ShardCount < 0 {
		return fmt.Errorf("asap_edge: shard_count must be >= 0 (0/unset => default)")
	}
	if c.ShardCount == 0 {
		c.ShardCount = 12
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
