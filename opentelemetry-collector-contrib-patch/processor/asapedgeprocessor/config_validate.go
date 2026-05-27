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
	// MaxSeries: default the global cap so the sketch/sum maps are bounded
	// out-of-the-box. 0 (unset after the default) only if the operator
	// explicitly set a negative value, which we reject — there's no
	// "unset vs explicit 0" distinction for an int, so the default is applied
	// only when exactly 0, and an operator wanting unlimited sets a huge value.
	if c.MaxSeries < 0 {
		return fmt.Errorf("asap_edge: max_series must be >= 0 (0 => default 100000)")
	}
	if c.MaxSeries == 0 {
		c.MaxSeries = 100000
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
		// max_series: 0 inherits the top-level default; negative is invalid.
		if m.MaxSeries < 0 {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): max_series must be >= 0 (0 => inherit global default)", i, m.Metric)
		}
		if m.MaxSeries == 0 {
			m.MaxSeries = c.MaxSeries
		}
		// emit_heap: only the CountSketch family has a heap-bearing wire form
		// (the backend promotes it to CountSketchWithHeap / FrequencyTopk).
		// Reject it on any other family rather than silently ignoring so a
		// misconfiguration surfaces at agent boot. emit_heap implies the
		// msgpack heap encoding (set in the warm factory); default the heap
		// size to 100 (sketchlib-go's TOPK_SIZE) when unset.
		if m.EmitHeap {
			if m.Family != FamilyCountSketch {
				return fmt.Errorf("asap_edge: metrics[%d] (%s): emit_heap is only valid for family=countsketch (got %q)", i, m.Metric, m.Family)
			}
			if m.HeapSize <= 0 {
				m.HeapSize = 100
			}
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
	// Control-plane poll loop (only validated when enabled; disabled is the
	// default and leaves existing deployments untouched).
	if c.ControlChannel.enabled() {
		if c.ControlChannel.PollURL == "" {
			return fmt.Errorf("asap_edge: control_channel.poll_url must be set when control_channel is enabled")
		}
		if c.ControlChannel.PollInterval <= 0 {
			c.ControlChannel.PollInterval = 30 * time.Second
		}
		if c.ControlChannel.Timeout <= 0 {
			c.ControlChannel.Timeout = 10 * time.Second
		}
	}
	return nil
}

// deltaCapable reports whether the family supports delta transmission. KLL is
// the only wired sketch family that does not (no ComputeDeltaAgainst).
func (k FamilyKind) deltaCapable() bool {
	switch k {
	case FamilyDDSketch, FamilyCountSketch, FamilyHLL, FamilyCountMinSketch:
		return true
	}
	return false
}

// effectiveDelta resolves the per-metric delta-transmission setting: the
// explicit metrics[].delta_transmission if set, else the top-level default. It
// is forced off for families that cannot do delta (KLL, Sum).
func (m *MetricFamily) effectiveDelta(globalDefault bool) bool {
	if !m.Family.deltaCapable() {
		return false
	}
	if m.DeltaTransmission != nil {
		return *m.DeltaTransmission
	}
	return globalDefault
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
