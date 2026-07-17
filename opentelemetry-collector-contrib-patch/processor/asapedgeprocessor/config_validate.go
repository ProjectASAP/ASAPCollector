// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// countSketchMaxRowHashBits mirrors sketchlib-go's 64-bit single-item hash
// that CountSketch bit-slices across rows (sketches.maxRowHashBits, which is
// unexported). Each row consumes ceil(log2(cols)) bits; once
// rows*ceil(log2(cols)) > 64 the high rows read shifted-out bits and the
// wrapper constructor returns an error. Kept in sync with the sketches pkg.
const countSketchMaxRowHashBits = 64

// validateCountSketchDims rejects CountSketch dimensions that the wrapper
// constructor would reject (P0-1), using the same effective dims the warm
// factory's csmDims resolves: rows default 5, cols default 2048. cols must be
// a power of two (sketchlib bit-slices the hash with a pow2 column mask), and
// rows*log2(cols) must fit the 64-bit row-hash budget. CountSketch rejects
// rather than clamps, so we surface the misconfiguration at boot.
func validateCountSketchDims(i int, m *MetricFamily) error {
	rows, cols := m.Rows, m.Cols
	if rows < 1 {
		rows = 5
	}
	if cols < 2 {
		cols = 2048
	}
	if cols&(cols-1) != 0 {
		return fmt.Errorf("asap_edge: metrics[%d] (%s): countsketch cols=%d must be a power of two", i, m.Metric, cols)
	}
	bitsPerRow := bits.TrailingZeros(uint(cols)) // == log2(cols) for pow2 cols
	if bitsPerRow > 0 && rows*bitsPerRow > countSketchMaxRowHashBits {
		return fmt.Errorf(
			"asap_edge: metrics[%d] (%s): countsketch rows*ceil(log2(cols))=%d exceeds the %d-bit row-hash budget; reduce rows (%d) or cols (%d)",
			i, m.Metric, rows*bitsPerRow, countSketchMaxRowHashBits, rows, cols)
	}
	return nil
}

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
	// Sub-window producer: 0 disables; positive must be shorter than the window.
	if c.SubWindowInterval < 0 {
		return fmt.Errorf("asap_edge: sub_window_interval must be >= 0 (0/unset => disabled)")
	}
	if c.SubWindowInterval > 0 && c.SubWindowInterval >= c.WindowDuration {
		return fmt.Errorf("asap_edge: sub_window_interval (%s) must be < window_duration (%s)", c.SubWindowInterval, c.WindowDuration)
	}
	if c.SubWindowEpsilon < 0 || c.SubWindowEpsilon >= 1 {
		return fmt.Errorf("asap_edge: sub_window_epsilon must be in [0, 1) (0 => fixed mode, emit every tick)")
	}
	// WarmAllowedLateness: the warm tier's own late-data grace, decoupled
	// from cold.reorder_grace (P1-1). Default to the full WindowDuration so
	// any sample that actually falls within the active window is admitted
	// (a small grace borrowed from the cold tier would drop most
	// processing-delayed-but-in-window samples). Negative is invalid.
	if c.WarmAllowedLateness < 0 {
		return fmt.Errorf("asap_edge: warm_allowed_lateness must be >= 0 (0 => default = window_duration)")
	}
	if c.WarmAllowedLateness == 0 {
		c.WarmAllowedLateness = c.WindowDuration
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
			// weight_mode selects how the agent-built top-k heap is ranked
			// (value-sum vs occurrence count). Normalise the accepted aliases
			// to the canonical form here so the warm factory parse is a simple
			// switch, and reject an unknown value at boot rather than silently
			// defaulting (which would mask a config typo as value-weighted).
			switch strings.ToLower(strings.TrimSpace(m.WeightMode)) {
			case "", "value", "sum":
				m.WeightMode = "value"
			case "count", "frequency", "freq":
				m.WeightMode = "count"
			default:
				return fmt.Errorf("asap_edge: metrics[%d] (%s): unknown weight_mode %q (want value|sum or count|frequency|freq)", i, m.Metric, m.WeightMode)
			}
		} else if m.WeightMode != "" {
			// weight_mode only affects the heap-bearing CountSketch ranking;
			// reject it elsewhere so a misplaced knob surfaces at boot.
			return fmt.Errorf("asap_edge: metrics[%d] (%s): weight_mode is only valid for family=countsketch with emit_heap (got family=%q, emit_heap=%v)", i, m.Metric, m.Family, m.EmitHeap)
		}
		// gos_delta_epsilon: the insert-time GOS gate is implemented for
		// several families today, with the SAME config field carrying a
		// DIFFERENT per-family meaning for HLL (so no second knob is needed):
		//   - CountSketch (plain, non-heap), CountMinSketch, DDSketch, Sum,
		//     and KLL: an ε relative-error / count-fraction budget, (0,1).
		//     (F2 isotropic gate for CountSketch; L1 max-composition for
		//     CountMinSketch; L1 value-range gate, T=ε·N/(k·B), derivations
		//     §8.4, for DDSketch; the B=1 degenerate case, T=ε·N/k, for Sum;
		//     count-based gate, R≥ε·N, derivations §8.6, for KLL.) The
		//     emit_heap DELTA-HEAP wire form of CountSketch isn't
		//     GOS-converted, so it is rejected. None of CMS/DDSketch/Sum/KLL
		//     have a heap variant, so no emit_heap exclusion is needed for
		//     them.
		//   - HLL (dense only): REINTERPRETED as τ, a count of "doublings"
		//     (>=~1, NOT a fraction — so the (0,1) upper-bound check does NOT
		//     apply to it). The SPARSE base has no per-register GOS path, so
		//     gos_delta_epsilon + hll_sparse is rejected.
		// Reject it on every other family rather than silently ignoring so a
		// misconfiguration surfaces at agent boot.
		if m.GosDeltaEpsilon > 0 {
			switch {
			case m.Family == FamilyCountMinSketch || m.Family == FamilyDDSketch ||
				m.Family == FamilySum || m.Family == FamilyKLL ||
				(m.Family == FamilyCountSketch && !m.EmitHeap):
				if m.GosDeltaEpsilon >= 1 {
					return fmt.Errorf("asap_edge: metrics[%d] (%s): gos_delta_epsilon (ε) must be in (0, 1) for family=%q, got %v", i, m.Metric, m.Family, m.GosDeltaEpsilon)
				}
			case m.Family == FamilyHLL:
				if m.HLLSparse {
					return fmt.Errorf("asap_edge: metrics[%d] (%s): gos_delta_epsilon (τ) is not supported with hll_sparse=true (the sparse base has no per-register GOS path); use the dense HLL base", i, m.Metric)
				}
				// τ is a doublings count with no (0,1) upper bound; only >0 is
				// required (already guaranteed by the enclosing check).
			default:
				return fmt.Errorf("asap_edge: metrics[%d] (%s): gos_delta_epsilon is only valid for family=countsketch (emit_heap=false), countminsketch, ddsketch, sum, kll, or hll (got family=%q, emit_heap=%v)", i, m.Metric, m.Family, m.EmitHeap)
			}
		}
		// cms_point + GOS: CMS's local point-query readout (threshold.functional
		// = cms_point) is a min-across-rows estimate — poisoned by ANY single
		// row that was reset in place, which is exactly what a GOS-active CMS
		// cell does at insert time (design-gos-unified-edge-telemetry.md §11).
		// Reject the combination at boot rather than silently serving a
		// corrupted read. Plain CountSketch is UNAFFECTED and keeps using this
		// same functional: its EstimateCount is a median across signed rows,
		// which tolerates a single reset row fine, so it is not gated here.
		if m.Family == FamilyCountMinSketch && m.GosDeltaEpsilon > 0 &&
			m.Threshold != nil && m.Threshold.Enabled && m.Threshold.Functional == "cms_point" {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): threshold.functional=cms_point is not valid for family=countminsketch once gos_delta_epsilon>0 (GOS resets cells in place, poisoning the min-based local read); remove gos_delta_epsilon or the cms_point monitor", i, m.Metric)
		}
		// hll_sparse: only the HLL family has a sparse in-memory base. Reject
		// it on any other family rather than silently ignoring so a
		// misconfiguration surfaces at agent boot (mirrors the emit_heap family
		// check above). It is a pure in-memory footprint choice; the serialized
		// output is byte-identical to dense, so default false stays
		// wire-unchanged.
		if m.HLLSparse && m.Family != FamilyHLL {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): hll_sparse is only valid for family=hll (got %q)", i, m.Metric, m.Family)
		}
		// CountSketch row-hash budget (P0-1): sketchlib bit-slices a single
		// 64-bit per-item hash as rows*ceil(log2(cols)); once that exceeds 64
		// bits the high rows read shifted-out (zero) bits and silently
		// collapse onto column 0, AND the wrapper constructor
		// (NewCountSketch*Wrapper) returns (nil, err). The warm factory used
		// to discard that error, leaving a nil-backed wrapper that panics on
		// the first sample. CountSketch rejects (unlike CMS, which clamps), so
		// reject the misconfiguration at boot with a clear error instead.
		// Uses the same effective dimensions newSketchAggregator does
		// (csmDims defaults: 5 rows x 2048 cols).
		if m.Family == FamilyCountSketch {
			if err := validateCountSketchDims(i, m); err != nil {
				return err
			}
		}
		// mode: the per-series vs whole-stream aggregation scope (plumbed into
		// precompute.PrecomputeConfig.Scope). Empty ⇒ per_series (default).
		// Reject an unknown value at boot rather than silently defaulting.
		if _, ok := precompute.ParseAggMode(m.Mode); !ok {
			return fmt.Errorf("asap_edge: metrics[%d] (%s): mode must be per_series or whole_stream (got %q)", i, m.Metric, m.Mode)
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
	case FamilyDDSketch, FamilyCountSketch, FamilyHLL, FamilyCountMinSketch, FamilySum, FamilyKLL:
		// All families now ride the sub-window machinery (gated on this flag):
		// the subtractive families (DDSketch/CMS/CountSketch/HLL) and Sum ({Δsum,
		// Δcount}+PWR) ship incremental deltas; KLL — which cannot subtract —
		// uses the disjoint-SEGMENT model (the runtime resets the sketch after
		// each sub-window emit, so each frame covers only the between-emits data
		// and the backend merges the segments). Either way the backend's
		// merge_all reconstructs the window total without inflation.
		return true
	}
	return false
}

// scope resolves the metric's aggregation scope (precompute.AggMode), applying
// the per_series default for an empty Mode. Validate() checks Mode first, so the
// parse always succeeds here; an unexpected bad value falls back to PerSeries.
func (m *MetricFamily) scope() precompute.AggMode {
	s, _ := precompute.ParseAggMode(m.Mode)
	return s
}

// effectiveDelta resolves the per-metric delta-transmission setting: the
// explicit metrics[].delta_transmission if set, else the top-level default. It
// is forced off for families that cannot do delta (KLL).
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
