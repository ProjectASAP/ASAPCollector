package hllprocessor

import (
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/component"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// InputMode controls when the processor flushes its HLL output.
// - "batch": per-batch flush (one cardinality estimate per batch)
// - "window": tumbling window flush over a configurable duration.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

// LabelMatcher specifies an exact label key=value filter.
// A data point matches only if the named label exists and its string value equals Value.
type LabelMatcher struct {
	Key   string `mapstructure:"key"`
	Value string `mapstructure:"value"`
}

// Config configures the HLL cardinality-estimation processor.
// The processor ingests Gauge metrics and outputs the estimated cardinality
// of distinct float64 values seen per series using HyperLogLog (precision=14).
type Config struct {
	Mode           InputMode     `mapstructure:"mode"`
	WindowDuration time.Duration `mapstructure:"window_duration"`
	// TransmitSketch embeds the serialized HLL registers in a gauge attribute
	// instead of emitting only the cardinality estimate.
	TransmitSketch       bool `mapstructure:"transmit_sketch"`
	DropOriginal         bool `mapstructure:"drop_original"`
	EnableSelfMonitoring bool `mapstructure:"enable_self_monitoring"`

	// AggregateBy lists label keys to group by for cross-series (matrix) aggregation.
	// All data points sharing the same values for these labels are merged into one sketch.
	// The output data point carries only these labels.
	// Empty (default) preserves per-series behavior: each distinct attribute set → own sketch.
	AggregateBy []string `mapstructure:"aggregate_by"`

	// LabelMatchers filters which data points to include before aggregation.
	// A data point is included only if ALL matchers are satisfied (exact match).
	// Empty (default) = include all data points.
	LabelMatchers []LabelMatcher `mapstructure:"label_matchers"`

	// DeltaTransmission enables sparse delta encoding: only registers that
	// increased since the last snapshot are transmitted (max semantics).
	// Requires TransmitSketch=true; has no effect in batch mode.
	DeltaTransmission bool `mapstructure:"delta_transmission"`

	// Encoding selects the wire format for the `HLLSketchDataPoint.Sketch`
	// bytes. See `SketchEncoding` for supported values. Defaults to "proto".
	Encoding SketchEncoding `mapstructure:"encoding"`

	// SampleP is the per-sketch hash-threshold sampling probability in
	// (0,1]. The control plane sets it per metric from the workload spec.
	// 1.0 (the default — 0/unset is normalised to 1.0 in Validate) disables
	// sampling so the emitted wire bytes are byte-identical to the
	// pre-sampling format. A value <1 keeps each distinct element with
	// probability p; sketchlib-go stamps p on the SketchEnvelope so the
	// backend rescales cardinality by 1/p at query time.
	SampleP float64 `mapstructure:"sample_p"`
}

// SketchEncoding selects the wire format for the serialized HLL bytes
// carried in `HLLSketchDataPoint.Sketch`.
//
//   - "proto" (default) — sketchlib-go `SerializeProtoBytes` →
//     sketchlib `HyperLogLogState` proto. Tag =
//     `HLLSketchEncodingProto` / `HLLSketchEncodingDelta`.
//   - "msgpack"           — sketchlib-go `SerializeMsgpack`. Tag =
//     `HLLSketchEncodingMsgpack`. Delta transmission is proto-only
//     today; `encoding = msgpack` + `delta_transmission = true`
//     still falls back to proto deltas per window.
type SketchEncoding string

const (
	EncodingProto   SketchEncoding = "proto"
	EncodingMsgpack SketchEncoding = "msgpack"
)

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	switch c.Mode {
	case "":
		c.Mode = ModeBatch
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", c.Mode, ModeBatch, ModeWindow)
	}
	if c.Mode == ModeWindow && c.WindowDuration <= 0 {
		return fmt.Errorf("window_duration must be > 0 in window mode, got %v", c.WindowDuration)
	}
	// Sort AggregateBy so seriesKey always produces a consistent ordering.
	sort.Strings(c.AggregateBy)

	switch c.Encoding {
	case "":
		c.Encoding = EncodingProto
	case EncodingProto, EncodingMsgpack:
	default:
		return fmt.Errorf(
			"invalid encoding %q, must be %q or %q",
			c.Encoding, EncodingProto, EncodingMsgpack)
	}

	// SampleP: 0/unset normalises to 1.0 (sampling disabled — the safe
	// default). Reject out-of-range values (negative or >1) rather than
	// silently clamping, so a typo in the wire config surfaces at agent
	// boot instead of producing a mis-scaled sketch.
	if c.SampleP == 0 {
		c.SampleP = 1.0
	}
	if c.SampleP < 0 || c.SampleP > 1.0 {
		return fmt.Errorf("sample_p must be in (0, 1] (got %v)", c.SampleP)
	}

	return nil
}

// toPrecomputeConfig translates the legacy Config to the host-neutral
// PrecomputeConfig the shim hands to precompute.New. The translation
// pins the Phase-2 parity flags the legacy HLL processor's series key
// and emit shape require:
//
//   - OmitResourceAttrs=true: legacy HLL builds its series key from
//     dp-attrs only and emits into a freshly-appended ResourceMetrics
//     with an empty Resource (see processBatch / accumulateGaugeMetric
//     prior to refactor). Without this flag the runtime would key
//     series by (resource, dp-attrs) and surface non-empty
//     ResourceLabels on the envelope, breaking byte-parity.
//   - EmitWindowStats=false: legacy HLL does not stamp sample_count
//     / window_duration_seconds attrs onto its emit. Only CountSketch
//     does (see PrecomputeConfig.EmitWindowStats docs).
//
// MetricName is intentionally left empty here — the legacy HLL emits
// `<input>_hll_cardinality`, where
// `<input>` varies per ingested metric. The shim resolves the final
// output name in its encode path; the runtime's MetricName field is
// a static-per-Precompute value and would not honor the per-input
// naming the legacy emit guarantees.
func (c *Config) toPrecomputeConfig(metricName string) *precompute.PrecomputeConfig {
	matchers := make([]precompute.LabelMatcher, 0, 1)
	if metricName != "" {
		// The shim uses one Precompute per input metric; pin the
		// metric-name matcher so observations from other metrics
		// fed into the same instance are filtered. (Today the shim
		// only routes matching observations here, but the matcher
		// makes the contract explicit and preserves the invariant
		// when the runtime is shared with control-channel-driven
		// callers in 2.10.)
		matchers = append(matchers, precompute.LabelMatcher{Value: metricName})
	}
	mode := precompute.Batch
	winSize := time.Duration(0) // Batch: zero size triggers always-flushable rotate
	if c.Mode == ModeWindow {
		mode = precompute.Tumbling
		winSize = c.WindowDuration
	}
	return &precompute.PrecomputeConfig{
		SketchType:        precompute.SketchTypeHLLSketch,
		Mode:              mode,
		Window:            precompute.WindowSpec{Size: winSize},
		Matchers:          matchers,
		AggregateBy:       append([]string(nil), c.AggregateBy...),
		TransmitSketch:    c.TransmitSketch,
		DeltaTransmission: c.DeltaTransmission,
		Encoding:          precompute.EncodingProtoFull,
		// Cumulative — matches legacy SetAggregationTemporality(Cumulative).
		Temporality:       int32(2),
		OmitResourceAttrs: true,
		GlobalAggregation: false,
		EmitWindowStats:   false,
		// MetricName left empty: the shim's encode path computes
		// `<input>_hll_cardinality` from
		// the per-envelope context; see encodeEnvelopes.
	}
}
