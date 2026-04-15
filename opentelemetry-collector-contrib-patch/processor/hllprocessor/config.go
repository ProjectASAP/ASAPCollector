package hllprocessor

import (
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/component"
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
	TransmitSketch       bool   `mapstructure:"transmit_sketch"`
	DropOriginal         bool   `mapstructure:"drop_original"`
	MetricSuffix         string `mapstructure:"metric_suffix"`
	EnableSelfMonitoring bool   `mapstructure:"enable_self_monitoring"`

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

	return nil
}
