// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its CountMinSketch output.
// - "batch": per-batch summary flush (no background window ticker)
// - "window": tumbling window flush driven by WindowDuration.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

// SketchEncoding selects the wire format for the serialized sketch bytes
// carried in `CountMinSketchDataPoint.Sketch`. The corresponding
// `CountMinSketchDataPoint.Encoding` enum value is written alongside
// so the downstream consumer knows how to decode.
//
//   - "proto" (default) — sketchlib-go `SerializeProtoBytesFO`
//     (sketchlib `CountMinState` proto). Tag = `CountMinSketchEncodingProto`
//     or `CountMinSketchEncodingDelta` depending on DeltaTransmission.
//   - "msgpack"           — sketchlib-go `SerializeMsgpack` (the
//     cross-language wire format consumed by ASAPQuery-backend's
//     `CountMinSketchAccumulator::from_msgpack_bytes`). Tag =
//     `CountMinSketchEncodingMsgpack`. Delta transmission is currently
//     proto-only, so when `encoding = msgpack` and
//     `delta_transmission = true`, the processor still falls back to
//     proto for per-window deltas until sketchlib-go grows a msgpack
//     delta path.
type SketchEncoding string

const (
	EncodingProto   SketchEncoding = "proto"
	EncodingMsgpack SketchEncoding = "msgpack"
)

// LabelMatcher specifies an exact label key=value filter.
// A data point matches only if the named label exists and its string value equals Value.
type LabelMatcher struct {
	Key   string `mapstructure:"key"`
	Value string `mapstructure:"value"`
}

type Config struct {
	// Mode controls when this processor flushes CountMinSketch output.
	//   - "batch": per-batch aggregation/flush
	//   - "window": accumulate across a tumbling time window before flushing
	Mode InputMode `mapstructure:"mode"`

	MetricName string   `mapstructure:"metric_name"`
	GroupBy    []string `mapstructure:"group_by"`

	// CMS Parameters
	Rows    int `mapstructure:"rows"`
	Columns int `mapstructure:"columns"`

	EnableSelfMonitoring bool `mapstructure:"enable_self_monitoring"`
	TransmitSketch       bool `mapstructure:"transmit_sketch"`
	DropOriginal         bool `mapstructure:"drop_original"`

	// Encoding controls the wire format of the sketch bytes written to
	// `CountMinSketchDataPoint.Sketch` when `TransmitSketch = true`.
	// See the [`SketchEncoding`] doc for the supported values. Defaults
	// to "proto" for backwards compatibility.
	Encoding SketchEncoding `mapstructure:"encoding"`

	// WindowDuration is the time window to accumulate data before emitting a sketch (window mode only).
	WindowDuration time.Duration `mapstructure:"window_duration"`

	// AggregateBy lists label keys to group by for cross-series (matrix) aggregation.
	// All data points sharing the same values for these labels are merged into one sketch.
	// The output data point carries only these labels.
	// Empty (default) preserves per-series behavior: each distinct attribute set → own sketch.
	AggregateBy []string `mapstructure:"aggregate_by"`

	// LabelMatchers filters which data points to include before aggregation.
	// A data point is included only if ALL matchers are satisfied (exact match).
	// Empty (default) = include all data points.
	LabelMatchers []LabelMatcher `mapstructure:"label_matchers"`

	// DeltaTransmission enables sparse delta encoding: only cells that changed
	// by at least DeltaThreshold since the last snapshot are transmitted.
	// Requires TransmitSketch=true; has no effect in batch mode.
	DeltaTransmission bool `mapstructure:"delta_transmission"`

	// DeltaThreshold is the minimum absolute cell change required to include a
	// cell in the delta payload. Defaults to 1.0 when DeltaTransmission=true.
	DeltaThreshold float64 `mapstructure:"delta_threshold"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	// Default to batch mode.
	switch c.Mode {
	case "":
		c.Mode = ModeBatch
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", c.Mode, ModeBatch, ModeWindow)
	}

	if c.MetricName == "" {
		return fmt.Errorf("metric_name must be specified")
	}
	if c.Rows <= 0 || c.Columns <= 0 {
		return fmt.Errorf("rows and columns must be positive")
	}

	// WindowDuration is only relevant in window mode.
	if c.Mode == ModeWindow {
		if c.WindowDuration <= 0 {
			// Default to 10s for backwards compatibility.
			c.WindowDuration = 10 * time.Second
		}
		if c.WindowDuration < 1*time.Second {
			return fmt.Errorf("window_duration is too small: %s (minimum is 1s)", c.WindowDuration)
		}
	}

	// Sort AggregateBy so seriesKey always produces a consistent ordering.
	sort.Strings(c.AggregateBy)

	if c.DeltaTransmission {
		if c.DeltaThreshold <= 0 {
			c.DeltaThreshold = 1.0
		}
	}

	// Default Encoding to proto when unset. Accept both supported
	// values; anything else is a config error rather than a silent
	// fallback.
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
