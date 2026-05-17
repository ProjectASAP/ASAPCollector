package kllprocessor

import (
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/component"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// InputMode controls when the processor flushes its KLL output.
// - "batch": per-batch flush (no tumbling window)
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

type Config struct {
	Mode                 InputMode     `mapstructure:"mode"`
	WindowDuration       time.Duration `mapstructure:"window_duration"`
	K                    int           `mapstructure:"k"`
	Quantiles            []float64     `mapstructure:"quantiles"`
	TransmitSketch       bool          `mapstructure:"transmit_sketch"`
	WriteSeen            bool          `mapstructure:"write_seen"`
	DropOriginal         bool          `mapstructure:"drop_original"`
	ReadAsInt            bool          `mapstructure:"is_int"` // gauge has separate int and double fields, we default to double
	EnableSelfMonitoring bool          `mapstructure:"enable_self_monitoring"`

	// AggregateBy lists label keys to group by for cross-series (matrix) aggregation.
	// All data points sharing the same values for these labels are merged into one sketch.
	// The output data point carries only these labels.
	// Empty (default) preserves per-series behavior: each distinct attribute set → own sketch.
	AggregateBy []string `mapstructure:"aggregate_by"`

	// LabelMatchers filters which data points to include before aggregation.
	// A data point is included only if ALL matchers are satisfied (exact match).
	// Empty (default) = include all data points.
	LabelMatchers []LabelMatcher `mapstructure:"label_matchers"`

	// DeltaTransmission must not be set to true for KLL: KLL uses random
	// compaction and is not additively mergeable. Validate() returns an error
	// if this is set.
	DeltaTransmission bool `mapstructure:"delta_transmission"`

	// Seed is the optional explicit RNG seed for KLL's compaction coin.
	// When nil (default), the processor uses sketchlib-go's time-seeded
	// constructor — the production behavior. When set to a non-nil value,
	// the processor instead uses NewKLLSketchWithSeed so two processors
	// fed the same input produce byte-identical sketch state. This is
	// only set in test contexts (parity harness, deterministic-replay
	// fixtures); production deployments leave it nil.
	Seed *int64 `mapstructure:"seed"`

	suffixes map[float64]string // suffix to attach to output quantiles, e.g. _p50, _p99, ...
}

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
	if c.K < 2 {
		return fmt.Errorf("invalid argument: k must be >= 2 (k=%d)", c.K)
	}
	if !c.TransmitSketch && len(c.Quantiles) == 0 {
		return fmt.Errorf("at least one quantile must be configured when transmit_sketch=false")
	}
	c.suffixes = make(map[float64]string)
	for _, q := range c.Quantiles {
		if q < 0 || q > 1 {
			return fmt.Errorf("invalid argument: quantiles must be in [0, 1] (q=%f)", q)
		}
		c.suffixes[q] = fmt.Sprintf("_p%d", int(q*100))
	}
	// Sort AggregateBy so seriesKey always produces a consistent ordering.
	sort.Strings(c.AggregateBy)

	// KLL uses random compaction so sketches are not linearly mergeable.
	// Delta transmission (sparse additive encoding) is not defined for KLL.
	if c.DeltaTransmission {
		return fmt.Errorf("delta_transmission is not supported for KLL sketches: KLL uses random compaction and is not additively mergeable")
	}
	return nil
}

// toPrecomputeConfig translates the legacy Config to the host-neutral
// PrecomputeConfig the shim hands to precompute.New. The translation
// pins the Phase-2 parity flags the legacy KLL processor's series key
// and emit shape require:
//
//   - OmitResourceAttrs=true: legacy KLL builds its series key from
//     dp-attrs only and emits into a freshly-appended ResourceMetrics
//     with an empty Resource (see processBatch / accumulateIntoWindow
//     prior to refactor). Without this flag the runtime would key
//     series by (resource, dp-attrs) and surface non-empty
//     ResourceLabels on the envelope, breaking byte-parity.
//   - EmitWindowStats=false: legacy KLL does not stamp sample_count
//     / window_duration_seconds attrs onto its emit. Only CountSketch
//     does (see PrecomputeConfig.EmitWindowStats docs).
//
// MetricName is intentionally left empty here — the legacy KLL emits
// `<input>_kll` (),
// where `<input>` varies per ingested metric. The shim resolves the
// final output name in its encode path; the runtime's MetricName
// field is a static-per-Precompute value and would not honor the
// per-input naming the legacy emit guarantees.
func (c *Config) toPrecomputeConfig(metricName string) *precompute.PrecomputeConfig {
	matchers := make([]precompute.LabelMatcher, 0, len(c.LabelMatchers)+1)
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
	for _, m := range c.LabelMatchers {
		matchers = append(matchers, precompute.LabelMatcher{
			Name:  m.Key,
			Value: m.Value,
		})
	}
	mode := precompute.Batch
	winSize := time.Duration(0) // Batch: zero size triggers always-flushable rotate
	if c.Mode == ModeWindow {
		mode = precompute.Tumbling
		winSize = c.WindowDuration
	}
	return &precompute.PrecomputeConfig{
		SketchType:        precompute.SketchTypeKLLSketch,
		Mode:              mode,
		Window:            precompute.WindowSpec{Size: winSize},
		Matchers:          matchers,
		AggregateBy:       append([]string(nil), c.AggregateBy...),
		TransmitSketch:    c.TransmitSketch,
		DeltaTransmission: false, // KLL.Validate rejects DeltaTransmission=true
		Encoding:          precompute.EncodingProtoFull,
		Temporality:       1, // delta — matches legacy SetAggregationTemporality(Delta)
		OmitResourceAttrs: true,
		GlobalAggregation: false,
		EmitWindowStats:   false,
		// MetricName left empty: the shim's encode path computes
		// `<input>_kll` from
		// the per-envelope context; see encodeEnvelopes.
	}
}
