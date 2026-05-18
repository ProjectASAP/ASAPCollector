// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its DDSketch output.
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

// Config holds processor configuration.
type Config struct {
	// Mode controls when this processor flushes DDSketch output.
	//   - "batch": per-batch aggregation/flush
	//   - "window": accumulate across a tumbling time window before flushing
	Mode InputMode `mapstructure:"mode"`
	// WindowDuration controls how often sketches are flushed when mode = "window".
	// Ignored in "batch" mode.
	WindowDuration time.Duration `mapstructure:"window_duration"`
	// RelativeAccuracy configures the DDSketch relative accuracy.
	RelativeAccuracy float64 `mapstructure:"relative_accuracy"`
	// Quantiles controls which quantiles are exported from the DDSketch.
	Quantiles []float64 `mapstructure:"quantiles"`
	// EnableSelfMonitoring controls whether processor self-monitoring metrics are emitted.
	EnableSelfMonitoring bool `mapstructure:"enable_self_monitoring"`
	// TransmitSketch controls whether merged sketches are output as DDSketch payloads (true)
	// or converted into gauge metrics at the configured quantiles (false).
	TransmitSketch bool `mapstructure:"transmit_sketch"`

	// AggregateBy lists label keys to group by for cross-series (matrix) aggregation.
	// All data points sharing the same values for these labels are merged into one sketch.
	// The output data point carries only these labels.
	// Empty (default) preserves per-series behavior: each distinct attribute set → own sketch.
	AggregateBy []string `mapstructure:"aggregate_by"`

	// LabelMatchers filters which data points to include before aggregation.
	// A data point is included only if ALL matchers are satisfied (exact match).
	// Empty (default) = include all data points.
	LabelMatchers []LabelMatcher `mapstructure:"label_matchers"`

	// DeltaTransmission enables sparse delta encoding: only buckets that
	// changed by at least DeltaThreshold counts since the last snapshot are
	// transmitted. Requires TransmitSketch=true.
	DeltaTransmission bool `mapstructure:"delta_transmission"`

	// DeltaThreshold is the minimum bucket count increase required to include
	// a bucket in the delta payload. Defaults to 1 when DeltaTransmission=true.
	DeltaThreshold uint64 `mapstructure:"delta_threshold"`

	// DropOriginal controls whether the raw input metrics are dropped
	// from the outbound pmetric stream so only the synthesized sketch
	// summaries (or DDSketch envelopes) flow downstream. The MVP demo
	// pipeline relies on DropOriginal=true so the agent→gateway and
	// gateway→backend wire payload is sketch-only — the raw is already
	// archived by the gorillas3processor that runs UPSTREAM in the
	// pipeline. Operators that want the legacy "originals + sketch"
	// shape (e.g. raw_passthrough debugging or downstream consumers
	// that need both) must set drop_original: false explicitly in YAML.
	//
	// Defaults to true (set in createDefaultConfig).
	DropOriginal bool `mapstructure:"drop_original"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	return &Config{
		Mode:                 ModeBatch,
		WindowDuration:       60 * time.Second,
		RelativeAccuracy:     0.01,
		Quantiles:            []float64{0.5, 0.9, 0.99},
		EnableSelfMonitoring: true,
		TransmitSketch:       true,
		// Delta-encoded transmission is the operational default for the
		// MVP demo: at 10 Hz × 60 s = 600 samples/window the per-window
		// wire footprint is dominated by sketch state size, so emitting
		// only the bucket diff since the last flush (instead of the full
		// state) keeps DDSketch comfortably above the bandwidth break-even
		// versus raw scrape. DeltaThreshold defaults to 1 in validate().
		DeltaTransmission: true,
		// DropOriginal=true is the MVP-bandwidth default: the sketch
		// summary (or DDSketch envelope) REPLACES the raw on the
		// outbound pmetric stream. The raw remains available via the
		// gorillas3processor archive write that runs UPSTREAM of this
		// processor in the agent pipeline (see ① bandwidth FAIL fix).
		// Operators that want the legacy "raw passthrough + sketch
		// graft" shape must set drop_original: false explicitly.
		DropOriginal: true,
	}
}

func (cfg *Config) validate() error {
	switch cfg.Mode {
	case "":
		// Preserve backwards compatibility if mode is omitted.
		cfg.Mode = ModeBatch
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", cfg.Mode, ModeBatch, ModeWindow)
	}

	if cfg.Mode == ModeWindow {
		if cfg.WindowDuration <= 0 {
			return fmt.Errorf("window_duration must be > 0 in window mode, got %v", cfg.WindowDuration)
		}
	}

	if cfg.RelativeAccuracy <= 0 || cfg.RelativeAccuracy >= 1 {
		return fmt.Errorf("relative_accuracy must be within (0,1), got %v", cfg.RelativeAccuracy)
	}
	// Sort AggregateBy so seriesKey always produces a consistent ordering.
	sort.Strings(cfg.AggregateBy)

	if cfg.DeltaTransmission && cfg.DeltaThreshold == 0 {
		cfg.DeltaThreshold = 1
	}

	if !cfg.TransmitSketch {
		if len(cfg.Quantiles) == 0 {
			return fmt.Errorf("at least one quantile must be configured")
		}
	}
	for _, q := range cfg.Quantiles {
		if q < 0 || q > 1 {
			return fmt.Errorf("quantiles must be within [0,1], got %v", q)
		}
	}
	return nil
}
