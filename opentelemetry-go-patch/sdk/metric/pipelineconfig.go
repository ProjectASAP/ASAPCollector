// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric // import "go.opentelemetry.io/otel/sdk/metric"

import (
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v2"
)

// PipelineConfig is the top-level SDK pipeline configuration loaded from a
// YAML file. It covers the full SDK→collector path: exporter destination,
// reader interval, sketch type and transmit mode, instrument definitions, an
// ASAPQuery controller block, and load-generation parameters.
type PipelineConfig struct {
	Exporter    PipelineExporterConfig  `yaml:"exporter"`
	Reader      PipelineReaderConfig    `yaml:"reader"`
	Sketch      PipelineSketchConfig    `yaml:"sketch"`
	Instruments []PipelineInstrument    `yaml:"instruments"`
	ASAPQuery   *PipelineASAPQuery      `yaml:"asap_query,omitempty"`
	Load        PipelineLoadConfig      `yaml:"load"`
}

// PipelineExporterConfig holds the OTLP gRPC exporter settings.
type PipelineExporterConfig struct {
	Endpoint         string `yaml:"endpoint"`
	Insecure         bool   `yaml:"insecure"`
	MaxSendMsgSizeMB int    `yaml:"max_send_msg_size_mb"`
}

// PipelineReaderConfig controls the SDK periodic export interval.
type PipelineReaderConfig struct {
	Interval time.Duration `yaml:"interval"`
}

// PipelineSketchConfig selects the sketch type and whether the SDK or the
// collector performs aggregation (transmit_sketch).
type PipelineSketchConfig struct {
	// Type is one of: ddsketch | kll | hll | countsketch | countminsketch | baseline.
	// "baseline" disables sketch aggregation — raw LastValue gauges are sent.
	Type string `yaml:"type"`

	// TransmitSketch controls which side of the pipeline aggregates.
	//   true  → SDK attaches a sketch Aggregation View; the collector receives
	//            a single compact sketch data point per series per window.
	//   false → SDK emits raw Float64Gauge observations; the collector-side
	//            processor (kllprocessor, hllprocessor, …) aggregates.
	TransmitSketch bool `yaml:"transmit_sketch"`

	DDSketch       PipelineDDSketchParams       `yaml:"ddsketch"`
	KLL            PipelineKLLParams             `yaml:"kll"`
	CountSketch    PipelineCountSketchParams     `yaml:"countsketch"`
	CountMinSketch PipelineCountMinSketchParams  `yaml:"countminsketch"`
	// HLL has no tunable parameters; its presence in the YAML is informational.
}

// PipelineDDSketchParams are the tuning knobs for DDSketch aggregation.
type PipelineDDSketchParams struct {
	RelativeAccuracy float64 `yaml:"relative_accuracy"`
	NoMinMax         bool    `yaml:"no_min_max"`
}

// PipelineKLLParams are the tuning knobs for KLL sketch aggregation.
type PipelineKLLParams struct {
	K int `yaml:"k"`
}

// PipelineCountSketchParams are the tuning knobs for CountSketch aggregation.
type PipelineCountSketchParams struct {
	Rows      int     `yaml:"rows"`
	Cols      int     `yaml:"cols"`
	Epsilon   float64 `yaml:"epsilon"`
	Delta     float64 `yaml:"delta"`
	Dimension string  `yaml:"dimension"`
}

// PipelineCountMinSketchParams are the tuning knobs for CountMinSketch aggregation.
type PipelineCountMinSketchParams struct {
	Rows int `yaml:"rows"`
	Cols int `yaml:"cols"`
}

// PipelineInstrument describes a single OTel instrument to be aggregated.
type PipelineInstrument struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Unit        string `yaml:"unit"`

	// InstrumentKind is "histogram" or "gauge".
	// "histogram" is used for transmit_sketch: true (sketch Aggregation View).
	// "gauge" is used for transmit_sketch: false (raw LastValue observations).
	InstrumentKind string `yaml:"instrument_kind"`

	// GroupBy lists the attribute keys that form the sketch grouping dimension.
	// One sketch (or gauge) is produced per unique combination of these values.
	GroupBy []string `yaml:"group_by"`

	// SeriesPerSketch controls how many distinct label-value combinations are
	// collapsed into a single sketch:
	//   1 → one sketch per series (fine-grained, default)
	//   0 → all series collapsed into one sketch (coarsest)
	//   N → every N series share one sketch (matrix grouping)
	SeriesPerSketch int `yaml:"series_per_sketch"`
}

// PipelineASAPQuery mirrors the ControllerConfig schema from asap-planner-rs
// so that the same YAML block can be consumed by asap-planner-rs directly.
type PipelineASAPQuery struct {
	Metrics          []PipelineASAPMetric       `yaml:"metrics"`
	QueryGroups      []PipelineASAPQueryGroup   `yaml:"query_groups"`
	SketchParameters map[string]any             `yaml:"sketch_parameters,omitempty"`
}

// PipelineASAPMetric names a metric and its label dimensions for the planner.
type PipelineASAPMetric struct {
	Metric string   `yaml:"metric"`
	Labels []string `yaml:"labels"`
}

// PipelineASAPQueryGroup is a group of related queries sharing SLA targets.
type PipelineASAPQueryGroup struct {
	ID                *uint32                     `yaml:"id,omitempty"`
	Queries           []string                    `yaml:"queries"`
	RepetitionDelay   uint64                      `yaml:"repetition_delay"`
	ControllerOptions PipelineASAPControllerOpts  `yaml:"controller_options"`
}

// PipelineASAPControllerOpts carries the accuracy and latency SLA targets.
type PipelineASAPControllerOpts struct {
	AccuracySLA float64 `yaml:"accuracy_sla"`
	LatencySLA  float64 `yaml:"latency_sla"`
}

// PipelineLoadConfig holds load-generation parameters for fakemetricload and
// e2esdkbench.
type PipelineLoadConfig struct {
	Workers                int                      `yaml:"workers"`
	Series                 int                      `yaml:"series"`
	SamplesPerSecPerSeries float64                  `yaml:"samples_per_sec_per_series"`
	Duration               time.Duration            `yaml:"duration"`
	Distribution           PipelineDistributionConfig `yaml:"distribution"`
}

// PipelineDistributionConfig parameterises the synthetic data distribution.
type PipelineDistributionConfig struct {
	Type string  `yaml:"type"`
	S    float64 `yaml:"s"`
	V    float64 `yaml:"v"`
	Max  uint64  `yaml:"max"`
	Mean float64 `yaml:"mean"`
}

// LoadPipelineConfig reads and parses a YAML pipeline config file.
func LoadPipelineConfig(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading pipeline config %s: %w", path, err)
	}
	var cfg PipelineConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing pipeline config %s: %w", path, err)
	}
	return &cfg, nil
}

// ToAggregation returns the SDK Aggregation corresponding to sketch.type and
// the sketch-specific parameters. Returns nil for "baseline" (no aggregation
// override — the instrument's default LastValue aggregation is used).
func (c *PipelineConfig) ToAggregation() Aggregation {
	s := c.Sketch
	switch s.Type {
	case "ddsketch":
		return AggregationDDSketch{
			RelativeAccuracy: s.DDSketch.RelativeAccuracy,
			NoMinMax:         s.DDSketch.NoMinMax,
		}
	case "kll":
		return AggregationKLLSketch{K: s.KLL.K}
	case "hll":
		return AggregationHLLSketch{}
	case "countsketch":
		return AggregationCountSketch{
			Rows:      s.CountSketch.Rows,
			Cols:      s.CountSketch.Cols,
			Epsilon:   s.CountSketch.Epsilon,
			Delta:     s.CountSketch.Delta,
			Dimension: s.CountSketch.Dimension,
		}
	case "countminsketch":
		return AggregationCountMinSketch{
			Rows: s.CountMinSketch.Rows,
			Cols: s.CountMinSketch.Cols,
		}
	default: // "baseline" or unrecognised
		return nil
	}
}

// ToViews returns one View per configured instrument that attaches the sketch
// aggregation. Returns nil when transmit_sketch is false or the type is
// "baseline" (no aggregation override needed).
func (c *PipelineConfig) ToViews() []View {
	if !c.Sketch.TransmitSketch {
		return nil
	}
	agg := c.ToAggregation()
	if agg == nil {
		return nil
	}
	views := make([]View, 0, len(c.Instruments))
	for _, inst := range c.Instruments {
		views = append(views, NewView(
			Instrument{Name: inst.Name},
			Stream{Aggregation: agg},
		))
	}
	return views
}

// ReaderInterval returns the configured reader export interval, defaulting to
// 1 second when unset.
func (c *PipelineConfig) ReaderInterval() time.Duration {
	if c.Reader.Interval <= 0 {
		return time.Second
	}
	return c.Reader.Interval
}
