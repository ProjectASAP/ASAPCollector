// Package allsketches is the unified Telegraf StreamingProcessor that
// adapts the host-neutral asap-precompute-go runtime to Telegraf's
// metric pipeline. One plugin, one sketch_type knob — the design is
// pinned in docs/design-asap-telegraf-integration.md §3.
//
// This file contains the small helpers that translate the plugin's
// flat TOML-derived config into the runtime's PrecomputeConfig and
// the codec's AdapterConfig, plus the sketch-type dispatch table.
package allsketches

import (
	"fmt"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	telegrafcodec "github.com/ProjectASAP/asap-precompute-go/telegraf"
)

// Sketch type spellings accepted in the `sketch_type` config field.
// The strings match the design-doc §8 config example verbatim.
const (
	SketchTypeDDSketch       = "ddsketch"
	SketchTypeKLL            = "kll"
	SketchTypeHLL            = "hll"
	SketchTypeCountSketch    = "countsketch"
	SketchTypeCountMinSketch = "countminsketch"
)

// resolveSketchType maps the TOML sketch_type string to the runtime's
// SketchType enum. Unknown spellings return an error so the plugin's
// Start can surface a clear config-mismatch message.
func resolveSketchType(s string) (precompute.SketchType, error) {
	switch s {
	case SketchTypeDDSketch:
		return precompute.SketchTypeDDSketch, nil
	case SketchTypeKLL:
		return precompute.SketchTypeKLLSketch, nil
	case SketchTypeHLL:
		return precompute.SketchTypeHLLSketch, nil
	case SketchTypeCountSketch:
		return precompute.SketchTypeCountSketch, nil
	case SketchTypeCountMinSketch:
		return precompute.SketchTypeCountMinSketch, nil
	}
	return precompute.SketchTypeUnspecified, fmt.Errorf(
		"allsketches: unsupported sketch_type %q (want one of: %s, %s, %s, %s, %s)",
		s, SketchTypeDDSketch, SketchTypeKLL, SketchTypeHLL,
		SketchTypeCountSketch, SketchTypeCountMinSketch)
}

// toPrecomputeConfig translates plugin fields into the runtime's
// host-neutral PrecomputeConfig. Most fields map 1:1 — the runtime
// already has knobs for delta transmission, omit-resource-attrs,
// global-aggregation, and window-stats emission.
func (a *AllSketches) toPrecomputeConfig(window time.Duration, st precompute.SketchType) *precompute.PrecomputeConfig {
	return &precompute.PrecomputeConfig{
		SketchType:        st,
		Mode:              precompute.Tumbling,
		Window:            precompute.WindowSpec{Size: window},
		TransmitSketch:    true,
		DeltaTransmission: a.DeltaTransmission,
		DeltaThreshold:    a.DeltaThreshold,
		Encoding:          precompute.EncodingProtoFull,
		MetricName:        a.OutputMetricName,
		OmitResourceAttrs: a.OmitResourceAttrs,
		GlobalAggregation: a.GlobalAggregation,
		EmitWindowStats:   a.EmitWindowStats,
	}
}

// toAdapterConfig translates the plugin fields that affect codec
// behavior (value field name, envelope field name, output metric
// name) into the codec's AdapterConfig.
func (a *AllSketches) toAdapterConfig() *telegrafcodec.AdapterConfig {
	cfg := telegrafcodec.DefaultAdapterConfig()
	if a.ValueField != "" {
		cfg.ValueField = a.ValueField
	}
	if a.EnvelopeField != "" {
		cfg.EnvelopeField = a.EnvelopeField
	}
	if a.OutputMetricName != "" {
		cfg.OutputMetricName = a.OutputMetricName
	}
	return cfg
}

// sketchFactory returns a (precompute.SketchFactory, SketchObserver)
// pair appropriate for the configured sketch_type. The factory is
// the constructor the runtime calls to materialize a fresh sketch
// per-series; the observer is the per-Kind dispatch glue from
// asap-precompute-go/sketches/.
func sketchFactory(sketchType string, p *AllSketches) (precompute.SketchFactory, precompute.SketchObserver, error) {
	switch sketchType {
	case SketchTypeDDSketch:
		alpha := p.Alpha
		if alpha <= 0 || alpha >= 1 {
			alpha = 0.01
		}
		return func() precompute.Sketch {
			return sketches.NewDDSketchWrapper(alpha)
		}, sketches.DDSketchObserver{}, nil

	case SketchTypeKLL:
		k := p.K
		if k <= 0 {
			k = 200
		}
		var seedPtr *int64
		if p.Seed != 0 {
			s := p.Seed
			seedPtr = &s
		}
		return func() precompute.Sketch {
			return sketches.NewKLLWrapper(k, seedPtr)
		}, sketches.KLLObserver{}, nil

	case SketchTypeHLL:
		// sketchlib-go's HLL constructor is parameterless; precision
		// is hard-coded to hll.HLLPrecision (14). The Precision config
		// field is accepted for forward-compat but currently ignored.
		return func() precompute.Sketch {
			return sketches.NewHLLWrapper()
		}, sketches.HLLObserver{}, nil

	case SketchTypeCountSketch:
		rows, cols := p.Depth, p.Width
		if rows <= 0 {
			rows = 4
		}
		if cols <= 0 {
			cols = 2048
		}
		return func() precompute.Sketch {
			w, _ := sketches.NewCountSketchWrapper(rows, cols)
			return w
		}, sketches.CountSketchObserver{DefaultKey: p.OutputMetricName}, nil

	case SketchTypeCountMinSketch:
		rows, cols := p.Depth, p.Width
		if rows <= 0 {
			rows = 4
		}
		if cols <= 0 {
			cols = 2048
		}
		// Msgpack encoding is a legacy mode the unified plugin doesn't
		// expose via TOML — the proto path supports delta transmission
		// and is the canonical wire format for the unified plugin.
		return func() precompute.Sketch {
			return sketches.NewCMSWrapper(rows, cols, false)
		}, sketches.CMSObserver{}, nil
	}
	return nil, nil, fmt.Errorf("allsketches: sketchFactory: unsupported sketch_type %q", sketchType)
}
