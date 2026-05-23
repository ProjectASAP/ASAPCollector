// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	oteladapter "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// sketchAggregator wraps a precompute.Precompute for one sketch-family metric.
// Samples are fed via ObserveKeyed (the precompute-format key built once
// upstream from the shared decode); flush ticks the window and encodes the
// resulting envelopes back to pmetric for forwarding. Mirrors the standalone
// sketch processors (ddsketch etc.), generalized + keyed.
type sketchAggregator struct {
	pc   precompute.Precompute
	pcfg *precompute.PrecomputeConfig
	enc  *oteladapter.AdapterConfig
}

// fnv64 derives a stable per-metric AggID (matches the standalone sketch
// processors' seriesNameHash).
func fnv64(s string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// newSketchAggregator builds the aggregator for fam, or (nil,false) if the
// family isn't wired yet. DDSketch (observes the number value) is wired;
// KLL/HLL/CS/CMS follow with their per-family params + observe-subject.
func newSketchAggregator(metric string, fam *MetricFamily, window time.Duration) (*sketchAggregator, bool) {
	var (
		st       precompute.SketchType
		factory  precompute.SketchFactory
		observer precompute.SketchObserver
	)
	switch fam.Family {
	case FamilyDDSketch:
		alpha := fam.RelativeAccuracy
		st = precompute.SketchTypeDDSketch
		factory = func() precompute.Sketch { return sketches.NewDDSketchWrapper(alpha) }
		observer = sketches.DDSketchObserver{}
	case FamilyKLL:
		k := fam.K
		if k < 2 {
			k = 200
		}
		st = precompute.SketchTypeKLLSketch
		factory = func() precompute.Sketch { return sketches.NewKLLWrapper(k, nil) }
		observer = sketches.KLLObserver{}
	case FamilyHLL:
		st = precompute.SketchTypeHLLSketch
		factory = func() precompute.Sketch { return sketches.NewHLLWrapper() }
		observer = sketches.HLLObserver{}
	case FamilyCountSketch:
		rows, cols := csmDims(fam)
		st = precompute.SketchTypeCountSketch
		factory = func() precompute.Sketch {
			w, _ := sketches.NewCountSketchWrapper(rows, cols)
			return w
		}
		observer = sketches.CountSketchObserver{DefaultKey: metric}
	case FamilyCountMinSketch:
		rows, cols := csmDims(fam)
		st = precompute.SketchTypeCountMinSketch
		factory = func() precompute.Sketch { return sketches.NewCMSWrapper(rows, cols, false) }
		observer = sketches.CMSObserver{}
	default:
		return nil, false
	}
	pcfg := &precompute.PrecomputeConfig{
		AggID:          precompute.AggId(fnv64(metric)),
		SketchType:     st,
		Mode:           precompute.Tumbling,
		Window:         precompute.WindowSpec{Size: window},
		AggregateBy:    fam.AggregateBy,
		TransmitSketch: true,
		Encoding:       precompute.EncodingProtoFull,
		MetricName:     metric,
		Temporality:    int32(pmetric.AggregationTemporalityDelta),
	}
	return &sketchAggregator{
		pc:   precompute.New(pcfg, factory, observer),
		pcfg: pcfg,
		enc:  &oteladapter.AdapterConfig{MetricSuffix: "_" + string(fam.Family), DropOriginal: true},
	}, true
}

// csmDims returns the CountSketch/CountMinSketch matrix dimensions, with
// defaults (5 x 2048) mirroring the standalone processors.
func csmDims(fam *MetricFamily) (rows, cols int) {
	rows, cols = fam.Rows, fam.Cols
	if rows < 1 {
		rows = 5
	}
	if cols < 2 {
		cols = 2048
	}
	return rows, cols
}

func kvFromMap(am map[string]string) []precompute.KeyValue {
	out := make([]precompute.KeyValue, 0, len(am))
	for k, v := range am {
		out = append(out, precompute.KeyValue{Key: k, Value: v})
	}
	return out
}

// observe feeds one sample. The precompute key is built once here from the
// shared decoded attrs and passed via ObserveKeyed (no internal re-key).
func (s *sketchAggregator) observe(am map[string]string, val float64, tsMs uint64) {
	obs := &precompute.Observation{
		TimestampMs: tsMs,
		Labels:      kvFromMap(am),
		Value:       precompute.FloatValue(val),
	}
	_ = s.pc.ObserveKeyed(s.pcfg.SeriesKeyFor(obs), obs)
}

// flush force-rotates the window (Drain, regardless of wall-clock — the
// asap_edge flush tick IS the window boundary) and appends the encoded
// sketch envelopes to dst.
func (s *sketchAggregator) flush(dst pmetric.Metrics) {
	envs := s.pc.Drain()
	if len(envs) == 0 {
		return
	}
	out, err := oteladapter.Encode(envs, s.enc)
	if err != nil || out.ResourceMetrics().Len() == 0 {
		return
	}
	out.ResourceMetrics().MoveAndAppendTo(dst.ResourceMetrics())
}
