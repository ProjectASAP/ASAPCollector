// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"sync/atomic"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	oteladapter "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
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
	// factory is the per-window sketch constructor handed to precompute.New
	// (it bakes in the per-family params + the warm-sketch sample_p). Retained
	// so the built sampling probability is observable (e.g. in tests) without
	// reaching into precompute internals.
	factory precompute.SketchFactory
	// obsKind selects how observe() shapes each ObservationValue for the wired
	// family's observer:
	//   obsKindFloat     — Kind=KindFloat, Float=value (DDSketch / KLL / HLL).
	//   obsKindBytesHash — Kind=KindBytes, Bytes=attrKey: CMS hashes the encoded
	//                      attribute set to count series frequency (its observer
	//                      requires KindBytes; a KindFloat is rejected).
	//   obsKindKeyedFreq — Kind=KindFloat, Float=1, Bytes=attrKey: CountSketch's
	//                      observer UpdateString(key, weight)s the attribute-set
	//                      key (NOT the metric name) with weight 1, so it counts
	//                      the SAME subject CMS does — per-attribute-set frequency
	//                      — instead of degenerately counting one key (the metric
	//                      name) weighted by the sample value (see B6).
	obsKind observeKind
	logger  *zap.Logger
	// lastObserveErr is the most recent ObserveKeyed result (nil when the last
	// sample recorded cleanly). The observe error used to be discarded, which
	// hid exactly the CMS KindBytes mismatch above; it is now retained (and
	// logged once) so a value-kind regression is visible instead of silent.
	lastObserveErr   error
	loggedObserveErr bool
	// droppedSamples counts samples ObserveKeyed rejected. The log is latched
	// (loggedObserveErr), so without this counter later drops would be invisible;
	// it keeps every drop observable even after the one-time log fires.
	droppedSamples atomic.Uint64
	// procDropCount, when non-nil, is the processor-wide sketch-drop counter the
	// aggregator also bumps so all aggregators' drops roll up to one number.
	procDropCount *atomic.Uint64
}

// observeKind selects how observe() shapes each ObservationValue for the wired
// family's observer (see sketchAggregator.obsKind).
type observeKind uint8

const (
	// obsKindFloat: numeric value via KindFloat (DDSketch / KLL / HLL).
	obsKindFloat observeKind = iota
	// obsKindBytesHash: attribute-set key via KindBytes (CountMinSketch).
	obsKindBytesHash
	// obsKindKeyedFreq: attribute-set key + weight 1 via KindFloat (CountSketch).
	obsKindKeyedFreq
)

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

// sketchOpts bundles the cross-cutting runtime settings (window length, the
// series-cardinality cap, delta-transmission, and late-data grace) the
// processor resolves once from config and threads into every per-shard sketch
// aggregator. Keeping them in one struct avoids growing newSketchAggregator's
// positional signature each time a PrecomputeConfig knob is plumbed.
type sketchOpts struct {
	window time.Duration
	// maxSeries caps the per-shard precompute series map (0 => unlimited).
	maxSeries uint64
	// delta enables PROTO_DELTA transmission for delta-capable families.
	delta bool
	// deltaThreshold caps the delta size (0 => runtime default).
	deltaThreshold uint64
	// allowedLateness mirrors Cold.ReorderGrace so warm late-data semantics
	// match the cold tier (precompute drops samples older than
	// activeStart-allowedLateness instead of silently accepting them).
	allowedLateness time.Duration
}

// newSketchAggregator builds the aggregator for fam, or (nil,false) if the
// family isn't wired yet. DDSketch (observes the number value) is wired;
// KLL/HLL/CS/CMS follow with their per-family params + observe-subject.
func newSketchAggregator(metric string, fam *MetricFamily, opts sketchOpts, logger *zap.Logger) (*sketchAggregator, bool) {
	if logger == nil {
		logger = zap.NewNop()
	}
	window := opts.window
	var (
		st       precompute.SketchType
		factory  precompute.SketchFactory
		observer precompute.SketchObserver
	)
	// sampleP is the warm-sketch sampling probability (1.0 = disabled). It is
	// applied via sketchlib-go's WithSampleP to the families whose geometric
	// skip actually avoids work: DDSketch (value-independent skip avoids the
	// bucket-index mapping + store increment) and CountMinSketch (skip avoids
	// the d×w cell update). WithSampleP(1.0) is an exact no-op, so the default
	// path stays byte-identical to the pre-sampling build. HLL is deliberately
	// NEVER sampled: its hash must be computed regardless (it is both the
	// admission threshold AND the register index), so sampling buys ~0 CPU while
	// degrading cardinality accuracy — it is forced to no-sampling here. KLL /
	// CountSketch have no sampling support and ignore it.
	sampleP := fam.SampleP
	if sampleP <= 0 {
		sampleP = 1.0
	}
	switch fam.Family {
	case FamilyDDSketch:
		alpha := fam.RelativeAccuracy
		st = precompute.SketchTypeDDSketch
		factory = func() precompute.Sketch { return sketches.NewDDSketchWrapper(alpha).WithSampleP(sampleP) }
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
		// HLL is never sampled: the hash is needed for both the admission
		// threshold and the register index, so sampling saves ~0 CPU while
		// degrading cardinality accuracy. Force no-sampling regardless of
		// fam.SampleP.
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
		factory = func() precompute.Sketch { return sketches.NewCMSWrapper(rows, cols, false).WithSampleP(sampleP) }
		observer = sketches.CMSObserver{}
	default:
		return nil, false
	}
	pcfg := &precompute.PrecomputeConfig{
		AggID:          precompute.AggId(fnv64(metric)),
		SketchType:     st,
		Mode:           precompute.Tumbling,
		Window:         precompute.WindowSpec{Size: window, AllowedLateness: opts.allowedLateness},
		AggregateBy:    fam.AggregateBy,
		TransmitSketch: true,
		Encoding:       precompute.EncodingProtoFull,
		MetricName:     metric,
		Temporality:    int32(pmetric.AggregationTemporalityDelta),
		// Bound the per-shard series map so a cardinality explosion cannot grow
		// it without limit; a new series past the cap is dropped (counted via
		// Stats().DroppedOverflow).
		MaxSeries:  opts.maxSeries,
		OnOverflow: precompute.OnOverflowDrop,
		// Delta transmission: when enabled the runtime emits PROTO_DELTA frames
		// after the first PROTO_FULL snapshot. Only set for delta-capable
		// families (KLL/Sum cannot delta) — opts.delta is already gated on
		// family by config.effectiveDelta.
		DeltaTransmission: opts.delta,
		DeltaThreshold:    opts.deltaThreshold,
	}
	return &sketchAggregator{
		pc:      precompute.New(pcfg, factory, observer),
		pcfg:    pcfg,
		enc:     &oteladapter.AdapterConfig{MetricSuffix: "_" + string(fam.Family), DropOriginal: true},
		factory: factory,
		obsKind: observeKindFor(fam.Family),
		logger:  logger,
	}, true
}

// observeKindFor maps a sketch family to its observe shaping. CMS hashes the
// attribute-set key (KindBytes); CountSketch counts the attribute-set key with
// weight 1 (KindFloat + Bytes) so it counts the same subject as CMS rather than
// the degenerate metric-name single key (B6); every other family observes the
// numeric value (KindFloat).
func observeKindFor(f FamilyKind) observeKind {
	switch f {
	case FamilyCountMinSketch:
		return obsKindBytesHash
	case FamilyCountSketch:
		return obsKindKeyedFreq
	default:
		return obsKindFloat
	}
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
	kv := kvFromMap(am)
	obs := &precompute.Observation{
		TimestampMs: tsMs,
		Labels:      kv,
		Value:       precompute.FloatValue(val),
	}
	switch s.obsKind {
	case obsKindBytesHash:
		// CountMinSketch's observer consumes KindBytes: it hashes the encoded
		// attribute key (matching the standalone countminsketchprocessor's
		// AttributesKey(labels, nil)) to count series cardinality, not the numeric
		// value. AggregateBy grouping is applied separately by SeriesKeyFor below,
		// so the inserted key is the full attribute set (nil), identical to the
		// standalone shim.
		obs.Value = precompute.BytesValue([]byte(precompute.AttributesKey(kv, nil)))
	case obsKindKeyedFreq:
		// CountSketch's observer is UpdateString(key, weight) where key defaults
		// to DefaultKey (the metric NAME) when Bytes is empty — the degenerate
		// single-key case (B6). To count the SAME subject CMS does (per
		// attribute-set frequency), supply the encoded attribute set as the key
		// (Bytes) and weight 1 (Float), so each sample increments its own
		// attribute set's frequency by one.
		obs.Value = precompute.ObservationValue{
			Kind:  precompute.KindFloat,
			Float: 1,
			Bytes: []byte(precompute.AttributesKey(kv, nil)),
		}
	}
	if err := s.pc.ObserveKeyed(s.pcfg.SeriesKeyFor(obs), obs); err != nil {
		s.lastObserveErr = err
		// Always count the drop so it stays observable; the log is latched to
		// avoid spam but the counter is not (fix B8).
		s.droppedSamples.Add(1)
		if s.procDropCount != nil {
			s.procDropCount.Add(1)
		}
		if !s.loggedObserveErr {
			s.loggedObserveErr = true
			s.logger.Warn("asap_edge: sketch observe dropped sample (further drops counted, not logged)",
				zap.String("metric", s.pcfg.MetricName), zap.Error(err))
		}
		return
	}
	s.lastObserveErr = nil
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
