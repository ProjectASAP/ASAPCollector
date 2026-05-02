package harness

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/sketchlib-go/common"
)

// RuntimeConfig pins the per-sketch knobs that both paths share.
// Keep these in lock-step with LegacyConfig so the byte payloads
// align — divergence here is a config bug, not a runtime bug.
type RuntimeConfig struct {
	WindowSize        time.Duration
	DDSketchAlpha     float64
	KLLK              int
	HLLPrecision      int // documented even though sketchlib hard-codes 14
	CountSketchEps    float64
	CountSketchDelta  float64
	CMSRows           int
	CMSCols           int
	DeltaTransmission bool
	DeltaThreshold    uint64
}

// DefaultRuntimeConfig matches the spec's per-sketch params.
func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		WindowSize:       10 * time.Second,
		DDSketchAlpha:    0.01,
		KLLK:             200,
		HLLPrecision:     14,
		CountSketchEps:   0.05, // → cols=512 after nextPowerOfTwo
		CountSketchDelta: 0.05, // → rows=3
		// CMS: depth=4, width=2048 per spec; sketchlib calls them
		// rows × columns.
		CMSRows:           4,
		CMSCols:           2048,
		DeltaTransmission: false, // see runtime.go preamble
		DeltaThreshold:    0,
	}
}

// RuntimeOutput holds the envelopes Path A produced for one sketch
// type. EnvelopesByMetric.Encode is what the harness compares.
type RuntimeOutput struct {
	SketchType precompute.SketchType
	Envelopes  []*precompute.SketchEnvelope
	// Encoded is the OTel adapter's pmetric.Metrics that the
	// runtime would forward downstream — captured for parity-test
	// completeness even though the byte assertion compares the
	// envelope bytes directly.
	Encoded pmetric.Metrics
}

// RunRuntimePath drives 5 Precompute instances (one per sketch type)
// against the input. Each Precompute is configured with a label
// matcher that admits ONLY its own metric, so the sketch-type
// configurations stay isolated. After all observations are fed,
// Tick is called past the window boundary to drain the closed
// window's envelopes.
//
// Determinism: BuildInput emits samples in strict (resource,
// labelset, sample-index) order; Decode walks pmetric in the same
// order; the Precompute window is single-threaded inside Observe
// so the sketch state evolves deterministically.
func RunRuntimePath(input pmetric.Metrics, cfg RuntimeConfig) (map[string]*RuntimeOutput, error) {
	out := make(map[string]*RuntimeOutput, 5)
	syncfg := DefaultSyntheticConfig()
	// Tick at a time strictly past the last sample's window to
	// force a single rotation. We choose now := startMs +
	// max(samples) * intervalMs + WindowSize so the runtime
	// considers the active window closed.
	maxSamples := uint64(syncfg.QuantileSamples)
	if uint64(syncfg.HLLSamples) > maxSamples {
		maxSamples = uint64(syncfg.HLLSamples)
	}
	if uint64(syncfg.CountSamples) > maxSamples {
		maxSamples = uint64(syncfg.CountSamples)
	}
	tickMs := syncfg.StartTimestampMs +
		(maxSamples+1)*syncfg.SampleIntervalMs +
		uint64(cfg.WindowSize/time.Millisecond)

	for _, sd := range []sketchDescriptor{
		{
			name: MetricDDSketch, sk: precompute.SketchTypeDDSketch,
			factory: func() precompute.Sketch {
				return newDDSketchWrapper(cfg.DDSketchAlpha)
			},
			observer: ddSketchObserver{},
			// DDSketch is the canonical resource-aware case — both
			// paths key by (resource, dp-attrs) and emit one
			// envelope per (resource, labelset).
			outMetricName: MetricDDSketch,
		},
		{
			name: MetricKLL, sk: precompute.SketchTypeKLLSketch,
			factory: func() precompute.Sketch {
				return newKLLWrapper(cfg.KLLK)
			},
			observer: kllObserver{},
			// Legacy kllprocessor.processBatch keys series by
			// `metricName + "::" + dpAttrs(attrs)` (no resource
			// segment) and emits the output metric named
			// `<base>_kll`. We mirror both via OmitResourceAttrs
			// + a baked-in suffix on the runtime's MetricName.
			omitResourceAttrs: true,
			outMetricName:     MetricKLL + "_kll",
		},
		{
			name: MetricHLL, sk: precompute.SketchTypeHLLSketch,
			factory: func() precompute.Sketch { return newHLLWrapper() },
			observer: hllObserver{},
			// Legacy hllprocessor.processBatch shape mirrors KLL:
			// series key is `metricName + "::" + dpAttrs`, output
			// metric name is `<base>_hll_cardinality`.
			omitResourceAttrs: true,
			outMetricName:     MetricHLL + "_hll_cardinality",
		},
		{
			name: MetricCountSketch, sk: precompute.SketchTypeCountSketch,
			factory: func() precompute.Sketch {
				return newCountSketchWrapper(
					countSketchRows(cfg), countSketchCols(cfg),
				)
			},
			observer: countSketchObserver{metricName: MetricCountSketch},
			// Legacy countsketchprocessor with empty AggregateBy
			// uses a single "global" partition for ALL data points;
			// the emitted metric name is the literal
			// "countsketch_partition" (not the input name).
			globalAggregation: true,
			outMetricName:     "countsketch_partition",
		},
		{
			name: MetricCMS, sk: precompute.SketchTypeCountMinSketch,
			factory: func() precompute.Sketch {
				return newCMSWrapper(cfg.CMSRows, cfg.CMSCols)
			},
			observer: cmsObserver{},
			// Legacy countminsketchprocessor.seriesKey is
			// `metricName + "::" + encodeKey(dpAttrs)`; resource
			// attrs are not in the key. The output metric name
			// comes from cfg.MetricName which we set to the input
			// metric name (no implicit suffix).
			omitResourceAttrs: true,
			outMetricName:     MetricCMS,
		},
	} {
		ro, err := runOnePrecompute(input, sd, cfg, tickMs)
		if err != nil {
			return nil, fmt.Errorf("runtime path %s: %w", sd.name, err)
		}
		out[sd.name] = ro
	}
	return out, nil
}

type sketchDescriptor struct {
	name     string
	sk       precompute.SketchType
	factory  precompute.SketchFactory
	observer precompute.SketchObserver
	// outMetricName is the metric name baked into the runtime's
	// SketchEnvelope. The diff key reads SketchEnvelope.MetricName
	// directly (not the encoded pmetric output), so legacy-side
	// suffixes like `_kll` / `_hll_cardinality` and the literal
	// "countsketch_partition" are baked here rather than going via
	// AdapterConfig.MetricSuffix on encode.
	outMetricName string
	// omitResourceAttrs strips the resource-segment from SeriesKey
	// AND clears the entry's ResourceLabels so the emitted envelope
	// matches the legacy "appended-empty-RM" output shape.
	omitResourceAttrs bool
	// globalAggregation collapses every observation into a single
	// shared series — both resource and dp labels are ignored when
	// constructing the series key, and both fields are nil on the
	// emitted envelope. CountSketch's batch-mode behavior.
	globalAggregation bool
}

// runOnePrecompute wires up one Precompute, drives the input through
// the OTel adapter, and ticks the window closed.
func runOnePrecompute(
	input pmetric.Metrics,
	sd sketchDescriptor,
	cfg RuntimeConfig,
	tickMs uint64,
) (*RuntimeOutput, error) {
	metricName := sd.outMetricName
	if metricName == "" {
		metricName = sd.name
	}
	pcfg := &precompute.PrecomputeConfig{
		AggID:      precompute.AggId(uint64(sd.sk)),
		SketchType: sd.sk,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: cfg.WindowSize},
		// Match-by-metric-name: Name="" means the matcher's value
		// is compared against Observation.Metric (per matchers.go).
		Matchers: []precompute.LabelMatcher{
			{Name: "", Value: sd.name},
		},
		MetricName:        metricName,
		TransmitSketch:    true,
		DeltaTransmission: cfg.DeltaTransmission,
		DeltaThreshold:    cfg.DeltaThreshold,
		Encoding:          precompute.EncodingProtoFull,
		Temporality:       1, // delta
		OmitResourceAttrs: sd.omitResourceAttrs,
		GlobalAggregation: sd.globalAggregation,
	}
	pp := precompute.New(pcfg, sd.factory, sd.observer)

	adapter := otelpre.New(&otelpre.AdapterConfig{
		ScopeName: "asap.parity.harness",
	}, nil)

	obs, err := adapter.Decode(CloneInput(input))
	if err != nil {
		return nil, fmt.Errorf("Decode: %w", err)
	}
	for i := range obs {
		// CMS ingestion in the legacy processor hashes the
		// data-point's encoded attribute set, not the float value.
		// Replace the OTel adapter's KindFloat observation with
		// a KindBytes one carrying the same encoded-attrs string
		// the legacy `encodeAttributesAsKey` produces, so the CMS
		// observer's InsertWithHash sees the same hash on both
		// paths.
		if sd.sk == precompute.SketchTypeCountMinSketch &&
			obs[i].Metric == sd.name {
			obs[i].Value = precompute.BytesValue(
				[]byte(precompute.AttributesKey(obs[i].Labels, nil)),
			)
		}
		// Observe is the canonical entry; matchers filter out
		// observations whose metric name doesn't match.
		if err := pp.Observe(&obs[i]); err != nil {
			return nil, fmt.Errorf("Observe[%d]: %w", i, err)
		}
	}
	envs := pp.Tick(tickMs)
	encoded, err := adapter.Encode(envs)
	if err != nil {
		return nil, fmt.Errorf("Encode: %w", err)
	}
	encMetrics, _ := encoded.(pmetric.Metrics)
	return &RuntimeOutput{
		SketchType: sd.sk,
		Envelopes:  envs,
		Encoded:    encMetrics,
	}, nil
}

func countSketchRows(cfg RuntimeConfig) int {
	r, _ := CountSketchDims(cfg.CountSketchEps, cfg.CountSketchDelta)
	return r
}

func countSketchCols(cfg RuntimeConfig) int {
	_, c := CountSketchDims(cfg.CountSketchEps, cfg.CountSketchDelta)
	return c
}

// ===== Sketch observers =====
//
// Each observer translates an ObservationValue into the appropriate
// sketchlib-go entry point. The legacy processors call these same
// entry points on the same input — that's what makes the resulting
// sketch state byte-identical (modulo iteration order, which both
// sides preserve via single-threaded Observe).

type ddSketchObserver struct{}

func (ddSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	if v.Kind != precompute.KindFloat {
		return fmt.Errorf("ddSketchObserver: expected KindFloat, got %s", v.Kind)
	}
	w, ok := s.(*ddSketchWrapper)
	if !ok {
		return fmt.Errorf("ddSketchObserver: sketch is %T", s)
	}
	w.update(v.Float)
	return nil
}

type kllObserver struct{}

func (kllObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	if v.Kind != precompute.KindFloat {
		return fmt.Errorf("kllObserver: expected KindFloat, got %s", v.Kind)
	}
	w, ok := s.(*kllWrapper)
	if !ok {
		return fmt.Errorf("kllObserver: sketch is %T", s)
	}
	w.update(v.Float)
	return nil
}

type hllObserver struct{}

func (hllObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	if v.Kind != precompute.KindFloat {
		return fmt.Errorf("hllObserver: expected KindFloat, got %s", v.Kind)
	}
	w, ok := s.(*hllWrapper)
	if !ok {
		return fmt.Errorf("hllObserver: sketch is %T", s)
	}
	// Mirror legacy hllprocessor batch path: bs.sketch.UpdateValue(...)
	w.updateValue(v.Float)
	return nil
}

// countSketchObserver mirrors legacy ws.cs.UpdateString(metricName, value).
type countSketchObserver struct{ metricName string }

func (o countSketchObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	if v.Kind != precompute.KindFloat {
		return fmt.Errorf("countSketchObserver: expected KindFloat, got %s", v.Kind)
	}
	w, ok := s.(*countSketchWrapper)
	if !ok {
		return fmt.Errorf("countSketchObserver: sketch is %T", s)
	}
	w.updateString(o.metricName, v.Float)
	return nil
}

// cmsObserver mirrors legacy InsertWithHash(common.FromString(flowKey).Hash).
// flowKey is the encoded data-point attribute set; see legacy
// encodeAttributesAsKey. The harness rebuilds that string from
// Observation.Labels.
type cmsObserver struct{}

func (cmsObserver) Observe(s precompute.Sketch, v precompute.ObservationValue) error {
	if v.Kind != precompute.KindBytes {
		return fmt.Errorf("cmsObserver: expected KindBytes, got %s", v.Kind)
	}
	w, ok := s.(*cmsWrapper)
	if !ok {
		return fmt.Errorf("cmsObserver: sketch is %T", s)
	}
	w.insertHash(common.FromBytes(v.Bytes).Hash)
	return nil
}
