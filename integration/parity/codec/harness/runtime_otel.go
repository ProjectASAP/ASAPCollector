package harness

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/sketchlib-go/common"
)

// RuntimeConfig pins the per-sketch knobs both runtime drivers share.
// Identical config across the OTel and Telegraf paths is what makes
// the byte-parity check meaningful.
type RuntimeConfig struct {
	WindowSize       time.Duration
	DDSketchAlpha    float64
	KLLK             int
	CountSketchEps   float64
	CountSketchDelta float64
	CMSRows          int
	CMSCols          int
}

// DefaultRuntimeConfig matches the per-sketch knobs the parity harness
// pins for the OTel <-> legacy comparison. Reusing them here keeps the
// cross-host parity check apples-to-apples with the in-tree parity
// regression tests.
func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		WindowSize:       10 * time.Second,
		DDSketchAlpha:    0.01,
		KLLK:             200,
		CountSketchEps:   0.05,
		CountSketchDelta: 0.05,
		CMSRows:          4,
		CMSCols:          2048,
	}
}

// RunOTel drives all five Precompute instances against the OTel
// pmetric.Metrics input. Returns the per-sketch envelopes the runtime
// emits at Drain time. The same RuntimeConfig is used by RunTelegraf
// so the comparison is meaningful.
func RunOTel(input pmetric.Metrics, cfg RuntimeConfig) (map[string][]*precompute.SketchEnvelope, error) {
	out := make(map[string][]*precompute.SketchEnvelope, 5)
	adapter := otelpre.New(&otelpre.AdapterConfig{
		ScopeName: "asap.cross-host.parity",
	}, nil)

	for _, sd := range allSketchDescriptors(cfg) {
		envs, err := runOTelOne(input, adapter, sd, cfg)
		if err != nil {
			return nil, fmt.Errorf("RunOTel %s: %w", sd.metric, err)
		}
		out[sd.metric] = envs
	}
	return out, nil
}

// runOTelOne configures one Precompute, drives the input through
// the OTel adapter, and Drains. Returns the closed-window envelopes.
func runOTelOne(
	input pmetric.Metrics,
	adapter *otelpre.Adapter,
	sd sketchDescriptor,
	cfg RuntimeConfig,
) ([]*precompute.SketchEnvelope, error) {
	pp := precompute.New(sd.precomputeConfig(cfg), sd.factory(cfg), sd.observer)

	// Decode against a fresh clone so subsequent OTel runs (or any
	// downstream hook) never see mutated input. Mirrors the parity
	// harness's CloneInput defense.
	obs, err := adapter.Decode(CloneOTelInput(input))
	if err != nil {
		return nil, fmt.Errorf("Decode: %w", err)
	}
	for i := range obs {
		// CMS observation kind hand-off: the OTel adapter emits
		// KindFloat for Gauge / Sum data points, but the harness's
		// cmsObserver hashes a KindBytes value built from the
		// observation's labels (the same flowKey the legacy CMS
		// processor produces). Replace the value in-place; the
		// observation is sd's own — no risk of leaking the rewrite
		// to the Telegraf-side run.
		if sd.sketchType == precompute.SketchTypeCountMinSketch &&
			obs[i].Metric == sd.metric {
			obs[i].Value = precompute.BytesValue(
				[]byte(precompute.AttributesKey(obs[i].Labels, nil)),
			)
		}
		if err := pp.Observe(&obs[i]); err != nil {
			return nil, fmt.Errorf("Observe[%d]: %w", i, err)
		}
	}
	// Drain forces window rotation regardless of wall-clock time —
	// the harness has no goroutines, no tickers; it walks the
	// pipeline synchronously and asks for the closed envelope set.
	return pp.Drain(), nil
}

// ===== Sketch descriptors shared by both runtime paths =====

// sketchDescriptor packages the per-sketch knobs both runtime drivers
// need. Keeping them in one place guarantees the OTel and Telegraf
// paths build their Precompute instances from byte-identical configs.
type sketchDescriptor struct {
	metric     string
	sketchType precompute.SketchType
	// metricNameOut is the MetricName stamped onto every emitted
	// envelope. The cross-host harness uses the input metric name
	// verbatim — there's no legacy-side suffix to mirror because
	// neither path is a legacy processor.
	metricNameOut string
	// observer is the SketchObserver implementation that knows how
	// to apply ObservationValue kinds to the concrete sketch type.
	observer precompute.SketchObserver
	// factory returns a SketchFactory closure parameterized by the
	// shared RuntimeConfig.
	factory func(cfg RuntimeConfig) precompute.SketchFactory
	// emitWindowStats turns on the runtime's CountSketch-style
	// per-envelope sample_count / window_duration_seconds labels.
	// Only CountSketch needs it; mirrors the parity harness convention.
	emitWindowStats bool
	// globalAggregation collapses every observation into a single
	// shared series. Only CountSketch sets this true.
	globalAggregation bool
}

func (sd sketchDescriptor) precomputeConfig(cfg RuntimeConfig) *precompute.PrecomputeConfig {
	return &precompute.PrecomputeConfig{
		AggID:      precompute.AggId(uint64(sd.sketchType)),
		SketchType: sd.sketchType,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: cfg.WindowSize},
		Matchers: []precompute.LabelMatcher{
			{Name: "", Value: sd.metric},
		},
		MetricName:        sd.metricNameOut,
		TransmitSketch:    true,
		DeltaTransmission: false,
		Encoding:          precompute.EncodingProtoFull,
		Temporality:       1, // delta
		// OTel input uses an empty Resource() and Telegraf has no
		// resource scope at all, so resource attrs never contribute
		// to the SeriesKey on either path. We leave OmitResourceAttrs
		// at its default (false) — both sides see an empty resource
		// segment, and the resulting key is identical.
		OmitResourceAttrs: false,
		GlobalAggregation: sd.globalAggregation,
		EmitWindowStats:   sd.emitWindowStats,
	}
}

// allSketchDescriptors returns the canonical descriptor set for the
// five sketch domains. Both RunOTel and RunTelegraf iterate the same
// slice so per-sketch ordering and config are guaranteed identical.
func allSketchDescriptors(cfg RuntimeConfig) []sketchDescriptor {
	return []sketchDescriptor{
		{
			metric:        MetricDDSketch,
			sketchType:    precompute.SketchTypeDDSketch,
			metricNameOut: MetricDDSketch,
			observer:      ddSketchObserver{},
			factory: func(c RuntimeConfig) precompute.SketchFactory {
				return func() precompute.Sketch {
					return newDDSketchWrapper(c.DDSketchAlpha)
				}
			},
		},
		{
			metric:        MetricKLL,
			sketchType:    precompute.SketchTypeKLLSketch,
			metricNameOut: MetricKLL,
			observer:      kllObserver{},
			factory: func(c RuntimeConfig) precompute.SketchFactory {
				return func() precompute.Sketch {
					return newKLLWrapper(c.KLLK, HarnessKLLSeed)
				}
			},
		},
		{
			metric:        MetricHLL,
			sketchType:    precompute.SketchTypeHLLSketch,
			metricNameOut: MetricHLL,
			observer:      hllObserver{},
			factory: func(_ RuntimeConfig) precompute.SketchFactory {
				return func() precompute.Sketch {
					return newHLLWrapper()
				}
			},
		},
		{
			metric:        MetricCountSketch,
			sketchType:    precompute.SketchTypeCountSketch,
			metricNameOut: MetricCountSketch,
			observer:      countSketchObserver{metricName: MetricCountSketch},
			factory: func(c RuntimeConfig) precompute.SketchFactory {
				rows, cols := CountSketchDims(c.CountSketchEps, c.CountSketchDelta)
				return func() precompute.Sketch {
					return newCountSketchWrapper(rows, cols)
				}
			},
			// CountSketch's runtime emits per-window operator stats
			// (sample_count, window_duration_seconds) onto the
			// envelope's labels; pin EmitWindowStats=true for parity
			// with the broader project's convention.
			emitWindowStats: true,
			// Match the legacy countsketchprocessor's batch-mode
			// "single-global-bucket" behavior. Without this, every
			// (labelset, key) tuple would be its own sketch series
			// — fine for parity within one path, but it's the
			// project-wide convention to use one CountSketch per
			// emission window so we keep the same shape here.
			globalAggregation: true,
		},
		{
			metric:        MetricCMS,
			sketchType:    precompute.SketchTypeCountMinSketch,
			metricNameOut: MetricCMS,
			observer:      cmsObserver{},
			factory: func(c RuntimeConfig) precompute.SketchFactory {
				return func() precompute.Sketch {
					return newCMSWrapper(c.CMSRows, c.CMSCols)
				}
			},
		},
	}
}

// ===== Sketch observers =====
//
// One per sketch type. They translate ObservationValue kinds into the
// underlying sketch's update call. Mirrors the parity harness's
// observer set; both runtime paths in this harness reuse the same
// observer instance per sketch type, so the value-routing logic is
// identical on both sides by construction.

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
	w.updateValue(v.Float)
	return nil
}

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
