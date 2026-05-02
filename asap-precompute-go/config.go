package precompute

import "time"

// AggId is the controller-plan join key. One PrecomputeConfig per
// AggId; the controller's plan emits a flat list keyed by AggId.
type AggId uint64

// AggregationMode picks the windowing strategy.
type AggregationMode uint8

const (
	// Tumbling rotates the window every WindowSpec.Size; observations
	// land in exactly one window. This is what all five OTel
	// processors use today.
	Tumbling AggregationMode = iota
	// Sliding rotates every WindowSpec.Slide; observations land in
	// every window whose [start, end) range covers their timestamp.
	// Phase 2 implements only Tumbling — see window.go for the
	// follow-up.
	Sliding
	// Batch processes one batch end-to-end with no windowing; used
	// by the original ddsketchprocessor's `mode: batch` config.
	Batch
)

// String returns a debug name for the aggregation mode.
func (m AggregationMode) String() string {
	switch m {
	case Tumbling:
		return "Tumbling"
	case Sliding:
		return "Sliding"
	case Batch:
		return "Batch"
	}
	return "unknown"
}

// OnOverflow controls behavior when a window's series-cardinality
// would exceed PrecomputeConfig.MaxSeries.
//
// MaxSeries + OnOverflow are new fields necessary because extracting
// the runtime out of the OTel pipeline removes the implicit
// channel-based backpressure (ADR-0002 §"Public API").
type OnOverflow uint8

const (
	// OnOverflowDrop drops new observations whose series isn't
	// already tracked. Existing series keep accepting samples.
	OnOverflowDrop OnOverflow = iota
	// OnOverflowBlock blocks the caller until the next window
	// rotation frees capacity. Latency-hostile; reserved for
	// integration tests.
	OnOverflowBlock
	// OnOverflowEvictOldest evicts the least-recently-seen series
	// to make room for the new one.
	OnOverflowEvictOldest
)

// String returns a debug name for the overflow policy.
func (o OnOverflow) String() string {
	switch o {
	case OnOverflowDrop:
		return "Drop"
	case OnOverflowBlock:
		return "Block"
	case OnOverflowEvictOldest:
		return "EvictOldest"
	}
	return "unknown"
}

// WindowSpec configures the windowing parameters.
type WindowSpec struct {
	// Size is the window duration. For Tumbling, this is the rotation
	// period. For Sliding, it's the active-range length.
	Size time.Duration
	// Slide is the rotation interval for sliding windows. Zero means
	// tumbling (Slide == Size implicitly).
	Slide time.Duration
	// AllowedLateness is the grace period for late-arriving samples;
	// observations whose TimestampMs is older than activeStart -
	// AllowedLateness are dropped as late.
	AllowedLateness time.Duration
}

// SketchParams is a flexible bag of sketch-specific tuning knobs:
//
//	DDSketch:    "alpha" (relative accuracy in (0,1))
//	KLL:         "k"     (compactor capacity)
//	HLL:         "precision" (register bits)
//	CountSketch: "epsilon", "delta" (or "width", "depth")
//	CountMin:    "epsilon", "delta"
//
// Encoded as a plain map so adapters can stuff in algorithm-specific
// values without the runtime needing a typed schema for each.
type SketchParams map[string]float64

// Get returns the parameter value or the default if not present.
func (p SketchParams) Get(key string, def float64) float64 {
	if v, ok := p[key]; ok {
		return v
	}
	return def
}

// PrecomputeConfig is the host-neutral form of today's per-OTel-processor
// Config struct, plus MaxSeries / OnOverflow. Mirrors design-doc §6.3.
type PrecomputeConfig struct {
	// AggID is the controller-plan join key. One PrecomputeConfig
	// per AggId.
	AggID AggId
	// SketchType determines which Sketch implementation handles
	// observations for this AggId.
	SketchType SketchType
	// Mode picks the windowing strategy.
	Mode AggregationMode
	// Window configures size / slide / lateness.
	Window WindowSpec
	// Matchers select observations by metric name and label values.
	// All matchers must hold for the observation to be admitted.
	Matchers []LabelMatcher
	// AggregateBy lists label keys to aggregate over. When
	// non-empty, the SeriesKey is built only from these keys
	// (cross-series aggregation). When empty, every distinct
	// label set is its own series.
	AggregateBy []string
	// TransmitSketch chooses between sketch-on-the-wire and
	// quantile-summary-on-the-wire output (see DDSketch's existing
	// buildMergedSketchMetric vs buildQuantileMetric).
	TransmitSketch bool
	// DeltaTransmission enables delta encoding against the cached
	// outbound snapshot.
	DeltaTransmission bool
	// DeltaThreshold caps the absolute delta size; if the computed
	// delta exceeds DeltaThreshold * full-state size, the runtime
	// emits the full state instead. Mirrors today's
	// ddsketch.ComputeDelta threshold semantics. The unit is
	// sketch-specific (uint64 for DDSketch/CMS bucket counts;
	// float64 for CountSketch L2 cells, stored here as
	// rounded-up uint64 — adapters convert as needed).
	DeltaThreshold uint64
	// Encoding overrides the wire encoding of emitted envelopes.
	// Default zero value (PROTO_FULL) is used for non-delta
	// transmissions. When DeltaTransmission is true, the runtime
	// emits PROTO_DELTA frames after the initial PROTO_FULL
	// snapshot. MSGPACK is supported for legacy paths
	// (HLL / CountSketch / CMS today carry an explicit `encoding`
	// knob in their existing config; map to MSGPACK when set).
	Encoding Encoding
	// Quantiles controls the output mode when TransmitSketch is
	// false. For QuantileSketch types (DDSketch / KLL), each
	// quantile in this list becomes a separate gauge metric on
	// emit. Empty list means "always emit envelope" (semantically
	// equivalent to TransmitSketch=true). Ignored for non-quantile
	// sketches (HLL / CountSketch / CMS).
	Quantiles []float64
	// SketchParams holds algorithm-specific tuning knobs. The
	// expected keys per sketch type are:
	//   DDSketch       : "relative_accuracy"          (float, 0<v<1)
	//   KLL            : "k"                          (uint, ≥8)
	//   HLL            : "precision"                  (uint, 4-18)
	//   CountSketch    : "epsilon", "delta"           (floats, 0<v<1)
	//                    "width", "depth" (derived if absent)
	//   CountMin       : "rows", "columns"            (uints)
	// SketchParams is a flat map for now to keep config
	// deserialization simple; a future ADR may switch to a
	// sum-typed struct if the map approach grows footguns.
	SketchParams SketchParams
	// MaxSeries caps the per-Precompute series cardinality. Zero
	// means unbounded (matches today's processors which had no cap).
	MaxSeries uint64
	// OnOverflow controls behavior when MaxSeries is exceeded.
	OnOverflow OnOverflow
	// MetricName is the metric name to stamp onto every
	// SketchEnvelope emitted by Tick. Mirrors today's per-processor
	// MetricName config knob (see e.g. ddsketchprocessor.Config.
	// MetricName). The runtime copies it verbatim into
	// SketchEnvelope.MetricName so Adapter.Encode can set
	// pmetric.Metric.Name() without a side-channel label hack.
	MetricName string
	// Temporality is the OTel aggregation-temporality enum to stamp
	// onto every SketchEnvelope emitted by Tick: 0 = unspecified,
	// 1 = delta, 2 = cumulative. Stored as int32 (not
	// pmetric.AggregationTemporality) to keep the runtime
	// host-neutral. Today's processors emit delta sums, so adapters
	// that don't set this explicitly will see the zero value
	// (unspecified) and should default to 1 (delta) themselves;
	// the Phase-2 OTel shim sets it to 1.
	Temporality int32
	// OmitResourceAttrs controls whether resource-scope attributes
	// participate in series-key construction (and whether they are
	// carried through to the emitted SketchEnvelope's ResourceLabels).
	// The zero value (false) means "include resource attrs" — the
	// runtime's default and the shape today's DDSketch processor
	// expects: SeriesKey distinguishes (resource, dp-labels) tuples.
	//
	// The four other legacy processors (KLL, HLL, CountSketch,
	// CountMinSketch) build their batch-mode series key from the
	// data-point attributes ONLY: they ignore resource attrs in the
	// key AND emit output into a freshly-appended ResourceMetrics
	// with an empty Resource. Setting this knob to true makes the
	// runtime mirror that behavior: cross-resource observations with
	// the same dp-labels collapse into a single series, and the
	// emitted SketchEnvelope.ResourceLabels is empty.
	//
	// This is part of the Phase-2 parity gate (ADR-0002 "Behavior
	// preservation"); shims that wrap the legacy processors set this
	// to true to keep wire-bytes identical to the pre-refactor
	// emitter. Naming note: the field is stated as `Omit*` rather
	// than `Include*` so the zero value is the today-correct default
	// and existing PrecomputeConfig literals don't need to change.
	OmitResourceAttrs bool
	// GlobalAggregation collapses every admitted observation into a
	// single "global" series — both resource attrs and dp-labels are
	// ignored when constructing SeriesKey, and both are stripped from
	// the emitted SketchEnvelope. Used by the legacy CountSketch
	// processor when AggregateBy is empty (its `buildPartitionKey`
	// returns the literal string "global", driving every observation
	// into one shared sketch). When this is true the
	// IncludeResourceAttrs flag is also implicitly false.
	//
	// Defaults to false; only the CountSketch shim sets it to true.
	GlobalAggregation bool
}

// SeriesKeyFor builds the canonical series key for an Observation,
// honoring the OmitResourceAttrs and GlobalAggregation flags.
//
// This is the single call site used by both the window's observe
// path and the serializeSeries flush path so the two stay aligned —
// a divergence between them would silently mis-route or duplicate
// envelopes.
//
// Encoding rules:
//   - GlobalAggregation=true   → key = "<aggID>|||" (one global bucket)
//   - OmitResourceAttrs=true   → resource segment is empty, dp segment
//     is the AttributesKey of obs.Labels (legacy KLL/HLL/CMS shape)
//   - default                  → resource and dp segments both populated
//     (today's DDSketch shape)
func (cfg *PrecomputeConfig) SeriesKeyFor(obs *Observation) string {
	if cfg == nil {
		return ""
	}
	if cfg.GlobalAggregation {
		return SeriesKey(cfg.AggID, nil, nil, nil)
	}
	if cfg.OmitResourceAttrs {
		return SeriesKey(cfg.AggID, nil, obs.Labels, cfg.AggregateBy)
	}
	return SeriesKey(cfg.AggID, obs.ResourceLabels, obs.Labels, cfg.AggregateBy)
}

// SeriesKeyForEntry rebuilds the same key from a series entry's
// stored labels. Used by serializeSeries on flush; the invariant is
// that for a given config and an observation that produced an entry,
// SeriesKeyFor(obs) == SeriesKeyForEntry(entry).
func (cfg *PrecomputeConfig) SeriesKeyForEntry(resourceLabels, labels []KeyValue) string {
	if cfg == nil {
		return ""
	}
	if cfg.GlobalAggregation {
		return SeriesKey(cfg.AggID, nil, nil, nil)
	}
	if cfg.OmitResourceAttrs {
		return SeriesKey(cfg.AggID, nil, labels, cfg.AggregateBy)
	}
	return SeriesKey(cfg.AggID, resourceLabels, labels, cfg.AggregateBy)
}

// Encoding (the type and its constants/String method) is declared in
// envelope.go. config.go references those names but does not redeclare
// them — keeping the canonical definition next to SketchEnvelope where
// the wire encoding actually lives.

// PrecomputeConfigSet is a versioned bundle of configs delivered by
// ControlChannel. Mirrors design-doc §8.
type PrecomputeConfigSet struct {
	// Version is monotonically increasing across plans; ack with
	// ControlChannel.Ack.
	Version uint64
	// Configs lists the PrecomputeConfigs active for this host.
	Configs []PrecomputeConfig
}

// FindByAggID returns the config matching aggID, or nil.
func (s *PrecomputeConfigSet) FindByAggID(aggID AggId) *PrecomputeConfig {
	if s == nil {
		return nil
	}
	for i := range s.Configs {
		if s.Configs[i].AggID == aggID {
			return &s.Configs[i]
		}
	}
	return nil
}
