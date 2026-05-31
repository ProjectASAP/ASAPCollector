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
	// every window whose [start, end) range covers their timestamp, so
	// successive emitted windows OVERLAP (a sample near a pane boundary
	// is re-emitted in each window that still covers it).
	//
	// WARNING — do NOT use Sliding for output shipped to an additive-merge
	// backend (e.g. ASAPQuery-backend, which merges consecutive edge
	// sketches cell-/bucket-additively assuming NON-overlapping windows):
	// overlapping emissions would be counted multiple times → inflated
	// frequencies/quantiles. The documented architecture keeps the EDGE
	// tumbling and does sliding at the BACKEND's window_manager. Sliding
	// here is for standalone/local aggregation that is not fed into the
	// additive two-stage merge. The asap_edge OTel processor only ever
	// configures Tumbling, so this footgun is not reachable from it today.
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

// AggMode picks the aggregation SCOPE — how a window groups observations into
// sketch instances — orthogonally to the windowing strategy (AggregationMode,
// which is Tumbling/Sliding/Batch). One config carries both: a Tumbling
// PerSeries config is today's default behavior; a Tumbling WholeStream config
// collapses every matching datapoint into a single sketch per AggID.
//
// The control plane selects the scope per aggregation given the query:
// per-label-group queries → PerSeries; distinct-count / global-quantile /
// grand-total / global-top-k queries → WholeStream.
//
// Naming note: the struct field is PrecomputeConfig.Scope rather than `Mode`
// because PrecomputeConfig.Mode is already the AggregationMode (windowing)
// field; the type stays AggMode and the constants stay Mode* so the enum reads
// naturally at use sites (cfg.Scope == ModeWholeStream).
type AggMode uint8

const (
	// ModePerSeries is the default (zero value): one sketch per series key
	// (per AggregateBy group, honoring OmitResourceAttrs). Each datapoint's
	// value folds into its own series' sketch and the window emits one
	// envelope per series. This is the historical behavior, so existing
	// plans that never set Scope are byte-compatible.
	ModePerSeries AggMode = iota
	// ModeWholeStream collapses grouping: a single sketch instance per AggID
	// (per shard, merged at flush) ingests from EVERY matching datapoint
	// regardless of series identity, and the window emits exactly one
	// envelope per AggID per window. Resource attrs and data-point labels are
	// stripped from both the series key and the emitted envelope (the emitted
	// labels are AggregateBy-derived only). Per-family the ingested subject is
	// the metric VALUE by default (Sum=grand total, DDSketch/KLL=pooled
	// distribution, HLL=distinct values, CMS/CountSketch=value frequency);
	// when an item source is configured (asap_edge ItemLabel) the inner item
	// dimension is ingested instead (HLL=distinct items, CMS/TopK=heavy items).
	// MaxSeries is a no-op in this scope (cardinality is 1).
	ModeWholeStream
)

// String returns a debug name for the aggregation scope.
func (m AggMode) String() string {
	switch m {
	case ModePerSeries:
		return "PerSeries"
	case ModeWholeStream:
		return "WholeStream"
	}
	return "unknown"
}

// ParseAggMode parses an operator-facing scope string into an AggMode. It
// accepts the canonical "per_series" / "whole_stream" spellings (plus a few
// tolerant aliases), and an empty string maps to ModePerSeries (the default)
// so an omitted `mode:` config key keeps today's behavior. An unrecognized
// non-empty value returns ok=false so callers can reject it at validation.
func ParseAggMode(s string) (AggMode, bool) {
	switch s {
	case "", "per_series", "perseries", "PerSeries", "series":
		return ModePerSeries, true
	case "whole_stream", "wholestream", "WholeStream", "whole", "global":
		return ModeWholeStream, true
	}
	return ModePerSeries, false
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
	// AggKind is the umbrella aggregation kind stamped onto every emitted
	// envelope (Sketch vs Sum). Unset (AggKindUnspecified) is resolved to
	// AggKindSketch for any SketchType-bearing config, so existing sketch
	// configs are unaffected; a Sum config sets AggKindSum.
	AggKind AggregationKind
	// Mode picks the windowing strategy (Tumbling / Sliding / Batch).
	Mode AggregationMode
	// Scope picks the aggregation SCOPE (PerSeries vs WholeStream),
	// orthogonal to Mode's windowing strategy. Zero value (ModePerSeries) is
	// today's behavior, so existing plans are byte-compatible. WholeStream
	// collapses every matching observation into a single sketch per AggID and
	// emits one envelope per AggID per window. See AggMode.
	//
	// Scope subsumes the legacy GlobalAggregation bool: a config with
	// GlobalAggregation=true is treated as Scope=ModeWholeStream by
	// effectiveScope(), so the two never disagree and one WholeStream code
	// path drives both. Prefer Scope in new configs.
	Scope AggMode
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
	//                    "sparse"                     (0=dense default, 1=sparse base)
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
	// GlobalAggregation is the LEGACY alias for Scope=ModeWholeStream; it
	// collapses every admitted observation into a single "global" series —
	// both resource attrs and dp-labels are ignored when constructing
	// SeriesKey, and both are stripped from the emitted SketchEnvelope. Used
	// by the legacy CountSketch / heap path when AggregateBy is empty (its
	// `buildPartitionKey` returned the literal string "global", driving every
	// observation into one shared sketch).
	//
	// DEPRECATED: set Scope=ModeWholeStream instead. This bool is retained
	// for backward compatibility and is folded into the unified scope by
	// effectiveScope() — a config with EITHER GlobalAggregation=true OR
	// Scope=ModeWholeStream behaves identically through the one WholeStream
	// code path. New code should not read GlobalAggregation directly; call
	// effectiveScope() (or isWholeStream) so the two stay aligned.
	//
	// Defaults to false; existing CountSketch-heap shims still set it to true.
	GlobalAggregation bool

	// EmitWindowStats appends two operator-visibility attributes onto
	// every emitted SketchEnvelope's Labels at flush time:
	//   - "sample_count"            (entry.Count, the number of admitted
	//                                observations contributing to the window)
	//   - "window_duration_seconds" (Window.Size, in whole seconds)
	//
	// These mirror the legacy countsketchprocessor's per-data-point
	// attributes; the backend ignores them for routing, they're just
	// observability hints. The runtime omits them by default because
	// the other 4 sketch processors (DDSketch / KLL / HLL / CMS) do
	// not emit them, and adding them unconditionally would break
	// byte-parity for those four. CountSketch's parity harness flips
	// this to true so its envelope shape matches the legacy emission
	// without a diff-side projection-strip.
	EmitWindowStats bool
}

// effectiveScope resolves the aggregation scope, folding the legacy
// GlobalAggregation bool into the unified AggMode: GlobalAggregation=true is
// read as ModeWholeStream so the single WholeStream code path drives both. A
// nil config is PerSeries (the default).
func (cfg *PrecomputeConfig) effectiveScope() AggMode {
	if cfg == nil {
		return ModePerSeries
	}
	if cfg.Scope == ModeWholeStream || cfg.GlobalAggregation {
		return ModeWholeStream
	}
	return ModePerSeries
}

// isWholeStream reports whether this config aggregates the whole stream into a
// single sketch per AggID (Scope=ModeWholeStream, or the legacy
// GlobalAggregation alias).
func (cfg *PrecomputeConfig) isWholeStream() bool {
	return cfg.effectiveScope() == ModeWholeStream
}

// SeriesKeyFor builds the canonical series key for an Observation,
// honoring the OmitResourceAttrs flag and the aggregation scope.
//
// This is the single call site used by both the window's observe
// path and the serializeSeries flush path so the two stay aligned —
// a divergence between them would silently mis-route or duplicate
// envelopes.
//
// Encoding rules:
//   - WholeStream (or legacy GlobalAggregation) → key = "<aggID>|||" (one
//     global bucket per AggID; resource + dp labels collapsed)
//   - OmitResourceAttrs=true   → resource segment is empty, dp segment
//     is the AttributesKey of obs.Labels (legacy KLL/HLL/CMS shape)
//   - default                  → resource and dp segments both populated
//     (today's DDSketch shape)
func (cfg *PrecomputeConfig) SeriesKeyFor(obs *Observation) string {
	if cfg == nil {
		return ""
	}
	if cfg.isWholeStream() {
		return SeriesKey(cfg.AggID, nil, nil, nil)
	}
	if cfg.OmitResourceAttrs {
		return SeriesKey(cfg.AggID, nil, obs.Labels, cfg.AggregateBy)
	}
	return SeriesKey(cfg.AggID, obs.ResourceLabels, obs.Labels, cfg.AggregateBy)
}

// buildSeriesKey is the zero-alloc twin of SeriesKeyFor: it appends
// the key into the scratch buffer (already reset by the caller)
// instead of returning a freshly-allocated string. Output is
// byte-identical to SeriesKeyFor so the resulting map key matches
// the one serializeSeries rebuilds via SeriesKeyForEntry.
func (cfg *PrecomputeConfig) buildSeriesKey(s *seriesKeyScratch, obs *Observation) {
	if cfg == nil {
		return
	}
	if cfg.isWholeStream() {
		s.appendSeriesKey(cfg.AggID, nil, nil, nil)
		return
	}
	if cfg.OmitResourceAttrs {
		s.appendSeriesKey(cfg.AggID, nil, obs.Labels, cfg.AggregateBy)
		return
	}
	s.appendSeriesKey(cfg.AggID, obs.ResourceLabels, obs.Labels, cfg.AggregateBy)
}

// SeriesKeyForEntry rebuilds the same key from a series entry's
// stored labels. Used by serializeSeries on flush; the invariant is
// that for a given config and an observation that produced an entry,
// SeriesKeyFor(obs) == SeriesKeyForEntry(entry).
func (cfg *PrecomputeConfig) SeriesKeyForEntry(resourceLabels, labels []KeyValue) string {
	if cfg == nil {
		return ""
	}
	if cfg.isWholeStream() {
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
