package precompute

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// Sketch is the narrow interface the Layer-3 runtime needs from a
// Layer-1 sketch implementation. Real sketches (DDSketch, KLL, HLL,
// CountSketch, CountMinSketch) live in sketchlib-go; thin wrappers
// in steps 2.5–2.9 implement this interface against each concrete
// sketch's methods.
//
// The interface is intentionally minimal — Observe is per-sketch
// (because each sketch type has different value-shapes: float,
// hash, bytes), so the routing layer above lives in the per-shim
// glue, not here.
type Sketch interface {
	// Snapshot serializes the current sketch state to a portable
	// proto-encoded byte slice. Used by ComputeDelta and as the
	// PROTO_FULL payload.
	Snapshot() ([]byte, error)
	// ComputeDeltaAgainst computes a sparse delta between this
	// sketch and the previous snapshot bytes; if the resulting
	// delta is at least as large as a full snapshot scaled by
	// threshold, returns the full state with isFull=true so the
	// caller can avoid wasted work re-marshaling. Threshold is the
	// per-sketch absolute count cap (see today's
	// computeDDSketchDelta / countsketch.ComputeDelta).
	ComputeDeltaAgainst(prev []byte, threshold uint64) (delta []byte, isFull bool, err error)
	// ApplyDelta merges a previously-computed delta into this
	// sketch in place. Used by Precompute.ObserveEnvelope when an
	// inbound envelope is encoded as PROTO_DELTA.
	ApplyDelta(delta []byte) error
	// Merge folds another sketch (typically a freshly-decoded
	// envelope payload) into this one. Used by
	// Precompute.ObserveEnvelope on PROTO_FULL inbound envelopes.
	Merge(other Sketch) error
	// Reset zeros the sketch in place. Used by window rotation
	// and by sketch object pools.
	Reset()
}

// QuantileSketch is implemented by sketches that can answer
// quantile queries. DDSketch and KLL are the two QuantileSketch
// implementations in sketchlib-go. The runtime never type-asserts
// to QuantileSketch — only adapter code does, when materializing
// typed quantile output (e.g. emitting one gauge per configured
// quantile when TransmitSketch=false).
type QuantileSketch interface {
	Sketch
	// Quantile returns the q-th rank value (0 ≤ q ≤ 1) from the
	// sketch's current state. Implementations should clamp q to
	// [0,1] and return a finite value (NaN is acceptable for an
	// empty sketch).
	Quantile(q float64) float64
}

// CardinalitySketch is implemented by sketches that answer
// distinct-count queries. HyperLogLog is the canonical
// CardinalitySketch in sketchlib-go. Adapter code type-asserts
// `s.(CardinalitySketch)` to call EstimateCardinality() when
// emitting a typed cardinality gauge from an HLL-backed envelope.
type CardinalitySketch interface {
	Sketch
	// EstimateCardinality returns the sketch's current
	// distinct-element estimate as a float (HLL's bias-corrected
	// estimator returns a non-integer; callers round if they want
	// an integer gauge).
	EstimateCardinality() float64
}

// FrequencySketch is implemented by sketches that answer
// count/top-k queries. CountSketch and CountMinSketch are the
// two FrequencySketch implementations in sketchlib-go. Adapter
// code type-asserts `s.(FrequencySketch)` to materialize per-key
// counts or top-k tables.
type FrequencySketch interface {
	Sketch
	// EstimateCount returns the estimated frequency for the given
	// key. Implementations may return a non-integer value (e.g.
	// CountSketch's median-of-rows estimator).
	EstimateCount(key []byte) float64
	// TopK returns the top-k highest-frequency entries observed by
	// the sketch. Order is descending by Count; implementations
	// may return fewer than k entries when the sketch hasn't seen
	// enough distinct keys.
	TopK(k int) []FrequencyEntry
}

// FrequencyEntry is one entry in a FrequencySketch.TopK result.
// Key is the opaque byte slice the sketch indexes by (the same
// shape passed to ObservationValue.Bytes); Count is the estimated
// frequency.
type FrequencyEntry struct {
	Key   []byte
	Count float64
}

// SketchFactory constructs an empty Sketch of the type owned by a
// specific Precompute instance. Phase 2 keeps construction
// per-Precompute rather than registry-based to avoid global state.
type SketchFactory func() Sketch

// SketchObserver is implemented by the per-shim glue that knows
// how to feed a raw observation (Float / Hash / Bytes) into the
// concrete sketch. The runtime calls Observe on this interface
// after admitting an Observation through matchers + window.
//
// SketchObserver is NOT part of the Sketch interface itself
// because the value type is sketch-specific (DDSketch wants float,
// HLL wants hash, set-aggregator wants bytes), and forcing all
// sketches to accept all kinds would push pointless type-switches
// into every Layer-1 implementation.
type SketchObserver interface {
	// Observe applies the observation's Value to the sketch.
	// Implementations should panic-proof against unsupported kinds
	// (return an error) and call Sketch methods directly.
	Observe(s Sketch, v ObservationValue) error
}

// Errors returned by Precompute.
var (
	// ErrSeriesCapExceeded is returned by Observe when MaxSeries
	// is reached and OnOverflow is OnOverflowDrop.
	ErrSeriesCapExceeded = errors.New("precompute: series cap exceeded")
	// ErrLateData is returned by Observe when an observation's
	// timestamp is older than the active window's lower bound
	// minus AllowedLateness.
	ErrLateData = errors.New("precompute: observation timestamp outside allowed lateness")
	// ErrNoConfig is returned when Precompute has no PrecomputeConfig.
	ErrNoConfig = errors.New("precompute: no config installed")
	// ErrAggIDMismatch is returned by ObserveEnvelope when the
	// envelope's AggID doesn't match this Precompute's config.
	ErrAggIDMismatch = errors.New("precompute: envelope agg_id does not match config")
	// ErrSketchTypeMismatch is returned by ObserveEnvelope when
	// the envelope's SketchType doesn't match this Precompute's
	// config.
	ErrSketchTypeMismatch = errors.New("precompute: envelope sketch_type does not match config")
)

// Precompute is the host-neutral state machine described in
// design-doc §6.2. One Precompute instance owns one sketch type
// (see config.SketchType); a deployment with multiple sketch types
// runs multiple Precompute instances side-by-side.
type Precompute interface {
	// Observe routes a raw observation into the active window.
	// May return ErrSeriesCapExceeded or ErrLateData; other errors
	// indicate config/state problems.
	Observe(obs *Observation) error
	// ObserveEnvelope merges a pre-aggregated upstream sketch into
	// the active window. The envelope's bytes are NEVER expanded
	// to scalar samples (design-doc §5.2 bandwidth invariant).
	ObserveEnvelope(env *SketchEnvelope) error
	// Tick rotates the active window and returns the closed
	// window's series as []SketchEnvelope ready for emit. nowMs
	// is the wall-clock time the caller (Adapter.ScheduleTick)
	// considers "now"; the runtime uses it to decide whether the
	// window is due for rotation.
	Tick(nowMs uint64) []*SketchEnvelope
	// UpdateConfig atomically swaps the active config. The
	// in-flight window is preserved (matchers/aggregateBy may
	// change, but bytes already accumulated stay where they are);
	// see ADR-0003 §3 for why this is the only way every adapter
	// updates config.
	UpdateConfig(cs *PrecomputeConfigSet)
	// Stats returns the live counters; safe to call concurrently.
	Stats() *PrecomputeStats
	// Shutdown flushes any in-progress state; intended for the
	// shim's Shutdown path to run a final Tick before returning.
	Shutdown(ctx context.Context) error
}

// precompute is the concrete implementation of Precompute.
//
// Lock discipline:
//   - cfg is an atomic.Pointer; UpdateConfig swaps the pointer in
//     place rather than locking around the struct.
//   - window has its own mutex (see window.go).
//   - snapshotCache has its own RWMutex (see snapshot_cache.go).
//
// No global mutex around the Precompute itself.
type precompute struct {
	cfg           atomic.Pointer[PrecomputeConfig]
	sketchFactory SketchFactory
	observer      SketchObserver
	window        *windowState
	snapshotCache *SnapshotCache
	stats         *PrecomputeStats
	sketchType    SketchType
	closed        atomic.Bool
}

// New constructs a Precompute given an initial config, a sketch
// factory that produces empty sketches of the configured type,
// and a SketchObserver that knows how to apply ObservationValue
// kinds to the sketch.
//
// Either cfg or observer may be nil at construction time, but
// Observe will return ErrNoConfig until UpdateConfig is called.
func New(initialCfg *PrecomputeConfig, sketchFactory SketchFactory, observer SketchObserver) Precompute {
	p := &precompute{
		sketchFactory: sketchFactory,
		observer:      observer,
		window:        newWindowState(),
		snapshotCache: NewSnapshotCache(),
		stats:         NewPrecomputeStats(),
	}
	if initialCfg != nil {
		cfgCopy := *initialCfg
		p.cfg.Store(&cfgCopy)
		p.sketchType = initialCfg.SketchType
	}
	return p
}

// activeConfig returns the currently installed config or nil.
func (p *precompute) activeConfig() *PrecomputeConfig {
	return p.cfg.Load()
}

// Observe implements Precompute.Observe.
func (p *precompute) Observe(obs *Observation) error {
	if p.closed.Load() {
		return errors.New("precompute: instance is closed")
	}
	cfg := p.activeConfig()
	if cfg == nil {
		return ErrNoConfig
	}
	p.stats.InputObservations.Add(1)

	// Envelope-valued observations route through the dedicated
	// pre-aggregated path so we never explode them to scalars.
	if obs.Value.Kind == KindEnvelope && obs.Value.Envelope != nil {
		return p.ObserveEnvelope(obs.Value.Envelope)
	}

	if !cfg.Matches(obs) {
		return nil
	}

	if p.sketchFactory == nil {
		return errors.New("precompute: sketch factory not configured")
	}
	if p.observer == nil {
		return errors.New("precompute: sketch observer not configured")
	}

	if err := p.window.observe(obs, cfg, p.sketchFactory, p.observer, p.stats); err != nil {
		switch {
		case errors.Is(err, ErrSeriesCapExceeded):
			p.stats.DroppedOverflow.Add(1)
		case errors.Is(err, ErrLateData):
			p.stats.DroppedLate.Add(1)
		}
		return err
	}
	return nil
}

// ObserveEnvelope implements Precompute.ObserveEnvelope.
func (p *precompute) ObserveEnvelope(env *SketchEnvelope) error {
	if p.closed.Load() {
		return errors.New("precompute: instance is closed")
	}
	if env == nil {
		return errors.New("precompute: nil envelope")
	}
	cfg := p.activeConfig()
	if cfg == nil {
		return ErrNoConfig
	}
	// AggID match — strict, per design-doc §5.2 enforcement
	// point #4. Mismatches are hard errors, not silent drops.
	if cfg.AggID != 0 && env.AggID != 0 && env.AggID != cfg.AggID {
		return fmt.Errorf("%w: envelope=%d config=%d", ErrAggIDMismatch, env.AggID, cfg.AggID)
	}
	if cfg.SketchType != SketchTypeUnspecified && env.SketchType != SketchTypeUnspecified && env.SketchType != cfg.SketchType {
		return fmt.Errorf("%w: envelope=%s config=%s", ErrSketchTypeMismatch, env.SketchType, cfg.SketchType)
	}

	if p.sketchFactory == nil {
		return errors.New("precompute: sketch factory not configured")
	}
	p.stats.InputObservations.Add(1)
	if err := p.window.observeEnvelope(env, cfg, p.sketchFactory, p.snapshotCache, p.stats); err != nil {
		if errors.Is(err, ErrSeriesCapExceeded) {
			p.stats.DroppedOverflow.Add(1)
		}
		return err
	}
	return nil
}

// Tick implements Precompute.Tick.
//
// Behavior: rotates the window when nowMs >= activeEndMs. For
// Tumbling, this is "drain everything older than now". For Batch,
// every Tick drains. Sliding is deferred (see window.go).
func (p *precompute) Tick(nowMs uint64) []*SketchEnvelope {
	cfg := p.activeConfig()
	if cfg == nil {
		return nil
	}
	closed, rng := p.window.rotate(nowMs, cfg)
	if len(closed) == 0 {
		return nil
	}
	envelopes := make([]*SketchEnvelope, 0, len(closed))
	for _, entry := range closed {
		env, err := p.serializeSeries(entry, cfg, rng)
		if err != nil {
			// Best-effort: skip this series and continue. Real
			// shims log via their host's logger; the Layer-3
			// runtime is host-neutral and has no logger.
			continue
		}
		if env != nil {
			envelopes = append(envelopes, env)
		}
	}
	p.stats.OutputEnvelopes.Add(uint64(len(envelopes)))
	p.stats.LastTickMs.Store(nowMs)
	return envelopes
}

// serializeSeries turns a closed series entry into a SketchEnvelope.
// Honors DeltaTransmission via the snapshot cache.
func (p *precompute) serializeSeries(entry *seriesEntry, cfg *PrecomputeConfig, rng [2]uint64) (*SketchEnvelope, error) {
	if entry == nil || entry.Sketch == nil {
		return nil, nil
	}
	// Rebuild the same key the window used at admit time. Going
	// through cfg.SeriesKeyForEntry guarantees the snapshot-cache
	// lookup in the delta path agrees with the observe-time bucket
	// regardless of the OmitResourceAttrs / GlobalAggregation flags.
	seriesKey := cfg.SeriesKeyForEntry(entry.ResourceLabels, entry.Labels)
	var (
		payload []byte
		isFull  bool
		enc     Encoding
		err     error
	)
	if cfg.DeltaTransmission {
		payload, isFull, err = p.snapshotCache.ComputeDelta(seriesKey, entry.Sketch, cfg.DeltaThreshold)
		if err != nil {
			return nil, fmt.Errorf("compute delta: %w", err)
		}
		if isFull {
			enc = EncodingProtoFull
		} else {
			enc = EncodingProtoDelta
		}
	} else {
		payload, err = entry.Sketch.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}
		enc = EncodingProtoFull
		// Even without delta transmission, refreshing the cached
		// outbound snapshot keeps the cache consistent for any
		// later config change that flips DeltaTransmission to true.
		p.snapshotCache.CacheOutbound(seriesKey, payload)
	}
	if payload == nil {
		return nil, nil
	}
	labels := SeriesAttrs(entry.Labels, cfg.AggregateBy)
	if cfg.EmitWindowStats {
		// Append the two operator-visibility attrs the legacy
		// countsketchprocessor stamps onto each emitted data point
		// (see countsketchprocessor/processor.go ~line 452). Adding
		// them at the envelope-Labels layer makes them flow through
		// otel/encode.go::KeyValuesToAttributes naturally, so
		// runtime and legacy data points carry the same attribute
		// set without a diff-side projection-strip. Other sketches
		// leave EmitWindowStats=false; their parity stays untouched.
		windowSeconds := uint64(cfg.Window.Size / time.Second)
		labels = append(labels,
			KeyValue{Key: "sample_count", Value: strconv.FormatUint(entry.Count, 10)},
			KeyValue{Key: "window_duration_seconds", Value: strconv.FormatUint(windowSeconds, 10)},
		)
	}
	return &SketchEnvelope{
		SchemaVersion:          1,
		SketchType:             cfg.SketchType,
		AggID:                  cfg.AggID,
		ResourceLabels:         entry.ResourceLabels,
		Labels:                 labels,
		WindowStartMs:          rng[0],
		WindowEndMs:            rng[1],
		Encoding:               enc,
		Payload:                payload,
		MetricName:             cfg.MetricName,
		Count:                  entry.Count,
		AggregationTemporality: cfg.Temporality,
	}, nil
}

// UpdateConfig implements Precompute.UpdateConfig.
//
// Currently picks the FIRST config in the set whose AggID matches
// the active config (or the first one if no active config). A
// future refactor will route between multiple configs by AggID.
func (p *precompute) UpdateConfig(cs *PrecomputeConfigSet) {
	if cs == nil || len(cs.Configs) == 0 {
		return
	}
	active := p.activeConfig()
	var chosen *PrecomputeConfig
	if active != nil {
		for i := range cs.Configs {
			if cs.Configs[i].AggID == active.AggID {
				chosen = &cs.Configs[i]
				break
			}
		}
	}
	if chosen == nil {
		chosen = &cs.Configs[0]
	}
	cfgCopy := *chosen
	p.cfg.Store(&cfgCopy)
	p.sketchType = cfgCopy.SketchType
}

// Stats implements Precompute.Stats.
func (p *precompute) Stats() *PrecomputeStats {
	return p.stats
}

// Shutdown implements Precompute.Shutdown.
func (p *precompute) Shutdown(ctx context.Context) error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Best-effort final tick to drain any in-flight window. Use
	// the deadline if present to bound work.
	deadline, ok := ctx.Deadline()
	_ = deadline
	_ = ok
	return ctx.Err()
}
