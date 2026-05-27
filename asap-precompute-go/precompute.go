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
//
// When the host wires a sketch pool (see SketchSink), the factory is
// the pool's Get side: it returns a recycled, already-Reset sketch
// when one is available and a fresh one otherwise.
type SketchFactory func() Sketch

// SketchSink receives a series' sketch at flush time, after its
// envelope has been serialized and the Precompute is done with it.
// It is the symmetric Put side of a host-provided sketch pool: a
// pooled adapter Resets the sketch and returns it for reuse next
// window. When no sink is installed the sketch is simply dropped and
// garbage-collected (the prior behavior).
type SketchSink func(Sketch)

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

// LatencyObserver is the host-neutral hook that the runtime invokes
// for every Observe call with the wall-clock duration spent inside
// Observe (matchers, sketch factory, observer dispatch, window
// admission). Adapters wire this to a Prometheus / OTel histogram so
// the deployed shim publishes per-observation latency continuously,
// not just under `testing.B` (Phase 2.11B gap #3 — closes the
// deployment-level confirmation of ADR-0002 §"Performance contract").
//
// The hook is invoked exactly once per Observe call regardless of
// outcome (success, ErrSeriesCapExceeded, ErrLateData, matcher miss).
// Implementations must be cheap — the hook runs on the hot path. Nil
// observers (the default) cost a single nil-check.
type LatencyObserver func(d time.Duration)

// Precompute is the host-neutral runtime described in
// design-doc §6.2. One Precompute instance owns one sketch type
// (see config.SketchType); a deployment with multiple sketch types
// runs multiple Precompute instances side-by-side.
type Precompute interface {
	// Observe routes a raw observation into the active window.
	// May return ErrSeriesCapExceeded or ErrLateData; other errors
	// indicate config/state problems.
	Observe(obs *Observation) error
	// ObserveKeyed is Observe with a caller-supplied series key (built
	// once upstream by the fused asap_edge processor), skipping the
	// internal buildSeriesKey. key MUST equal cfg.SeriesKeyFor(obs).
	ObserveKeyed(key string, obs *Observation) error
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
	// Drain forces rotation of the active window regardless of
	// wall-clock time and returns any envelopes that result. Use
	// this on shutdown paths to flush pending observations that
	// haven't reached their natural window boundary.
	//
	// Distinct from Tick(nowMs): Tick only rotates when nowMs >=
	// activeEndMs, which is correct for normal time-driven
	// flushing but silently drops mid-window data on early
	// termination. Drain is the dedicated shutdown / batch-flush
	// path. After Drain the next active window's bounds are
	// advanced to the same boundary Tick would have used at the
	// natural rotation point (activeStartMs := activeEndMs;
	// activeEndMs += windowSize).
	Drain() []*SketchEnvelope
	// UpdateConfig atomically swaps the active config. The
	// in-flight window is preserved (matchers/aggregateBy may
	// change, but bytes already accumulated stay where they are);
	// see ADR-0003 §3 for why this is the only way every adapter
	// updates config.
	UpdateConfig(cs *PrecomputeConfigSet)
	// Stats returns the live counters; safe to call concurrently.
	Stats() *PrecomputeStats
	// SetLatencyObserver installs (or replaces) the per-Observe
	// latency hook. Pass nil to disable. Safe to call concurrently
	// with Observe; the runtime stores the function pointer atomically.
	// See LatencyObserver godoc for semantics.
	SetLatencyObserver(fn LatencyObserver)
	// SetSketchSink installs (or replaces) the flush-time sketch sink
	// that recycles a series' sketch once its envelope is serialized.
	// Pass nil to disable (sketches are dropped/GC'd). Safe to call
	// concurrently; the runtime stores the function pointer atomically.
	SetSketchSink(fn SketchSink)
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
	cfg             atomic.Pointer[PrecomputeConfig]
	sketchFactory   SketchFactory
	observer        SketchObserver
	window          *windowState
	snapshotCache   *SnapshotCache
	stats           *PrecomputeStats
	sketchType      SketchType
	closed          atomic.Bool
	latencyObserver atomic.Pointer[LatencyObserver]
	sketchSink      atomic.Pointer[SketchSink]
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
	// Time the entire Observe path including early-exit branches —
	// the deployed-stack consumer (a Prom histogram on each shim)
	// wants the same envelope `testing.B` measures, not just the
	// "happy path admit" subset. Closes Phase 2.11B gap #3.
	if fn := p.latencyObserver.Load(); fn != nil && *fn != nil {
		start := time.Now()
		defer func() { (*fn)(time.Since(start)) }()
	}
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

// ObserveKeyed is the shared-key entry point for the fused asap_edge
// processor: the caller built the series key once (and decoded the obs
// labels once) and passes the key, so the window skips cfg.buildSeriesKey.
// The key MUST equal cfg.SeriesKeyFor(obs) — asap_edge derives it the same
// way. Otherwise identical to Observe.
func (p *precompute) ObserveKeyed(key string, obs *Observation) error {
	if fn := p.latencyObserver.Load(); fn != nil && *fn != nil {
		start := time.Now()
		defer func() { (*fn)(time.Since(start)) }()
	}
	if p.closed.Load() {
		return errors.New("precompute: instance is closed")
	}
	cfg := p.activeConfig()
	if cfg == nil {
		return ErrNoConfig
	}
	p.stats.InputObservations.Add(1)
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
	if err := p.window.observeKeyed(key, obs, cfg, p.sketchFactory, p.observer, p.stats); err != nil {
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
	// InputEnvelopes is the per-ObserveEnvelope counter (inbound merge
	// path). We do NOT bump InputObservations here: when an envelope
	// arrives via Observe() (KindEnvelope) that wrapper already counted
	// it once, so bumping again would double-count; direct
	// ObserveEnvelope callers are tracked by InputEnvelopes instead.
	p.stats.InputEnvelopes.Add(1)
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
// every Tick drains. For Sliding, each Tick that crosses a slide
// boundary closes the current pane and emits one merged envelope per
// series covering the trailing window (panesPerWindow × slide) — see
// windowState.rotateSlidingLocked.
func (p *precompute) Tick(nowMs uint64) []*SketchEnvelope {
	cfg := p.activeConfig()
	if cfg == nil {
		return nil
	}
	closed, rng := p.window.rotate(nowMs, cfg)
	return p.finishRotate(closed, rng, nowMs)
}

// Drain implements Precompute.Drain. Unconditionally rotates the
// active window, regardless of wall-clock time, and returns the
// resulting envelopes. Intended for shutdown / batch-flush paths.
//
// Implementation: delegates to windowState.drain which mirrors
// rotate's body but skips the `nowMs < activeEndMs` gate.
func (p *precompute) Drain() []*SketchEnvelope {
	cfg := p.activeConfig()
	if cfg == nil {
		return nil
	}
	closed, rng := p.window.drain(cfg)
	return p.finishRotate(closed, rng, rng[1])
}

// finishRotate is the shared envelope-serialization tail used by
// both Tick and Drain. Walks the closed series, serializes each
// into a SketchEnvelope (honoring DeltaTransmission), and updates
// the rolling stats counters.
func (p *precompute) finishRotate(closed []*seriesEntry, rng [2]uint64, nowMs uint64) []*SketchEnvelope {
	if len(closed) == 0 {
		return nil
	}
	cfg := p.activeConfig()
	sink := p.sketchSink.Load()
	envelopes := make([]*SketchEnvelope, 0, len(closed))
	// closedKeys collects every series key in the just-closed window so the
	// snapshot cache can prune entries for keys that did NOT reappear this
	// window (P1-2: the outbound/inbound maps would otherwise grow forever,
	// retaining a snapshot copy for every series key ever seen). Built only
	// when the delta path is active (the only consumer of the cache) and a
	// cache is present.
	var closedKeys map[string]struct{}
	if cfg != nil && cfg.DeltaTransmission && p.snapshotCache != nil {
		closedKeys = make(map[string]struct{}, len(closed))
	}
	for _, entry := range closed {
		if closedKeys != nil && entry != nil {
			closedKeys[cfg.SeriesKeyForEntry(entry.ResourceLabels, entry.Labels)] = struct{}{}
		}
		env, err := p.serializeSeries(entry, cfg, rng)
		if err == nil && env != nil {
			envelopes = append(envelopes, env)
		} else {
			// On err (or a nil/empty payload) we skip the envelope but
			// still recycle the sketch. The host-neutral runtime has no
			// logger, so bump DroppedSerialize to keep the loss observable
			// (P0-2) instead of dropping the series silently.
			p.stats.DroppedSerialize.Add(1)
		}
		// The entry is detached from the live window (rotateLocked
		// replaced the map), so once its envelope is serialized
		// nothing else references the sketch — hand it to the pool.
		if sink != nil && entry != nil && entry.Sketch != nil {
			(*sink)(entry.Sketch)
			entry.Sketch = nil
		}
	}
	// Prune the snapshot cache to only the keys present in the just-closed
	// window. A series that vanished (never observed again this window) no
	// longer needs its cached outbound/inbound snapshot, and keeping it
	// would pin agent memory for the lifetime of the process (P1-2).
	if closedKeys != nil {
		p.snapshotCache.RetainKeys(closedKeys)
	}
	p.stats.OutputEnvelopes.Add(uint64(len(envelopes)))
	// LastEmittedEnvelopes is a snapshot (not a running total) of the
	// envelopes produced by this single rotate, so Store rather than Add.
	p.stats.LastEmittedEnvelopes.Store(uint64(len(envelopes)))
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
		// In msgpack mode (the heap-bearing CountSketch) the wire form is
		// the DELTA-HEAP msgpack frame: a full MSGPACK frame on the first
		// window / forced-full path, and a MSGPACK_DELTA frame otherwise.
		// Every other sketch keeps the proto full/delta tag pair.
		if cfg.Encoding == EncodingMsgpack {
			if isFull {
				enc = EncodingMsgpack
			} else {
				enc = EncodingMsgpackDelta
			}
		} else if isFull {
			enc = EncodingProtoFull
		} else {
			enc = EncodingProtoDelta
		}
	} else {
		payload, err = entry.Sketch.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}
		// The full-state encoding tag follows cfg.Encoding so a sketch
		// whose Snapshot() emits a msgpack full state (e.g. the
		// heap-bearing CountSketch, whose msgpack carries the top-k heap
		// the backend needs to promote the sid to FrequencyTopk) is
		// tagged MSGPACK rather than mislabeled PROTO_FULL. Default
		// (PROTO_FULL) is unchanged for every proto-snapshot sketch.
		if cfg.Encoding == EncodingMsgpack {
			enc = EncodingMsgpack
		} else {
			enc = EncodingProtoFull
		}
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
	// AggregationTemporality is stamped straight from cfg.Temporality.
	// Temporality note: the warm runtime accumulates each window's
	// observations into a fresh per-series sketch (the window map is
	// replaced on every rotate), so each emitted envelope describes only
	// THIS window's contribution. That is delta semantics — additive
	// families (Sum / CMS / CountSketch) consume per-window deltas and the
	// backend re-aggregates across windows. Configs that feed such a
	// family therefore set cfg.Temporality = AGGREGATION_TEMPORALITY_DELTA
	// (see the asap_edge warm factory). A cumulative source must be
	// pre-deltaed upstream; the runtime never converts temporality itself.
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

// SetLatencyObserver implements Precompute.SetLatencyObserver. The
// hook is stored in an atomic pointer so concurrent Observe calls
// see a coherent snapshot without locking; replacing the hook never
// races with the Observe deferred-call.
func (p *precompute) SetLatencyObserver(fn LatencyObserver) {
	if fn == nil {
		// Storing a nil-valued LatencyObserver pointer is harmless
		// (Observe nil-checks the dereferenced function), but storing
		// a nil pointer makes the hot-path Load return nil so the
		// caller skips the deferred-call entirely. Cheaper.
		p.latencyObserver.Store(nil)
		return
	}
	p.latencyObserver.Store(&fn)
}

// SetSketchSink implements Precompute.SetSketchSink.
func (p *precompute) SetSketchSink(fn SketchSink) {
	if fn == nil {
		p.sketchSink.Store(nil)
		return
	}
	p.sketchSink.Store(&fn)
}

// Shutdown implements Precompute.Shutdown.
func (p *precompute) Shutdown(ctx context.Context) error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Shutdown only flips the closed flag (above) so subsequent Observe
	// calls fail fast; the shim's Shutdown path runs its own final
	// Drain/Tick to flush in-flight state. We simply propagate any
	// context cancellation/deadline error to the caller.
	return ctx.Err()
}
