package precompute

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
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
	// ErrFutureData is returned when an observation belongs to a window that
	// has not been activated yet. The host must rotate/catch up and retry it.
	ErrFutureData = errors.New("precompute: observation timestamp at or beyond active window end")
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
	// EmitSubWindow emits an INCREMENTAL delta for active series WITHOUT
	// rotating the window — the threshold-driven sub-window producer. On each
	// call (the host's SubWindowInterval check tick) a series emits only if its
	// sketch has diverged from the backend's last-acked copy by ≥
	// SubWindowEpsilon in the family norm (ε=0 ⇒ emit every series, the static
	// "fixed" mode). Keeps the open window queryable to relative ε under large
	// tumbling windows. No-op unless DeltaTransmission is on (no in-window base
	// to diff against) and for Sliding mode (no stable base). nowMs is the
	// check-tick wall-clock for stats.
	EmitSubWindow(nowMs uint64) []*SketchEnvelope
	// ResetDeltaBase forces the next delta-capable serialization to be a full
	// checkpoint. Physical-plan runtimes use it at checkpoint deadlines and
	// after fail-closed recovery.
	ResetDeltaBase()
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
	// SetMonitorEngine installs (or replaces) the continuous-monitoring
	// engine (Discipline B). Pass nil to disable. The adapter constructs the
	// engine with the edge identity + a gRPC reporter to the coordinator and
	// wires it here; the runtime activates the per-observation hook whenever
	// the active config has Monitor.Enabled. Safe to call concurrently.
	SetMonitorEngine(e *monitor.Engine)
	// SetWakeHook installs (or replaces) the out-of-cycle wake hook: invoked
	// whenever an insert-time GOS threshold crossing happens on any series
	// (see window.go's wakeSignaler). Pass nil to disable. The host adapter
	// wires this to its flush loop's wake-on-demand trigger (e.g. the
	// asap_edge processor's wakeSubWindow) at construction time — unlike
	// SetMonitorEngine/config, this is installed once, not per config swap.
	// Safe to call concurrently.
	SetWakeHook(fn func())
	// Shutdown flushes any in-progress state; intended for the
	// shim's Shutdown path to run a final Tick before returning.
	Shutdown(ctx context.Context) error
}

// ResetDeltaBase implements Precompute.ResetDeltaBase.
func (p *precompute) ResetDeltaBase() {
	if p.snapshotCache != nil {
		p.snapshotCache.Reset()
	}
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
	pendingCfg      atomic.Pointer[PrecomputeConfig]
	sketchFactory   SketchFactory
	observer        SketchObserver
	window          *windowState
	snapshotCache   *SnapshotCache
	stats           *PrecomputeStats
	sketchType      SketchType
	closed          atomic.Bool
	latencyObserver atomic.Pointer[LatencyObserver]
	sketchSink      atomic.Pointer[SketchSink]
	frameReceiver   frameReceiver
	envelopeMu      sync.Mutex
	pendingMu       sync.Mutex
	pendingOutput   []*SketchEnvelope
	// monitorEngine is the continuous-monitoring (Discipline B) engine. nil
	// until SetMonitorEngine is called by the adapter; when set AND the active
	// config has Monitor.Enabled, the window's per-observation hook routes the
	// series' additive value into the engine.
	monitorEngine atomic.Pointer[monitor.Engine]
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
		cfgCopy := clonePrecomputeConfig(initialCfg)
		p.cfg.Store(cfgCopy)
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

	err := p.window.observe(obs, cfg, p.sketchFactory, p.observer, p.stats)
	if errors.Is(err, ErrFutureData) {
		p.rotateForFuture(obs.TimestampMs, cfg)
		err = p.window.observe(obs, cfg, p.sketchFactory, p.observer, p.stats)
	}
	if err != nil {
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
	err := p.window.observeKeyed(key, obs, cfg, p.sketchFactory, p.observer, p.stats)
	if errors.Is(err, ErrFutureData) {
		p.rotateForFuture(obs.TimestampMs, cfg)
		err = p.window.observeKeyed(key, obs, cfg, p.sketchFactory, p.observer, p.stats)
	}
	if err != nil {
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
	p.envelopeMu.Lock()
	defer p.envelopeMu.Unlock()
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
	receipt, err := p.frameReceiver.prepare(env, cfg.MaxSeries)
	if err != nil {
		return err
	}
	if receipt.duplicate {
		return nil
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
	p.frameReceiver.commit(receipt)
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
	envelopes := p.finishRotate(closed, rng, nowMs)
	if rng != [2]uint64{} {
		p.activatePendingConfig()
	}
	return p.takePendingOutput(envelopes)
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
	envelopes := p.finishRotate(closed, rng, rng[1])
	p.activatePendingConfig()
	return p.takePendingOutput(envelopes)
}

func (p *precompute) rotateForFuture(timestampMs uint64, cfg *PrecomputeConfig) {
	closed, rng := p.window.rotate(timestampMs, cfg)
	envelopes := p.finishRotate(closed, rng, timestampMs)
	if rng != [2]uint64{} {
		p.activatePendingConfig()
	}
	if len(envelopes) == 0 {
		return
	}
	p.pendingMu.Lock()
	p.pendingOutput = append(p.pendingOutput, envelopes...)
	p.pendingMu.Unlock()
}

func (p *precompute) takePendingOutput(current []*SketchEnvelope) []*SketchEnvelope {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	if len(p.pendingOutput) == 0 {
		return current
	}
	result := make([]*SketchEnvelope, 0, len(p.pendingOutput)+len(current))
	result = append(result, p.pendingOutput...)
	result = append(result, current...)
	p.pendingOutput = nil
	return result
}

// EmitSubWindow implements Precompute.EmitSubWindow: serialize an incremental
// delta for each active series that has DIVERGED past the per-family threshold,
// under the window lock, without rotating — so accumulation continues.
func (p *precompute) EmitSubWindow(nowMs uint64) []*SketchEnvelope {
	cfg := p.activeConfig()
	if cfg == nil || !cfg.DeltaTransmission {
		// Without delta encoding there is no in-window base to diff against —
		// an emit-without-rotate would re-ship full state. Boundary Tick/Drain
		// still emit the full frame.
		return nil
	}
	segmentMode := subWindowSegmentMode(cfg)
	var envelopes []*SketchEnvelope
	rng := p.window.subWindowVisit(cfg, func(entry *seriesEntry) {
		if !subWindowShouldEmit(entry, cfg) {
			return // below the ε divergence threshold; keep accumulating
		}
		env, err := p.serializeSubWindowSeries(entry, cfg)
		if err == nil && env != nil {
			envelopes = append(envelopes, env)
			subWindowMarkEmitted(entry, cfg) // advance the divergence reference
			if segmentMode {
				// Disjoint-segment model (KLL and other non-subtractable
				// mergeable summaries): the emit just serialized covers the
				// data accumulated SINCE the last emit, so reset the sketch to
				// empty and let the next segment accumulate fresh. The backend
				// merges the disjoint segment rows (merge_all) into the window
				// total — exactly as it merges [full, delta, delta] for the
				// subtractive families — with no over-count, because the
				// segments share no data. This is what aligns KLL with the
				// other families: "full vs delta" is about WHAT data the frame
				// covers (cumulative vs the between-emits segment), not whether
				// the sketch can subtract.
				entry.Sketch.Reset()
			}
		}
	})
	if rng == ([2]uint64{0, 0}) || len(envelopes) == 0 {
		return nil
	}
	for _, env := range envelopes {
		env.WindowStartMs = rng[0]
		env.WindowEndMs = rng[1]
	}
	p.stats.OutputEnvelopes.Add(uint64(len(envelopes)))
	p.stats.LastEmittedEnvelopes.Store(uint64(len(envelopes)))
	p.stats.LastTickMs.Store(nowMs)
	return envelopes
}

// serializeSubWindowSeries turns a still-active series into an incremental
// SketchEnvelope via the always-refresh in-window base (ComputeSubWindowDelta),
// WITHOUT the boundary empty-base reset and WITHOUT detaching the live sketch.
// WindowStart/End are stamped by the caller. The sketch MUST NOT be recycled
// here — the live window is still writing it.
func (p *precompute) serializeSubWindowSeries(entry *seriesEntry, cfg *PrecomputeConfig) (*SketchEnvelope, error) {
	if entry == nil || entry.Sketch == nil {
		return nil, nil
	}
	seriesKey := cfg.SeriesKeyForEntry(entry.ResourceLabels, entry.Labels)
	applyGosMode(entry.Sketch, cfg)
	payload, isFull, err := p.snapshotCache.ComputeSubWindowDelta(seriesKey, entry.Sketch, cfg.DeltaThreshold)
	if err != nil {
		return nil, fmt.Errorf("compute sub-window delta: %w", err)
	}
	if payload == nil {
		return nil, nil
	}
	enc := EncodingProtoDelta
	if isFull {
		enc = EncodingProtoFull
	}
	labels := SeriesAttrs(entry.Labels, cfg.AggregateBy)
	if cfg.EmitWindowStats {
		windowSeconds := uint64(cfg.Window.Size / time.Second)
		labels = append(labels,
			KeyValue{Key: "sample_count", Value: strconv.FormatUint(entry.Count, 10)},
			KeyValue{Key: "window_duration_seconds", Value: strconv.FormatUint(windowSeconds, 10)},
		)
	}
	return &SketchEnvelope{
		SchemaVersion:          1,
		SketchType:             cfg.SketchType,
		AggKind:                cfg.AggKind, // route Sum (SketchType=Unspecified) by AggKind at encode
		AggID:                  cfg.AggID,
		ResourceLabels:         entry.ResourceLabels,
		Labels:                 labels,
		Encoding:               enc,
		Payload:                payload,
		MetricName:             cfg.MetricName,
		Count:                  entry.Count,
		AggregationTemporality: cfg.Temporality,
	}, nil
}

// subWindowSegmentMode reports whether this family must use the DISJOINT-SEGMENT
// sub-window model (emit-then-reset) rather than the subtractive-delta model.
// KLL is a mergeable-but-not-subtractable summary: it cannot compute (current −
// prev), so its "delta" can't be a subtractive diff. Instead the runtime resets
// the sketch after each emit, so each emit is a KLL over the disjoint segment of
// data since the last emit; the backend merges the segment rows into the window
// total (merge is associative + no-compounding for mergeable summaries). The
// subtractive families (Sum/DDSketch/Count-Min/Count-Sketch/HLL) keep the
// cumulative sketch and ship subtractive deltas, so they are NOT segment-mode.
func subWindowSegmentMode(cfg *PrecomputeConfig) bool {
	return cfg.SketchType == SketchTypeKLLSketch
}

// applyGosMode configures the sketch's GOS insert-time delta mode from cfg
// when GosDeltaEpsilon > 0 and the sketch supports it (Count-Sketch, CMS,
// DDSketch, and Sum today via the shared (epsilon, k) structural interface
// below, HLL via the same interface — reinterpreting GosDeltaEpsilon as τ,
// see sketches.HLLWrapper.SetGosMode — and KLL via its own family-specific
// branch — more families follow the same pattern as their GOS conversions
// land). A no-op otherwise, leaving the fixed DeltaThreshold path unchanged.
// Called at flush time (on every already-live series, so a control-plane
// config change takes effect at the next flush) — each GOS-converted
// family's factory ALSO primes this at series creation (warm_sketch.go) so
// inserts before the first flush of a brand-new window are gated too.
// Structural interface asserts avoid a Sketch-interface change for these
// per-family knobs.
//
// KLL's trigger (derivations doc §8.6: R>=epsilon*N) has no per-cell/sites
// term, so its SetGosMode takes epsilon alone — a distinct structural shape
// from Count-Sketch/CMS/DDSketch/Sum/HLL's (epsilon, k), hence the
// family-specific branch here.
func applyGosMode(sketch Sketch, cfg *PrecomputeConfig) {
	if cfg.GosDeltaEpsilon <= 0 {
		return
	}
	if cfg.SketchType == SketchTypeKLLSketch {
		if gm, ok := sketch.(interface{ SetGosMode(epsilon float64) }); ok {
			gm.SetGosMode(cfg.GosDeltaEpsilon)
		}
		return
	}
	if gm, ok := sketch.(interface {
		SetGosMode(epsilon float64, k uint32)
	}); ok {
		gm.SetGosMode(cfg.GosDeltaEpsilon, cfg.GosSites)
	}
}

// subWindowShouldEmit gates a sub-window emit on per-family divergence: emit iff
// the series has moved ≥ ε·norm since its last emit (ε=0 ⇒ always; first emit of
// a window always fires and ships full state).
//
// GOS-converted families (Count-Sketch, CMS, DDSketch, Sum, KLL, HLL) bypass
// this entirely: their own insert-time threshold check already decided
// what's dirty (design-gos-unified-edge-telemetry.md §11 — Gate 1's periodic
// divergence pre-check is redundant once a family detects crossings at
// insert time), so always attempt the emit and let the empty-dirty-set case
// fall out as a nil payload downstream.
func subWindowShouldEmit(entry *seriesEntry, cfg *PrecomputeConfig) bool {
	if (cfg.SketchType == SketchTypeCountSketch || cfg.SketchType == SketchTypeCountMinSketch || cfg.SketchType == SketchTypeDDSketch || cfg.SketchType == SketchTypeHLLSketch) && cfg.GosDeltaEpsilon > 0 {
		return true
	}
	if cfg.AggKind == AggKindSum && cfg.GosDeltaEpsilon > 0 {
		return true
	}
	if cfg.SketchType == SketchTypeKLLSketch && cfg.GosDeltaEpsilon > 0 {
		// KLL's own insert-time trigger (R>=epsilon*N, derivations doc
		// §8.6) already decided this series is due; always attempt the
		// emit. If nothing was actually inserted since the last segment
		// reset (R==0), KLLWrapper.Snapshot returns a nil payload and
		// serializeSubWindowSeries/EmitSubWindow skip it as "nothing to
		// send" — the same harmless empty case Count-Sketch's bypass
		// relies on.
		return true
	}
	eps := cfg.SubWindowEpsilon
	if eps <= 0 || !entry.subWindowAcked {
		return true
	}
	div, norm := subWindowDivergence(entry, cfg)
	return norm <= 0 || div >= eps*norm
}

// subWindowDivergence returns (divergence, norm) in the family's accuracy
// metric: Sum→value, HLL→cardinality, Count-Sketch→L2 (Frobenius), and
// DDSketch/KLL/CMS→count (rank/L1 staleness is bounded by the un-acked count;
// KLL uses the SAME external-count trigger even though it emits disjoint
// segments rather than subtractive deltas — the trigger is independent of the
// sketch's subtractability).
func subWindowDivergence(entry *seriesEntry, cfg *PrecomputeConfig) (div, norm float64) {
	switch {
	case cfg.AggKind == AggKindSum:
		if r, ok := entry.Sketch.(interface{ Sum() float64 }); ok {
			cur := r.Sum()
			return math.Abs(cur - entry.ackVal), math.Abs(cur)
		}
	case cfg.SketchType == SketchTypeHLLSketch:
		if r, ok := entry.Sketch.(interface{ EstimateCardinality() float64 }); ok {
			cur := r.EstimateCardinality()
			return math.Abs(cur - entry.ackVal), cur
		}
	case cfg.SketchType == SketchTypeCountSketch:
		if r, ok := entry.Sketch.(interface {
			L2DivergenceSinceEmit() (float64, float64)
		}); ok {
			return r.L2DivergenceSinceEmit()
		}
	}
	cur := float64(entry.Count)
	return cur - entry.ackVal, cur
}

// subWindowMarkEmitted advances the divergence reference after a successful
// emit (scalar families store it on the entry; Count-Sketch snapshots its cells
// in the wrapper).
func subWindowMarkEmitted(entry *seriesEntry, cfg *PrecomputeConfig) {
	entry.subWindowAcked = true
	switch {
	case cfg.AggKind == AggKindSum:
		if cfg.GosDeltaEpsilon > 0 {
			// GOS mode already captured + reset the since-crossing
			// accumulator in place at insert time (SumWrapper.Update) and
			// never reads entry.ackVal (subWindowShouldEmit bypasses
			// subWindowDivergence for this family+mode entirely) — skip the
			// Sum() read/store, mirroring Count-Sketch's analogous skip
			// below.
			return
		}
		if r, ok := entry.Sketch.(interface{ Sum() float64 }); ok {
			entry.ackVal = r.Sum()
		}
	case cfg.SketchType == SketchTypeHLLSketch:
		if cfg.GosDeltaEpsilon > 0 {
			// GOS mode already detected + queued each crossed register in
			// place at insert time (UpdateValue/UpdateBytes ->
			// recordGosCrossing) and never reads entry.ackVal
			// (subWindowShouldEmit bypasses subWindowDivergence for this
			// family+mode entirely) — skip the EstimateCardinality() call the
			// old divergence path below would otherwise do.
			return
		}
		if r, ok := entry.Sketch.(interface{ EstimateCardinality() float64 }); ok {
			entry.ackVal = r.EstimateCardinality()
		}
	case cfg.SketchType == SketchTypeCountSketch:
		if cfg.GosDeltaEpsilon > 0 {
			// GOS mode already reset each sent cell in place at insert time
			// (UpdateStringGOS) and never reads ackedCells (subWindowShouldEmit
			// bypasses subWindowDivergence for this family+mode entirely) — skip
			// the O(rows·cols) snapshot copy MarkSubWindowEmitted would do.
			return
		}
		if r, ok := entry.Sketch.(interface{ MarkSubWindowEmitted() }); ok {
			r.MarkSubWindowEmitted()
		}
	case cfg.SketchType == SketchTypeCountMinSketch:
		if cfg.GosDeltaEpsilon > 0 {
			// GOS mode already reset each sent cell in place at insert time
			// (InsertWithHashGOS) — CMS has no ackedCells-style snapshot to
			// advance in the first place (unlike CountSketch), so this is a
			// pure documentation/symmetry no-op; falling to the default
			// branch below would have been equally harmless (ackVal is never
			// consulted once subWindowShouldEmit bypasses divergence for
			// GOS-mode CMS entirely).
			return
		}
		entry.ackVal = float64(entry.Count)
	case cfg.SketchType == SketchTypeKLLSketch:
		if cfg.GosDeltaEpsilon > 0 {
			// GOS mode's trigger state (windowTotal/gosWake) lives entirely
			// inside the KLLWrapper and updates itself at insert time;
			// ackVal (the OLD external-count Gate-1 reference) is never read
			// for this family+mode since subWindowShouldEmit bypasses
			// subWindowDivergence entirely. Nothing to advance here — the
			// segment Reset() that follows this call (EmitSubWindow) already
			// zeros the sketch's own R via KLLWrapper.Reset.
			return
		}
		entry.ackVal = float64(entry.Count)
	case cfg.SketchType == SketchTypeDDSketch:
		// Unlike CountSketch, DDSketch has no O(dw)-equivalent snapshot to
		// skip here in GOS mode: the default branch's ackVal update below is
		// already an O(1) copy of entry.Count (not a sketch-internal scan),
		// and subWindowShouldEmit bypasses the ackVal-based divergence check
		// entirely once GosDeltaEpsilon>0 for this family, so this value is
		// simply unread while GOS is active. Keep it updated anyway (fall
		// through to the same O(1) update every other family gets) so a
		// control-plane toggle back to fixed mode has a correct reference
		// immediately rather than a stale pre-GOS value.
		fallthrough
	default:
		entry.ackVal = float64(entry.Count)
	}
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
		applyGosMode(entry.Sketch, cfg)
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
		AggKind:                cfg.AggKind,
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
		RelativeAccuracy:       sketchRelativeAccuracy(entry.Sketch),
	}, nil
}

// sketchRelativeAccuracy reads the DDSketch relative-accuracy alpha off a
// sketch instance when it exposes one (DDSketchWrapper); 0 for every other
// family. Stamped onto the envelope so the OTel encoder can set the output
// pmetric.DDSketch container's relative_accuracy (an ε=0 container is a
// degenerate sketch the backend can't answer quantiles from).
func sketchRelativeAccuracy(s Sketch) float64 {
	if ra, ok := s.(interface{ RelativeAccuracy() float64 }); ok {
		return ra.RelativeAccuracy()
	}
	return 0
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
	cfgCopy := clonePrecomputeConfig(chosen)
	// An in-flight window is owned by its current immutable config. Stage the
	// replacement until Tick/Drain closes that generation; otherwise old sketch
	// bytes could be serialized with new family, grouping, or delta semantics.
	if active != nil && p.window.hasAccumulatedState() {
		p.pendingCfg.Store(cfgCopy)
		return
	}
	p.cfg.Store(cfgCopy)
	p.pendingCfg.Store(nil)
	p.sketchType = cfgCopy.SketchType
	// Re-evaluate the monitor hooks against the newly installed config so a
	// control-plane toggle of Monitor.Enabled (or a functional/key change)
	// takes effect immediately.
	p.rewireMonitorHooks()
}

func clonePrecomputeConfig(source *PrecomputeConfig) *PrecomputeConfig {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Matchers = append([]LabelMatcher(nil), source.Matchers...)
	cloned.AggregateBy = append([]string(nil), source.AggregateBy...)
	cloned.Quantiles = append([]float64(nil), source.Quantiles...)
	if source.SketchParams != nil {
		cloned.SketchParams = make(SketchParams, len(source.SketchParams))
		for key, value := range source.SketchParams {
			cloned.SketchParams[key] = value
		}
	}
	cloned.Monitor.Key = append([]byte(nil), source.Monitor.Key...)
	cloned.Monitor.Coeffs = append([]float64(nil), source.Monitor.Coeffs...)
	return &cloned
}

func (p *precompute) activatePendingConfig() {
	pending := p.pendingCfg.Swap(nil)
	if pending == nil {
		return
	}
	p.cfg.Store(pending)
	p.sketchType = pending.SketchType
	p.snapshotCache.Reset() // a new generation must start from a full checkpoint
	p.rewireMonitorHooks()
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

// SetMonitorEngine implements Precompute.SetMonitorEngine.
func (p *precompute) SetMonitorEngine(e *monitor.Engine) {
	p.monitorEngine.Store(e)
	p.rewireMonitorHooks()
}

// SetWakeHook implements Precompute.SetWakeHook. windowState already owns its
// own mutex (guarding the same field the observe path reads), so this
// delegates directly rather than adding an atomic pointer here too.
func (p *precompute) SetWakeHook(fn func()) {
	p.window.setWakeHook(fn)
}

// rewireMonitorHooks installs or clears the window's monitor hooks based on the
// current (engine, config) pair. Called on engine install and on every config
// swap so a control-plane flip of Monitor.Enabled takes effect at runtime. A
// spec that fails validation (e.g. negative linear coefficients — non-monotone,
// which would void the countdown's correctness) is treated as disabled.
func (p *precompute) rewireMonitorHooks() {
	eng := p.monitorEngine.Load()
	cfg := p.activeConfig()
	if eng == nil || cfg == nil || !cfg.Monitor.Enabled || cfg.Monitor.Validate() != nil {
		p.window.setMonitorHooks(nil, nil, nil)
		return
	}
	// CMS's local point-query readout (FunctionalCMSPoint / EstimateCount) is a
	// min-across-rows estimate, poisoned by ANY single row a GOS-active cell
	// reset in place at insert time. The asapedgeprocessor's boot-time YAML
	// validation already rejects this combination, but a live control-plane
	// config push (UpdateConfig) reaches PrecomputeConfig directly and skips
	// that path — so gate it here too rather than serve a corrupted read.
	// Plain CountSketch is UNAFFECTED (median-based EstimateCount tolerates a
	// reset row) and keeps using this same functional untouched.
	if cfg.SketchType == SketchTypeCountMinSketch && cfg.Monitor.Functional == monitor.FunctionalCMSPoint && cfg.GosDeltaEpsilon > 0 {
		p.window.setMonitorHooks(nil, nil, nil)
		return
	}
	spec := cfg.Monitor
	aggID := uint64(cfg.AggID)
	sketchType := cfg.SketchType
	observe := func(entry *seriesEntry, windowStartMs uint64) {
		v, ok := monitorValue(spec, entry.Sketch)
		if !ok {
			return
		}
		// Monitor key selects which monitored quantity this observation feeds:
		//   - CMSPoint: the point-frequency key x (whole-stream over the agg).
		//   - Sum / LinearBuckets: the SERIES GROUP key (the AggregateBy tuple),
		//     so per-group series (e.g. one per zone) get independent monitor
		//     state instead of colliding on a single agg-wide slot. The edge has
		//     already folded all raw series of a group into this one entry, so
		//     the value is the group's combined local aggregate — exactly the
		//     per-(agg,group) quantity the coordinator sums across edges.
		key := spec.Key
		if spec.Functional != monitor.FunctionalCMSPoint {
			key = groupKeyBytes(entry.Labels)
		}
		eng.Observe(aggID, key, v, windowStartMs)
	}
	reset := func(newWindowStartMs uint64) {
		eng.EpochReset(newWindowStartMs)
	}
	// sample stamps the coordinator-granted distributed-NitroSketch probability
	// onto each new window's series wrapper at creation (the reverse of the
	// observe path: precompute → engine here reads engine → wrapper). It is
	// family-gated: only Count-Min, Count-Sketch and DDSketch expose update
	// sampling; Sum/KLL/HLL are left untouched (HLL's hash-threshold sampling is
	// force-disabled in this coordinated-sampling path). Reading the granted p
	// here — at sketch birth, before any data — keeps p constant for the whole
	// window so both merge operands share it.
	sample := applyGrantedSampleP(eng, aggID, sketchType)
	p.window.setMonitorHooks(observe, reset, sample)
}

// SampleSetter is the narrow boundary the coordinated-sampling wrappers expose
// for the runtime to install a coordinator-granted update-sampling probability.
// It mirrors each wrapper's chainable WithSampleP but is a plain mutator so a
// SINGLE interface captures all three families regardless of their differing
// (per-type) chaining return values — the bare interface{ WithSampleP(float64) }
// cannot, since each wrapper's WithSampleP returns its own concrete type.
//
// Only Count-Min, Count-Sketch and DDSketch implement SampleSetter. Sum and KLL
// have no sampling at all; HLL's hash-threshold sampling is deliberately kept
// OUT of this coordinated path, so it does not implement SampleSetter either.
// The runtime therefore gates first by SketchType (the configured family) and
// then by this interface assert — a sketch that doesn't support coordinated
// sampling is left untouched, never panicked on the observe path. Implemented in
// the sketches package (which imports this one), so no import cycle.
type SampleSetter interface {
	SetSampleP(p float64)
}

// applyGrantedSampleP returns a window sample-hook that stamps the engine's
// currently-granted sampling probability onto a new wrapper, or nil for families
// that don't support coordinated sampling (Sum/KLL/HLL) — yielding a true no-op
// (no hook installed) rather than a per-sketch type assertion on the hot path.
func applyGrantedSampleP(eng *monitor.Engine, aggID uint64, st SketchType) func(s Sketch) {
	switch st {
	case SketchTypeCountMinSketch, SketchTypeCountSketch, SketchTypeDDSketch:
		// supported below
	default:
		return nil // Sum / KLL / HLL: no-op, never call WithSampleP
	}
	return func(s Sketch) {
		p := eng.GrantedSampleP(aggID)
		// p==1 is the unsampled default; WithSampleP(1.0) is itself a no-op, but
		// skip the call entirely so an unsampled config never touches the sketch.
		if p >= 1.0 || p <= 0 {
			return
		}
		// Family already vetted by SketchType above; the interface assert guards
		// against a test double or mismatched factory — never panic on observe.
		if ss, ok := s.(SampleSetter); ok {
			ss.SetSampleP(p)
		}
	}
}

// monitorValue reads the current additive value of a series' sketch for the
// configured functional, using narrow read-only interfaces so the runtime never
// depends on concrete sketch types. Returns ok=false when the sketch does not
// implement the expected readout (a misconfiguration that disables the monitor
// for that series rather than panicking on the hot path).
func monitorValue(spec monitor.Spec, s Sketch) (float64, bool) {
	switch spec.Functional {
	case monitor.FunctionalSum:
		if r, ok := s.(interface{ Sum() float64 }); ok {
			return r.Sum(), true
		}
	case monitor.FunctionalCMSPoint:
		if r, ok := s.(interface{ EstimateCount([]byte) float64 }); ok {
			return r.EstimateCount(spec.Key), true
		}
	case monitor.FunctionalLinearBuckets:
		if r, ok := s.(interface {
			LinearReadout([]float64) float64
		}); ok {
			return r.LinearReadout(spec.Coeffs), true
		}
	}
	return 0, false
}

// groupKeyBytes canonically encodes a series' grouping labels into a stable
// monitor key so the same group (e.g. zone=z0) maps to identical bytes on every
// edge — the coordinator merges per (agg_id, group) across edges. Empty labels
// (whole-stream aggregation) yield a nil key (one global monitor per agg_id).
func groupKeyBytes(labels []KeyValue) []byte {
	if len(labels) == 0 {
		return nil
	}
	sorted := make([]KeyValue, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	var b []byte
	for i, kv := range sorted {
		if i > 0 {
			b = append(b, ';')
		}
		b = append(b, kv.Key...)
		b = append(b, '=')
		b = append(b, kv.Value...)
	}
	return b
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
