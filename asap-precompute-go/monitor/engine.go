package monitor

import (
	"sync"
)

// monitorState is the per-(AggID,key) edge state for one monitoring epoch.
type monitorState struct {
	aggID uint64
	key   []byte

	// windowStart is the tumbling epoch this state belongs to. A value seen
	// for a different epoch triggers an epoch reset + re-register.
	windowStart uint64

	// roundBaseline is the value the edge most recently REPORTED to the
	// coordinator (0 at epoch start). It is the coordinator's known value for
	// this edge, so resetting it only on report — never on grant — keeps the
	// edge and coordinator in lock-step and loses no mass when slack shrinks.
	// The edge reports whenever lastValue - roundBaseline >= grantedSlack, then
	// advances roundBaseline to the reported value.
	roundBaseline float64
	// grantedSlack is the coordinator-allocated local budget for the current
	// round. Zero means "no grant yet" — the edge stays silent until the first
	// grant. A grant only updates this (and the round number); it never resets
	// the baseline, so a shrinking slack simply lowers the bar for the next
	// report.
	grantedSlack float64
	// lastValue is the most recent local additive value observed.
	lastValue float64
	// obsCount is the number of observations admitted for this monitor since
	// the current epoch began. It is the edge's observed items/window (rate)
	// reported to the coordinator so it can size this edge's sampling
	// probability (AllocateSampleRates). Reset to 0 at each epoch boundary.
	obsCount uint64
	// grantedSampleP is the coordinator-allocated distributed-NitroSketch
	// update-sampling probability for this monitor's agg. 0 (unset) ⇒ no
	// sampling grant (treated as p=1). It is stored on grant and read by the
	// precompute at the NEXT epoch boundary, where it is applied via WithSampleP
	// to the new window's sketch wrapper (never mid-window). Unlike the slack
	// fields it survives a round re-grant and is NOT cleared on epoch reset —
	// the edge keeps sampling at the last granted p until told otherwise (a new
	// grant or coordinator restart, which ForceReregister clears).
	grantedSampleP float64

	round      uint64
	seq        uint64 // per-monitor monotonic report counter (idempotency)
	registered bool   // Register sent for this epoch
}

// mapKey identifies a monitor in the engine's state map. Using a comparable
// struct (rather than a synthesized byte string) keeps the hot-path lookup
// allocation-free for the common Sum case (empty key → string("") is a no-alloc
// conversion); only CMS-point monitors with a non-empty key allocate, and only
// the first time their state is created.
type mapKey struct {
	aggID uint64
	key   string
}

// Engine is the edge-side monitor state machine. It is safe for concurrent use:
// Observe runs on the window hot path (under the window lock), while
// OnGrant/OnPoll/OnClose run on the transport read goroutine. A single mutex
// serializes both; the critical sections are arithmetic-only and short.
type Engine struct {
	mu            sync.Mutex
	states        map[mapKey]*monitorState
	reporter      Reporter
	edgeID        string
	epochWindowMs uint64
}

// NewEngine builds an Engine for the given edge identity and tumbling window
// size (used as the epoch length reported at registration so the coordinator
// can guard alignment). The reporter may be nil until SetReporter is called.
func NewEngine(edgeID string, epochWindowMs uint64, reporter Reporter) *Engine {
	return &Engine{
		states:        make(map[mapKey]*monitorState),
		reporter:      reporter,
		edgeID:        edgeID,
		epochWindowMs: epochWindowMs,
	}
}

// SetReporter installs (or replaces) the transport. Safe to call concurrently.
func (e *Engine) SetReporter(r Reporter) {
	e.mu.Lock()
	e.reporter = r
	e.mu.Unlock()
}

// Observe is the hot-path entry point: called once per admitted observation on
// a monitored series, with the series' CURRENT additive value already read
// cheaply by the caller (SumWrapper.Sum / CMSWrapper.EstimateCount / linear
// readout). value MUST be monotone non-decreasing within an epoch.
//
// On the first observation of an epoch it registers the monitor and stays
// silent (no slack yet). Once a grant has arrived, it reports whenever the
// increase since the last reported value reaches the granted slack, advancing
// the baseline to the reported value each time (so a single large jump yields
// one report and a shrinking slack lowers the bar for the next).
func (e *Engine) Observe(aggID uint64, key []byte, value float64, windowStart uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	mk := mapKey{aggID, string(key)} // no-alloc for empty key (Sum)
	st := e.states[mk]
	if st == nil {
		st = &monitorState{aggID: aggID, key: key, windowStart: windowStart}
		e.states[mk] = st
	}
	// Epoch change for this monitor: reset and re-register.
	if st.windowStart != windowStart {
		e.resetStateLocked(st, windowStart)
	}
	st.lastValue = value
	st.obsCount++ // per-epoch observed rate (items/window) reported to the coordinator

	if !st.registered {
		st.registered = true
		if e.reporter != nil {
			e.reporter.Register(Registration{
				EdgeID:        e.edgeID,
				AggID:         aggID,
				Key:           key,
				EpochWindowMs: e.epochWindowMs,
				WindowStartMs: windowStart,
			})
		}
	}

	// No grant yet → stay silent (the coordinator drives the first round).
	if st.grantedSlack <= 0 {
		return
	}
	if value-st.roundBaseline >= st.grantedSlack {
		st.seq++
		st.roundBaseline = value // advance baseline to the reported value
		e.sendReportLocked(st)
	}
}

// OnGrant installs the current round's slack budget. Stale-epoch grants are
// dropped. It deliberately does NOT touch the baseline: the baseline tracks the
// last reported value (the coordinator's known value for this edge), so a
// shrinking slack simply lowers the bar for the next report without losing the
// unreported-but-below-slack mass.
func (e *Engine) OnGrant(g Grant) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.states[mapKey{g.AggID, string(g.Key)}]
	if st == nil || st.windowStart != g.WindowStartMs {
		return // unknown monitor or stale epoch
	}
	st.round = g.Round
	st.grantedSlack = g.LocalSlack
	// Store the granted sampling probability for the precompute to read and
	// apply at the next epoch boundary. A grant always carries the coordinator's
	// current decision, so 0 means "no sampling this round" and is recorded as
	// such; the precompute treats <=0 as p=1 (unsampled).
	st.grantedSampleP = g.SampleP
}

// OnPoll answers a poll with the current local value and advances the baseline
// to it (the coordinator now knows this value authoritatively). Stale-epoch
// polls are dropped. The single-phase v1 coordinator does not emit polls; this
// path exists for protocol completeness and future poll-based variants.
func (e *Engine) OnPoll(p Poll) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.states[mapKey{p.AggID, string(p.Key)}]
	if st == nil || st.windowStart != p.WindowStartMs {
		return
	}
	st.round = p.Round
	st.seq++
	st.roundBaseline = st.lastValue
	e.sendReportLocked(st)
}

// OnClose just records the round number. Baselines advance on report, not on
// close, so there is nothing else to do. Stale-epoch closes are dropped. The
// v1 coordinator does not emit closes (a re-grant subsumes them).
func (e *Engine) OnClose(c Close) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.states {
		if s.aggID == c.AggID && s.windowStart == c.WindowStartMs {
			s.round = c.Round
		}
	}
}

// OnRef is a no-op for the scalar countdown engine — geometric-F2 reference
// broadcasts are consumed by F2Engine, not this one. Present so *Engine still
// satisfies the Inbound interface.
func (e *Engine) OnRef(RefBroadcast) {}

// EpochReset is called by the runtime at every tumbling-window rotation. It
// resets every monitor to a fresh epoch (baseline/slack/round cleared) so the
// next window is an independent monitoring instance, and clears the registered
// flag so the next Observe re-registers with the coordinator.
func (e *Engine) EpochReset(newWindowStart uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.states {
		e.resetStateLocked(st, newWindowStart)
	}
}

// ForceReregister clears the registered flag and round state for every monitor
// (keeping the current epoch window and last observed value) so the next
// Observe re-announces each monitor to the coordinator. The transport calls
// this on (re)connect: a coordinator that restarted has no memory of this edge,
// so re-registering with a zeroed baseline makes the edge re-report its full
// current value to the fresh coordinator. lastValue is preserved so the very
// next Observe can immediately re-report if it already exceeds a new grant.
func (e *Engine) ForceReregister() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.states {
		st.registered = false
		st.roundBaseline = 0
		st.grantedSlack = 0
		st.grantedSampleP = 0 // a restarted coordinator has no sampling allocation for this edge
		st.round = 0
	}
}

// GrantedSampleP returns the coordinator-allocated distributed-NitroSketch
// sampling probability for aggID, or 1.0 (unsampled) when no positive grant has
// arrived. The coordinator allocates one p_i per edge per agg, so every monitor
// state under aggID carries the same value; this returns the first positive one
// it finds (falling back to 1.0). The precompute calls this at each epoch
// boundary to stamp the new window's sampling-capable wrapper via WithSampleP —
// never mid-window, so both merge operands share one p. Safe for concurrent use.
func (e *Engine) GrantedSampleP(aggID uint64) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.states {
		if st.aggID == aggID && st.grantedSampleP > 0 {
			return st.grantedSampleP
		}
	}
	return 1.0
}

func (e *Engine) resetStateLocked(st *monitorState, windowStart uint64) {
	st.windowStart = windowStart
	st.roundBaseline = 0
	st.grantedSlack = 0
	st.lastValue = 0
	st.obsCount = 0
	st.round = 0
	st.registered = false
	// grantedSampleP is deliberately NOT reset: the coordinator's sampling
	// allocation persists across epochs until a new grant changes it, so the
	// edge keeps sampling at the last granted p (which the precompute applies to
	// each new window's wrapper at rotation). A coordinator restart clears it via
	// ForceReregister, not here.
	//
	// seq is per-monitor monotonic ACROSS epochs so the coordinator can dedup
	// re-deliveries that straddle a boundary; intentionally NOT reset.
}

func (e *Engine) sendReportLocked(st *monitorState) {
	if e.reporter == nil {
		return
	}
	e.reporter.Report(Report{
		EdgeID:        e.edgeID,
		AggID:         st.aggID,
		Key:           st.key,
		WindowStartMs: st.windowStart,
		LocalValue:    st.lastValue,
		Round:         st.round,
		Seq:           st.seq,
		Rate:          float64(st.obsCount),
	})
}
