package monitor

import (
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/ProjectASAP/sketchlib-go/wire/asapmsgpack"
)

var f2Debug = os.Getenv("F2_DEBUG") != ""

// F2Engine is the edge side of whole-sketch F2 (‖f‖₂²) monitoring — the
// counterpart to the scalar slack-countdown Engine for the non-linear
// functional. Per configured monitor it holds the edge's last-shipped
// Count-Sketch (its coordinator-side reference) and, in geometric mode, the
// broadcast reference C_ref. Each window the runtime calls OnWindow with the
// series' current Count-Sketch cell matrix; F2Engine decides whether to ship it:
//
//   - Distributed: ship every window (bandwidth baseline).
//   - Geometric: run the Sharfman–Schuster–Keren safe-zone test locally against
//     C_ref and ship only on a local violation (or the first window of an epoch).
//
// The safe-zone test and the msgpack Count-Sketch serialization are byte-for-byte
// the mirror of the Rust coordinator (data_plane/src/monitor/f2.rs
// is_locally_safe and asap_sketchlib CountSketch), so the coordinator's linear
// merge of shipped sketches is meaningful.
type F2Engine struct {
	mu            sync.Mutex
	edgeID        string
	epochWindowMs uint64
	reporter      Reporter
	states        map[mapKey]*f2State
	ships         uint64
	silent        uint64
	refsRecv      uint64
	refErrs       uint64
}

type f2State struct {
	aggID       uint64
	key         []byte
	rows, cols  int
	tau, eps    float64
	mode        F2Mode
	windowStart uint64
	ownRef      [][]float64 // this edge's last-shipped matrix (its reference)
	cRef        [][]float64 // broadcast merged reference (geometric mode)
	k           int         // site count from the last RefBroadcast
	haveRef     bool        // a RefBroadcast has been received this epoch
	// needFull marks the cached cRef as UNTRUSTWORTHY: a sparse delta could not
	// be applied (arrived with no base, or failed to decode), so cRef has
	// diverged from the coordinator's and every later delta compounds onto a
	// wrong base. While set, the edge refuses to stay silent (a corrupt cRef
	// could make the safe-zone test wrongly pass → a silent missed violation) and
	// force-ships its sketch every window, keeping the coordinator's global
	// estimate exact until a Full keyframe (IsDelta=false) resyncs the reference
	// and clears the flag.
	needFull bool
	seq      uint64
}

// NewF2Engine builds an F2 engine for one edge. epochWindowMs is reported at
// registration so the coordinator can guard alignment; reporter may be nil until
// SetReporter is called.
func NewF2Engine(edgeID string, epochWindowMs uint64, reporter Reporter) *F2Engine {
	return &F2Engine{
		edgeID:        edgeID,
		epochWindowMs: epochWindowMs,
		reporter:      reporter,
		states:        make(map[mapKey]*f2State),
	}
}

// SetReporter installs (or replaces) the transport. Safe to call concurrently.
func (e *F2Engine) SetReporter(r Reporter) {
	e.mu.Lock()
	e.reporter = r
	e.mu.Unlock()
}

// Configure registers an F2 monitor for (aggID, key). Must be called before
// OnWindow. Idempotent: re-configuring the same monitor updates its parameters.
func (e *F2Engine) Configure(aggID uint64, key []byte, spec Spec) {
	e.mu.Lock()
	defer e.mu.Unlock()
	mk := mapKey{aggID, string(key)}
	e.states[mk] = &f2State{
		aggID: aggID,
		key:   key,
		rows:  spec.SketchRows,
		cols:  spec.SketchCols,
		tau:   spec.Tau,
		eps:   spec.Epsilon,
		mode:  spec.Mode,
	}
}

// OnWindow feeds the current Count-Sketch cell matrix for a configured monitor at
// the close of a tumbling window. It registers on the first window of an epoch,
// then ships the sketch (distributed: always; geometric: only on a local
// safe-zone violation). No-op for an unconfigured monitor.
func (e *F2Engine) OnWindow(aggID uint64, key []byte, matrix [][]float64, windowStart uint64) {
	e.mu.Lock()
	st, ok := e.states[mapKey{aggID, string(key)}]
	if !ok {
		e.mu.Unlock()
		return
	}
	reporter := e.reporter
	// Epoch rollover: coordinator resets references at the tumbling boundary, so
	// the edge drops its own/broadcast reference and re-registers.
	registered := true
	if windowStart != st.windowStart {
		st.windowStart = windowStart
		st.ownRef = nil
		st.cRef = nil
		st.haveRef = false
		st.needFull = false // fresh epoch: coordinator resets refs, no divergence yet
		registered = false
	}

	ship := true
	if st.mode == F2ModeGeometric && registered {
		// Safe-zone radius monitors the ALERT threshold (1-ε)τ, not the raw τ, so
		// that "all sites locally safe ⇒ F2 < (1-ε)τ". Using √(d·τ) here (the old
		// bug) let edges stay silent through the whole [(1-ε)τ, τ) band, so the
		// coordinator never resynced and the alert fired late; √(d·(1-ε)τ) makes
		// geometric fire in the same band as the distributed baseline.
		//
		// !st.needFull guards against a diverged cRef: if a delta could not be
		// applied the cached reference is untrustworthy, so the edge must NOT
		// trust f2LocallySafe (it could wrongly pass) — force-ship until a Full
		// keyframe resyncs it.
		radius := math.Sqrt(float64(st.rows) * (1.0 - st.eps) * st.tau)
		if st.haveRef && !st.needFull && f2LocallySafe(matrix, st.ownRef, st.cRef, st.k, radius) {
			ship = false
		}
	}

	if ship {
		atomic.AddUint64(&e.ships, 1)
	} else {
		atomic.AddUint64(&e.silent, 1)
	}

	var report *Report
	if ship {
		if buf, err := asapmsgpack.MarshalCountSketch(uint64(st.rows), uint64(st.cols), matrix); err == nil {
			st.seq++
			report = &Report{
				EdgeID:        e.edgeID,
				AggID:         aggID,
				Key:           key,
				WindowStartMs: windowStart,
				Seq:           st.seq,
				Sketch:        buf,
			}
			st.ownRef = cloneMatrix(matrix) // this shipped sketch becomes the reference
		}
	}
	e.mu.Unlock()

	// Transport calls happen after releasing the lock (Reporter is non-blocking).
	if reporter == nil {
		return
	}
	if !registered {
		reporter.Register(Registration{
			EdgeID:        e.edgeID,
			AggID:         aggID,
			Key:           key,
			EpochWindowMs: e.epochWindowMs,
			WindowStartMs: windowStart,
		})
	}
	if report != nil {
		reporter.Report(*report)
	}
}

// OnRef stores the geometric-F2 reference broadcast for the local safe-zone
// test. A full frame (IsDelta=false) replaces the cached C_ref; a sparse delta
// (IsDelta=true) is applied cell-wise to it (the coordinator ships only the
// cells that changed since the last broadcast — no O(k) full-matrix
// amplification). A delta with no cached base is dropped (a Full keyframe
// follows for a fresh edge). Implements part of the Inbound interface.
func (e *F2Engine) OnRef(rb RefBroadcast) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.states[mapKey{rb.AggID, string(rb.Key)}]
	if !ok {
		return
	}
	if rb.IsDelta {
		if st.cRef == nil {
			// No base to apply onto: the reference is now behind the coordinator's.
			// Mark it untrustworthy so the edge force-ships (never silently trusts
			// a missing/stale ref) until a Full keyframe arrives.
			st.needFull = true
			atomic.AddUint64(&e.refErrs, 1)
			return
		}
		_, _, ri, ci, vs, err := asapmsgpack.UnmarshalCountSketchDeltaSparse(rb.CRef)
		if err != nil {
			if f2Debug {
				fmt.Fprintf(os.Stderr, "F2Engine.OnRef: delta decode failed: %v\n", err)
			}
			// The delta is undecodable → applying nothing leaves cRef diverged
			// from the coordinator for the rest of the epoch. Force resync.
			st.needFull = true
			atomic.AddUint64(&e.refErrs, 1)
			return
		}
		for i := range vs {
			r, c := int(ri[i]), int(ci[i])
			if r < len(st.cRef) && c < len(st.cRef[r]) {
				st.cRef[r][c] += vs[i]
			}
		}
	} else {
		_, _, matrix, err := asapmsgpack.UnmarshalCountSketch(rb.CRef)
		if err != nil {
			if f2Debug {
				fmt.Fprintf(os.Stderr, "F2Engine.OnRef: full decode failed (%d bytes): %v\n", len(rb.CRef), err)
			}
			atomic.AddUint64(&e.refErrs, 1)
			return
		}
		st.cRef = matrix
		st.needFull = false // a Full keyframe resyncs the reference exactly
	}
	st.k = int(rb.K)
	st.haveRef = true
	atomic.AddUint64(&e.refsRecv, 1)
}

// Stats returns cumulative (ships, silent, refsReceived, refErrors) for the eval.
func (e *F2Engine) Stats() (ships, silent, refsRecv, refErrs uint64) {
	return atomic.LoadUint64(&e.ships), atomic.LoadUint64(&e.silent),
		atomic.LoadUint64(&e.refsRecv), atomic.LoadUint64(&e.refErrs)
}

// OnGrant / OnPoll / OnClose are no-ops: F2 does not use the scalar slack
// countdown. Present so *F2Engine satisfies the Inbound interface.
func (e *F2Engine) OnGrant(Grant) {}
func (e *F2Engine) OnPoll(Poll)   {}
func (e *F2Engine) OnClose(Close) {}

// f2LocallySafe is the byte-for-byte mirror of Rust
// GeometricF2Monitor::is_locally_safe (f2.rs): the site's drift ball
// B(C_ref + (k/2)·ΔC, (k/2)‖ΔC‖) ⊆ B(0, radius), i.e.
// ‖C_ref + (k/2)·ΔC‖ + (k/2)‖ΔC‖ ≤ radius, where ΔC = current − ownRef and
// radius = √(rows·τ). true ⇒ the site may stay silent this window.
func f2LocallySafe(current, ownRef, cRef [][]float64, k int, radius float64) bool {
	scale := float64(k) / 2.0
	if k < 1 {
		scale = 0.5
	}
	var centreSq, deltaSq float64
	for r := range current {
		for c := range current[r] {
			var refv, crefv float64
			if ownRef != nil {
				refv = ownRef[r][c]
			}
			if cRef != nil {
				crefv = cRef[r][c]
			}
			d := current[r][c] - refv
			deltaSq += d * d
			v := crefv + scale*d
			centreSq += v * v
		}
	}
	return math.Sqrt(centreSq)+scale*math.Sqrt(deltaSq) <= radius
}

func cloneMatrix(m [][]float64) [][]float64 {
	out := make([][]float64, len(m))
	for i := range m {
		out[i] = append([]float64(nil), m[i]...)
	}
	return out
}
