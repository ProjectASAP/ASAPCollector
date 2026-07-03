// Package monitor implements the edge side of the continuous distributed
// monitoring (CDM) model — Discipline B from ASAPCollector
// docs/continuous-monitoring-tumbling-cost-analysis.md. The runtime already
// emits one sketch per tumbling window (Discipline A); this package adds the
// intra-window early-alert protocol: the edge holds a per-monitor slack budget
// granted by the coordinator and sends a report only when its LOCAL additive
// value climbs past that slack, staying silent otherwise. State resets at the
// tumbling boundary (one window = one independent monitoring epoch).
//
// The package is intentionally gRPC-free: the engine talks to the coordinator
// through the Reporter / Inbound interfaces, which the nested
// monitor/grpcclient module implements. That keeps the heavy gRPC dependency
// tree out of the core runtime's module graph.
package monitor

import "fmt"

// Functional selects which additive readout of a series' sketch the monitor
// thresholds. v1 covers only ADDITIVE functionals, which are monotone
// non-decreasing within a tumbling window — the property the slack countdown
// relies on.
type Functional uint8

const (
	// FunctionalSum thresholds the running window-sum (SumWrapper.Sum). O(1).
	FunctionalSum Functional = iota
	// FunctionalCMSPoint thresholds a Count-Min point-frequency f(x) for a
	// fixed key x (CMSWrapper.EstimateCount). O(rows).
	FunctionalCMSPoint
	// FunctionalLinearBuckets thresholds a non-negative linear functional over an
	// additive sketch's buckets. In v1 the only sketch that implements it is
	// DDSketch, where the monotone realization is a VALUE-RANGE COUNT: Coeffs
	// carries the value bounds — Coeffs[0]=lo, Coeffs[1]=hi (optional, default
	// +Inf) — and the readout is the count of samples whose bucket value is in
	// [lo, hi] (e.g. "number of requests slower than 500ms"). This is an
	// additive non-negative aggregate, hence monotone within a window. The
	// signed/quantile-threshold variants the cost-analysis doc also describes are
	// non-monotone and out of scope; Validate rejects negative coefficients.
	FunctionalLinearBuckets
	// FunctionalF2 thresholds the whole-sketch second moment F2 = ‖f‖₂²
	// (self-join size / variance / heavy-hitter energy) of a series' Count-Sketch.
	// F2 is NON-LINEAR across edges — F2(Σf_i)=ΣF2(f_i)+2Σ⟨f_i,f_j⟩ — so unlike
	// the scalar functionals it cannot be reduced to a per-edge scalar; the edge
	// ships (or safe-zone-gates) its whole Count-Sketch cell matrix and the
	// coordinator merges then squares. Handled by F2Engine, not the scalar
	// slack-countdown Engine.
	FunctionalF2
)

// F2Mode is the distributed-monitoring variant for FunctionalF2.
type F2Mode uint8

const (
	// F2ModeDistributed ships the Count-Sketch every window (bandwidth baseline).
	F2ModeDistributed F2Mode = iota
	// F2ModeGeometric runs the Sharfman–Schuster–Keren safe-zone test locally and
	// ships only on a local violation or a coordinator resync pull.
	F2ModeGeometric
)

// ParseF2Mode maps the pushed-config mode string to an F2Mode ("geometric" ⇒
// geometric, anything else ⇒ distributed).
func ParseF2Mode(s string) F2Mode {
	switch s {
	case "geometric", "geom", "safezone", "safe_zone":
		return F2ModeGeometric
	default:
		return F2ModeDistributed
	}
}

func (f Functional) String() string {
	switch f {
	case FunctionalSum:
		return "sum"
	case FunctionalCMSPoint:
		return "cms_point"
	case FunctionalLinearBuckets:
		return "linear_buckets"
	case FunctionalF2:
		return "f2"
	default:
		return fmt.Sprintf("functional(%d)", uint8(f))
	}
}

// Spec is the per-AggID threshold-monitor configuration. It rides inside
// PrecomputeConfig and is delivered to edges through the existing control
// channel. τ is advisory at the edge (used for diagnostics); the authoritative
// τ lives at the coordinator.
type Spec struct {
	Enabled        bool
	Functional     Functional
	Key            []byte    // CMS point-frequency key x (FunctionalCMSPoint)
	Coeffs         []float64 // linear-functional coefficients (FunctionalLinearBuckets); non-negative
	CoordinatorURL string    // edge→coordinator dial target
	Tau            float64   // advisory threshold (authoritative copy at coordinator)
	Epsilon        float64   // advisory relative tolerance
	// F2 fields (FunctionalF2 only): the Count-Sketch dimensions the whole-sketch
	// monitor squares/merges — MUST match the series' configured Count-Sketch on
	// every edge (identical rows/cols/seed) so the coordinator's linear merge is
	// meaningful. Mode selects the ship-every-window vs geometric-safe-zone path.
	SketchRows int
	SketchCols int
	Mode       F2Mode
}

// Validate rejects specs whose functional would violate the within-window
// monotonicity the slack countdown requires (negative linear coefficients), or
// that are missing required fields.
func (s Spec) Validate() error {
	if !s.Enabled {
		return nil
	}
	switch s.Functional {
	case FunctionalSum:
		// no extra fields required
	case FunctionalCMSPoint:
		if len(s.Key) == 0 {
			return fmt.Errorf("monitor: cms_point functional requires a non-empty key")
		}
	case FunctionalLinearBuckets:
		if len(s.Coeffs) == 0 {
			return fmt.Errorf("monitor: linear_buckets functional requires coeffs")
		}
		for i, c := range s.Coeffs {
			if c < 0 {
				return fmt.Errorf("monitor: linear_buckets coeff[%d]=%g is negative; only non-negative (monotone) functionals are supported in v1", i, c)
			}
		}
	case FunctionalF2:
		if s.SketchRows <= 0 || s.SketchCols <= 0 {
			return fmt.Errorf("monitor: f2 functional requires positive sketch dims (rows=%d cols=%d)", s.SketchRows, s.SketchCols)
		}
	default:
		return fmt.Errorf("monitor: unknown functional %d", uint8(s.Functional))
	}
	if s.CoordinatorURL == "" {
		return fmt.Errorf("monitor: enabled spec requires a coordinator_url")
	}
	return nil
}

// Registration announces a monitor to the coordinator at the start of an epoch.
type Registration struct {
	EdgeID        string
	AggID         uint64
	Key           []byte
	EpochWindowMs uint64
	// WindowStartMs is the edge's current aligned epoch start, so the
	// coordinator pins the monitor to the same epoch without a server clock.
	WindowStartMs uint64
}

// Report is one outbound edge→coordinator message: the current local additive
// value for a monitor, sent when the local increase since round start crossed
// the granted slack, or in answer to a Poll at round close.
type Report struct {
	EdgeID        string
	AggID         uint64
	Key           []byte
	WindowStartMs uint64
	LocalValue    float64
	Round         uint64
	Seq           uint64
	// Rate is the edge's observed item count for this monitor over the current
	// epoch (items/window). The coordinator feeds it into AllocateSampleRates
	// (p_i ∝ √(f_i/rate_i)) to size this edge's distributed-NitroSketch sampling
	// probability. 0 (unset) ⇒ the coordinator falls back to an unsampled
	// allocation for this edge.
	Rate float64
	// Sketch is the msgpack-serialized Count-Sketch cell matrix for whole-sketch
	// (FunctionalF2) monitors (asapmsgpack.MarshalCountSketch). Empty for scalar
	// monitors, which carry LocalValue instead.
	Sketch []byte
}

// Grant is the coordinator→edge per-round slack budget.
type Grant struct {
	AggID         uint64
	Key           []byte
	Round         uint64
	LocalSlack    float64
	WindowStartMs uint64
	// SampleP is the distributed-NitroSketch update-sampling probability the
	// coordinator allocates this edge (AllocateSampleRates: p_i ∝ √(f_i/rate_i)).
	// 0 (unset) ⇒ no sampling grant (p=1). The edge applies it via WithSampleP on
	// the metric's sketch wrapper at the next EpochReset (never mid-window, so
	// both merge operands share one p). Orthogonal to LocalSlack (which governs
	// emission/bandwidth; this governs update CPU). See
	// docs/distributed-nitrosketch-coordinated-sampling.md.
	SampleP float64
}

// Poll is the coordinator→edge demand for the current local value (round close).
type Poll struct {
	AggID         uint64
	Key           []byte
	Round         uint64
	WindowStartMs uint64
}

// Close is the coordinator→edge round-closed notice; the edge advances its
// round baseline.
type Close struct {
	AggID         uint64
	Round         uint64
	WindowStartMs uint64
}

// RefBroadcast is the coordinator→edge geometric-F2 reference: the merged
// Count-Sketch C_ref at the last sync plus the site count k. The edge runs its
// local safe-zone test against this reference and stays silent while safe.
type RefBroadcast struct {
	AggID         uint64
	Key           []byte
	Round         uint64
	WindowStartMs uint64
	K             uint64
	CRef          []byte // msgpack-serialized merged Count-Sketch matrix
}

// Reporter is the transport boundary the engine calls to reach the coordinator.
// Implementations (monitor/grpcclient) MUST be non-blocking: Register and Report
// run on (or just off) the observation hot path and must never block on network
// I/O — buffer internally and drop/flush asynchronously.
type Reporter interface {
	Register(Registration)
	Report(Report)
}

// Inbound is the boundary the transport calls to deliver coordinator directives
// into the engine. The engine implements it; the transport's read loop invokes
// these from its own goroutine.
type Inbound interface {
	OnGrant(Grant)
	OnPoll(Poll)
	OnClose(Close)
	// OnRef delivers a geometric-F2 reference broadcast (C_ref, k). Scalar-only
	// engines may ignore it.
	OnRef(RefBroadcast)
}
