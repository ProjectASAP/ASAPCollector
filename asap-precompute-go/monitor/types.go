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
)

func (f Functional) String() string {
	switch f {
	case FunctionalSum:
		return "sum"
	case FunctionalCMSPoint:
		return "cms_point"
	case FunctionalLinearBuckets:
		return "linear_buckets"
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
}

// Grant is the coordinator→edge per-round slack budget.
type Grant struct {
	AggID         uint64
	Key           []byte
	Round         uint64
	LocalSlack    float64
	WindowStartMs uint64
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
}
