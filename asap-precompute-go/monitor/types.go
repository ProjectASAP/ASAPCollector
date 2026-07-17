// Package monitor implements the edge side of coordinated update-sampling —
// what used to be called "Discipline B" in ASAPCollector
// docs/continuous-monitoring-tumbling-cost-analysis.md.
//
// Global-threshold ALERTING (the CMY slack-countdown: register → grant
// (slack, sample_p) → countdown → report → alert) is RETIRED as of the
// 2026-07 insert-time-GOS redesign (docs/design-gos-unified-edge-telemetry.md
// §11): "Alerting and any other query-time decision is made entirely at the
// backend against [the reconstructed sketch state]; the edge no longer makes
// alerting decisions itself." This edge never decides to fire an alert.
//
// What's left: the edge reports its per-epoch observation RATE (obsCount) on
// its own periodic cadence (Engine.Observe, decoupled from any value/slack
// threshold — see reportEveryN), and the coordinator answers with a
// coordinated-sampling grant (Grant.SampleP), the whole-sketch ε-floor
// p_i = 1/(1+ε²·rate_i). State resets at the tumbling boundary (one window =
// one independent monitoring epoch).
//
// The package is intentionally gRPC-free: the engine talks to the coordinator
// through the Reporter / Inbound interfaces, which the nested
// monitor/grpcclient module implements. That keeps the heavy gRPC dependency
// tree out of the core runtime's module graph.
package monitor

import "fmt"

// Functional selects which additive readout of a series' sketch a monitor
// reports as Report.LocalValue — informational only since alerting retired
// (see the package doc comment); it no longer drives any threshold decision.
// Identity is still (AggID, Key): CmsPoint carries a point-frequency key,
// Sum/LinearBuckets are whole-stream (key="").
type Functional uint8

const (
	// FunctionalSum reports the running window-sum (SumWrapper.Sum). O(1).
	FunctionalSum Functional = iota
	// FunctionalCMSPoint reports a Count-Min point-frequency f(x) for a
	// fixed key x (CMSWrapper.EstimateCount). O(rows).
	FunctionalCMSPoint
	// FunctionalLinearBuckets reports a non-negative linear functional over an
	// additive sketch's buckets. In v1 the only sketch that implements it is
	// DDSketch, where the realization is a VALUE-RANGE COUNT: Coeffs carries
	// the value bounds — Coeffs[0]=lo, Coeffs[1]=hi (optional, default +Inf) —
	// and the readout is the count of samples whose bucket value is in
	// [lo, hi] (e.g. "number of requests slower than 500ms").
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

// Spec is the per-AggID monitor configuration. It rides inside
// PrecomputeConfig and is delivered to edges through the existing control
// channel. Tau is vestigial (alerting retired — see the package doc comment);
// Epsilon still feeds the coordinator's whole-sketch ε-floor sampling law.
type Spec struct {
	Enabled        bool
	Functional     Functional
	Key            []byte    // CMS point-frequency key x (FunctionalCMSPoint)
	Coeffs         []float64 // linear-functional coefficients (FunctionalLinearBuckets); non-negative
	CoordinatorURL string    // edge→coordinator dial target
	Tau            float64   // unused (alerting retired); kept for wire/config-schema compatibility
	Epsilon        float64   // feeds the coordinator's whole-sketch ε-floor sampling law
}

// Validate rejects specs with malformed per-functional fields (e.g. a missing
// CMSPoint key, or negative LinearBuckets coefficients).
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
				return fmt.Errorf("monitor: linear_buckets coeff[%d]=%g is negative; only non-negative value-range bounds are supported", i, c)
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

// Report is one outbound edge→coordinator message, sent on the engine's own
// periodic cadence (reportEveryN observations, decoupled from any value/slack
// threshold — see the package doc comment). LocalValue is informational only
// (alerting retired); Rate is what the coordinator actually acts on.
type Report struct {
	EdgeID        string
	AggID         uint64
	Key           []byte
	WindowStartMs uint64
	LocalValue    float64
	Round         uint64
	Seq           uint64
	// Rate is the edge's observed item count for this monitor over the current
	// epoch (items/window). The coordinator feeds it into the whole-sketch
	// ε-floor (p_i = 1/(1+ε²·rate_i); see data_plane allocate_p /
	// epsilon_sample_floor) to size this edge's distributed-NitroSketch sampling
	// probability. 0 (unset) ⇒ the coordinator falls back to an unsampled
	// allocation for this edge.
	Rate float64
}

// Grant is the coordinator's reply to one Report: this edge's freshly
// computed coordinated-sampling grant.
type Grant struct {
	AggID         uint64
	Key           []byte
	Round         uint64
	// LocalSlack is unused (alerting retired); kept for SlackGrant wire
	// compatibility. The edge no longer gates anything on it.
	LocalSlack    float64
	WindowStartMs uint64
	// SampleP is the distributed-NitroSketch update-sampling probability the
	// coordinator allocates this edge via the whole-sketch ε-floor
	// (p_i = 1/(1+ε²·rate_i); see data_plane allocate_p / epsilon_sample_floor).
	// 0 (unset) ⇒ no sampling grant (p=1). The edge applies it via WithSampleP on
	// the metric's sketch wrapper at the next EpochReset (never mid-window, so
	// both merge operands share one p). See
	// docs/distributed-nitrosketch-coordinated-sampling.md.
	SampleP float64
}

// Poll is the coordinator→edge demand for an immediate out-of-cadence report.
type Poll struct {
	AggID         uint64
	Key           []byte
	Round         uint64
	WindowStartMs uint64
}

// Close is the coordinator→edge round-closed notice.
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
