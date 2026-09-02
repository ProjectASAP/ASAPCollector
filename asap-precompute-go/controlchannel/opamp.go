package controlchannel

import (
	"errors"
	"log"
	"sync"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// OpAmpChannel is the OpAMP-backed ControlChannel implementation.
//
// It owns typed CollectorPlan validation and activation state. A transport
// adapter supplies received OpAMP bodies through ReceiveCollectorPlan and uses
// ReportStatus to publish validation/application outcomes.
type OpAmpChannel struct {
	cfg OpAmpConfig

	mu        sync.Mutex
	closed    bool
	pending   *precompute.PrecomputeConfigSet
	delivered uint64
	lastAcked uint64
}

// PlanStatus is reported to the OpAMP transport adapter after validation or
// runtime acknowledgement.
type PlanStatus string

const (
	// PlanStatusApplied means the runtime acknowledged the exact plan version.
	PlanStatusApplied PlanStatus = "applied"
	// PlanStatusFailed means decoding/validation failed; the active plan stays unchanged.
	PlanStatusFailed PlanStatus = "failed"
)

// OpAmpConfig configures OpAmpChannel.
type OpAmpConfig struct {
	// ServerEndpoint is the OpAMP supervisor's URL (ws/wss/http(s)).
	// Required.
	ServerEndpoint string

	// InstanceUid is the agent's stable identifier used by the
	// supervisor to address this runtime.
	InstanceUid string

	// Capabilities is the bitmask of OpAMP capabilities advertised
	// to the supervisor (see opamp-go's protobuf for values).
	Capabilities uint64

	// Logger receives one-line log messages. Defaults to log.Default().
	Logger *log.Logger

	// ReportStatus bridges validation/runtime acknowledgement back to the
	// concrete OpAMP client without importing opamp-go into this host-neutral
	// package.
	ReportStatus func(planVersion uint64, status PlanStatus, err error)
}

// NewOpAmpChannel constructs a new OpAmpChannel. Validates that
// ServerEndpoint is non-empty; the rest of the fields are accepted as-is
// pending real OpAMP integration.
func NewOpAmpChannel(cfg OpAmpConfig) (*OpAmpChannel, error) {
	if cfg.ServerEndpoint == "" {
		return nil, errors.New("controlchannel: OpAmpConfig.ServerEndpoint is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &OpAmpChannel{cfg: cfg}, nil
}

// ReceiveCollectorPlan validates an ASAPQuery CollectorPlan and queues it for
// one atomic Poll delivery. A rejected plan never replaces the pending/active
// valid plan.
func (o *OpAmpChannel) ReceiveCollectorPlan(body []byte) error {
	set, err := precompute.DecodeCollectorPlan(body, o.cfg.InstanceUid)
	if err != nil {
		if o.cfg.ReportStatus != nil {
			o.cfg.ReportStatus(0, PlanStatusFailed, err)
		}
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return errors.New("controlchannel: OpAmpChannel is closed")
	}
	if set.Version <= o.lastAcked || set.Version <= o.delivered ||
		(o.pending != nil && set.Version <= o.pending.Version) {
		return errors.New("controlchannel: stale or duplicate CollectorPlan version")
	}
	o.pending = set
	return nil
}

// Poll returns each successfully validated plan once.
func (o *OpAmpChannel) Poll() *precompute.PrecomputeConfigSet {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.pending == nil {
		return nil
	}
	set := o.pending
	o.pending = nil
	o.delivered = set.Version
	return set
}

// Ack reports APPLIED only for the exact version most recently delivered.
func (o *OpAmpChannel) Ack(planVersion uint64) {
	o.mu.Lock()
	if o.closed || planVersion == 0 || planVersion != o.delivered {
		o.mu.Unlock()
		return
	}
	o.lastAcked = planVersion
	o.delivered = 0
	o.mu.Unlock()
	if o.cfg.ReportStatus != nil {
		o.cfg.ReportStatus(planVersion, PlanStatusApplied, nil)
	}
}

// Close prevents subsequent receipt, delivery, and acknowledgement.
// It is idempotent.
func (o *OpAmpChannel) Close() error {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	return nil
}

// Compile-time check.
var _ ControlChannel = (*OpAmpChannel)(nil)
