package controlchannel

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// OpAmpChannel is the OpAMP-backed ControlChannel implementation.
//
// It owns typed CollectorPlan validation and activation state. A transport
// adapter supplies received OpAMP bodies through ReceiveCollectorPlan and uses
// ReportStatus to publish validation/application outcomes.
type OpAmpChannel struct {
	cfg OpAmpConfig

	mu                      sync.Mutex
	closed                  bool
	pending                 *precompute.PrecomputeConfigSet
	pendingPlanID           uint64
	pendingActivationUnixMS uint64
	pendingExpiryUnixMS     *uint64
	delivered               uint64
	deliveredPlanID         uint64
	lastAcked               uint64
	lastAckedPlanID         uint64
	maxVersion              uint64
}

// PlanStatus is reported to the OpAMP transport adapter after validation or
// runtime acknowledgement.
type PlanStatus string

const (
	// PlanStatusStaged means the complete generation is validated and scheduled.
	PlanStatusStaged PlanStatus = "staged"
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
	ReportStatus func(planID, planVersion uint64, status PlanStatus, err error)

	// Now is injectable for activation-boundary tests. Defaults to time.Now.
	Now func() time.Time
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
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &OpAmpChannel{cfg: cfg}, nil
}

// ReceiveCollectorPlan validates an ASAPQuery CollectorPlan and queues it for
// one atomic Poll delivery. A rejected plan never replaces the pending/active
// valid plan.
func (o *OpAmpChannel) ReceiveCollectorPlan(body []byte) error {
	set, err := precompute.DecodeCollectorPlan(body, o.cfg.InstanceUid)
	if err != nil {
		identity := planIdentityForStatus(body)
		o.reportFailed(identity.PlanID, identity.PlanVersion, err)
		return err
	}
	identity := planIdentityForStatus(body)
	nowUnixMS := uint64(o.cfg.Now().UnixMilli())
	if identity.ExpiryUnixMS != nil && *identity.ExpiryUnixMS <= nowUnixMS {
		err = errors.New("controlchannel: CollectorPlan is expired")
		o.reportFailed(identity.PlanID, set.Version, err)
		return err
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		err = errors.New("controlchannel: OpAmpChannel is closed")
		o.reportFailed(identity.PlanID, set.Version, err)
		return err
	}
	if set.Version <= o.maxVersion || o.pending != nil || o.delivered != 0 {
		o.mu.Unlock()
		err = errors.New("controlchannel: stale or duplicate CollectorPlan version")
		o.reportFailed(identity.PlanID, set.Version, err)
		return err
	}
	o.pending = set
	o.maxVersion = set.Version
	o.pendingPlanID = identity.PlanID
	o.pendingActivationUnixMS = identity.ActivationUnixMS
	o.pendingExpiryUnixMS = identity.ExpiryUnixMS
	o.mu.Unlock()
	if o.cfg.ReportStatus != nil {
		o.cfg.ReportStatus(identity.PlanID, set.Version, PlanStatusStaged, nil)
	}
	return nil
}

func (o *OpAmpChannel) reportFailed(planID, planVersion uint64, err error) {
	if o.cfg.ReportStatus != nil {
		o.cfg.ReportStatus(planID, planVersion, PlanStatusFailed, err)
	}
}

type planIdentity struct {
	PlanID           uint64  `json:"plan_id"`
	PlanVersion      uint64  `json:"plan_version"`
	ActivationUnixMS uint64  `json:"activation_unix_ms"`
	ExpiryUnixMS     *uint64 `json:"expiry_unix_ms"`
}

// planIdentityForStatus is correlation-only. DecodeCollectorPlan remains the sole
// authority for accepting the plan and still rejects unknown/malformed fields.
func planIdentityForStatus(body []byte) planIdentity {
	var identity struct {
		Envelope planIdentity `json:"envelope"`
	}
	_ = json.Unmarshal(body, &identity)
	return identity.Envelope
}

// Poll returns each successfully validated plan once.
func (o *OpAmpChannel) Poll() *precompute.PrecomputeConfigSet {
	o.mu.Lock()
	if o.closed || o.pending == nil {
		o.mu.Unlock()
		return nil
	}
	nowUnixMS := uint64(o.cfg.Now().UnixMilli())
	if nowUnixMS < o.pendingActivationUnixMS {
		o.mu.Unlock()
		return nil
	}
	if o.pendingExpiryUnixMS != nil && *o.pendingExpiryUnixMS <= nowUnixMS {
		planID, planVersion := o.pendingPlanID, o.pending.Version
		o.pending = nil
		o.pendingPlanID = 0
		o.pendingExpiryUnixMS = nil
		o.mu.Unlock()
		o.reportFailed(planID, planVersion, errors.New("controlchannel: staged CollectorPlan expired before application"))
		return nil
	}
	set := o.pending
	o.pending = nil
	o.deliveredPlanID = o.pendingPlanID
	o.pendingPlanID = 0
	o.pendingExpiryUnixMS = nil
	o.delivered = set.Version
	o.mu.Unlock()
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
	o.lastAckedPlanID = o.deliveredPlanID
	planID := o.lastAckedPlanID
	o.delivered = 0
	o.deliveredPlanID = 0
	o.mu.Unlock()
	if o.cfg.ReportStatus != nil {
		o.cfg.ReportStatus(planID, planVersion, PlanStatusApplied, nil)
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
