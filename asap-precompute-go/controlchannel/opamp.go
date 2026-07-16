package controlchannel

import (
	"errors"
	"log"
	"sync"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// OpAmpChannel is the OpAMP-backed ControlChannel implementation.
//
// TODO(phase-5): wire real OpAMP client (github.com/open-telemetry/
// opamp-go/client); today this is an interface-satisfying stub so the
// controller side
// can compile against the trait without forcing a transitive
// opamp-go dep into Phase 2.
type OpAmpChannel struct {
	cfg    OpAmpConfig
	logger *log.Logger

	mu       sync.Mutex
	closed   bool
	warnedOnce bool
}

// OpAmpConfig configures OpAmpChannel. Real fields will grow as the
// Phase 5 wiring lands.
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
}

// NewOpAmpChannel constructs a new OpAmpChannel. Validates that
// ServerEndpoint is non-empty; the rest of the fields are accepted as-is
// pending real OpAMP integration.
func NewOpAmpChannel(cfg OpAmpConfig) (*OpAmpChannel, error) {
	if cfg.ServerEndpoint == "" {
		return nil, errors.New("controlchannel: OpAmpConfig.ServerEndpoint is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &OpAmpChannel{cfg: cfg, logger: logger}, nil
}

// Poll satisfies ControlChannel; the stub never reports a change. A
// single warning is emitted per instance to avoid log spam.
func (o *OpAmpChannel) Poll() *precompute.PrecomputeConfigSet {
	o.mu.Lock()
	warn := !o.warnedOnce && !o.closed
	o.warnedOnce = true
	o.mu.Unlock()
	if warn {
		o.logger.Printf("OpAMP control channel: not yet wired, returning no-op (endpoint=%s)",
			o.cfg.ServerEndpoint)
	}
	return nil
}

// Ack is a no-op for the stub. Logged at debug-level via the configured
// logger; the standard library log.Logger has no levels, so we simply
// write a one-line debug-style message.
func (o *OpAmpChannel) Ack(planVersion uint64) {
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return
	}
	// Intentionally low-volume — Ack is rare relative to Poll.
	o.logger.Printf("debug: OpAMP control channel: stub Ack(plan_version=%d) (endpoint=%s)",
		planVersion, o.cfg.ServerEndpoint)
}

// Close is a no-op for the stub but flips the internal flag so Poll/Ack
// can short-circuit after shutdown. Idempotent.
func (o *OpAmpChannel) Close() error {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	return nil
}

// Compile-time check.
var _ ControlChannel = (*OpAmpChannel)(nil)
