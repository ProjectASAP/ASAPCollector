// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/controlchannel"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampcustommessages"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

const (
	asapPlanCapability = "io.projectasap.collector-plan.v1"
	asapPlanMessage    = "collector_plan"
	asapStatusMessage  = "plan_status"
)

type planStatusBody struct {
	PlanID      uint64 `json:"plan_id"`
	PlanVersion uint64 `json:"plan_version"`
	Status      string `json:"status"`
	Error       string `json:"error,omitempty"`
}

// opAMPPlanBridge adapts the Collector OpAMP custom-message registry to the
// host-neutral typed plan state machine in asap-precompute-go.
type opAMPPlanBridge struct {
	channel *controlchannel.OpAmpChannel
	handler opampcustommessages.CustomCapabilityHandler
	logger  *zap.Logger
	stop    chan struct{}
	done    chan struct{}
	close   sync.Once
}

func newOpAMPPlanBridge(cfg ControlChannelConfig, host component.Host, logger *zap.Logger) (*opAMPPlanBridge, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	ext, ok := host.GetExtensions()[*cfg.OpAMPExtension]
	if !ok {
		return nil, fmt.Errorf("asap_edge: OpAMP extension %q does not exist", cfg.OpAMPExtension.String())
	}
	registry, ok := ext.(opampcustommessages.CustomCapabilityRegistry)
	if !ok {
		return nil, fmt.Errorf("asap_edge: extension %q is not an OpAMP custom-message registry", cfg.OpAMPExtension.String())
	}
	handler, err := registry.Register(asapPlanCapability)
	if err != nil {
		return nil, fmt.Errorf("asap_edge: register CollectorPlan capability: %w", err)
	}
	if handler == nil {
		return nil, errors.New("asap_edge: OpAMP CollectorPlan handler is nil")
	}
	b := &opAMPPlanBridge{handler: handler, logger: logger, stop: make(chan struct{}), done: make(chan struct{})}
	b.channel, err = controlchannel.NewOpAmpChannel(controlchannel.OpAmpConfig{
		ServerEndpoint: cfg.OpAMPExtension.String(),
		InstanceUid:    cfg.CollectorID,
		StateFile:      cfg.PlanStateFile,
		ReportStatus:   b.reportStatus,
	})
	if err != nil {
		handler.Unregister()
		return nil, err
	}
	go b.receive()
	return b, nil
}

func (b *opAMPPlanBridge) receive() {
	defer close(b.done)
	for {
		select {
		case <-b.stop:
			return
		case msg, ok := <-b.handler.Message():
			if !ok {
				return
			}
			if msg == nil || msg.Type != asapPlanMessage {
				continue
			}
			if err := b.channel.ReceiveCollectorPlan(msg.Data); err != nil {
				b.logger.Warn("asap_edge: rejected OpAMP CollectorPlan", zap.Error(err))
			}
		}
	}
}

func (b *opAMPPlanBridge) reportStatus(planID, planVersion uint64, status controlchannel.PlanStatus, statusErr error) {
	body := planStatusBody{PlanID: planID, PlanVersion: planVersion, Status: string(status)}
	if status == controlchannel.PlanStatusStaged {
		body.Status = "STAGED"
	} else if status == controlchannel.PlanStatusApplied {
		body.Status = "APPLIED"
	} else {
		body.Status = "FAILED"
	}
	if statusErr != nil {
		body.Error = statusErr.Error()
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		b.logger.Error("asap_edge: encode OpAMP plan status", zap.Error(err))
		return
	}
	for attempt := 1; attempt <= 3; attempt++ {
		pending, sendErr := b.handler.SendMessage(asapStatusMessage, encoded)
		if sendErr == nil {
			return
		}
		if !errors.Is(sendErr, types.ErrCustomMessagePending) || pending == nil {
			b.logger.Error("asap_edge: send OpAMP plan status", zap.Error(sendErr))
			return
		}
		select {
		case <-pending:
		case <-b.stop:
			return
		}
	}
	b.logger.Error("asap_edge: send OpAMP plan status exhausted retries", zap.Uint64("plan_id", planID))
}

func (b *opAMPPlanBridge) Poll() *precompute.PrecomputeConfigSet { return b.channel.Poll() }
func (b *opAMPPlanBridge) Ack(version uint64)                    { b.channel.Ack(version) }
func (b *opAMPPlanBridge) Reject(version uint64, err error)      { b.channel.Reject(version, err) }

func (b *opAMPPlanBridge) Close() error {
	b.close.Do(func() {
		close(b.stop)
		b.handler.Unregister()
		<-b.done
		_ = b.channel.Close()
	})
	return nil
}
