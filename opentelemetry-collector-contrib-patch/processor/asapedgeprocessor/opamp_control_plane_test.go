// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampcustommessages"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

type testOpAMPHandler struct {
	mux          sync.Mutex
	messages     chan *protobufs.CustomMessage
	sentType     string
	sentBody     []byte
	unregistered bool
	pendingOnce  chan struct{}
	sendCalls    int
}

func newTestOpAMPHandler() *testOpAMPHandler {
	return &testOpAMPHandler{messages: make(chan *protobufs.CustomMessage, 2)}
}

func (h *testOpAMPHandler) Message() <-chan *protobufs.CustomMessage { return h.messages }
func (h *testOpAMPHandler) SendMessage(messageType string, body []byte) (chan struct{}, error) {
	h.mux.Lock()
	h.sendCalls++
	if h.pendingOnce != nil {
		pending := h.pendingOnce
		h.pendingOnce = nil
		h.mux.Unlock()
		return pending, types.ErrCustomMessagePending
	}
	h.sentType = messageType
	h.sentBody = append([]byte(nil), body...)
	h.mux.Unlock()
	return nil, nil
}

func TestOpAMPPlanStatusRetriesAfterPendingSend(t *testing.T) {
	pending := make(chan struct{})
	close(pending)
	handler := newTestOpAMPHandler()
	handler.pendingOnce = pending
	bridge := &opAMPPlanBridge{handler: handler, logger: zap.NewNop(), stop: make(chan struct{})}
	bridge.reportStatus(9, "applied", nil)
	handler.mux.Lock()
	defer handler.mux.Unlock()
	if handler.sendCalls != 2 || handler.sentType != asapStatusMessage {
		t.Fatalf("status retry calls=%d type=%q", handler.sendCalls, handler.sentType)
	}
}
func (h *testOpAMPHandler) Unregister() {
	h.mux.Lock()
	h.unregistered = true
	h.mux.Unlock()
}

type testOpAMPRegistry struct {
	handler    *testOpAMPHandler
	capability string
}

func (*testOpAMPRegistry) Start(context.Context, component.Host) error { return nil }
func (*testOpAMPRegistry) Shutdown(context.Context) error              { return nil }
func (r *testOpAMPRegistry) Register(capability string, _ ...opampcustommessages.CustomCapabilityRegisterOption) (opampcustommessages.CustomCapabilityHandler, error) {
	r.capability = capability
	return r.handler, nil
}

type opAMPTestHost struct {
	extensions map[component.ID]component.Component
}

func (h opAMPTestHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

func testCollectorPlan(t *testing.T) []byte {
	t.Helper()
	body, err := json.Marshal(precompute.CollectorPlan{
		CollectorID: "edge-a",
		Envelope: precompute.CollectorPlanEnvelope{
			PlanID: 42, PlannerRevision: "3afcba6", CapabilitySnapshotID: "caps-7",
		},
		Materializations: []precompute.CollectorMaterialization{{
			QueryID: "q", Metric: "requests", Algorithm: "hll",
			Parameters: map[string]float64{"precision": 14}, WindowSecs: 60,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestOpAMPPlanBridgeReceivePollAck(t *testing.T) {
	id := component.MustNewID("opamp")
	handler := newTestOpAMPHandler()
	registry := &testOpAMPRegistry{handler: handler}
	bridge, err := newOpAMPPlanBridge(ControlChannelConfig{
		OpAMPExtension: &id, CollectorID: "edge-a",
	}, opAMPTestHost{extensions: map[component.ID]component.Component{id: registry}}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bridge.Close() })
	if registry.capability != asapPlanCapability {
		t.Fatalf("registered capability %q", registry.capability)
	}
	handler.messages <- &protobufs.CustomMessage{Type: asapPlanMessage, Data: testCollectorPlan(t)}

	deadline := time.Now().Add(time.Second)
	var set *precompute.PrecomputeConfigSet
	for set == nil && time.Now().Before(deadline) {
		set = bridge.Poll()
		time.Sleep(time.Millisecond)
	}
	if set == nil || set.Version != 42 || len(set.Configs) != 1 {
		t.Fatalf("typed plan not delivered: %+v", set)
	}
	bridge.Ack(42)

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handler.mux.Lock()
		messageType, body := handler.sentType, append([]byte(nil), handler.sentBody...)
		handler.mux.Unlock()
		if messageType == asapStatusMessage {
			var status planStatusBody
			if err := json.Unmarshal(body, &status); err != nil {
				t.Fatal(err)
			}
			if status.PlanID != 42 || status.Status != "applied" || status.Error != "" {
				t.Fatalf("unexpected status: %+v", status)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("APPLIED status was not sent")
}

func TestControlChannelConfigRejectsTwoTransports(t *testing.T) {
	id := component.MustNewID("opamp")
	cfg := &Config{
		ShardCount: 1, WindowDuration: time.Minute,
		Metrics: []MetricFamily{{Metric: "requests", Family: FamilyHLL}},
		ControlChannel: ControlChannelConfig{
			PollURL: "https://controller/plans", OpAMPExtension: &id, CollectorID: "edge-a",
		},
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatal("HTTP and OpAMP control sources were accepted together")
	}
}
