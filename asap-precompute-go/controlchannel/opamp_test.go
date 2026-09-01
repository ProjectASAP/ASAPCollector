package controlchannel

import (
	"encoding/json"
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func TestOpAmp_ConstructorValidatesEndpoint(t *testing.T) {
	t.Parallel()
	if _, err := NewOpAmpChannel(OpAmpConfig{}); err == nil {
		t.Fatalf("NewOpAmpChannel(empty): want error, got nil")
	}
	ch, err := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "http://localhost:4320"})
	if err != nil {
		t.Fatalf("NewOpAmpChannel: %v", err)
	}
	if ch == nil {
		t.Fatalf("NewOpAmpChannel: want non-nil channel")
	}
}

func TestOpAmp_PollReturnsNoOp(t *testing.T) {
	t.Parallel()
	ch, err := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "http://localhost:4320"})
	if err != nil {
		t.Fatalf("NewOpAmpChannel: %v", err)
	}
	if got := ch.Poll(); got != nil {
		t.Errorf("Poll: want nil, got %+v", got)
	}
	if got := ch.Poll(); got != nil {
		t.Errorf("second Poll: want nil, got %+v", got)
	}
}

func TestOpAmp_AckWithoutDeliveryIsNoOp(t *testing.T) {
	t.Parallel()
	ch, err := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "http://localhost:4320"})
	if err != nil {
		t.Fatalf("NewOpAmpChannel: %v", err)
	}
	ch.Ack(0)
	ch.Ack(42)
}

func TestOpAmp_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	ch, err := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "http://localhost:4320"})
	if err != nil {
		t.Fatalf("NewOpAmpChannel: %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Errorf("first Close: want nil, got %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Errorf("second Close: want nil, got %v", err)
	}
	if got := ch.Poll(); got != nil {
		t.Errorf("Poll after Close: want nil, got %+v", got)
	}
}

func TestOpAmp_SatisfiesControlChannel(t *testing.T) {
	t.Parallel()
	var _ ControlChannel = (*OpAmpChannel)(nil)
}

func channelPlan(t *testing.T, version uint64) []byte {
	t.Helper()
	body, err := json.Marshal(precompute.CollectorPlan{
		CollectorID: "edge-a",
		Envelope: precompute.CollectorPlanEnvelope{
			PlanID: version, PlannerRevision: "3afcba6", CapabilitySnapshotID: "caps",
		},
		Materializations: []precompute.CollectorMaterialization{{
			QueryID: "q", Metric: "m", Algorithm: "hll",
			Parameters: map[string]float64{"precision": 14}, WindowSecs: 60,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestOpAmpChannelReceivePollAckLifecycle(t *testing.T) {
	var statuses []PlanStatus
	channel, err := NewOpAmpChannel(OpAmpConfig{
		ServerEndpoint: "ws://controller", InstanceUid: "edge-a",
		ReportStatus: func(_ uint64, status PlanStatus, _ error) { statuses = append(statuses, status) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.ReceiveCollectorPlan(channelPlan(t, 42)); err != nil {
		t.Fatal(err)
	}
	set := channel.Poll()
	if set == nil || set.Version != 42 {
		t.Fatalf("plan not delivered: %+v", set)
	}
	if got, want := set.Configs[0].AggID, precompute.AggId(9843981254622943340); got != want {
		t.Fatalf("cross-language materialization id: got %d want %d", got, want)
	}
	if channel.Poll() != nil {
		t.Fatal("same plan delivered more than once")
	}
	if err := channel.ReceiveCollectorPlan(channelPlan(t, 41)); err == nil {
		t.Fatal("plan older than the delivered version was accepted")
	}
	channel.Ack(41)
	if len(statuses) != 0 {
		t.Fatal("wrong-version ack reported APPLIED")
	}
	channel.Ack(42)
	if len(statuses) != 1 || statuses[0] != PlanStatusApplied {
		t.Fatalf("statuses: %v", statuses)
	}
	if err := channel.ReceiveCollectorPlan(channelPlan(t, 42)); err == nil {
		t.Fatal("acked plan version accepted again")
	}
}

func TestOpAmpChannelRejectedPlanDoesNotReplacePendingValidPlan(t *testing.T) {
	channel, _ := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "ws://controller", InstanceUid: "edge-a"})
	if err := channel.ReceiveCollectorPlan(channelPlan(t, 7)); err != nil {
		t.Fatal(err)
	}
	if err := channel.ReceiveCollectorPlan([]byte(`{"collector_id":"wrong"}`)); err == nil {
		t.Fatal("invalid plan accepted")
	}
	if got := channel.Poll(); got == nil || got.Version != 7 {
		t.Fatalf("valid pending plan was lost: %+v", got)
	}
}
