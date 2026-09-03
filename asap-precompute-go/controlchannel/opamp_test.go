package controlchannel

import (
	"encoding/json"
	"testing"
	"time"

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
			PlanID: 42, PlanVersion: version, GeneratedAtUnixMS: 1,
			ActivationUnixMS: 1, BackendCompat: "asap-query-backend.v1",
			PlannerRevision: "3afcba6", CapabilitySnapshotID: "caps",
		},
		Materializations: []precompute.CollectorMaterialization{{
			QueryID: "q", Materialization: 9001, Metric: "m", Algorithm: "hll",
			Parameters: map[string]float64{"precision": 14}, WindowSecs: 60,
			Lifecycle: precompute.SupportedCollectorLifecycle(),
		}},
		TransmissionRules: []precompute.TransmissionRule{{
			Materialization: 9001, ProducerID: "edge-a", SchemaID: "summary-state-v1-9001",
			Mode:        precompute.TransmissionModeFull,
			Encoding:    precompute.StateEncodingSketchlibProtobufV1,
			EmitEveryMS: 60_000, DestinationRef: "asapquery-backend",
			RuntimePolicy: precompute.RuntimeRulePolicy{
				Sampling: precompute.SamplingPolicy{Mode: "disabled"},
			},
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
		ReportStatus: func(_, _ uint64, status PlanStatus, _ error) { statuses = append(statuses, status) },
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
	if got, want := set.Configs[0].AggID, precompute.AggId(9001); got != want {
		t.Fatalf("cross-language materialization id: got %d want %d", got, want)
	}
	if channel.Poll() != nil {
		t.Fatal("same plan delivered more than once")
	}
	if err := channel.ReceiveCollectorPlan(channelPlan(t, 41)); err == nil {
		t.Fatal("plan older than the delivered version was accepted")
	}
	if len(statuses) != 2 || statuses[0] != PlanStatusStaged || statuses[1] != PlanStatusFailed {
		t.Fatalf("stale plan statuses: %v", statuses)
	}
	channel.Ack(41)
	if len(statuses) != 2 {
		t.Fatal("wrong-version ack reported APPLIED")
	}
	channel.Ack(42)
	if len(statuses) != 3 || statuses[2] != PlanStatusApplied {
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

func TestOpAmpChannelRejectedPlanReportsEnvelopeID(t *testing.T) {
	var reported uint64
	channel, _ := NewOpAmpChannel(OpAmpConfig{
		ServerEndpoint: "ws://controller", InstanceUid: "edge-a",
		ReportStatus: func(_ uint64, version uint64, status PlanStatus, _ error) {
			if status == PlanStatusFailed {
				reported = version
			}
		},
	})
	body := channelPlan(t, 73)
	body = append(body[:len(body)-1], []byte(`,"unknown_semantics":true}`)...)
	if err := channel.ReceiveCollectorPlan(body); err == nil {
		t.Fatal("unknown plan semantics were accepted")
	}
	if reported != 73 {
		t.Fatalf("reported plan id = %d, want 73", reported)
	}
}

func TestOpAmpChannelStagesUntilActivation(t *testing.T) {
	now := time.UnixMilli(10_000)
	channel, err := NewOpAmpChannel(OpAmpConfig{
		ServerEndpoint: "ws://controller", InstanceUid: "edge-a", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	body := channelPlan(t, 1)
	var plan precompute.CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Envelope.GeneratedAtUnixMS = 10_000
	plan.Envelope.ActivationUnixMS = 11_000
	body, _ = json.Marshal(plan)
	if err := channel.ReceiveCollectorPlan(body); err != nil {
		t.Fatal(err)
	}
	if got := channel.Poll(); got != nil {
		t.Fatalf("future plan activated early: %+v", got)
	}
	now = time.UnixMilli(11_000)
	if got := channel.Poll(); got == nil || got.Version != 1 {
		t.Fatalf("plan did not activate: %+v", got)
	}
}

func TestOpAmpChannelAllowsVersionOneForNewPlanIdentity(t *testing.T) {
	channel, err := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "ws://controller", InstanceUid: "edge-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.ReceiveCollectorPlan(channelPlan(t, 7)); err != nil {
		t.Fatal(err)
	}
	if channel.Poll() == nil {
		t.Fatal("first plan not delivered")
	}
	channel.Ack(7)
	body := channelPlan(t, 1)
	var plan precompute.CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Envelope.PlanID = 99
	body, _ = json.Marshal(plan)
	if err := channel.ReceiveCollectorPlan(body); err != nil {
		t.Fatalf("new plan identity version 1 rejected as stale: %v", err)
	}
}
