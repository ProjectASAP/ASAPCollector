package controlchannel

import (
	"testing"
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
	// Second Poll should still return nil and (silently) skip the
	// once-per-instance warn log.
	if got := ch.Poll(); got != nil {
		t.Errorf("second Poll: want nil, got %+v", got)
	}
}

func TestOpAmp_AckIsNoOp(t *testing.T) {
	t.Parallel()
	ch, err := NewOpAmpChannel(OpAmpConfig{ServerEndpoint: "http://localhost:4320"})
	if err != nil {
		t.Fatalf("NewOpAmpChannel: %v", err)
	}
	// Must not panic and must accept any plan version.
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
	// Poll after Close should still return nil, no panic.
	if got := ch.Poll(); got != nil {
		t.Errorf("Poll after Close: want nil, got %+v", got)
	}
}

func TestOpAmp_SatisfiesControlChannel(t *testing.T) {
	t.Parallel()
	var _ ControlChannel = (*OpAmpChannel)(nil)
}
