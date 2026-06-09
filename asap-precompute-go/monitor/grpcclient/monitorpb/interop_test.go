package monitorpb

import (
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestInteropFixture round-trips a known EdgeToCoord and prints the wire bytes
// so the Rust side (asap_otel_proto) can decode the identical fixture and assert
// field-for-field equality — the cross-language Phase-1 gate.
func TestInteropFixture(t *testing.T) {
	msg := &EdgeToCoord{Msg: &EdgeToCoord_Report{Report: &MonitorReport{
		EdgeId:        "edge-7",
		AggId:         42,
		Key:           []byte("svc=checkout"),
		WindowStartMs: 1_700_000_000_000,
		LocalValue:    1234.5,
		Round:         3,
		Seq:           9,
	}}}
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FIXTURE_HEX=%s", hex.EncodeToString(b))

	var back EdgeToCoord
	if err := proto.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	r := back.GetReport()
	if r == nil || r.EdgeId != "edge-7" || r.AggId != 42 || r.LocalValue != 1234.5 || r.Round != 3 || r.Seq != 9 || string(r.Key) != "svc=checkout" {
		t.Fatalf("round-trip mismatch: %+v", r)
	}
}
