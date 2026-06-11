package monitorpb

import (
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestCouplingFixture emits wire bytes for the two NEW coupling fields
// (SlackGrant.sample_p, MonitorReport.rate) so the Rust side decodes the
// identical bytes and asserts cross-language wire compat.
func TestCouplingFixture(t *testing.T) {
	grant := &CoordToEdge{Msg: &CoordToEdge_Grant{Grant: &SlackGrant{
		AggId: 42, Round: 7, LocalSlack: 3.5, WindowStartMs: 1_700_000_000_000,
		SampleP: 0.25,
	}}}
	gb, _ := proto.Marshal(grant)
	t.Logf("GRANT_HEX=%s", hex.EncodeToString(gb))

	rep := &EdgeToCoord{Msg: &EdgeToCoord_Report{Report: &MonitorReport{
		EdgeId: "edge-7", AggId: 42, LocalValue: 1234.5, Round: 3, Seq: 9,
		Rate: 1200,
	}}}
	rb, _ := proto.Marshal(rep)
	t.Logf("REPORT_HEX=%s", hex.EncodeToString(rb))
}

// TestRustGrantDecodesInGo decodes a SlackGrant emitted by the Rust coordinator
// (prost) and asserts sample_p survives the wire into the Go edge — the real
// coord->edge direction of the coupling.
func TestRustGrantDecodesInGo(t *testing.T) {
	bytes, err := hex.DecodeString("0a1d0863180421000000000000f03f2880d095ffbc3131333333333333d33f")
	if err != nil {
		t.Fatal(err)
	}
	var env CoordToEdge
	if err := proto.Unmarshal(bytes, &env); err != nil {
		t.Fatal(err)
	}
	g := env.GetGrant()
	if g == nil {
		t.Fatalf("expected Grant, got %+v", env.Msg)
	}
	if g.AggId != 99 || g.Round != 4 {
		t.Fatalf("envelope mismatch: %+v", g)
	}
	if d := g.SampleP - 0.3; d < -1e-12 || d > 1e-12 {
		t.Fatalf("sample_p crossed wrong: got %v want 0.3", g.SampleP)
	}
}
