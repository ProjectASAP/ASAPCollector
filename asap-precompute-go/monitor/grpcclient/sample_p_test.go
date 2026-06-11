package grpcclient

import (
	"testing"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
	pb "github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient/monitorpb"
)

// fakeInbound records the directives the client delivers into the engine.
type fakeInbound struct {
	grants []monitor.Grant
	polls  []monitor.Poll
	closes []monitor.Close
}

func (f *fakeInbound) OnGrant(g monitor.Grant) { f.grants = append(f.grants, g) }
func (f *fakeInbound) OnPoll(p monitor.Poll)   { f.polls = append(f.polls, p) }
func (f *fakeInbound) OnClose(c monitor.Close) { f.closes = append(f.closes, c) }

// TestInboundGrantSamplePRoundTrip checks the inbound proto→domain mapping:
// a SlackGrant carrying sample_p decodes to monitor.Grant.SampleP.
func TestInboundGrantSamplePRoundTrip(t *testing.T) {
	fi := &fakeInbound{}
	c := &Client{inbound: fi}

	c.deliver(&pb.CoordToEdge{Msg: &pb.CoordToEdge_Grant{Grant: &pb.SlackGrant{
		AggId:         7,
		Round:         3,
		LocalSlack:    10,
		WindowStartMs: 60_000,
		SampleP:       0.25,
	}}})

	if len(fi.grants) != 1 {
		t.Fatalf("expected one delivered grant, got %d", len(fi.grants))
	}
	g := fi.grants[0]
	if g.AggID != 7 || g.Round != 3 || g.LocalSlack != 10 || g.WindowStartMs != 60_000 {
		t.Fatalf("grant base fields wrong: %+v", g)
	}
	if g.SampleP != 0.25 {
		t.Fatalf("Grant.SampleP = %v, want 0.25", g.SampleP)
	}
}

// TestOutboundReportRateRoundTrip checks the outbound domain→proto mapping:
// a monitor.Report carrying Rate enqueues a MonitorReport with rate set.
func TestOutboundReportRateRoundTrip(t *testing.T) {
	c := &Client{sendCh: make(chan *pb.EdgeToCoord, 1)}

	c.Report(monitor.Report{
		EdgeID:        "edge-1",
		AggID:         7,
		WindowStartMs: 60_000,
		LocalValue:    42,
		Round:         3,
		Seq:           5,
		Rate:          1200,
	})

	select {
	case m := <-c.sendCh:
		rep, ok := m.Msg.(*pb.EdgeToCoord_Report)
		if !ok {
			t.Fatalf("enqueued message is not a MonitorReport: %T", m.Msg)
		}
		if rep.Report.Rate != 1200 {
			t.Fatalf("proto MonitorReport.rate = %v, want 1200", rep.Report.Rate)
		}
		if rep.Report.LocalValue != 42 || rep.Report.AggId != 7 {
			t.Fatalf("report base fields wrong: %+v", rep.Report)
		}
	default:
		t.Fatalf("Report did not enqueue a message")
	}
}
