// Package grpcclient is the gRPC transport for the edge side of the continuous
// distributed monitoring (CDM) protocol. It implements monitor.Reporter by
// streaming MonitorRegister/MonitorReport to the coordinator over the bidi
// MonitorService, and delivers inbound SlackGrant/PollLocal/RoundClose into the
// engine's monitor.Inbound. It lives in a SEPARATE Go module so the gRPC
// dependency tree never enters the core asap-precompute-go runtime graph.
package grpcclient

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
	pb "github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient/monitorpb"
)

// reconnectBackoff caps the dial retry interval.
const reconnectBackoff = 2 * time.Second

// sendQueueDepth bounds the outbound buffer. The hot-path enqueue is
// non-blocking: when the queue is full (coordinator slow/unreachable) reports
// are DROPPED rather than blocking Observe — the coordinator's round-close
// re-collection (and the next crossing) recovers, so dropping costs at most
// extra rounds, never correctness.
const sendQueueDepth = 256

// Client is a monitor.Reporter backed by a reconnecting bidi gRPC stream.
type Client struct {
	url     string
	inbound monitor.Inbound

	sendCh chan *pb.EdgeToCoord
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	dropped uint64 // reports dropped due to a full queue (observability)
}

var _ monitor.Reporter = (*Client)(nil)

// New dials the coordinator at url and starts the background stream loop.
// inbound receives coordinator directives (typically the *monitor.Engine).
func New(url string, inbound monitor.Inbound) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		url:     url,
		inbound: inbound,
		sendCh:  make(chan *pb.EdgeToCoord, sendQueueDepth),
		cancel:  cancel,
	}
	c.wg.Add(1)
	go c.run(ctx)
	return c
}

// Close stops the background loop and waits for it to exit.
func (c *Client) Close() {
	c.cancel()
	c.wg.Wait()
}

// DroppedReports returns the count of reports dropped due to a full send queue.
func (c *Client) DroppedReports() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropped
}

// Register implements monitor.Reporter (non-blocking).
func (c *Client) Register(r monitor.Registration) {
	c.enqueue(&pb.EdgeToCoord{Msg: &pb.EdgeToCoord_Reg{Reg: &pb.MonitorRegister{
		EdgeId:        r.EdgeID,
		AggId:         r.AggID,
		Key:           r.Key,
		EpochWindowMs: r.EpochWindowMs,
		WindowStartMs: r.WindowStartMs,
	}}})
}

// Report implements monitor.Reporter (non-blocking).
func (c *Client) Report(r monitor.Report) {
	c.enqueue(&pb.EdgeToCoord{Msg: &pb.EdgeToCoord_Report{Report: &pb.MonitorReport{
		EdgeId:        r.EdgeID,
		AggId:         r.AggID,
		Key:           r.Key,
		WindowStartMs: r.WindowStartMs,
		LocalValue:    r.LocalValue,
		Round:         r.Round,
		Seq:           r.Seq,
		Rate:          r.Rate,
		Sketch:        r.Sketch,
	}}})
}

func (c *Client) enqueue(m *pb.EdgeToCoord) {
	select {
	case c.sendCh <- m:
	default:
		c.mu.Lock()
		c.dropped++
		c.mu.Unlock()
	}
}

// run is the reconnecting stream loop. On each (re)connect it asks the engine to
// re-register (ForceReregister via the optional Reregisterer), then pumps the
// send queue to the stream and the stream's inbound to the engine until the
// stream errors, then backs off and redials.
func (c *Client) run(ctx context.Context) {
	defer c.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := grpc.NewClient(c.url, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			if sleepCtx(ctx, reconnectBackoff) {
				return
			}
			continue
		}
		c.session(ctx, conn)
		_ = conn.Close()
		if sleepCtx(ctx, reconnectBackoff) {
			return
		}
	}
}

// Reregisterer is implemented by engines that can be asked to re-announce all
// monitors after a (re)connect. *monitor.Engine satisfies it.
type Reregisterer interface{ ForceReregister() }

func (c *Client) session(ctx context.Context, conn *grpc.ClientConn) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := pb.NewMonitorServiceClient(conn).Monitor(streamCtx)
	if err != nil {
		return
	}

	// Ask the engine to re-announce its monitors over the fresh stream.
	if rr, ok := c.inbound.(Reregisterer); ok {
		rr.ForceReregister()
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Sender: drain the send queue into the stream.
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			select {
			case <-streamCtx.Done():
				return
			case m := <-c.sendCh:
				if err := stream.Send(m); err != nil {
					return
				}
			}
		}
	}()

	// Receiver: deliver coordinator directives into the engine.
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			c.deliver(msg)
		}
	}()

	wg.Wait()
}

func (c *Client) deliver(msg *pb.CoordToEdge) {
	switch m := msg.Msg.(type) {
	case *pb.CoordToEdge_Grant:
		g := m.Grant
		c.inbound.OnGrant(monitor.Grant{
			AggID:         g.AggId,
			Key:           g.Key,
			Round:         g.Round,
			LocalSlack:    g.LocalSlack,
			WindowStartMs: g.WindowStartMs,
			SampleP:       g.SampleP,
		})
	case *pb.CoordToEdge_Poll:
		p := m.Poll
		c.inbound.OnPoll(monitor.Poll{
			AggID:         p.AggId,
			Key:           p.Key,
			Round:         p.Round,
			WindowStartMs: p.WindowStartMs,
		})
	case *pb.CoordToEdge_Close:
		cl := m.Close
		c.inbound.OnClose(monitor.Close{
			AggID:         cl.AggId,
			Round:         cl.Round,
			WindowStartMs: cl.WindowStartMs,
		})
	case *pb.CoordToEdge_Ref:
		r := m.Ref
		c.inbound.OnRef(monitor.RefBroadcast{
			AggID:         r.AggId,
			Key:           r.Key,
			Round:         r.Round,
			WindowStartMs: r.WindowStartMs,
			K:             r.K,
			CRef:          r.CRef,
		})
	}
}

// sleepCtx sleeps for d or until ctx is cancelled; returns true if cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}
