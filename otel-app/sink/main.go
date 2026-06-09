// sink — a minimal OTLP/gRPC metrics receiver that counts and discards.
//
// Used only to give otel-app a real export endpoint so the producer's
// marshal+gzip export path is exercised. It does NO sketching or
// aggregation, so any producer-side CPU difference between -agg modes is
// attributable to the producer, not the sink. It prints received
// request/datapoint/byte totals on shutdown (SIGINT/SIGTERM).
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	cmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type server struct {
	cmetrics.UnimplementedMetricsServiceServer
	reqs       atomic.Int64
	dataPoints atomic.Int64
	bytes      atomic.Int64
}

func (s *server) Export(ctx context.Context, req *cmetrics.ExportMetricsServiceRequest) (*cmetrics.ExportMetricsServiceResponse, error) {
	s.reqs.Add(1)
	s.bytes.Add(int64(proto.Size(req)))
	var dp int64
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				// Count datapoints across the common metric shapes.
				if g := m.GetGauge(); g != nil {
					dp += int64(len(g.GetDataPoints()))
				}
				if sum := m.GetSum(); sum != nil {
					dp += int64(len(sum.GetDataPoints()))
				}
				if h := m.GetHistogram(); h != nil {
					dp += int64(len(h.GetDataPoints()))
				}
				if eh := m.GetExponentialHistogram(); eh != nil {
					dp += int64(len(eh.GetDataPoints()))
				}
				if su := m.GetSummary(); su != nil {
					dp += int64(len(su.GetDataPoints()))
				}
			}
		}
	}
	s.dataPoints.Add(dp)
	return &cmetrics.ExportMetricsServiceResponse{}, nil
}

func main() {
	addr := ":4317"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	// Generous limits so large raw-buffer payloads are accepted, not rejected.
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(1 << 30))
	srv := &server{}
	cmetrics.RegisterMetricsServiceServer(gs, srv)
	log.Printf("sink listening on %s", addr)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Printf("SINK_STATS reqs=%d datapoints=%d bytes=%d",
			srv.reqs.Load(), srv.dataPoints.Load(), srv.bytes.Load())
		gs.GracefulStop()
	}()

	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
