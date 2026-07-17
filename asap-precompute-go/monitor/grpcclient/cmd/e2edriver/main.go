// Command e2edriver is the EDGE side of the cross-language coordinated-
// sampling e2e (deploy/mvp-multinode/scripts/monitor_e2e.sh). It wires the
// REAL edge runtime — precompute.Precompute (Sum) + monitor.Engine + the gRPC
// grpcclient transport — against a running Rust monitor-coordinator harness,
// then feeds a stream of observations. The engine reports its observed rate
// on its own periodic cadence (monitor.ReportEveryN observations — alerting
// retired, see the monitor package docs), and the coordinator should answer
// with a computed coordinated-sampling grant (SlackGrant.sample_p) over the
// live bidi stream.
//
// Usage: e2edriver <coordinator_url> [agg_id] [tau] [per_obs] [count]
// (tau is accepted for CLI/back-compat but unused — alerting retired.)
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

func atoiOr(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}
func atofOr(s string, def float64) float64 {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	return def
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: e2edriver <coordinator_url> [agg_id] [tau] [per_obs] [count]")
		os.Exit(64)
	}
	url := os.Args[1]
	aggID := uint64(1)
	if len(os.Args) > 2 {
		aggID = uint64(atoiOr(os.Args[2], 1))
	}
	tau := 100.0
	if len(os.Args) > 3 {
		tau = atofOr(os.Args[3], 100.0)
	}
	perObs := 5.0
	if len(os.Args) > 4 {
		perObs = atofOr(os.Args[4], 5.0)
	}
	count := 200
	if len(os.Args) > 5 {
		count = atoiOr(os.Args[5], 200)
	}

	// 1h tumbling window so the stream never rotates during the run; the
	// coordinator harness must be started with the same window_ms (3600000).
	const windowMs = uint64(3_600_000)
	pcfg := &precompute.PrecomputeConfig{
		AggID:   precompute.AggId(aggID),
		AggKind: precompute.AggKindSum,
		Mode:    precompute.Tumbling,
		Window:  precompute.WindowSpec{Size: time.Hour},
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalSum,
			CoordinatorURL: url,
			Tau:            tau,
			Epsilon:        0.05,
		},
	}
	factory := func() precompute.Sketch { return sketches.NewSumWrapper() }
	pc := precompute.New(pcfg, factory, sketches.SumObserver{})

	eng := monitor.NewEngine("e2e-edge", windowMs, nil)
	client := grpcclient.New(url, eng)
	eng.SetReporter(client)
	pc.SetMonitorEngine(eng)
	defer client.Close()

	// Let the bidi stream connect before the first observation registers.
	time.Sleep(800 * time.Millisecond)

	ts := uint64(time.Now().UnixMilli())
	labels := []precompute.KeyValue{{Key: "svc", Value: "checkout"}}
	for i := 0; i < count; i++ {
		_ = pc.Observe(&precompute.Observation{
			TimestampMs: ts, // fixed ts → stable window start (no rotation)
			Metric:      "bytes_sent",
			Labels:      labels,
			Value:       precompute.FloatValue(perObs),
		})
		time.Sleep(30 * time.Millisecond)
	}
	// Grace for the final report → grant round-trip.
	time.Sleep(1 * time.Second)
	fmt.Fprintf(os.Stderr, "e2edriver: fed %d observations of %g (sum=%g, tau=%g unused), dropped_reports=%d\n",
		count, perObs, float64(count)*perObs, tau, client.DroppedReports())
}
