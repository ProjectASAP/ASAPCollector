// Command f2driver is the EDGE side of the cross-language F2 / geometric
// distributed-monitoring eval (deploy/mvp-multinode/scripts/f2_monitor_eval.sh).
// It spins up N edges, each a monitor.F2Engine + grpcclient transport against a
// running Rust f2_monitor_harness, and drives a single monitoring epoch as a
// sequence of SUB-WINDOW steps: every step each edge accumulates more mass into
// its Count-Sketch and calls OnWindow. Distributed mode ships every step;
// geometric mode ships only when the local safe-zone trips — the eval compares
// the two by reading the harness's F2_COMM accounting.
//
// The synthetic workload adds `drift` to each of H shared keys per edge per
// step, so the merged frequency of every key is N*drift*(step+1) and the exact
// global F2 = H*(N*drift*(step+1))² grows quadratically and crosses τ.
//
// A second workload, `stable`, loads a baseline below τ then applies small
// zero-mean perturbations each step so the global F2 hovers below τ — the
// scenario geometric monitoring is built for: sites stay locally safe and go
// silent, so the coordinator receives almost nothing and never rebroadcasts.
//
// Usage: f2driver <url> <mode> <agg_id> <tau> <eps> <rows> <cols> <edges> <steps> <drift> [pattern]
package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

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
func argOr(i int, def string) string {
	if len(os.Args) > i {
		return os.Args[i]
	}
	return def
}

const numSharedKeys = 4

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: f2driver <url> <mode> <agg_id> <tau> <eps> <rows> <cols> <edges> <steps> <drift>")
		os.Exit(64)
	}
	url := os.Args[1]
	mode := monitor.ParseF2Mode(argOr(2, "distributed"))
	aggID := uint64(atoiOr(argOr(3, "1"), 1))
	tau := atofOr(argOr(4, "1000000"), 1_000_000)
	eps := atofOr(argOr(5, "0.1"), 0.1)
	rows := atoiOr(argOr(6, "5"), 5)
	cols := atoiOr(argOr(7, "256"), 256)
	nEdges := atoiOr(argOr(8, "4"), 4)
	nSteps := atoiOr(argOr(9, "20"), 20)
	drift := atofOr(argOr(10, "10"), 10)
	pattern := argOr(11, "ramp")

	const windowMs = uint64(3_600_000)
	windowStart := uint64(time.Now().UnixMilli()) / windowMs * windowMs

	type edge struct {
		eng    *monitor.F2Engine
		client *grpcclient.Client
		cs     *sketches.CountSketchWrapper
	}
	edges := make([]edge, nEdges)
	for i := 0; i < nEdges; i++ {
		eng := monitor.NewF2Engine(fmt.Sprintf("edge-%d", i), windowMs, nil)
		eng.Configure(aggID, nil, monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalF2,
			CoordinatorURL: url,
			Tau:            tau,
			Epsilon:        eps,
			SketchRows:     rows,
			SketchCols:     cols,
			Mode:           mode,
		})
		client := grpcclient.New(url, eng)
		eng.SetReporter(client)
		cs, err := sketches.NewCountSketchWrapper(rows, cols)
		if err != nil {
			fmt.Fprintf(os.Stderr, "f2driver: bad sketch dims %dx%d: %v\n", rows, cols, err)
			os.Exit(1)
		}
		edges[i] = edge{eng, client, cs}
	}
	defer func() {
		for i := range edges {
			edges[i].client.Close()
		}
	}()

	// Let the bidi streams connect before the first step registers.
	time.Sleep(800 * time.Millisecond)

	// Stable baseline: preload each edge so the merged F2 sits below τ.
	if pattern == "stable" {
		baseline := 0.6 * math.Sqrt(tau/float64(numSharedKeys)) / float64(nEdges)
		for i := range edges {
			for k := 0; k < numSharedKeys; k++ {
				edges[i].cs.UpdateString(fmt.Sprintf("k%d", k), baseline)
			}
		}
	}

	for step := 0; step < nSteps; step++ {
		for i := range edges {
			switch pattern {
			case "stable":
				// Zero-mean perturbation: shift a little mass between two keys so
				// the global F2 barely moves and stays below τ.
				edges[i].cs.UpdateString(fmt.Sprintf("k%d", step%numSharedKeys), drift)
				edges[i].cs.UpdateString(fmt.Sprintf("k%d", (step+1)%numSharedKeys), -drift)
			default: // ramp: monotone growth that crosses τ
				for k := 0; k < numSharedKeys; k++ {
					edges[i].cs.UpdateString(fmt.Sprintf("k%d", k), drift)
				}
			}
			edges[i].eng.OnWindow(aggID, nil, edges[i].cs.CellMatrix(), windowStart)
		}
		if pattern != "stable" {
			// Exact merged F2 = H*(N*drift*(step+1))² (ramp ground truth).
			f := float64(nEdges) * drift * float64(step+1)
			exactF2 := float64(numSharedKeys) * f * f
			fmt.Fprintf(os.Stderr, "f2driver: step=%d exact_f2=%.0f tau=%.0f\n", step, exactF2, tau)
		}
		// Grace for geometric RefBroadcast to propagate before the next step.
		time.Sleep(80 * time.Millisecond)
	}
	// Grace for the final ship → alert round-trip.
	time.Sleep(1500 * time.Millisecond)
	var dropped, ships, silent, refsRecv, refErrs uint64
	for i := range edges {
		dropped += edges[i].client.DroppedReports()
		s, sl, rr, re := edges[i].eng.Stats()
		ships += s
		silent += sl
		refsRecv += rr
		refErrs += re
	}
	fmt.Fprintf(os.Stderr, "f2driver: done mode=%v edges=%d steps=%d drift=%g dropped=%d ships=%d silent=%d refs_recv=%d ref_errs=%d\n",
		mode, nEdges, nSteps, drift, dropped, ships, silent, refsRecv, refErrs)
}
