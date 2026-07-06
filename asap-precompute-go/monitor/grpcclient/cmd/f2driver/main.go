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

const defaultSharedKeys = 4

// sharedKeys returns the workload's distinct-key count H (default 4). The
// F2_KEYS env var overrides it so the raw-vs-sketch crossover can be swept:
// raw bytes scale with samples (∝ H·steps), sketch bytes with d·w — on the
// default tiny-H protocol-stress workload RAW IS CHEAPER, by design.
func sharedKeys() int {
	if v := atoiOr(os.Getenv("F2_KEYS"), 0); v > 0 {
		return v
	}
	return defaultSharedKeys
}

// mpRawBatch serializes one step's raw samples as a msgpack array of
// [ts_ms(uint64), series_key(str), value(float64)] triples — the "no sketch
// aggregation" baseline's wire unit (matched-none: no compression), hand-rolled
// with the same msgpack primitives style as asapmsgpack. Returns the frame.
func mpRawBatch(ts uint64, keys []string, vals []float64) []byte {
	var b []byte
	appendUint := func(u uint64) {
		b = append(b, 0xcf, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
			byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
	}
	appendF64 := func(f float64) {
		u := math.Float64bits(f)
		b = append(b, 0xcb, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
			byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
	}
	appendArrHdr := func(n int) {
		if n < 16 {
			b = append(b, 0x90|byte(n))
		} else {
			b = append(b, 0xdc, byte(n>>8), byte(n))
		}
	}
	appendStr := func(s string) {
		if len(s) < 32 {
			b = append(b, 0xa0|byte(len(s)))
		} else {
			b = append(b, 0xd9, byte(len(s)))
		}
		b = append(b, s...)
	}
	appendArrHdr(len(keys))
	for i := range keys {
		appendArrHdr(3)
		appendUint(ts)
		appendStr(keys[i])
		appendF64(vals[i])
	}
	return b
}

// runRaw is the no-aggregation baseline: every edge ships its raw samples
// every step (serialized per-step msgpack batches; bytes counted from the real
// frames). No coordinator: the protocol is trivial (ship everything), and the
// alert ground truth is computed EXACTLY from the raw data (the coordinator
// would hold complete information) — alert when global F2 = Σ_k f(k)² crosses
// (1−ε)τ, the same rule the sketch modes fire on.
func runRaw(tau, eps float64, nEdges, nSteps int, drift float64, pattern string, windowStart uint64) {
	h := sharedKeys()
	global := make(map[string]float64) // merged exact frequency vector
	var totalBytes, samples uint64
	alertStep := -1
	exactF2 := func() float64 {
		var s float64
		for _, f := range global {
			s += f * f
		}
		return s
	}
	shipBatch := func(ts uint64, keys []string, vals []float64) {
		frame := mpRawBatch(ts, keys, vals)
		totalBytes += uint64(len(frame))
		samples += uint64(len(keys))
	}
	// Stable preload is raw data too — it must cross the wire like any sample.
	if pattern == "stable" {
		baseline := 0.6 * math.Sqrt(tau/float64(h)) / float64(nEdges)
		keys := make([]string, h)
		vals := make([]float64, h)
		for k := 0; k < h; k++ {
			keys[k] = fmt.Sprintf("k%d", k)
			vals[k] = baseline
		}
		for i := 0; i < nEdges; i++ {
			shipBatch(windowStart, keys, vals)
			for k := 0; k < h; k++ {
				global[keys[k]] += baseline
			}
		}
	}
	for step := 0; step < nSteps; step++ {
		ts := windowStart + uint64(step)*80
		for i := 0; i < nEdges; i++ {
			var keys []string
			var vals []float64
			switch pattern {
			case "stable":
				keys = []string{fmt.Sprintf("k%d", step%h), fmt.Sprintf("k%d", (step+1)%h)}
				vals = []float64{drift, -drift}
			default: // ramp
				for k := 0; k < h; k++ {
					keys = append(keys, fmt.Sprintf("k%d", k))
					vals = append(vals, drift)
				}
			}
			shipBatch(ts, keys, vals)
			for j := range keys {
				global[keys[j]] += vals[j]
			}
		}
		if alertStep < 0 && exactF2() >= (1.0-eps)*tau {
			alertStep = step
		}
	}
	alert := 0
	if alertStep >= 0 {
		alert = 1
	}
	fmt.Fprintf(os.Stderr,
		"f2driver: done mode=raw edges=%d steps=%d keys=%d drift=%g raw_samples=%d total_bytes=%d alert=%d alert_step=%d exact_f2=%.0f\n",
		nEdges, nSteps, h, drift, samples, totalBytes, alert, alertStep, exactF2())
}

// lossyInbound wraps an F2Engine to SIMULATE C_ref delta loss on the
// coordinator→edge path, so the eval can exercise the delta-loss guards
// (f2engine needFull force-ship + coordinator periodic keyframe). Gated by env
// vars, default OFF (the wrapper forwards every message unchanged):
//
//	F2_INJECT=corrupt  — replace the Nth delta's bytes with garbage → the edge's
//	                     OnRef decode fails → needFull → force-ship (detected).
//	F2_INJECT=drop     — silently swallow the Nth delta → the edge never sees it,
//	                     so needFull is NOT set (a pure mid-stream loss has no
//	                     sequence gap to detect); safety then rests on the
//	                     coordinator's periodic Full keyframe recovery.
//	F2_INJECT_NTH=<n>  — which delta broadcast (1-based) to affect (default 1).
//
// Only the injected edge is wrapped, modelling "one edge misses a delta".
type lossyInbound struct {
	inner *monitor.F2Engine
	mode  string
	nth   int
	seen  int
}

func (l *lossyInbound) OnGrant(g monitor.Grant) { l.inner.OnGrant(g) }
func (l *lossyInbound) OnPoll(p monitor.Poll)   { l.inner.OnPoll(p) }
func (l *lossyInbound) OnClose(c monitor.Close) { l.inner.OnClose(c) }
func (l *lossyInbound) OnRef(rb monitor.RefBroadcast) {
	if l.nth > 0 && rb.IsDelta {
		l.seen++
		if l.seen == l.nth {
			switch l.mode {
			case "drop":
				fmt.Fprintf(os.Stderr, "f2driver: INJECT drop delta #%d on edge-0 (silent loss)\n", l.seen)
				return
			case "corrupt":
				fmt.Fprintf(os.Stderr, "f2driver: INJECT corrupt delta #%d on edge-0\n", l.seen)
				rb.CRef = []byte{0xff, 0xff, 0xff, 0xff}
			}
		}
	}
	l.inner.OnRef(rb)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: f2driver <url> <mode> <agg_id> <tau> <eps> <rows> <cols> <edges> <steps> <drift>")
		os.Exit(64)
	}
	url := os.Args[1]
	modeArg := argOr(2, "distributed")
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

	// Raw (no-aggregation) baseline: ship every sample, no sketch, no
	// coordinator. url is accepted-but-unused so the CLI shape stays uniform.
	if modeArg == "raw" {
		_ = url
		runRaw(tau, eps, nEdges, nSteps, drift, pattern, windowStart)
		return
	}
	mode := monitor.ParseF2Mode(modeArg)
	wkKeys := sharedKeys()

	// Optional delta-loss injection (edge-0 only), gated by env vars — see
	// lossyInbound. Default OFF, so a normal eval run is byte-identical.
	injectMode := os.Getenv("F2_INJECT")
	injectNth := atoiOr(os.Getenv("F2_INJECT_NTH"), 1)

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
		var inbound monitor.Inbound = eng
		if i == 0 && (injectMode == "corrupt" || injectMode == "drop") {
			inbound = &lossyInbound{inner: eng, mode: injectMode, nth: injectNth}
		}
		client := grpcclient.New(url, inbound)
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
		baseline := 0.6 * math.Sqrt(tau/float64(wkKeys)) / float64(nEdges)
		for i := range edges {
			for k := 0; k < wkKeys; k++ {
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
				edges[i].cs.UpdateString(fmt.Sprintf("k%d", step%wkKeys), drift)
				edges[i].cs.UpdateString(fmt.Sprintf("k%d", (step+1)%wkKeys), -drift)
			default: // ramp: monotone growth that crosses τ
				for k := 0; k < wkKeys; k++ {
					edges[i].cs.UpdateString(fmt.Sprintf("k%d", k), drift)
				}
			}
			edges[i].eng.OnWindow(aggID, nil, edges[i].cs.CellMatrix(), windowStart)
		}
		if pattern != "stable" {
			// Exact merged F2 = H*(N*drift*(step+1))² (ramp ground truth).
			f := float64(nEdges) * drift * float64(step+1)
			exactF2 := float64(wkKeys) * f * f
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
