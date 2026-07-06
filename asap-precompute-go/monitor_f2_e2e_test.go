// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end (in-process) proof that a FunctionalF2 monitor on a CountSketch
// series is reachable from the runtime: Observe builds the whole-stream
// Count-Sketch, DriveF2Monitor (the sub-window tick) extracts the current cell
// matrix and feeds F2Engine.OnWindow, which registers and ships the serialized
// sketch. Before this wiring, precompute silently dropped FunctionalF2 (its
// scalar monitorValue switch had no F2 case), so the engine was reachable only
// from the eval driver.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

type f2Reporter struct {
	registers int
	reports   []monitor.Report
}

func (r *f2Reporter) Register(monitor.Registration) { r.registers++ }
func (r *f2Reporter) Report(rep monitor.Report)     { r.reports = append(r.reports, rep) }

func TestMonitor_F2_CountSketch_DriveShipsMatrix(t *testing.T) {
	const aggID = precompute.AggId(9)
	const windowStart = uint64(3_600_000) // aligned to the 1h window → no rotation
	const rows, cols = 3, 64

	pcfg := &precompute.PrecomputeConfig{
		AggID:      aggID,
		SketchType: precompute.SketchTypeCountSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: time.Hour},
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalF2,
			CoordinatorURL: "passthrough:///test",
			Tau:            1e9,
			Epsilon:        0.1,
			SketchRows:     rows,
			SketchCols:     cols,
			// Distributed ships every tick → deterministic wiring assertion.
			Mode: monitor.F2ModeDistributed,
		},
	}
	factory := func() precompute.Sketch {
		w, err := sketches.NewCountSketchWrapper(rows, cols)
		if err != nil {
			t.Fatalf("new count-sketch wrapper: %v", err)
		}
		return w
	}
	pc := precompute.New(pcfg, factory, sketches.CountSketchObserver{DefaultKey: "m"})

	rep := &f2Reporter{}
	eng := monitor.NewF2Engine("edge", uint64(time.Hour/time.Millisecond), rep)
	// Whole-stream F2: one monitor keyed by the empty group key — matches
	// DriveF2Monitor's groupKeyBytes(nil labels) for a no-labels series.
	eng.Configure(uint64(aggID), nil, pcfg.Monitor)
	pc.SetF2Engine(eng)

	// No labels → a single whole-stream series (group key = nil).
	for i := 0; i < 20; i++ {
		if err := pc.Observe(&precompute.Observation{
			TimestampMs: windowStart,
			Metric:      "m",
			Value:       precompute.FloatValue(1),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	// The sub-window tick drives the whole-sketch F2 monitor.
	pc.DriveF2Monitor(windowStart)

	if rep.registers != 1 {
		t.Errorf("F2 monitor must register once on the first window, got %d", rep.registers)
	}
	if len(rep.reports) != 1 {
		t.Fatalf("distributed F2 must ship on the tick, got %d reports", len(rep.reports))
	}
	if len(rep.reports[0].Sketch) == 0 {
		t.Error("F2 report must carry the serialized Count-Sketch matrix")
	}
	if got := rep.reports[0].AggID; got != uint64(aggID) {
		t.Errorf("report agg_id = %d, want %d", got, uint64(aggID))
	}

	// A second tick in the same epoch ships again (distributed) but does not
	// re-register.
	pc.DriveF2Monitor(windowStart)
	if rep.registers != 1 {
		t.Errorf("must not re-register within an epoch, got %d", rep.registers)
	}
	if len(rep.reports) != 2 {
		t.Errorf("second distributed tick must ship again, got %d reports", len(rep.reports))
	}
}
