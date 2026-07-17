// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end proof that rewireMonitorHooks gates CMS's local point-query
// monitor (FunctionalCMSPoint / EstimateCount) off once GOS mode is active for
// that CMS series — the runtime counterpart to config_validate.go's boot-time
// rejection, needed because a live control-plane config push (UpdateConfig)
// reaches PrecomputeConfig directly and skips YAML validation. Plain
// CountSketch implements the same functional via a median (not min) across
// signed rows, so it is deliberately left unaffected by GOS mode.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

func TestMonitor_CMSPoint_RetiredUnderGOS(t *testing.T) {
	const aggID = precompute.AggId(11)
	const windowStart = uint64(3_600_000)
	const pointKey = "checkout"

	pcfg := &precompute.PrecomputeConfig{
		AggID:           aggID,
		SketchType:      precompute.SketchTypeCountMinSketch,
		Mode:            precompute.Tumbling,
		Window:          precompute.WindowSpec{Size: time.Hour},
		GosDeltaEpsilon: 0.5, // GOS active for this CMS series
		GosSites:        1,
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalCMSPoint,
			Key:            []byte(pointKey),
			CoordinatorURL: "passthrough:///test",
			Tau:            100,
			Epsilon:        0.05,
		},
	}
	factory := func() precompute.Sketch { return sketches.NewCMSWrapper(5, 2048, false) }
	pc := precompute.New(pcfg, factory, sketches.CMSObserver{})

	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	obs := func() {
		if err := pc.Observe(&precompute.Observation{
			TimestampMs: windowStart,
			Metric:      "requests",
			Value:       precompute.BytesValue([]byte(pointKey)),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	// GOS-active CMS + cms_point: the monitor must never even register, let
	// alone report — rewireMonitorHooks disables the hooks entirely.
	obs()
	obs()
	if len(rep.regs) != 0 {
		t.Fatalf("GOS-active CMS cms_point monitor should never register, got %d registrations", len(rep.regs))
	}
	if len(rep.reports) != 0 {
		t.Fatalf("GOS-active CMS cms_point monitor should never report, got %d reports", len(rep.reports))
	}
}

// TestMonitor_CMSPoint_FixedModeStillWorks is the control: the SAME cms_point
// monitor on a CMS series NOT running in GOS mode (GosDeltaEpsilon=0) must
// still register and report normally — this retirement is GOS-scoped, not a
// blanket removal of CMS's point-query support.
func TestMonitor_CMSPoint_FixedModeStillWorks(t *testing.T) {
	const aggID = precompute.AggId(12)
	const windowStart = uint64(3_600_000)
	const pointKey = "checkout"

	pcfg := &precompute.PrecomputeConfig{
		AggID:      aggID,
		SketchType: precompute.SketchTypeCountMinSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: time.Hour},
		// GosDeltaEpsilon left at 0 (fixed mode).
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalCMSPoint,
			Key:            []byte(pointKey),
			CoordinatorURL: "passthrough:///test",
			Tau:            100,
			Epsilon:        0.05,
		},
	}
	factory := func() precompute.Sketch { return sketches.NewCMSWrapper(5, 2048, false) }
	pc := precompute.New(pcfg, factory, sketches.CMSObserver{})

	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	obs := func() {
		if err := pc.Observe(&precompute.Observation{
			TimestampMs: windowStart,
			Metric:      "requests",
			Value:       precompute.BytesValue([]byte(pointKey)),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	obs()
	if len(rep.regs) != 1 {
		t.Fatalf("fixed-mode CMS cms_point monitor should register on first observe, got %d registrations", len(rep.regs))
	}
}

// TestMonitor_CMSPoint_CountSketchUnaffectedByGOS is the other control: plain
// CountSketch serves the SAME FunctionalCMSPoint functional via a median
// (not min) across signed rows, so GOS mode must NOT gate it off.
func TestMonitor_CMSPoint_CountSketchUnaffectedByGOS(t *testing.T) {
	const aggID = precompute.AggId(13)
	const windowStart = uint64(3_600_000)
	const pointKey = "checkout"

	pcfg := &precompute.PrecomputeConfig{
		AggID:           aggID,
		SketchType:      precompute.SketchTypeCountSketch,
		Mode:            precompute.Tumbling,
		Window:          precompute.WindowSpec{Size: time.Hour},
		GosDeltaEpsilon: 0.5, // GOS active for this CountSketch series
		GosSites:        1,
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalCMSPoint,
			Key:            []byte(pointKey),
			CoordinatorURL: "passthrough:///test",
			Tau:            100,
			Epsilon:        0.05,
		},
	}
	factory := func() precompute.Sketch {
		w, err := sketches.NewCountSketchWrapper(5, 256)
		if err != nil {
			t.Fatalf("new count sketch: %v", err)
		}
		return w
	}
	pc := precompute.New(pcfg, factory, sketches.CountSketchObserver{DefaultKey: pointKey})

	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	obs := func() {
		if err := pc.Observe(&precompute.Observation{
			TimestampMs: windowStart,
			Metric:      "requests",
			Value:       precompute.FloatValue(1.0),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	obs()
	if len(rep.regs) != 1 {
		t.Fatalf("GOS-active CountSketch cms_point monitor should still register, got %d registrations", len(rep.regs))
	}
}
