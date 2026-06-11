// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end proof of the coordinator→edge sampling-coupling APPLY path:
// engine.OnGrant(SampleP) → (next window rotation) → window admit hook →
// precompute.applyGrantedSampleP → wrapper.WithSampleP. The granted p must reach
// a sampling-capable family's (Count-Sketch) NEW-window wrapper at the next
// epoch boundary, while a non-capable family (Sum) is never touched.

import (
	"sync"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

// capturingFactory records every sketch it hands out so a test can inspect the
// wrapper the live window is actually accumulating into.
type capturingFactory struct {
	mu   sync.Mutex
	make func() precompute.Sketch
	all  []precompute.Sketch
}

func (f *capturingFactory) New() precompute.Sketch {
	s := f.make()
	f.mu.Lock()
	f.all = append(f.all, s)
	f.mu.Unlock()
	return s
}

// last returns the most recently created sketch (the live window's wrapper after
// a post-rotation observation).
func (f *capturingFactory) last() precompute.Sketch {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.all) == 0 {
		return nil
	}
	return f.all[len(f.all)-1]
}

// TestMonitor_GrantedSampleP_AppliedToCountSketchAtRotation drives a Count-Sketch
// monitor through a grant + window rotation and asserts the new window's wrapper
// carries the coordinator-granted p.
func TestMonitor_GrantedSampleP_AppliedToCountSketchAtRotation(t *testing.T) {
	const aggID = precompute.AggId(7)
	const w0 = uint64(3_600_000) // first window start (1h aligned)
	const pointKey = "checkout"

	cf := &capturingFactory{make: func() precompute.Sketch {
		w, err := sketches.NewCountSketchWrapper(5, 256)
		if err != nil {
			t.Fatalf("new count sketch: %v", err)
		}
		return w
	}}
	pcfg := &precompute.PrecomputeConfig{
		AggID:      aggID,
		SketchType: precompute.SketchTypeCountSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: time.Hour},
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalCMSPoint,
			Key:            []byte(pointKey),
			CoordinatorURL: "passthrough:///test",
			Tau:            100,
			Epsilon:        0.05,
		},
	}
	pc := precompute.New(pcfg, cf.New, sketches.CountSketchObserver{DefaultKey: pointKey})
	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	obs := func(ts uint64) {
		if err := pc.Observe(&precompute.Observation{
			TimestampMs: ts,
			Metric:      "requests",
			Value:       precompute.FloatValue(1.0),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	// First window: create + register the monitor, then grant p=0.3 (keyed by
	// the CMS point key x, which is how CMSPoint monitors key their state).
	obs(w0)
	w0Wrapper := cf.last().(*sketches.CountSketchWrapper)
	if got := w0Wrapper.SampleP(); got != 1.0 {
		t.Fatalf("window-0 wrapper should be unsampled before any grant, got %v", got)
	}
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: []byte(pointKey), Round: 1, WindowStartMs: w0, SampleP: 0.3})

	// Mid-window: the grant must NOT retro-apply to the in-flight wrapper —
	// applying p mid-window would break the shared-p merge invariant.
	if got := w0Wrapper.SampleP(); got != 1.0 {
		t.Fatalf("grant must not apply mid-window; window-0 wrapper SampleP = %v, want 1.0", got)
	}

	// Rotate to the next tumbling window (one hour later), then observe so the
	// new window lazily creates its wrapper through the admit hook.
	const w1 = w0 + uint64(time.Hour/time.Millisecond)
	pc.Tick(w1 + 1)
	obs(w1)

	w1Wrapper := cf.last().(*sketches.CountSketchWrapper)
	if w1Wrapper == w0Wrapper {
		t.Fatalf("rotation should have created a fresh wrapper for the new window")
	}
	if got := w1Wrapper.SampleP(); got != 0.3 {
		t.Fatalf("new-window Count-Sketch wrapper SampleP = %v, want 0.3 (granted p applied at rotation)", got)
	}
}

// TestMonitor_GrantedSampleP_SumIsNoOp checks the family gate: a Sum monitor is
// not a sampling-capable family, so even a granted p must leave its wrapper
// untouched (SampleP stays 1) and must not panic on the observe path.
func TestMonitor_GrantedSampleP_SumIsNoOp(t *testing.T) {
	const aggID = precompute.AggId(8)
	const w0 = uint64(3_600_000)

	cf := &capturingFactory{make: func() precompute.Sketch { return sketches.NewSumWrapper() }}
	pcfg := &precompute.PrecomputeConfig{
		AggID:   aggID,
		AggKind: precompute.AggKindSum,
		Mode:    precompute.Tumbling,
		Window:  precompute.WindowSpec{Size: time.Hour},
		Monitor: monitor.Spec{
			Enabled: true, Functional: monitor.FunctionalSum,
			CoordinatorURL: "passthrough:///test", Tau: 100, Epsilon: 0.05,
		},
	}
	pc := precompute.New(pcfg, cf.New, sketches.SumObserver{})
	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	obs := func(ts uint64) {
		_ = pc.Observe(&precompute.Observation{
			TimestampMs: ts, Metric: "bytes", Value: precompute.FloatValue(1.0),
		})
	}

	obs(w0)
	// Grant a sampling p even though Sum can't use it (whole-stream → nil key).
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: nil, Round: 1, WindowStartMs: w0, SampleP: 0.3})

	const w1 = w0 + uint64(time.Hour/time.Millisecond)
	pc.Tick(w1 + 1)
	obs(w1) // must not panic — Sum has no WithSampleP / SampleSetter

	// Reaching here without a panic is the core assertion: the Sum family is
	// gated out by SketchType, so the sample hook is never installed and
	// WithSampleP is never called on a wrapper that doesn't support it. Belt and
	// braces: SumWrapper must not satisfy the coordinated-sampling interface, so
	// even a structural assert could never have stamped it.
	if _, ok := cf.last().(precompute.SampleSetter); ok {
		t.Fatalf("SumWrapper must NOT implement precompute.SampleSetter (sampling unsupported)")
	}
}
