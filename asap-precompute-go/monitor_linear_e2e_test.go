// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end (in-process) proof that a FunctionalLinearBuckets monitor on a
// DDSketch series actually drives the slack countdown: Observe → window hook →
// monitorValue → DDSketchWrapper.LinearReadout (value-range count) →
// engine.Observe → report. Sum + CMS-point are covered elsewhere; this closes
// the loop for the linear functional specifically.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

type capturingReporter struct {
	reports []monitor.Report
}

func (c *capturingReporter) Register(monitor.Registration) {}
func (c *capturingReporter) Report(r monitor.Report)       { c.reports = append(c.reports, r) }

func TestMonitor_LinearBuckets_DDSketch_RangeCountDrivesReports(t *testing.T) {
	const aggID = precompute.AggId(7)
	// ts aligned to a 1h window so the window start is deterministic and the
	// stream never rotates during the test.
	const windowStart = uint64(3_600_000)

	pcfg := &precompute.PrecomputeConfig{
		AggID:      aggID,
		SketchType: precompute.SketchTypeDDSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: time.Hour},
		Monitor: monitor.Spec{
			Enabled:        true,
			Functional:     monitor.FunctionalLinearBuckets,
			Coeffs:         []float64{50}, // count of samples with value >= 50
			CoordinatorURL: "passthrough:///test",
			Tau:            100,
			Epsilon:        0.05,
		},
	}
	factory := func() precompute.Sketch { return sketches.NewDDSketchWrapper(0.01) }
	pc := precompute.New(pcfg, factory, sketches.DDSketchObserver{})

	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	labels := []precompute.KeyValue{{Key: "svc", Value: "checkout"}}
	obs := func(v float64) {
		if err := pc.Observe(&precompute.Observation{
			TimestampMs: windowStart,
			Metric:      "latency_ms",
			Labels:      labels,
			Value:       precompute.FloatValue(v),
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}
	}

	// First observation registers the monitor (no grant yet → silent).
	obs(1.0) // below 50 → range-count stays 0
	if len(rep.reports) != 0 {
		t.Fatalf("reported before any grant")
	}

	// Coordinator grants slack 3 for this round.
	// Sum/LinearBuckets monitors key by the series GROUP key (the canonical
	// AggregateBy/label tuple), so the grant must target that same group.
	groupKey := []byte("svc=checkout")
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: groupKey, Round: 1, LocalSlack: 3, WindowStartMs: windowStart})

	// Two in-range samples: range-count = 2 < slack 3 → still silent.
	obs(100.0)
	obs(200.0)
	// An out-of-range sample does NOT advance the count.
	obs(2.0)
	if len(rep.reports) != 0 {
		t.Fatalf("reported too early: range-count below slack; got %d reports", len(rep.reports))
	}

	// Third in-range sample: range-count = 3 >= slack 3 → exactly one report,
	// carrying the range-count (3), not the raw observed value.
	obs(100.0)
	if len(rep.reports) != 1 {
		t.Fatalf("expected exactly one report once range-count crossed slack, got %d", len(rep.reports))
	}
	if got := rep.reports[0].LocalValue; got != 3 {
		t.Fatalf("report should carry the value-range count (3), got %v", got)
	}
}

// TestMonitor_Sum_PerGroup_NoCollision validates the (agg_id, group_key) keying
// fix: two per-group Sum series (zone=a, zone=b) must drive INDEPENDENT monitor
// state through the real window hook, instead of colliding on one agg-wide slot.
func TestMonitor_Sum_PerGroup_NoCollision(t *testing.T) {
	const aggID = precompute.AggId(7)
	const windowStart = uint64(3_600_000)
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
	pc := precompute.New(pcfg, func() precompute.Sketch { return sketches.NewSumWrapper() }, sketches.SumObserver{})
	rep := &capturingReporter{}
	eng := monitor.NewEngine("edge", uint64(time.Hour/time.Millisecond), rep)
	pc.SetMonitorEngine(eng)

	obs := func(zone string, v float64) {
		_ = pc.Observe(&precompute.Observation{
			TimestampMs: windowStart, Metric: "http_requests_total",
			Labels: []precompute.KeyValue{{Key: "zone", Value: zone}},
			Value:  precompute.FloatValue(v),
		})
	}
	obs("a", 0) // registers group "zone=a"
	obs("b", 0) // registers group "zone=b"
	// Grant each group its own slack (keyed by the group bytes).
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: []byte("zone=a"), Round: 1, LocalSlack: 5, WindowStartMs: windowStart})
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: []byte("zone=b"), Round: 1, LocalSlack: 5, WindowStartMs: windowStart})

	obs("a", 10) // zone=a sum=10 >= 5 → reports group a
	obs("b", 2)  // zone=b sum=2  <  5 → silent (no collision with a)
	if len(rep.reports) != 1 {
		t.Fatalf("expected exactly 1 report (only zone=a crossed), got %d", len(rep.reports))
	}
	if string(rep.reports[0].Key) != "zone=a" {
		t.Fatalf("report should be keyed by group zone=a, got %q", rep.reports[0].Key)
	}
	obs("b", 10) // zone=b sum=10 >= 5 → now reports group b independently
	if len(rep.reports) != 2 || string(rep.reports[1].Key) != "zone=b" {
		t.Fatalf("expected an independent zone=b report; got %d reports, last key %q",
			len(rep.reports), rep.reports[len(rep.reports)-1].Key)
	}
}
