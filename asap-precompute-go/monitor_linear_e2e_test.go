// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end (in-process) proof that a FunctionalLinearBuckets monitor on a
// DDSketch series actually drives the report pipeline: Observe → window hook
// → monitorValue → DDSketchWrapper.LinearReadout (value-range count) →
// engine.Observe → report, on the engine's own reportEveryN cadence (no
// slack/grant involved — alerting retired). Sum + CMS-point are covered
// elsewhere; this closes the loop for the linear functional specifically.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

type capturingReporter struct {
	reports []monitor.Report
	regs    []monitor.Registration
}

func (c *capturingReporter) Register(r monitor.Registration) { c.regs = append(c.regs, r) }
func (c *capturingReporter) Report(r monitor.Report)         { c.reports = append(c.reports, r) }

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

	// First observation registers the monitor AND fires an immediate report
	// (obsCount==1), carrying the range-count at that point (0: 1.0 < 50).
	obs(1.0)
	if len(rep.reports) != 1 {
		t.Fatalf("expected an immediate report on the first observation, got %d", len(rep.reports))
	}
	if got := rep.reports[0].LocalValue; got != 0 {
		t.Fatalf("first report should carry range-count 0, got %v", got)
	}

	// Coordinator grants a sampling probability for this group — no longer
	// gates reporting at all (that was the retired slack-crossing trigger).
	// Sum/LinearBuckets monitors key by the series GROUP key (the canonical
	// AggregateBy/label tuple), so the grant must target that same group.
	groupKey := []byte("svc=checkout")
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: groupKey, Round: 1, WindowStartMs: windowStart, SampleP: 0.5})

	// More in-range samples, short of the ReportEveryN cadence → no new report.
	obs(100.0)
	obs(200.0)
	obs(2.0) // out-of-range, does not advance the range-count
	obsCount := 4
	if len(rep.reports) != 1 {
		t.Fatalf("reported too early, before the cadence: got %d reports", len(rep.reports))
	}

	// Drive obsCount up to the ReportEveryN cadence to trigger report #2.
	for obsCount < monitor.ReportEveryN+1 {
		obs(1.0) // out-of-range; keeps range-count fixed while advancing obsCount
		obsCount++
	}
	if len(rep.reports) != 2 {
		t.Fatalf("expected a second report once the cadence was reached, got %d", len(rep.reports))
	}
	// range-count at this point: two in-range samples (100, 200) from above;
	// every filler observation was out-of-range, so the count stays 2.
	if got := rep.reports[1].LocalValue; got != 2 {
		t.Fatalf("second report should carry the value-range count (2), got %v", got)
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
	// Each group's first observation registers AND immediately reports (its
	// own obsCount==1) — independent of the other group.
	obs("a", 10) // registers + reports group "zone=a"
	obs("b", 2)  // registers + reports group "zone=b"
	if len(rep.reports) != 2 {
		t.Fatalf("expected one immediate report per group, got %d", len(rep.reports))
	}
	byKey := map[string]monitor.Report{}
	for _, r := range rep.reports {
		byKey[string(r.Key)] = r
	}
	if byKey["zone=a"].LocalValue != 10 || byKey["zone=b"].LocalValue != 2 {
		t.Fatalf("per-group first report should carry that group's own sum, got %+v", rep.reports)
	}

	// Grant each group its own sampling probability (keyed by the group bytes)
	// — no longer gates reporting, only SampleP.
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: []byte("zone=a"), Round: 1, WindowStartMs: windowStart, SampleP: 0.5})
	eng.OnGrant(monitor.Grant{AggID: uint64(aggID), Key: []byte("zone=b"), Round: 1, WindowStartMs: windowStart, SampleP: 0.9})

	// Drive ONLY zone=a to the ReportEveryN cadence (obsCount 1 → ReportEveryN+1);
	// zone=b stays far short of it. Only zone=a should produce a second
	// report — no collision.
	for i := 0; i < monitor.ReportEveryN; i++ {
		obs("a", 1)
	}
	obs("b", 1) // zone=b's second observation — nowhere near its own cadence
	if len(rep.reports) != 3 {
		t.Fatalf("expected exactly one new report (zone=a's cadence), got %d total", len(rep.reports))
	}
	if string(rep.reports[2].Key) != "zone=a" {
		t.Fatalf("the new report should be zone=a's, got key %q", rep.reports[2].Key)
	}
}
