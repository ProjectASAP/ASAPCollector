// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end (in-process) proof that a FunctionalSum monitor drives the
// report pipeline correctly per GROUP, not per agg_id as a whole. LinearBuckets
// (DDSketch) and CMS-point are covered elsewhere; this closes the loop for the
// Sum functional specifically. Split out of monitor_linear_e2e_test.go, whose
// name no longer described this test's scope.

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

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
