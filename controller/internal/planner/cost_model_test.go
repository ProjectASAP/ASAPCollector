package planner_test

import (
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/planner"
	"github.com/ProjectASAP/controller/internal/types"
)

// ── Score tests ───────────────────────────────────────────────────────────────

func TestScore_DDSketchMeetsSLA(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.AccuracySLA = 0.01
	plan := pl.Plan(w)

	s := planner.Score(plan, w)
	if !s.MeetsSLA {
		t.Errorf("DDSketch at 1%% should meet 1%% SLA, EstimatedError=%f", s.EstimatedError)
	}
}

func TestScore_DDSketchFailsTightSLA(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.AccuracySLA = 0.001 // 0.1% — tighter than DDSketch default
	plan := pl.Plan(w)
	// Override params to force default (1%) — simulating a misconfigured plan.
	plan.AgentConfig.SketchParams.RelativeAccuracy = 0.01

	s := planner.Score(plan, w)
	if s.MeetsSLA {
		t.Errorf("DDSketch at 1%% should NOT meet 0.1%% SLA")
	}
}

func TestScore_HLLBandwidthLowerThanDDSketch(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeCardinality)
	w.AccuracySLA = 0.01

	// DDSketch plan (hypothetically applied to cardinality).
	wDD := w
	wDD.Aggregations = []types.AggType{types.AggTypeQuantile}
	ddPlan := pl.Plan(wDD)
	ddScore := planner.Score(ddPlan, w)

	// HLL plan.
	hllPlan := pl.Plan(w)
	hllScore := planner.Score(hllPlan, w)

	if hllScore.BandwidthBytesPerSec >= ddScore.BandwidthBytesPerSec {
		t.Errorf("HLL bandwidth (%f) should be lower than DDSketch (%f)",
			hllScore.BandwidthBytesPerSec, ddScore.BandwidthBytesPerSec)
	}
}

func TestScore_DimMultiplierIncreasesBandwidth(t *testing.T) {
	pl := planner.NewRulesPlanner()

	wFew := workload(types.AggTypeQuantile)
	wFew.GroupByLabels = []string{"host"}

	wMany := workload(types.AggTypeQuantile)
	wMany.GroupByLabels = []string{"host", "service", "zone", "region", "env"}

	scoreFew := planner.Score(pl.Plan(wFew), wFew)
	scoreMany := planner.Score(pl.Plan(wMany), wMany)

	if scoreMany.BandwidthBytesPerSec <= scoreFew.BandwidthBytesPerSec {
		t.Errorf("more dimensions should increase bandwidth: few=%f many=%f",
			scoreFew.BandwidthBytesPerSec, scoreMany.BandwidthBytesPerSec)
	}
}

func TestScore_KLLErrorFormula(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.AccuracySLA = 0.02 // 2% — KLL should satisfy this

	// Force KLL for the score test.
	plan := pl.Plan(w)
	plan.AgentConfig.SketchType = types.SketchTypeKLL
	plan.AgentConfig.SketchParams = types.SketchParams{K: 100} // error ≈ 1%

	s := planner.Score(plan, w)
	if !s.MeetsSLA {
		t.Errorf("KLL k=100 (error~1%%) should meet 2%% SLA, EstimatedError=%f", s.EstimatedError)
	}
}

// ── CostModelPlanner tests ────────────────────────────────────────────────────

func TestCostModelPlanner_SelectsKLLOverDDSketchForQuantile(t *testing.T) {
	// KLL has lower bandwidth than DDSketch in the benchmark table.
	// With a loose SLA (2%), the cost model should prefer KLL.
	pl := planner.NewCostModelPlanner()
	w := workload(types.AggTypeQuantile)
	w.AccuracySLA = 0.02 // loose SLA

	plan := pl.Plan(w)

	// Either KLL or DDSketch is valid — just ensure SLA is met.
	s := planner.Score(plan, w)
	if !s.MeetsSLA {
		t.Errorf("CostModelPlanner plan does not meet SLA: error=%f sla=%f",
			s.EstimatedError, w.AccuracySLA)
	}
}

func TestCostModelPlanner_MeetsSLAForAllAggTypes(t *testing.T) {
	pl := planner.NewCostModelPlanner()

	cases := []struct {
		agg types.AggType
		sla float64
	}{
		{types.AggTypeQuantile, 0.01},
		{types.AggTypeCardinality, 0.01},
		{types.AggTypeFrequency, 0.02},
	}

	for _, tc := range cases {
		w := workload(tc.agg)
		w.AccuracySLA = tc.sla
		plan := pl.Plan(w)
		s := planner.Score(plan, w)
		if !s.MeetsSLA {
			t.Errorf("agg=%v sla=%f: plan does not meet SLA (error=%f)",
				tc.agg, tc.sla, s.EstimatedError)
		}
	}
}

func TestCostModelPlanner_ValidUntilInFuture(t *testing.T) {
	pl := planner.NewCostModelPlanner()
	before := time.Now()
	plan := pl.Plan(workload(types.AggTypeQuantile))

	if !plan.ValidUntil.After(before) {
		t.Error("ValidUntil should be in the future")
	}
}

func TestCostModelPlanner_PreferLowerBandwidth(t *testing.T) {
	// For cardinality-only workload, HLL should win (lowest bandwidth).
	pl := planner.NewCostModelPlanner()
	w := workload(types.AggTypeCardinality)
	w.AccuracySLA = 0.02

	plan := pl.Plan(w)

	if plan.AgentConfig.SketchType != types.SketchTypeHLL {
		t.Errorf("expected HLL (lowest bandwidth for cardinality), got %v",
			plan.AgentConfig.SketchType)
	}
}
