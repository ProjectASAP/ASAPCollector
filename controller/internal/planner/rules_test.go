package planner_test

import (
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/planner"
	"github.com/ProjectASAP/controller/internal/types"
)

func workload(aggs ...types.AggType) types.QueryWorkload {
	return types.QueryWorkload{
		MetricName:   "test_metric",
		Aggregations: aggs,
		TimeWindow:   5 * time.Minute,
		AccuracySLA:  0.01,
	}
}

func TestRulesPlanner_QuantileSelectsDDSketch(t *testing.T) {
	pl := planner.NewRulesPlanner()
	plan := pl.Plan(workload(types.AggTypeQuantile))

	if plan.AgentConfig.SketchType != types.SketchTypeDDSketch {
		t.Errorf("SketchType = %v, want DDSketch", plan.AgentConfig.SketchType)
	}
}

func TestRulesPlanner_CardinalitySelectsHLL(t *testing.T) {
	pl := planner.NewRulesPlanner()
	plan := pl.Plan(workload(types.AggTypeCardinality))

	if plan.AgentConfig.SketchType != types.SketchTypeHLL {
		t.Errorf("SketchType = %v, want HLL", plan.AgentConfig.SketchType)
	}
}

func TestRulesPlanner_FrequencySelectsCountSketch(t *testing.T) {
	pl := planner.NewRulesPlanner()
	plan := pl.Plan(workload(types.AggTypeFrequency))

	if plan.AgentConfig.SketchType != types.SketchTypeCountSketch {
		t.Errorf("SketchType = %v, want CountSketch", plan.AgentConfig.SketchType)
	}
}

func TestRulesPlanner_QuantilePriorityWins(t *testing.T) {
	// When multiple agg types are requested, quantile takes priority.
	pl := planner.NewRulesPlanner()
	plan := pl.Plan(workload(types.AggTypeQuantile, types.AggTypeCardinality))

	if plan.AgentConfig.SketchType != types.SketchTypeDDSketch {
		t.Errorf("SketchType = %v, want DDSketch (quantile priority)", plan.AgentConfig.SketchType)
	}
}

func TestRulesPlanner_WindowModeWhenLatencyGeqTimeWindow(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.TimeWindow = 5 * time.Minute
	w.LatencySLA = 10 * time.Minute // latency >= time_window → window mode

	plan := pl.Plan(w)

	if plan.AgentConfig.Mode != types.ProcessorModeWindow {
		t.Errorf("Mode = %v, want Window", plan.AgentConfig.Mode)
	}
	if plan.AgentConfig.WindowDuration != 5*time.Minute {
		t.Errorf("WindowDuration = %v, want 5m", plan.AgentConfig.WindowDuration)
	}
}

func TestRulesPlanner_BatchModeWhenLatencyLtTimeWindow(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.TimeWindow = 5 * time.Minute
	w.LatencySLA = 1 * time.Minute // latency < time_window → batch mode

	plan := pl.Plan(w)

	if plan.AgentConfig.Mode != types.ProcessorModeBatch {
		t.Errorf("Mode = %v, want Batch", plan.AgentConfig.Mode)
	}
	if plan.AgentConfig.WindowDuration != 0 {
		t.Errorf("WindowDuration = %v, want 0 (batch mode)", plan.AgentConfig.WindowDuration)
	}
}

func TestRulesPlanner_NoLatencySLADefaultsToWindow(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.TimeWindow = 5 * time.Minute
	w.LatencySLA = 0 // unset → window mode

	plan := pl.Plan(w)

	if plan.AgentConfig.Mode != types.ProcessorModeWindow {
		t.Errorf("Mode = %v, want Window when LatencySLA is unset", plan.AgentConfig.Mode)
	}
}

func TestRulesPlanner_AggregateBySorted(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.GroupByLabels = []string{"zone", "host.name", "service"}

	plan := pl.Plan(w)

	want := []string{"host.name", "service", "zone"}
	if len(plan.AgentConfig.AggregateBy) != len(want) {
		t.Fatalf("AggregateBy len = %d, want %d", len(plan.AgentConfig.AggregateBy), len(want))
	}
	for i, v := range want {
		if plan.AgentConfig.AggregateBy[i] != v {
			t.Errorf("AggregateBy[%d] = %q, want %q", i, plan.AgentConfig.AggregateBy[i], v)
		}
	}
}

func TestRulesPlanner_LabelMatchers(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.LabelFilters = map[string]string{"service": "web", "env": "prod"}

	plan := pl.Plan(w)

	if len(plan.AgentConfig.LabelMatchers) != 2 {
		t.Fatalf("LabelMatchers len = %d, want 2", len(plan.AgentConfig.LabelMatchers))
	}
}

func TestRulesPlanner_DDSketchAccuracyParams(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeQuantile)
	w.AccuracySLA = 0.005

	plan := pl.Plan(w)

	if plan.AgentConfig.SketchParams.RelativeAccuracy != 0.005 {
		t.Errorf("RelativeAccuracy = %f, want 0.005", plan.AgentConfig.SketchParams.RelativeAccuracy)
	}
}

func TestRulesPlanner_HLLPrecisionCoarseSLA(t *testing.T) {
	pl := planner.NewRulesPlanner()
	w := workload(types.AggTypeCardinality)
	w.AccuracySLA = 0.03 // coarse → lower precision

	plan := pl.Plan(w)

	if plan.AgentConfig.SketchParams.Precision != 10 {
		t.Errorf("HLL Precision = %d, want 10 (coarse SLA)", plan.AgentConfig.SketchParams.Precision)
	}
}

func TestRulesPlanner_ValidUntilSet(t *testing.T) {
	pl := planner.NewRulesPlanner()
	before := time.Now()
	plan := pl.Plan(workload(types.AggTypeQuantile))
	after := time.Now()

	if plan.ValidUntil.Before(before) {
		t.Error("ValidUntil should be after plan creation time")
	}
	if plan.ValidUntil.Before(after) {
		t.Error("ValidUntil should be in the future")
	}
}

func TestRulesPlanner_BackendConfigMatchesSketchType(t *testing.T) {
	pl := planner.NewRulesPlanner()
	plan := pl.Plan(workload(types.AggTypeQuantile))

	if plan.BackendConfig.MergeSketchType != plan.AgentConfig.SketchType {
		t.Errorf("BackendConfig.MergeSketchType = %v, want %v",
			plan.BackendConfig.MergeSketchType, plan.AgentConfig.SketchType)
	}
}

func TestRulesPlanner_GatewayPassthrough(t *testing.T) {
	pl := planner.NewRulesPlanner()
	plan := pl.Plan(workload(types.AggTypeQuantile))

	if !plan.GatewayConfig.Passthrough {
		t.Error("GatewayConfig.Passthrough should be true")
	}
}
