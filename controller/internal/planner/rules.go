// Package planner contains the rule-based (Phase 1) and cost-model (Phase 2)
// planners that convert a QueryWorkload into a CollectionPlan.
package planner

import (
	"sort"
	"time"

	"github.com/ProjectASAP/controller/internal/types"
)

// DefaultValidFor is how long a plan remains valid before re-planning.
const DefaultValidFor = 10 * time.Minute

// RulesPlanner is a deterministic, rule-based planner (Phase 1).
// It selects sketch types and window strategies without a cost model.
type RulesPlanner struct {
	// ValidFor controls the ValidUntil field of generated plans.
	ValidFor time.Duration
}

// NewRulesPlanner returns a RulesPlanner with sensible defaults.
func NewRulesPlanner() *RulesPlanner {
	return &RulesPlanner{ValidFor: DefaultValidFor}
}

// Plan converts a QueryWorkload into a CollectionPlan using deterministic rules.
func (p *RulesPlanner) Plan(w types.QueryWorkload) types.CollectionPlan {
	sketchType := selectSketchType(w.Aggregations)
	params := defaultSketchParams(sketchType, w.AccuracySLA)
	mode, windowDur := selectWindowStrategy(w)

	agentCfg := types.AgentCollectorConfig{
		OutputMode:     types.OutputModeSketch,
		SketchType:     sketchType,
		SketchParams:   params,
		AggregateBy:    sortedCopy(w.GroupByLabels),
		LabelMatchers:  labelMatchersFromFilters(w.LabelFilters),
		WindowDuration: windowDur,
		Mode:           mode,
		TransmitSketch: true,
		DropOriginal:   true,
	}

	backendCfg := types.BackendCollectorConfig{
		MergeSketchType: sketchType,
		GroupBy:         sortedCopy(w.GroupByLabels),
	}

	validUntil := time.Now().Add(p.ValidFor)

	return types.CollectionPlan{
		AgentConfig:   agentCfg,
		GatewayConfig: types.GatewayCollectorConfig{Passthrough: true},
		BackendConfig: backendCfg,
		ValidUntil:    validUntil,
	}
}

// ── sketch selection ──────────────────────────────────────────────────────────

// selectSketchType picks the best sketch type for the given aggregation set.
// When multiple aggregation types are requested the first one (in priority
// order: quantile > cardinality > frequency) wins; parallel processors are
// managed at a higher layer.
func selectSketchType(aggs []types.AggType) types.SketchType {
	for _, a := range aggs {
		switch a {
		case types.AggTypeQuantile:
			// DDSketch gives bounded relative error guarantees.
			return types.SketchTypeDDSketch
		case types.AggTypeCardinality:
			return types.SketchTypeHLL
		case types.AggTypeFrequency:
			return types.SketchTypeCountSketch
		}
	}
	// Fallback: DDSketch is the most general.
	return types.SketchTypeDDSketch
}

// defaultSketchParams returns type-appropriate defaults for the given accuracy SLA.
func defaultSketchParams(st types.SketchType, accuracySLA float64) types.SketchParams {
	acc := accuracySLA
	if acc <= 0 {
		acc = 0.01 // 1% default
	}

	switch st {
	case types.SketchTypeDDSketch:
		return types.SketchParams{
			RelativeAccuracy: acc,
			Quantiles:        []float64{0.5, 0.9, 0.99},
		}
	case types.SketchTypeKLL:
		// k controls the sketch size; larger k → better accuracy.
		// k ≈ 1/accuracy is a reasonable heuristic.
		k := int(1.0 / acc)
		if k < 32 {
			k = 32
		}
		return types.SketchParams{K: k, Quantiles: []float64{0.5, 0.9, 0.99}}
	case types.SketchTypeHLL:
		// precision = log2(number of registers); range 4–18.
		// Higher precision → lower error but more memory.
		prec := 14 // ~0.8% error, ~16KB
		if acc > 0.02 {
			prec = 10 // ~1.6% error, ~1KB
		}
		return types.SketchParams{Precision: prec}
	case types.SketchTypeCountSketch, types.SketchTypeCountMinSketch:
		// rows × cols matrix; more rows → lower error probability.
		return types.SketchParams{Rows: 5, Cols: 2048}
	default:
		return types.SketchParams{RelativeAccuracy: acc}
	}
}

// ── window strategy ───────────────────────────────────────────────────────────

// selectWindowStrategy decides whether to use window or batch mode.
//
// Rule (from design doc):
//
//	if LatencySLA >= TimeWindow → window mode, window_duration = TimeWindow
//	else                        → batch mode (gateway/backend merges on query)
func selectWindowStrategy(w types.QueryWorkload) (types.ProcessorMode, time.Duration) {
	if w.LatencySLA == 0 || w.LatencySLA >= w.TimeWindow {
		return types.ProcessorModeWindow, w.TimeWindow
	}
	return types.ProcessorModeBatch, 0
}

// ── helpers ───────────────────────────────────────────────────────────────────

func sortedCopy(s []string) []string {
	if s == nil {
		return nil
	}
	c := make([]string, len(s))
	copy(c, s)
	sort.Strings(c)
	return c
}

func labelMatchersFromFilters(filters map[string]string) []string {
	if len(filters) == 0 {
		return nil
	}
	out := make([]string, 0, len(filters))
	for k, v := range filters {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
