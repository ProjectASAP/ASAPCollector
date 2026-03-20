package planner

import (
	"math"
	"time"

	"github.com/ProjectASAP/controller/internal/types"
)

// ── Benchmark-derived estimates ───────────────────────────────────────────────
//
// These constants come from the e2e benchmark results (2026-03-15).
// Units: bandwidth in bytes/sec per series, CPU in μs/sample, memory in bytes/sketch.

type sketchCosts struct {
	// bytesPerSeriesPerSec is approximate serialized sketch size × flush rate.
	bytesPerSeriesPerSec float64
	// cpuMicrosPerSample is CPU time spent per ingested sample.
	cpuMicrosPerSample float64
	// baseMemoryBytes is the in-memory sketch footprint (one window, one series).
	baseMemoryBytes float64
	// relativeErrorAtDefault is the typical relative error at default params.
	relativeErrorAtDefault float64
}

// benchmarkTable maps sketch types to their measured costs.
// Values reflect the 1000 series, 1000 Hz benchmark row from the e2e results.
var benchmarkTable = map[types.SketchType]sketchCosts{
	types.SketchTypeDDSketch: {
		bytesPerSeriesPerSec:   120,
		cpuMicrosPerSample:     0.8,
		baseMemoryBytes:        4096,
		relativeErrorAtDefault: 0.01,
	},
	types.SketchTypeKLL: {
		bytesPerSeriesPerSec:   80,
		cpuMicrosPerSample:     0.5,
		baseMemoryBytes:        2048,
		relativeErrorAtDefault: 0.02,
	},
	types.SketchTypeHLL: {
		bytesPerSeriesPerSec:   40,
		cpuMicrosPerSample:     0.3,
		baseMemoryBytes:        16384, // 16KB at precision=14
		relativeErrorAtDefault: 0.008,
	},
	types.SketchTypeCountSketch: {
		bytesPerSeriesPerSec:   200,
		cpuMicrosPerSample:     1.2,
		baseMemoryBytes:        40960, // 5 rows × 2048 cols × 4 bytes
		relativeErrorAtDefault: 0.01,
	},
	types.SketchTypeCountMinSketch: {
		bytesPerSeriesPerSec:   200,
		cpuMicrosPerSample:     1.0,
		baseMemoryBytes:        40960,
		relativeErrorAtDefault: 0.01,
	},
}

// PlanScore holds the estimated cost breakdown for a CollectionPlan.
type PlanScore struct {
	BandwidthBytesPerSec float64
	CPUMicrosPerSample   float64
	MemoryBytes          float64
	// EstimatedError is the expected relative error for the given params.
	EstimatedError float64
	// MeetsSLA is true when EstimatedError <= AccuracySLA.
	MeetsSLA bool
}

// Score estimates the resource costs for a given plan and workload.
func Score(plan types.CollectionPlan, w types.QueryWorkload) PlanScore {
	st := plan.AgentConfig.SketchType
	costs, ok := benchmarkTable[st]
	if !ok {
		// Unknown sketch: return worst-case estimate.
		return PlanScore{
			BandwidthBytesPerSec: math.MaxFloat64,
			CPUMicrosPerSample:   math.MaxFloat64,
			MemoryBytes:          math.MaxFloat64,
			EstimatedError:       1.0,
			MeetsSLA:             false,
		}
	}

	// Scale bandwidth and memory by the number of preserved dimensions.
	// More dimensions = more distinct sketches = higher resource usage.
	dimMultiplier := math.Max(1, float64(len(plan.AgentConfig.AggregateBy)+1))

	bw := costs.bytesPerSeriesPerSec * dimMultiplier
	mem := costs.baseMemoryBytes * dimMultiplier

	// Estimate error for the given params.
	err := estimateError(st, plan.AgentConfig.SketchParams, costs)

	sla := w.AccuracySLA
	if sla <= 0 {
		sla = 0.01
	}

	return PlanScore{
		BandwidthBytesPerSec: bw,
		CPUMicrosPerSample:   costs.cpuMicrosPerSample,
		MemoryBytes:          mem,
		EstimatedError:       err,
		MeetsSLA:             err <= sla,
	}
}

// estimateError returns the expected relative error for the given sketch type
// and parameters.
func estimateError(st types.SketchType, p types.SketchParams, costs sketchCosts) float64 {
	switch st {
	case types.SketchTypeDDSketch:
		if p.RelativeAccuracy > 0 {
			return p.RelativeAccuracy
		}
	case types.SketchTypeKLL:
		// KLL error ≈ 1/k (rough approximation).
		if p.K > 0 {
			return 1.0 / float64(p.K)
		}
	case types.SketchTypeHLL:
		// HLL standard error ≈ 1.04/sqrt(2^precision).
		if p.Precision > 0 {
			return 1.04 / math.Sqrt(math.Pow(2, float64(p.Precision)))
		}
	}
	return costs.relativeErrorAtDefault
}

// ── CostModelPlanner ──────────────────────────────────────────────────────────

// CostModelPlanner extends RulesPlanner by selecting the Pareto-optimal sketch
// type (lowest bandwidth that meets the AccuracySLA).
type CostModelPlanner struct {
	RulesPlanner
}

// NewCostModelPlanner returns a planner that uses the benchmark cost table.
func NewCostModelPlanner() *CostModelPlanner {
	return &CostModelPlanner{
		RulesPlanner: RulesPlanner{ValidFor: DefaultValidFor},
	}
}

// Plan generates a CollectionPlan by scoring all viable sketch candidates and
// choosing the one with the lowest bandwidth that meets the AccuracySLA.
func (c *CostModelPlanner) Plan(w types.QueryWorkload) types.CollectionPlan {
	candidates := candidatesForWorkload(w)
	if len(candidates) == 0 {
		return c.RulesPlanner.Plan(w)
	}

	bestPlan := c.RulesPlanner.Plan(w)
	bestScore := Score(bestPlan, w)

	for _, st := range candidates {
		params := defaultSketchParams(st, w.AccuracySLA)
		// Build a trial AgentConfig with this sketch type.
		trialPlan := c.RulesPlanner.Plan(w)
		trialPlan.AgentConfig.SketchType = st
		trialPlan.AgentConfig.SketchParams = params
		trialPlan.BackendConfig.MergeSketchType = st

		score := Score(trialPlan, w)
		if !score.MeetsSLA {
			continue
		}
		if score.BandwidthBytesPerSec < bestScore.BandwidthBytesPerSec || !bestScore.MeetsSLA {
			bestPlan = trialPlan
			bestScore = score
		}
	}

	// Add ValidUntil with re-planning window.
	bestPlan.ValidUntil = time.Now().Add(c.ValidFor)
	return bestPlan
}

// candidatesForWorkload returns all sketch types that are semantically valid
// for the given workload's aggregation types.
func candidatesForWorkload(w types.QueryWorkload) []types.SketchType {
	var out []types.SketchType
	for _, agg := range w.Aggregations {
		switch agg {
		case types.AggTypeQuantile:
			out = append(out, types.SketchTypeDDSketch, types.SketchTypeKLL)
		case types.AggTypeCardinality:
			out = append(out, types.SketchTypeHLL)
		case types.AggTypeFrequency:
			out = append(out, types.SketchTypeCountSketch, types.SketchTypeCountMinSketch)
		}
	}
	return out
}
