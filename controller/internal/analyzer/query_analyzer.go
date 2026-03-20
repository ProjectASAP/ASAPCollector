// Package analyzer parses an incoming query description into a QueryWorkload.
// In Phase 1 this accepts a structured QuerySpec (JSON-friendly) rather than
// raw PromQL; a full PromQL parser can be plugged in later.
package analyzer

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ProjectASAP/controller/internal/types"
)

// QuerySpec is the JSON-friendly input representation of a query workload.
// Callers submit this via the gRPC API or HTTP endpoint.
type QuerySpec struct {
	MetricName    string            `json:"metric_name"`
	LabelFilters  map[string]string `json:"label_filters,omitempty"`
	GroupByLabels []string          `json:"group_by_labels,omitempty"`
	// Aggregations is a list of strings: "quantile", "cardinality", "frequency"
	Aggregations []string `json:"aggregations"`
	// Durations as strings: "5m", "1h", "30s"
	TimeWindow  string `json:"time_window"`
	RepeatEvery string `json:"repeat_every,omitempty"`
	// AccuracySLA is the maximum relative error, e.g. 0.01 for 1%.
	AccuracySLA float64 `json:"accuracy_sla"`
	LatencySLA  string  `json:"latency_sla,omitempty"`
}

// Analyzer converts QuerySpecs into QueryWorkloads.
type Analyzer struct{}

// New returns a new Analyzer.
func New() *Analyzer { return &Analyzer{} }

// Analyze converts a QuerySpec into a typed QueryWorkload, returning an error
// if the spec is missing required fields or contains invalid values.
func (a *Analyzer) Analyze(spec QuerySpec) (types.QueryWorkload, error) {
	if strings.TrimSpace(spec.MetricName) == "" {
		return types.QueryWorkload{}, errors.New("metric_name is required")
	}
	if len(spec.Aggregations) == 0 {
		return types.QueryWorkload{}, errors.New("at least one aggregation is required")
	}

	aggs, err := parseAggTypes(spec.Aggregations)
	if err != nil {
		return types.QueryWorkload{}, err
	}

	tw, err := time.ParseDuration(spec.TimeWindow)
	if err != nil {
		return types.QueryWorkload{}, fmt.Errorf("invalid time_window %q: %w", spec.TimeWindow, err)
	}
	if tw <= 0 {
		return types.QueryWorkload{}, fmt.Errorf("time_window must be positive")
	}

	var repeatEvery time.Duration
	if spec.RepeatEvery != "" {
		repeatEvery, err = time.ParseDuration(spec.RepeatEvery)
		if err != nil {
			return types.QueryWorkload{}, fmt.Errorf("invalid repeat_every %q: %w", spec.RepeatEvery, err)
		}
	}

	var latencySLA time.Duration
	if spec.LatencySLA != "" {
		latencySLA, err = time.ParseDuration(spec.LatencySLA)
		if err != nil {
			return types.QueryWorkload{}, fmt.Errorf("invalid latency_sla %q: %w", spec.LatencySLA, err)
		}
	}

	if spec.AccuracySLA < 0 || spec.AccuracySLA > 1 {
		return types.QueryWorkload{}, fmt.Errorf("accuracy_sla must be in [0,1], got %f", spec.AccuracySLA)
	}

	// Merge label filter keys and group-by labels into the minimum set of
	// dimensions that must be preserved.
	dims := dedupDims(spec.GroupByLabels, keys(spec.LabelFilters))

	return types.QueryWorkload{
		MetricName:    spec.MetricName,
		LabelFilters:  spec.LabelFilters,
		GroupByLabels: dims,
		Aggregations:  aggs,
		TimeWindow:    tw,
		RepeatEvery:   repeatEvery,
		AccuracySLA:   spec.AccuracySLA,
		LatencySLA:    latencySLA,
	}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func parseAggTypes(raw []string) ([]types.AggType, error) {
	out := make([]types.AggType, 0, len(raw))
	for _, s := range raw {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "quantile":
			out = append(out, types.AggTypeQuantile)
		case "cardinality":
			out = append(out, types.AggTypeCardinality)
		case "frequency":
			out = append(out, types.AggTypeFrequency)
		default:
			return nil, fmt.Errorf("unknown aggregation type %q (want: quantile, cardinality, frequency)", s)
		}
	}
	return out, nil
}

func keys(m map[string]string) []string {
	if m == nil {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func dedupDims(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, v := range append(a, b...) {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}
