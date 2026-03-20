package analyzer_test

import (
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/analyzer"
	"github.com/ProjectASAP/controller/internal/types"
)

func TestAnalyze_ValidSpec(t *testing.T) {
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		MetricName:    "request_latency",
		LabelFilters:  map[string]string{"service": "web"},
		GroupByLabels: []string{"host.name"},
		Aggregations:  []string{"quantile"},
		TimeWindow:    "5m",
		RepeatEvery:   "1m",
		AccuracySLA:   0.01,
		LatencySLA:    "10m",
	}

	w, err := an.Analyze(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if w.MetricName != "request_latency" {
		t.Errorf("MetricName = %q, want %q", w.MetricName, "request_latency")
	}
	if w.AccuracySLA != 0.01 {
		t.Errorf("AccuracySLA = %f, want 0.01", w.AccuracySLA)
	}
	if w.TimeWindow != 5*time.Minute {
		t.Errorf("TimeWindow = %v, want 5m", w.TimeWindow)
	}
	if w.RepeatEvery != 1*time.Minute {
		t.Errorf("RepeatEvery = %v, want 1m", w.RepeatEvery)
	}
	if w.LatencySLA != 10*time.Minute {
		t.Errorf("LatencySLA = %v, want 10m", w.LatencySLA)
	}
	if len(w.Aggregations) != 1 || w.Aggregations[0] != types.AggTypeQuantile {
		t.Errorf("Aggregations = %v, want [quantile]", w.Aggregations)
	}
}

func TestAnalyze_DimensionMerge(t *testing.T) {
	// GroupByLabels and LabelFilter keys should be merged (deduplicated).
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		MetricName:    "latency",
		LabelFilters:  map[string]string{"service": "api", "host.name": "h1"},
		GroupByLabels: []string{"host.name", "region"},
		Aggregations:  []string{"quantile"},
		TimeWindow:    "1m",
		AccuracySLA:   0.01,
	}

	w, err := an.Analyze(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dims := make(map[string]bool, len(w.GroupByLabels))
	for _, d := range w.GroupByLabels {
		dims[d] = true
	}
	for _, want := range []string{"host.name", "region", "service"} {
		if !dims[want] {
			t.Errorf("GroupByLabels missing %q, got %v", want, w.GroupByLabels)
		}
	}
	// host.name should appear only once.
	count := 0
	for _, d := range w.GroupByLabels {
		if d == "host.name" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("host.name appears %d times in GroupByLabels, want 1", count)
	}
}

func TestAnalyze_MultipleAggregations(t *testing.T) {
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		MetricName:   "events",
		Aggregations: []string{"cardinality", "frequency"},
		TimeWindow:   "1h",
		AccuracySLA:  0.02,
	}

	w, err := an.Analyze(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.Aggregations) != 2 {
		t.Fatalf("len(Aggregations) = %d, want 2", len(w.Aggregations))
	}
	if w.Aggregations[0] != types.AggTypeCardinality {
		t.Errorf("Aggregations[0] = %v, want cardinality", w.Aggregations[0])
	}
	if w.Aggregations[1] != types.AggTypeFrequency {
		t.Errorf("Aggregations[1] = %v, want frequency", w.Aggregations[1])
	}
}

func TestAnalyze_MissingMetricName(t *testing.T) {
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		Aggregations: []string{"quantile"},
		TimeWindow:   "5m",
		AccuracySLA:  0.01,
	}
	_, err := an.Analyze(spec)
	if err == nil {
		t.Error("expected error for missing metric_name, got nil")
	}
}

func TestAnalyze_MissingAggregations(t *testing.T) {
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		MetricName:  "latency",
		TimeWindow:  "5m",
		AccuracySLA: 0.01,
	}
	_, err := an.Analyze(spec)
	if err == nil {
		t.Error("expected error for missing aggregations, got nil")
	}
}

func TestAnalyze_InvalidAggregationType(t *testing.T) {
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		MetricName:   "latency",
		Aggregations: []string{"histogram"},
		TimeWindow:   "5m",
		AccuracySLA:  0.01,
	}
	_, err := an.Analyze(spec)
	if err == nil {
		t.Error("expected error for unknown aggregation type, got nil")
	}
}

func TestAnalyze_InvalidDuration(t *testing.T) {
	an := analyzer.New()
	spec := analyzer.QuerySpec{
		MetricName:   "latency",
		Aggregations: []string{"quantile"},
		TimeWindow:   "not-a-duration",
		AccuracySLA:  0.01,
	}
	_, err := an.Analyze(spec)
	if err == nil {
		t.Error("expected error for invalid time_window, got nil")
	}
}

func TestAnalyze_InvalidAccuracySLA(t *testing.T) {
	an := analyzer.New()
	for _, bad := range []float64{-0.1, 1.5} {
		spec := analyzer.QuerySpec{
			MetricName:   "latency",
			Aggregations: []string{"quantile"},
			TimeWindow:   "5m",
			AccuracySLA:  bad,
		}
		_, err := an.Analyze(spec)
		if err == nil {
			t.Errorf("expected error for accuracy_sla=%f, got nil", bad)
		}
	}
}
