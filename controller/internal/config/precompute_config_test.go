package config_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/config"
	"github.com/ProjectASAP/controller/internal/types"
)

// ── ShouldPrecompute ──────────────────────────────────────────────────────────

func TestShouldPrecompute_TrueWhenRepeatEveryLtLatencySLA(t *testing.T) {
	w := types.QueryWorkload{
		RepeatEvery: 1 * time.Minute,
		LatencySLA:  5 * time.Minute,
	}
	if !config.ShouldPrecompute(w) {
		t.Error("expected ShouldPrecompute=true when RepeatEvery < LatencySLA")
	}
}

func TestShouldPrecompute_FalseWhenRepeatEveryGeqLatencySLA(t *testing.T) {
	w := types.QueryWorkload{
		RepeatEvery: 5 * time.Minute,
		LatencySLA:  1 * time.Minute,
	}
	if config.ShouldPrecompute(w) {
		t.Error("expected ShouldPrecompute=false when RepeatEvery >= LatencySLA")
	}
}

func TestShouldPrecompute_FalseWhenNoRepeatEvery(t *testing.T) {
	w := types.QueryWorkload{LatencySLA: 5 * time.Minute}
	if config.ShouldPrecompute(w) {
		t.Error("expected ShouldPrecompute=false when RepeatEvery is zero")
	}
}

func TestShouldPrecompute_FalseWhenNoLatencySLA(t *testing.T) {
	w := types.QueryWorkload{RepeatEvery: 1 * time.Minute}
	if config.ShouldPrecompute(w) {
		t.Error("expected ShouldPrecompute=false when LatencySLA is zero")
	}
}

// ── BuildPrecomputeJobs ───────────────────────────────────────────────────────

func TestBuildPrecomputeJobs_ReturnsJobWhenEligible(t *testing.T) {
	w := types.QueryWorkload{
		MetricName:   "latency",
		Aggregations: []types.AggType{types.AggTypeQuantile},
		TimeWindow:   5 * time.Minute,
		RepeatEvery:  1 * time.Minute,
		LatencySLA:   10 * time.Minute,
	}
	plan := types.CollectionPlan{}
	jobs := config.BuildPrecomputeJobs(w, plan, "backend:4317")

	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].SketchSource != "backend:4317" {
		t.Errorf("SketchSource = %q, want backend:4317", jobs[0].SketchSource)
	}
	if jobs[0].Granularity != time.Minute {
		t.Errorf("Granularity = %v, want 1m", jobs[0].Granularity)
	}
	if !strings.Contains(jobs[0].QueryExpr, "latency") {
		t.Errorf("QueryExpr should mention metric name, got %q", jobs[0].QueryExpr)
	}
}

func TestBuildPrecomputeJobs_ReturnsNilWhenNotEligible(t *testing.T) {
	w := types.QueryWorkload{
		MetricName:  "latency",
		TimeWindow:  5 * time.Minute,
		RepeatEvery: 5 * time.Minute,
		LatencySLA:  1 * time.Minute, // RepeatEvery >= LatencySLA → no precompute
	}
	jobs := config.BuildPrecomputeJobs(w, types.CollectionPlan{}, "backend:4317")
	if jobs != nil {
		t.Errorf("expected nil jobs, got %v", jobs)
	}
}

func TestBuildPrecomputeJobs_StorePath(t *testing.T) {
	w := types.QueryWorkload{
		MetricName:  "errors",
		TimeWindow:  1 * time.Hour,
		RepeatEvery: 5 * time.Minute,
		LatencySLA:  1 * time.Hour,
	}
	jobs := config.BuildPrecomputeJobs(w, types.CollectionPlan{}, "backend:4317")
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if !strings.Contains(jobs[0].StorePath, "errors") {
		t.Errorf("StorePath should contain metric name, got %q", jobs[0].StorePath)
	}
	if !strings.Contains(jobs[0].StorePath, "1h") {
		t.Errorf("StorePath should contain time window, got %q", jobs[0].StorePath)
	}
}

func TestBuildPrecomputeJobs_CardinalityAggType(t *testing.T) {
	w := types.QueryWorkload{
		MetricName:   "users",
		Aggregations: []types.AggType{types.AggTypeCardinality},
		TimeWindow:   5 * time.Minute,
		RepeatEvery:  1 * time.Minute,
		LatencySLA:   10 * time.Minute,
	}
	jobs := config.BuildPrecomputeJobs(w, types.CollectionPlan{}, "backend:4317")
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if !strings.Contains(jobs[0].QueryExpr, "count_distinct_over_time") {
		t.Errorf("cardinality query should use count_distinct_over_time, got %q", jobs[0].QueryExpr)
	}
}

// ── PrecomputeClient ──────────────────────────────────────────────────────────

func TestPrecomputeClient_Register(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type: application/json")
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(config.PrecomputeJobResponse{
			JobID:     "job-123",
			Status:    "created",
			CreatedAt: time.Now(),
		})
	}))
	defer srv.Close()

	client := config.NewPrecomputeClient(srv.URL)
	resp, err := client.Register(context.Background(), types.PrecomputeJob{
		QueryExpr:    `quantile_over_time(0.99, latency[5m])`,
		Granularity:  time.Minute,
		SketchSource: "backend:4317",
		StorePath:    "precomputed/latency/p99/5m0s",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.JobID != "job-123" {
		t.Errorf("JobID = %q, want job-123", resp.JobID)
	}
}

func TestPrecomputeClient_Register_ErrorOnBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := config.NewPrecomputeClient(srv.URL)
	_, err := client.Register(context.Background(), types.PrecomputeJob{
		QueryExpr:   "latency",
		Granularity: time.Minute,
	})
	if err == nil {
		t.Error("expected error on 500 response, got nil")
	}
}

func TestPrecomputeClient_Deregister(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "job-123") {
			t.Errorf("path should contain job ID, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := config.NewPrecomputeClient(srv.URL)
	if err := client.Deregister(context.Background(), "job-123"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
}
