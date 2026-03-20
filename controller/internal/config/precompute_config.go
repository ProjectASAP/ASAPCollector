package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ProjectASAP/controller/internal/types"
)

// PrecomputeJobRequest is the JSON body sent to the ASAPQuery precompute API.
type PrecomputeJobRequest struct {
	Query       string `json:"query"`
	Granularity string `json:"granularity"`
	Source      string `json:"source"`
	SketchType  string `json:"sketch_type"`
	StorePath   string `json:"store_path"`
}

// PrecomputeJobResponse is returned by the ASAPQuery precompute API on success.
type PrecomputeJobResponse struct {
	JobID     string    `json:"job_id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// ShouldPrecompute returns true when a query should be materialized in advance.
//
// Rule (from design doc): precompute only when RepeatEvery < LatencySLA,
// meaning the query fires more often than the system can recompute it on demand.
func ShouldPrecompute(w types.QueryWorkload) bool {
	if w.RepeatEvery <= 0 || w.LatencySLA <= 0 {
		return false
	}
	return w.RepeatEvery < w.LatencySLA
}

// BuildPrecomputeJobs creates the PrecomputeJob list for a plan from a workload.
// If the workload does not benefit from precomputation it returns nil.
func BuildPrecomputeJobs(w types.QueryWorkload, plan types.CollectionPlan, backendAddr string) []types.PrecomputeJob {
	if !ShouldPrecompute(w) {
		return nil
	}

	queryExpr := buildQueryExpr(w)
	storePath := buildStorePath(w)

	return []types.PrecomputeJob{
		{
			QueryExpr:    queryExpr,
			Granularity:  w.RepeatEvery,
			SketchSource: backendAddr,
			StorePath:    storePath,
		},
	}
}

// buildQueryExpr generates a PromQL-style expression from a QueryWorkload.
// This is a simplified representation for Phase 4; a full PromQL builder can
// replace this later.
func buildQueryExpr(w types.QueryWorkload) string {
	agg := "quantile_over_time"
	if len(w.Aggregations) > 0 {
		switch w.Aggregations[0] {
		case types.AggTypeCardinality:
			agg = "count_distinct_over_time"
		case types.AggTypeFrequency:
			agg = "top_k_over_time"
		}
	}

	filters := ""
	for k, v := range w.LabelFilters {
		if filters != "" {
			filters += ","
		}
		filters += fmt.Sprintf(`%s="%s"`, k, v)
	}

	selector := w.MetricName
	if filters != "" {
		selector = fmt.Sprintf(`%s{%s}`, w.MetricName, filters)
	}

	return fmt.Sprintf(`%s(0.99, %s[%s])`, agg, selector, w.TimeWindow)
}

// buildStorePath constructs a SimpleStore key pattern from a QueryWorkload.
func buildStorePath(w types.QueryWorkload) string {
	return fmt.Sprintf("precomputed/%s/p99/%s",
		w.MetricName,
		w.TimeWindow.String())
}

// ── PrecomputeClient ──────────────────────────────────────────────────────────

// PrecomputeClient talks to the ASAPQuery precompute engine HTTP API.
type PrecomputeClient struct {
	baseURL string
	client  *http.Client
}

// NewPrecomputeClient returns a client pointed at the ASAPQuery engine.
func NewPrecomputeClient(baseURL string) *PrecomputeClient {
	return &PrecomputeClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// Register submits a precompute job to the ASAPQuery engine.
func (c *PrecomputeClient) Register(ctx context.Context, job types.PrecomputeJob) (PrecomputeJobResponse, error) {
	req := PrecomputeJobRequest{
		Query:       job.QueryExpr,
		Granularity: job.Granularity.String(),
		Source:      job.SketchSource,
		SketchType:  "ddsketch", // default; could be derived from plan
		StorePath:   job.StorePath,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return PrecomputeJobResponse{}, fmt.Errorf("marshal job: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/v1/precompute/jobs", bytes.NewReader(body))
	if err != nil {
		return PrecomputeJobResponse{}, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return PrecomputeJobResponse{}, fmt.Errorf("POST jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return PrecomputeJobResponse{}, fmt.Errorf("unexpected status %d from precompute API", resp.StatusCode)
	}

	var out PrecomputeJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return PrecomputeJobResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return out, nil
}

// Deregister removes a precompute job by its ID.
func (c *PrecomputeClient) Deregister(ctx context.Context, jobID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/api/v1/precompute/jobs/%s", c.baseURL, jobID), nil)
	if err != nil {
		return fmt.Errorf("build delete request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("DELETE job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d from precompute API", resp.StatusCode)
	}
	return nil
}
