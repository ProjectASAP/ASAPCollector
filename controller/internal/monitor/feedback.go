// Package monitor scrapes Prometheus /metrics from OTel collectors and uses
// the readings to detect SLA violations and trigger re-planning.
package monitor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CollectorMetrics is the set of measurements scraped from one collector.
type CollectorMetrics struct {
	// AgentID is the collector's identifier.
	AgentID string
	// SketchSizeBytes is the current in-memory sketch size (gauge).
	SketchSizeBytes float64
	// CPUSecondsTotal is the cumulative CPU usage (counter).
	CPUSecondsTotal float64
	// SamplesIngested is the cumulative count of ingested samples (counter).
	SamplesIngested float64
	// ErrorRate is the fraction of samples that failed to be sketched.
	ErrorRate float64
	// ScrapedAt records when these metrics were collected.
	ScrapedAt time.Time
}

// Violation describes a detected SLA breach or resource anomaly.
type Violation struct {
	AgentID    string
	MetricName string
	Kind       ViolationKind
	Observed   float64
	Threshold  float64
	DetectedAt time.Time
}

// ViolationKind classifies the type of SLA violation detected.
type ViolationKind int

const (
	// ViolationBandwidth fires when sketch size exceeds the bandwidth budget.
	ViolationBandwidth ViolationKind = iota + 1
	// ViolationAccuracy fires when the error rate exceeds the AccuracySLA.
	ViolationAccuracy
	// ViolationCPU fires when CPU usage per sample is unusually high.
	ViolationCPU
)

func (v ViolationKind) String() string {
	switch v {
	case ViolationBandwidth:
		return "bandwidth"
	case ViolationAccuracy:
		return "accuracy"
	case ViolationCPU:
		return "cpu"
	default:
		return "unknown"
	}
}

// Thresholds configure what counts as a violation.
type Thresholds struct {
	// MaxSketchSizeBytes triggers ViolationBandwidth when exceeded.
	MaxSketchSizeBytes float64
	// MaxErrorRate triggers ViolationAccuracy when exceeded.
	MaxErrorRate float64
	// MaxCPUMicrosPerSample triggers ViolationCPU when exceeded.
	MaxCPUMicrosPerSample float64
}

// DefaultThresholds are conservative defaults appropriate for Phase 1.
var DefaultThresholds = Thresholds{
	MaxSketchSizeBytes:    5 * 1024 * 1024, // 5 MB per collector
	MaxErrorRate:          0.02,            // 2% sample error rate
	MaxCPUMicrosPerSample: 5.0,            // 5 μs/sample
}

// OnViolationFunc is called when a violation is detected. Callers should
// trigger a re-plan in this callback.
type OnViolationFunc func(v Violation)

// Scraper periodically pulls /metrics from a list of collector endpoints and
// reports violations.
type Scraper struct {
	endpoints  []Endpoint
	thresholds Thresholds
	onViolation OnViolationFunc
	interval   time.Duration
	client     *http.Client
	logger     *slog.Logger

	mu   sync.Mutex
	last map[string]CollectorMetrics // last scrape per agent for delta computation
}

// Endpoint describes one collector's scrape target.
type Endpoint struct {
	AgentID    string
	MetricsURL string // e.g., "http://agent1:8888/metrics"
}

// NewScraper returns a Scraper that polls the given endpoints.
func NewScraper(
	endpoints []Endpoint,
	thresholds Thresholds,
	onViolation OnViolationFunc,
	interval time.Duration,
	logger *slog.Logger,
) *Scraper {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scraper{
		endpoints:   endpoints,
		thresholds:  thresholds,
		onViolation: onViolation,
		interval:    interval,
		client:      &http.Client{Timeout: 5 * time.Second},
		logger:      logger,
		last:        make(map[string]CollectorMetrics),
	}
}

// Run starts the scrape loop and blocks until ctx is cancelled.
func (s *Scraper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, ep := range s.endpoints {
				metrics, err := s.scrape(ep)
				if err != nil {
					s.logger.Warn("scrape failed", "agent", ep.AgentID, "err", err)
					continue
				}
				s.analyze(metrics)
			}
		}
	}
}

// scrape fetches and parses the Prometheus text format from one endpoint.
func (s *Scraper) scrape(ep Endpoint) (CollectorMetrics, error) {
	resp, err := s.client.Get(ep.MetricsURL)
	if err != nil {
		return CollectorMetrics{}, fmt.Errorf("GET %s: %w", ep.MetricsURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return CollectorMetrics{}, fmt.Errorf("read body: %w", err)
	}

	m := CollectorMetrics{
		AgentID:   ep.AgentID,
		ScrapedAt: time.Now(),
	}
	parsePrometheusText(string(body), &m)
	return m, nil
}

// analyze compares fresh metrics against thresholds and fires violations.
func (s *Scraper) analyze(m CollectorMetrics) {
	s.mu.Lock()
	prev, hasPrev := s.last[m.AgentID]
	s.last[m.AgentID] = m
	s.mu.Unlock()

	// Bandwidth / sketch size check.
	if m.SketchSizeBytes > s.thresholds.MaxSketchSizeBytes {
		s.onViolation(Violation{
			AgentID:    m.AgentID,
			Kind:       ViolationBandwidth,
			Observed:   m.SketchSizeBytes,
			Threshold:  s.thresholds.MaxSketchSizeBytes,
			DetectedAt: m.ScrapedAt,
		})
	}

	// Error rate check.
	if m.ErrorRate > s.thresholds.MaxErrorRate {
		s.onViolation(Violation{
			AgentID:    m.AgentID,
			Kind:       ViolationAccuracy,
			Observed:   m.ErrorRate,
			Threshold:  s.thresholds.MaxErrorRate,
			DetectedAt: m.ScrapedAt,
		})
	}

	// CPU check: compute μs/sample from counter deltas.
	if hasPrev {
		dt := m.ScrapedAt.Sub(prev.ScrapedAt).Seconds()
		if dt > 0 {
			deltasamples := m.SamplesIngested - prev.SamplesIngested
			deltaCPU := m.CPUSecondsTotal - prev.CPUSecondsTotal
			if deltasamples > 0 {
				microsPerSample := (deltaCPU / deltasamples) * 1e6
				if microsPerSample > s.thresholds.MaxCPUMicrosPerSample {
					s.onViolation(Violation{
						AgentID:    m.AgentID,
						Kind:       ViolationCPU,
						Observed:   microsPerSample,
						Threshold:  s.thresholds.MaxCPUMicrosPerSample,
						DetectedAt: m.ScrapedAt,
					})
				}
			}
		}
	}
}

// ── Prometheus text parser (minimal) ─────────────────────────────────────────

// parsePrometheusText extracts the known metric names from a Prometheus
// exposition text payload and populates the CollectorMetrics struct.
// It handles only the simple "metric_name value" format (no labels).
func parsePrometheusText(text string, m *CollectorMetrics) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		name := parts[0]
		val, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		switch name {
		case "otelcol_sketch_size_bytes":
			m.SketchSizeBytes = val
		case "process_cpu_seconds_total":
			m.CPUSecondsTotal = val
		case "otelcol_processor_accepted_metric_points":
			m.SamplesIngested = val
		case "otelcol_sketch_error_rate":
			m.ErrorRate = val
		}
	}
}

// ScrapeOnce performs a single scrape of all endpoints and returns the results.
// Useful for testing and one-shot health checks.
func (s *Scraper) ScrapeOnce(ctx context.Context) []CollectorMetrics {
	results := make([]CollectorMetrics, 0, len(s.endpoints))
	for _, ep := range s.endpoints {
		select {
		case <-ctx.Done():
			return results
		default:
		}
		m, err := s.scrape(ep)
		if err != nil {
			s.logger.Warn("scrape failed", "agent", ep.AgentID, "err", err)
			continue
		}
		s.analyze(m)
		results = append(results, m)
	}
	return results
}
