package precompute

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"strings"
	"time"
)

// CollectorPlanEnvelope identifies one physical-plan bundle.
type CollectorPlanEnvelope struct {
	PlanID               uint64 `json:"plan_id"`
	GeneratedAtUnixMS    uint64 `json:"generated_at_unix_ms"`
	PlannerRevision      string `json:"planner_revision"`
	CapabilitySnapshotID string `json:"capability_snapshot_id"`
}

// CollectorMaterialization is one committed physical sketch decision.
type CollectorMaterialization struct {
	QueryID        string             `json:"query_id"`
	Metric         string             `json:"metric"`
	Algorithm      string             `json:"algorithm"`
	Parameters     map[string]float64 `json:"parameters"`
	GroupBy        []string           `json:"group_by"`
	WindowSecs     uint64             `json:"window_secs"`
	EvidenceSource *string            `json:"evidence_source"`
	Lifecycle      CollectorLifecycle `json:"lifecycle"`
}

// CollectorLifecycle is the ASAPPlanner-selected summary-state commitment.
type CollectorLifecycle struct {
	Kind                 string `json:"kind"`
	MaintenanceMode      string `json:"maintenance_mode"`
	EvaluationSchedule   string `json:"evaluation_schedule"`
	OutputRepresentation string `json:"output_representation"`
}

// SupportedCollectorLifecycle returns the only lifecycle implemented by the
// current tumbling-window sketch runtime.
func SupportedCollectorLifecycle() CollectorLifecycle {
	return CollectorLifecycle{
		Kind:                 "continuously_maintained",
		MaintenanceMode:      "incremental",
		EvaluationSchedule:   "per_update",
		OutputRepresentation: "summary_state",
	}
}

// CollectorPlan is the per-target physical plan emitted by ASAPQuery.
type CollectorPlan struct {
	CollectorID      string                     `json:"collector_id"`
	Envelope         CollectorPlanEnvelope      `json:"envelope"`
	Materializations []CollectorMaterialization `json:"materializations"`
}

// DecodeCollectorPlan validates the complete plan before returning any runtime
// configuration. It never supplies a default for a committed algorithm knob.
func DecodeCollectorPlan(body []byte, collectorID string) (*PrecomputeConfigSet, error) {
	var plan CollectorPlan
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&plan); err != nil {
		return nil, fmt.Errorf("collector plan decode: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("collector plan decode: trailing or malformed JSON")
	}
	if plan.CollectorID != collectorID {
		return nil, fmt.Errorf("collector plan target mismatch: expected %q, got %q", collectorID, plan.CollectorID)
	}
	if plan.Envelope.PlanID == 0 || strings.TrimSpace(plan.Envelope.PlannerRevision) == "" ||
		strings.TrimSpace(plan.Envelope.CapabilitySnapshotID) == "" {
		return nil, errors.New("collector plan identity fields are required")
	}

	seen := make(map[string]struct{}, len(plan.Materializations))
	configs := make([]PrecomputeConfig, 0, len(plan.Materializations))
	for _, materialization := range plan.Materializations {
		if strings.TrimSpace(materialization.QueryID) == "" {
			return nil, errors.New("collector plan query_id is required")
		}
		if _, duplicate := seen[materialization.QueryID]; duplicate {
			return nil, fmt.Errorf("collector plan duplicate query_id %q", materialization.QueryID)
		}
		seen[materialization.QueryID] = struct{}{}
		cfg, err := materialization.precomputeConfig(plan.Envelope.PlanID)
		if err != nil {
			return nil, fmt.Errorf("materialization %q: %w", materialization.QueryID, err)
		}
		configs = append(configs, cfg)
	}
	return &PrecomputeConfigSet{Version: plan.Envelope.PlanID, Configs: configs}, nil
}

func (m CollectorMaterialization) precomputeConfig(planID uint64) (PrecomputeConfig, error) {
	if strings.TrimSpace(m.Metric) == "" || m.WindowSecs == 0 {
		return PrecomputeConfig{}, errors.New("metric and positive window_secs are required")
	}
	if m.Lifecycle.Kind != "continuously_maintained" ||
		m.Lifecycle.MaintenanceMode != "incremental" ||
		m.Lifecycle.EvaluationSchedule != "per_update" ||
		m.Lifecycle.OutputRepresentation != "summary_state" {
		return PrecomputeConfig{}, errors.New("unsupported lifecycle; collector requires continuously_maintained/incremental/per_update/summary_state")
	}
	kind, params, topk, err := m.runtimeSketch()
	if err != nil {
		return PrecomputeConfig{}, err
	}
	if topk && (m.EvidenceSource == nil || strings.TrimSpace(*m.EvidenceSource) == "") {
		return PrecomputeConfig{}, errors.New("heap-bearing TopK requires evidence_source")
	}
	window := time.Duration(m.WindowSecs) * time.Second
	return PrecomputeConfig{
		AggID:          collectorMaterializationID(planID, m.QueryID),
		SketchType:     kind,
		Mode:           Tumbling,
		Window:         WindowSpec{Size: window, Slide: window},
		AggregateBy:    append([]string(nil), m.GroupBy...),
		TransmitSketch: true,
		Encoding:       EncodingProtoFull,
		SketchParams:   params,
		MetricName:     m.Metric,
	}, nil
}

func (m CollectorMaterialization) runtimeSketch() (SketchType, SketchParams, bool, error) {
	params := make(SketchParams)
	require := func(wire, runtime string) error {
		value, ok := m.Parameters[wire]
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
			return fmt.Errorf("missing/invalid parameter %q", wire)
		}
		params[runtime] = value
		return nil
	}
	var kind SketchType
	var topk bool
	switch m.Algorithm {
	case "ddsketch":
		kind = SketchTypeDDSketch
		if err := require("alpha", "relative_accuracy"); err != nil {
			return 0, nil, false, err
		}
		if params["relative_accuracy"] >= 1 {
			return 0, nil, false, errors.New("alpha must be in (0, 1)")
		}
	case "kll":
		kind = SketchTypeKLLSketch
		if err := require("k", "k"); err != nil {
			return 0, nil, false, err
		}
		if params["k"] < 8 {
			return 0, nil, false, errors.New("KLL k must be at least 8")
		}
	case "hll":
		kind = SketchTypeHLLSketch
		if err := require("precision", "precision"); err != nil {
			return 0, nil, false, err
		}
		if params["precision"] < 4 || params["precision"] > 18 {
			return 0, nil, false, errors.New("HLL precision must be in [4, 18]")
		}
	case "cms", "cmswithheap":
		kind, topk = SketchTypeCountMinSketch, m.Algorithm == "cmswithheap"
		if err := require("width", "columns"); err != nil {
			return 0, nil, false, err
		}
		if err := require("depth", "rows"); err != nil {
			return 0, nil, false, err
		}
		if topk {
			if err := require("heap_size", "heap_size"); err != nil {
				return 0, nil, false, err
			}
		}
	case "countsketch", "countsketchwithheap":
		kind, topk = SketchTypeCountSketch, m.Algorithm == "countsketchwithheap"
		if err := require("width", "width"); err != nil {
			return 0, nil, false, err
		}
		if err := require("depth", "depth"); err != nil {
			return 0, nil, false, err
		}
		if topk {
			if err := require("heap_size", "heap_size"); err != nil {
				return 0, nil, false, err
			}
		}
	default:
		return 0, nil, false, fmt.Errorf("unsupported algorithm %q", m.Algorithm)
	}
	return kind, params, topk, nil
}

// collectorMaterializationID is deliberately language-neutral: FNV-1a over
// big-endian plan_id followed by a zero separator and UTF-8 query_id.
func collectorMaterializationID(planID uint64, queryID string) AggId {
	h := fnv.New64a()
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], planID)
	_, _ = h.Write(id[:])
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(queryID))
	value := h.Sum64()
	if value == 0 {
		value = 1
	}
	return AggId(value)
}
