package precompute

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// CollectorPlanEnvelope identifies one physical-plan bundle.
type CollectorPlanEnvelope struct {
	PlanID               uint64  `json:"plan_id"`
	PlanVersion          uint64  `json:"plan_version"`
	GeneratedAtUnixMS    uint64  `json:"generated_at_unix_ms"`
	ActivationUnixMS     uint64  `json:"activation_unix_ms"`
	ExpiryUnixMS         *uint64 `json:"expiry_unix_ms"`
	BackendCompat        string  `json:"backend_compat"`
	PlannerRevision      string  `json:"planner_revision"`
	CapabilitySnapshotID string  `json:"capability_snapshot_id"`
}

// CollectorMaterialization is one committed physical sketch decision.
type CollectorMaterialization struct {
	QueryID                 string                 `json:"query_id"`
	Materialization         uint64                 `json:"materialization"`
	Metric                  string                 `json:"metric"`
	Algorithm               string                 `json:"algorithm"`
	Parameters              map[string]float64     `json:"parameters"`
	GroupBy                 []string               `json:"group_by"`
	WindowSecs              uint64                 `json:"window_secs"`
	AbstractWindowFramework SummaryWindowFramework `json:"abstract_window_framework"`
	WindowImplementationID  string                 `json:"window_implementation_id"`
	PaneSecs                uint64                 `json:"pane_secs"`
	StateLayout             string                 `json:"state_layout"`
	EvidenceSource          *string                `json:"evidence_source"`
	Lifecycle               CollectorLifecycle     `json:"lifecycle"`
}

// SummaryWindowFramework is Planner-owned abstract IR. Collector validates
// the selected value and executes only the corresponding compiled realization.
type SummaryWindowFramework string

const (
	SummaryWindowFrameworkTumbling             SummaryWindowFramework = "tumbling"
	SummaryWindowFrameworkSliding              SummaryWindowFramework = "sliding"
	SummaryWindowFrameworkExponentialHistogram SummaryWindowFramework = "exponential_histogram"
)

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
	CollectorID       string                     `json:"collector_id"`
	Envelope          CollectorPlanEnvelope      `json:"envelope"`
	Materializations  []CollectorMaterialization `json:"materializations"`
	TransmissionRules []TransmissionRule         `json:"transmission_rules"`
}

// TransmissionRule is the exact producer and frame contract for one materialization.
type TransmissionRule struct {
	Materialization       uint64            `json:"materialization"`
	ProducerID            string            `json:"producer_id"`
	SchemaID              string            `json:"schema_id"`
	Mode                  TransmissionMode  `json:"mode"`
	Encoding              StateEncoding     `json:"encoding"`
	EmitEveryMS           uint64            `json:"emit_every_ms"`
	FullCheckpointEveryMS *uint64           `json:"full_checkpoint_every_ms"`
	DestinationRef        string            `json:"destination_ref"`
	RuntimePolicy         RuntimeRulePolicy `json:"runtime_policy"`
}

type TransmissionMode string

const (
	TransmissionModeFull  TransmissionMode = "full"
	TransmissionModeDelta TransmissionMode = "delta"
)

type StateEncoding string

const (
	StateEncodingSketchlibProtobufV1 StateEncoding = "sketchlib_protobuf_v1"
	StateEncodingSketchCoreMsgpackV1 StateEncoding = "sketch_core_msgpack_v1"
	StateEncodingExactAccumulatorV1  StateEncoding = "exact_accumulator_v1"
)

type RuntimeRulePolicy struct {
	Sampling   SamplingPolicy          `json:"sampling"`
	Delta      *DeltaPolicy            `json:"delta"`
	Adaptation RuntimeAdaptationPolicy `json:"adaptation"`
}

type SamplingPolicy struct {
	Mode        string            `json:"mode"`
	Probability float64           `json:"probability,omitempty"`
	Estimator   SamplingEstimator `json:"estimator,omitempty"`
}

type SamplingEstimator string

const (
	SamplingEstimatorHashThreshold      SamplingEstimator = "hash_threshold"
	SamplingEstimatorGeometricAdmission SamplingEstimator = "geometric_admission"
)

type DeltaPolicy struct {
	AbsoluteThreshold float64    `json:"absolute_threshold"`
	GOS               *GOSPolicy `json:"gos"`
}

type GOSPolicy struct {
	EpsilonStaleness float64 `json:"epsilon_staleness"`
	Sites            uint32  `json:"sites"`
	ThresholdMode    string  `json:"threshold_mode"`
}

type RuntimeAdaptationPolicy struct {
	Enabled             bool               `json:"enabled"`
	NotBeforeUnixMS     uint64             `json:"not_before_unix_ms"`
	MaxEvidenceAgeMS    uint64             `json:"max_evidence_age_ms"`
	MinEvidenceSamples  uint64             `json:"min_evidence_samples"`
	SampleProbability   *AdaptiveF64Bounds `json:"sample_probability"`
	EmitEveryMS         *AdaptiveU64Bounds `json:"emit_every_ms"`
	DeltaThreshold      *AdaptiveF64Bounds `json:"delta_threshold"`
	GOSEpsilonStaleness *AdaptiveF64Bounds `json:"gos_epsilon_staleness"`
}

type AdaptiveF64Bounds struct {
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
	MaxStep float64 `json:"max_step"`
}
type AdaptiveU64Bounds struct {
	Min     uint64 `json:"min"`
	Max     uint64 `json:"max"`
	MaxStep uint64 `json:"max_step"`
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
	if plan.Envelope.PlanID == 0 || plan.Envelope.PlanVersion == 0 ||
		plan.Envelope.GeneratedAtUnixMS == 0 || plan.Envelope.ActivationUnixMS == 0 || plan.Envelope.GeneratedAtUnixMS > plan.Envelope.ActivationUnixMS ||
		(plan.Envelope.ExpiryUnixMS != nil && *plan.Envelope.ExpiryUnixMS <= plan.Envelope.ActivationUnixMS) ||
		strings.TrimSpace(plan.Envelope.BackendCompat) == "" || strings.TrimSpace(plan.Envelope.PlannerRevision) == "" ||
		strings.TrimSpace(plan.Envelope.CapabilitySnapshotID) == "" {
		return nil, errors.New("collector plan identity fields are required")
	}

	seen := make(map[string]struct{}, len(plan.Materializations))
	materializations := make(map[uint64]CollectorMaterialization, len(plan.Materializations))
	rules := make(map[uint64]TransmissionRule, len(plan.TransmissionRules))
	for _, rule := range plan.TransmissionRules {
		if rule.Materialization == 0 || rule.ProducerID != plan.CollectorID {
			return nil, errors.New("transmission rules must bind this collector and a materialization")
		}
		if _, duplicate := rules[rule.Materialization]; duplicate {
			return nil, fmt.Errorf("duplicate transmission rule for materialization %d", rule.Materialization)
		}
		rules[rule.Materialization] = rule
	}
	configs := make([]PrecomputeConfig, 0, len(plan.Materializations))
	for _, materialization := range plan.Materializations {
		if strings.TrimSpace(materialization.QueryID) == "" || materialization.Materialization == 0 {
			return nil, errors.New("collector plan query_id is required")
		}
		if _, duplicate := seen[materialization.QueryID]; duplicate {
			return nil, fmt.Errorf("collector plan duplicate query_id %q", materialization.QueryID)
		}
		seen[materialization.QueryID] = struct{}{}
		if _, duplicate := materializations[materialization.Materialization]; duplicate {
			return nil, fmt.Errorf("duplicate materialization %d", materialization.Materialization)
		}
		materializations[materialization.Materialization] = materialization
		rule, ok := rules[materialization.Materialization]
		if !ok {
			return nil, fmt.Errorf("materialization %d has no transmission rule", materialization.Materialization)
		}
		if err := validateTransmissionRule(rule, materialization); err != nil {
			return nil, fmt.Errorf("materialization %q: %w", materialization.QueryID, err)
		}
		cfg, err := materialization.precomputeConfig(rule)
		if err != nil {
			return nil, fmt.Errorf("materialization %q: %w", materialization.QueryID, err)
		}
		configs = append(configs, cfg)
	}
	if len(rules) != len(materializations) {
		return nil, errors.New("transmission rules do not exactly match materializations")
	}
	return &PrecomputeConfigSet{Version: plan.Envelope.PlanVersion, CollectorPlan: &plan, Configs: configs}, nil
}

func (m CollectorMaterialization) precomputeConfig(rule TransmissionRule) (PrecomputeConfig, error) {
	if strings.TrimSpace(m.Metric) == "" || m.WindowSecs == 0 ||
		strings.TrimSpace(m.WindowImplementationID) == "" || m.PaneSecs == 0 ||
		strings.TrimSpace(m.StateLayout) == "" {
		return PrecomputeConfig{}, errors.New("metric, window implementation, pane, state layout, and positive window_secs are required")
	}
	if m.AbstractWindowFramework != SummaryWindowFrameworkTumbling ||
		m.WindowImplementationID != "collector-tumbling-v1" ||
		m.PaneSecs != m.WindowSecs || m.StateLayout != "anchored-pane-v1" {
		return PrecomputeConfig{}, errors.New("unsupported window realization: runtime requires Planner tumbling + equal anchored panes + anchored-pane-v1")
	}
	if m.Lifecycle.Kind != "continuously_maintained" ||
		m.Lifecycle.MaintenanceMode != "incremental" ||
		m.Lifecycle.EvaluationSchedule != "per_update" ||
		m.Lifecycle.OutputRepresentation != "summary_state" {
		return PrecomputeConfig{}, errors.New("unsupported lifecycle; collector requires continuously_maintained/incremental/per_update/summary_state")
	}
	kind, aggKind, params, topk, err := m.runtimeSketch()
	if err != nil {
		return PrecomputeConfig{}, err
	}
	if topk && (m.EvidenceSource == nil || strings.TrimSpace(*m.EvidenceSource) == "") {
		return PrecomputeConfig{}, errors.New("heap-bearing TopK requires evidence_source")
	}
	window := time.Duration(m.WindowSecs) * time.Second
	sampleP := 1.0
	if rule.RuntimePolicy.Sampling.Mode == "fixed" {
		sampleP = rule.RuntimePolicy.Sampling.Probability
		params["sample_p"] = sampleP
	}
	if rule.RuntimePolicy.Delta != nil && rule.RuntimePolicy.Delta.GOS != nil {
		params["gos_delta_epsilon"] = rule.RuntimePolicy.Delta.GOS.EpsilonStaleness
		params["gos_sites"] = float64(rule.RuntimePolicy.Delta.GOS.Sites)
	}
	encoding := EncodingProtoFull
	if rule.Encoding == StateEncodingSketchCoreMsgpackV1 {
		encoding = EncodingMsgpack
	}
	return PrecomputeConfig{
		AggID:             AggId(m.Materialization),
		SketchType:        kind,
		AggKind:           aggKind,
		Mode:              Tumbling,
		Window:            WindowSpec{Size: window, Slide: window},
		AggregateBy:       append([]string(nil), m.GroupBy...),
		TransmitSketch:    true,
		DeltaTransmission: rule.Mode == TransmissionModeDelta,
		DeltaThreshold:    deltaThreshold(rule.RuntimePolicy.Delta),
		Encoding:          encoding,
		SketchParams:      params,
		SampleP:           sampleP,
		GosDeltaEpsilon:   params.Get("gos_delta_epsilon", 0),
		GosSites:          uint32(params.Get("gos_sites", 0)),
		MetricName:        m.Metric,
	}, nil
}

func deltaThreshold(policy *DeltaPolicy) uint64 {
	if policy == nil {
		return 0
	}
	return uint64(math.Ceil(policy.AbsoluteThreshold))
}

func validateTransmissionRule(rule TransmissionRule, m CollectorMaterialization) error {
	if strings.TrimSpace(rule.SchemaID) == "" || strings.TrimSpace(rule.DestinationRef) == "" || rule.EmitEveryMS == 0 {
		return errors.New("invalid transmission identity or cadence")
	}
	if rule.EmitEveryMS != m.WindowSecs*1000 {
		return errors.New("transmission cadence must match the materialization window")
	}
	if rule.Mode == TransmissionModeDelta {
		if rule.FullCheckpointEveryMS == nil || *rule.FullCheckpointEveryMS == 0 || rule.RuntimePolicy.Delta == nil {
			return errors.New("delta mode requires policy and full checkpoint cadence")
		}
	} else if rule.Mode != TransmissionModeFull || rule.FullCheckpointEveryMS != nil || rule.RuntimePolicy.Delta != nil {
		return errors.New("full mode must not carry delta checkpoint or policy")
	}
	if rule.Encoding != StateEncodingSketchlibProtobufV1 && rule.Encoding != StateEncodingSketchCoreMsgpackV1 && rule.Encoding != StateEncodingExactAccumulatorV1 {
		return errors.New("unsupported state encoding")
	}
	if (m.Algorithm == "sum") != (rule.Encoding == StateEncodingExactAccumulatorV1) {
		return errors.New("state encoding is incompatible with the materialization family")
	}
	sampling := rule.RuntimePolicy.Sampling
	if sampling.Mode == "" {
		sampling.Mode = "disabled"
	}
	if sampling.Mode == "fixed" {
		if math.IsNaN(sampling.Probability) || math.IsInf(sampling.Probability, 0) || sampling.Probability <= 0 || sampling.Probability > 1 {
			return errors.New("sample probability must be in (0,1]")
		}
		valid := (m.Algorithm == "hll" && sampling.Estimator == SamplingEstimatorHashThreshold) ||
			((m.Algorithm == "cms" || m.Algorithm == "cmswithheap") && sampling.Estimator == SamplingEstimatorGeometricAdmission)
		if !valid {
			return errors.New("sampling estimator is unsupported for this family")
		}
	} else if sampling.Mode != "disabled" {
		return errors.New("unknown sampling mode")
	}
	if delta := rule.RuntimePolicy.Delta; delta != nil {
		if math.IsNaN(delta.AbsoluteThreshold) || math.IsInf(delta.AbsoluteThreshold, 0) || delta.AbsoluteThreshold < 0 || math.Trunc(delta.AbsoluteThreshold) != delta.AbsoluteThreshold {
			return errors.New("delta threshold must be a finite non-negative integer for this runtime")
		}
		switch m.Algorithm {
		case "ddsketch", "hll", "cms", "cmswithheap", "countsketch", "countsketchwithheap":
		default:
			return errors.New("delta transmission is unsupported for this family")
		}
		if gos := delta.GOS; gos != nil {
			if (m.Algorithm != "countsketch" && m.Algorithm != "countsketchwithheap") || gos.EpsilonStaleness <= 0 || gos.EpsilonStaleness > 1 || gos.Sites == 0 || gos.ThresholdMode != "isotropic" {
				return errors.New("invalid GOS policy")
			}
		}
	}
	adaptation := rule.RuntimePolicy.Adaptation
	if adaptation.Enabled && (adaptation.MaxEvidenceAgeMS == 0 || adaptation.MinEvidenceSamples == 0) {
		return errors.New("enabled adaptation requires evidence constraints")
	}
	if err := validateAdaptiveF64(adaptation.SampleProbability, 0, 1); err != nil {
		return err
	}
	if err := validateAdaptiveF64(adaptation.DeltaThreshold, 0, math.MaxFloat64); err != nil {
		return err
	}
	if err := validateAdaptiveF64(adaptation.GOSEpsilonStaleness, 0, 1); err != nil {
		return err
	}
	if bounds := adaptation.EmitEveryMS; bounds != nil && (bounds.Min == 0 || bounds.Min > bounds.Max || bounds.MaxStep == 0 || rule.EmitEveryMS < bounds.Min || rule.EmitEveryMS > bounds.Max) {
		return errors.New("emit interval guardrails are invalid")
	}
	currentSampleP := 1.0
	if sampling.Mode == "fixed" {
		currentSampleP = sampling.Probability
	}
	if bounds := adaptation.SampleProbability; bounds != nil && (currentSampleP < bounds.Min || currentSampleP > bounds.Max) {
		return errors.New("current sample probability is outside guardrails")
	}
	if bounds := adaptation.DeltaThreshold; bounds != nil {
		if rule.RuntimePolicy.Delta == nil || rule.RuntimePolicy.Delta.AbsoluteThreshold < bounds.Min || rule.RuntimePolicy.Delta.AbsoluteThreshold > bounds.Max {
			return errors.New("current delta threshold is outside guardrails")
		}
	}
	if bounds := adaptation.GOSEpsilonStaleness; bounds != nil {
		if rule.RuntimePolicy.Delta == nil || rule.RuntimePolicy.Delta.GOS == nil || rule.RuntimePolicy.Delta.GOS.EpsilonStaleness < bounds.Min || rule.RuntimePolicy.Delta.GOS.EpsilonStaleness > bounds.Max {
			return errors.New("current GOS epsilon is outside guardrails")
		}
	}
	return nil
}

func validateAdaptiveF64(bounds *AdaptiveF64Bounds, domainMin, domainMax float64) error {
	if bounds == nil {
		return nil
	}
	if math.IsNaN(bounds.Min) || math.IsNaN(bounds.Max) || math.IsNaN(bounds.MaxStep) ||
		math.IsInf(bounds.Min, 0) || math.IsInf(bounds.Max, 0) || math.IsInf(bounds.MaxStep, 0) ||
		bounds.Min < domainMin || bounds.Max > domainMax || bounds.Min > bounds.Max || bounds.MaxStep <= 0 {
		return errors.New("floating-point adaptation guardrails are invalid")
	}
	return nil
}

func (m CollectorMaterialization) runtimeSketch() (SketchType, AggregationKind, SketchParams, bool, error) {
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
	aggKind := AggKindSketch
	var topk bool
	switch m.Algorithm {
	case "sum":
		return 0, 0, nil, false, errors.New("exact sum is not implemented consistently by Collector runtimes")
	case "ddsketch":
		kind = SketchTypeDDSketch
		if err := require("alpha", "relative_accuracy"); err != nil {
			return 0, 0, nil, false, err
		}
		if params["relative_accuracy"] >= 1 {
			return 0, 0, nil, false, errors.New("alpha must be in (0, 1)")
		}
	case "kll":
		kind = SketchTypeKLLSketch
		if err := require("k", "k"); err != nil {
			return 0, 0, nil, false, err
		}
		if params["k"] < 8 {
			return 0, 0, nil, false, errors.New("KLL k must be at least 8")
		}
	case "hll":
		kind = SketchTypeHLLSketch
		if err := require("precision", "precision"); err != nil {
			return 0, 0, nil, false, err
		}
		if params["precision"] < 4 || params["precision"] > 18 {
			return 0, 0, nil, false, errors.New("HLL precision must be in [4, 18]")
		}
	case "cms":
		kind = SketchTypeCountMinSketch
		if err := require("width", "columns"); err != nil {
			return 0, 0, nil, false, err
		}
		if err := require("depth", "rows"); err != nil {
			return 0, 0, nil, false, err
		}
	case "cmswithheap":
		return 0, 0, nil, false, errors.New("keyed CMS heap serving path is not implemented")
	case "countsketch", "countsketchwithheap":
		kind, topk = SketchTypeCountSketch, m.Algorithm == "countsketchwithheap"
		if err := require("width", "width"); err != nil {
			return 0, 0, nil, false, err
		}
		if err := require("depth", "depth"); err != nil {
			return 0, 0, nil, false, err
		}
		if topk {
			if err := require("heap_size", "heap_size"); err != nil {
				return 0, 0, nil, false, err
			}
		}
	default:
		return 0, 0, nil, false, fmt.Errorf("unsupported algorithm %q", m.Algorithm)
	}
	return kind, aggKind, params, topk, nil
}
