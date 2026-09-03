package precompute

import (
	"encoding/json"
	"os"
	"testing"
)

func collectorPlanBody(t *testing.T, algorithm string, params map[string]float64, evidence *string) []byte {
	t.Helper()
	encoding := StateEncodingSketchlibProtobufV1
	if algorithm == "sum" {
		encoding = StateEncodingExactAccumulatorV1
	}
	body, err := json.Marshal(CollectorPlan{
		CollectorID: "edge-a",
		Envelope: CollectorPlanEnvelope{
			PlanID: 42, PlanVersion: 7, GeneratedAtUnixMS: 10_000,
			ActivationUnixMS: 11_000, BackendCompat: "asap-query-backend.v1",
			PlannerRevision: "264937ec", CapabilitySnapshotID: "caps-7",
		},
		Materializations: []CollectorMaterialization{{
			QueryID: "q", Materialization: 9001, Metric: "m", Algorithm: algorithm,
			Parameters: params, GroupBy: []string{"service"}, WindowSecs: 60,
			AbstractWindowFramework: SummaryWindowFrameworkTumbling,
			WindowImplementationID:  "collector-tumbling-v1", PaneSecs: 60,
			StateLayout:    "anchored-pane-v1",
			EvidenceSource: evidence, Lifecycle: SupportedCollectorLifecycle(),
		}},
		TransmissionRules: []TransmissionRule{{
			Materialization: 9001, ProducerID: "edge-a", SchemaID: "summary-state-v1-9001",
			Mode: TransmissionModeFull, Encoding: encoding, EmitEveryMS: 60_000,
			DestinationRef: "asapquery-backend",
			RuntimePolicy:  RuntimeRulePolicy{Sampling: SamplingPolicy{Mode: "disabled"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func decodePlan(t *testing.T, algorithm string, params map[string]float64, evidence *string) (*PrecomputeConfigSet, CollectorPlan) {
	t.Helper()
	body := collectorPlanBody(t, algorithm, params, evidence)
	set, err := DecodeCollectorPlan(body, "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	return set, plan
}

func TestDecodeCollectorPlanPreservesPhysicalDecision(t *testing.T) {
	set, _ := decodePlan(t, "ddsketch", map[string]float64{"alpha": .01}, nil)
	if set.Version != 7 || len(set.Configs) != 1 {
		t.Fatalf("unexpected set: %+v", set)
	}
	cfg := set.Configs[0]
	if cfg.AggID != 9001 {
		t.Fatalf("backend materialization fingerprint changed: %d", cfg.AggID)
	}
	if cfg.SketchType != SketchTypeDDSketch || cfg.SketchParams["relative_accuracy"] != .01 {
		t.Fatalf("physical family/params changed: %+v", cfg)
	}
	if cfg.MetricName != "m" || len(cfg.AggregateBy) != 1 || cfg.AggregateBy[0] != "service" {
		t.Fatalf("source/grouping changed: %+v", cfg)
	}
}

func TestDecodeCollectorPlanRejectsUnsupportedExactSumConsistently(t *testing.T) {
	if _, err := DecodeCollectorPlan(collectorPlanBody(t, "sum", map[string]float64{}, nil), "edge-a"); err == nil {
		t.Fatal("exact sum must fail closed until both Collector runtimes implement it")
	}
}

func TestDecodeCollectorPlanProjectsSamplingDeltaAndGOS(t *testing.T) {
	body := collectorPlanBody(t, "countsketch", map[string]float64{"width": 512, "depth": 5}, nil)
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	checkpoint := uint64(600_000)
	plan.TransmissionRules[0].Mode = TransmissionModeDelta
	plan.TransmissionRules[0].FullCheckpointEveryMS = &checkpoint
	plan.TransmissionRules[0].RuntimePolicy.Delta = &DeltaPolicy{
		AbsoluteThreshold: 4,
		GOS:               &GOSPolicy{EpsilonStaleness: .05, Sites: 4, ThresholdMode: "isotropic"},
	}
	body, _ = json.Marshal(plan)
	set, err := DecodeCollectorPlan(body, "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	cfg := set.Configs[0]
	if !cfg.DeltaTransmission || cfg.DeltaThreshold != 4 || cfg.GosDeltaEpsilon != .05 || cfg.GosSites != 4 {
		t.Fatalf("runtime policy did not project: %+v", cfg)
	}
}

func TestDecodeCollectorPlanProjectsAuthoritativeSampling(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	plan.TransmissionRules[0].RuntimePolicy.Sampling = SamplingPolicy{
		Mode: "fixed", Probability: .25, Estimator: SamplingEstimatorHashThreshold,
	}
	body, _ = json.Marshal(plan)
	set, err := DecodeCollectorPlan(body, "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	if got := set.Configs[0].SampleP; got != .25 {
		t.Fatalf("SampleP = %v, want .25", got)
	}
}

func TestDecodeCollectorPlanAcceptsBackendContractFixture(t *testing.T) {
	body, err := os.ReadFile("../asap-precompute-rs/tests/fixtures/collector_plan_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	set, err := DecodeCollectorPlan(body, "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	if set.Version != 7 || set.Configs[0].AggID != 9001 || set.Configs[0].SampleP != .5 {
		t.Fatalf("fixture drift: %+v", set)
	}
}

func TestDecodeCollectorPlanRejectsMissingTransmissionRule(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	_ = json.Unmarshal(body, &plan)
	plan.TransmissionRules = nil
	body, _ = json.Marshal(plan)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("missing transmission rule must fail closed")
	}
}

func TestDecodeCollectorPlanRejectsUnsupportedLifecycle(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	_ = json.Unmarshal(body, &plan)
	plan.Materializations[0].Lifecycle.Kind = "ephemeral"
	body, _ = json.Marshal(plan)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("unsupported lifecycle must fail closed")
	}
}

func TestDecodeCollectorPlanRejectsUnsupportedWindowRealization(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	_ = json.Unmarshal(body, &plan)
	plan.Materializations[0].AbstractWindowFramework = SummaryWindowFrameworkSliding
	body, _ = json.Marshal(plan)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("Collector must not substitute tumbling for Planner sliding")
	}

	_ = json.Unmarshal(collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil), &plan)
	plan.Materializations[0].PaneSecs = 30
	body, _ = json.Marshal(plan)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("mismatched concrete pane width must fail closed")
	}

	_ = json.Unmarshal(collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil), &plan)
	plan.Materializations[0].WindowImplementationID = "unknown-tumbling-runtime"
	body, _ = json.Marshal(plan)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("unknown concrete window implementation must fail closed")
	}
}

func TestDecodeCollectorPlanTopKRequiresEvidence(t *testing.T) {
	_, err := DecodeCollectorPlan(collectorPlanBody(t, "countsketchwithheap", map[string]float64{
		"width": 512, "depth": 5, "heap_size": 10,
	}, nil), "edge-a")
	if err == nil {
		t.Fatal("missing TopK evidence must fail closed")
	}
}

func TestDecodeCollectorPlanRejectsUnknownFields(t *testing.T) {
	body := collectorPlanBody(t, "kll", map[string]float64{"k": 269}, nil)
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	raw["future_semantics"] = true
	body, _ = json.Marshal(raw)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("unknown semantic fields must not be silently ignored")
	}
}
