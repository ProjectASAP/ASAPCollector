package precompute

import (
	"encoding/json"
	"testing"
)

func collectorPlanBody(t *testing.T, algorithm string, params map[string]float64, evidence *string) []byte {
	t.Helper()
	body, err := json.Marshal(CollectorPlan{
		CollectorID: "edge-a",
		Envelope: CollectorPlanEnvelope{
			PlanID: 42, PlannerRevision: "3afcba6", CapabilitySnapshotID: "caps-7",
		},
		Materializations: []CollectorMaterialization{{
			QueryID: "q", Metric: "m", Algorithm: algorithm, Parameters: params,
			GroupBy: []string{"service"}, WindowSecs: 60, EvidenceSource: evidence,
			Lifecycle: SupportedCollectorLifecycle(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDecodeCollectorPlanPreservesPhysicalDecision(t *testing.T) {
	set, err := DecodeCollectorPlan(
		collectorPlanBody(t, "ddsketch", map[string]float64{"alpha": .01}, nil), "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	if set.Version != 42 || len(set.Configs) != 1 {
		t.Fatalf("unexpected set: %+v", set)
	}
	cfg := set.Configs[0]
	if cfg.SketchType != SketchTypeDDSketch || cfg.SketchParams["relative_accuracy"] != .01 {
		t.Fatalf("physical family/params changed: %+v", cfg)
	}
	if cfg.MetricName != "m" || len(cfg.AggregateBy) != 1 || cfg.AggregateBy[0] != "service" {
		t.Fatalf("source/grouping changed: %+v", cfg)
	}
}

func TestDecodeCollectorPlanRejectsUnsupportedLifecycle(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Materializations[0].Lifecycle.Kind = "ephemeral"
	body, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("unsupported lifecycle must fail closed")
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
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	raw["future_semantics"] = true
	body, _ = json.Marshal(raw)
	if _, err := DecodeCollectorPlan(body, "edge-a"); err == nil {
		t.Fatal("unknown semantic fields must not be silently ignored")
	}
}

func TestCollectorMaterializationIDPinnedAcrossLanguages(t *testing.T) {
	const want AggId = 9843981254622943340
	if got := collectorMaterializationID(42, "q"); got != want {
		t.Fatalf("FNV contract changed: got %d want %d", got, want)
	}
}
