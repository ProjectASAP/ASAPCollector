package precompute

import (
	"encoding/json"
	"testing"
)

func TestFrameSequencerScopesSequenceByConcreteSeriesAndWindow(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	rule := plan.TransmissionRules[0]
	checkpointCadence := uint64(60_000)
	rule.Mode = TransmissionModeDelta
	rule.FullCheckpointEveryMS = &checkpointCadence
	rule.RuntimePolicy.Delta = &DeltaPolicy{AbsoluteThreshold: 1}
	plan.TransmissionRules[0] = rule
	var sequencer FrameSequencer
	full, err := sequencer.Next(plan, rule, "boot-7", "service=checkout,zone=a", 100, 200, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := sequencer.Next(plan, rule, "boot-7", "service=checkout,zone=a", 100, 200, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	other, err := sequencer.Next(plan, rule, "boot-7", "service=checkout,zone=b", 100, 200, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if full.Kind != "full" || full.Sequence != 1 || full.CheckpointID == "" {
		t.Fatalf("bad full: %+v", full)
	}
	if delta.Kind != "delta" || delta.Sequence != 2 || delta.BaseCheckpointID != full.CheckpointID {
		t.Fatalf("bad delta: %+v", delta)
	}
	if other.Kind != "full" || other.Sequence != 1 {
		t.Fatalf("series lineages collided: %+v", other)
	}
	attrs := delta.OTLPAttributes()
	if attrs["asap.frame.series_identity"] != "service=checkout,zone=a" || attrs["asap.frame.base_checkpoint_id"] == "" {
		t.Fatalf("backend attributes incomplete: %v", attrs)
	}
}
