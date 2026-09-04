package precompute

import (
	"encoding/json"
	"testing"
)

func TestFrameSequencerScopesSequenceByConcreteSeriesAcrossWindows(t *testing.T) {
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
	nextWindow, err := sequencer.Next(plan, rule, "boot-7", "service=checkout,zone=a", 200, 300, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if nextWindow.Kind != "delta" || nextWindow.Sequence != 3 || nextWindow.BaseCheckpointID != full.CheckpointID {
		t.Fatalf("sequence did not continue across windows: %+v", nextWindow)
	}
	attrs := delta.OTLPAttributes()
	if attrs["asap.frame.series_identity"] != "service=checkout,zone=a" || attrs["asap.frame.base_checkpoint_id"] == "" {
		t.Fatalf("backend attributes incomplete: %v", attrs)
	}
}

func TestFrameSequencerBatchFailureDoesNotAdvanceEarlierLineages(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	rule := plan.TransmissionRules[0]
	var sequencer FrameSequencer
	_, err := sequencer.NextBatchForEmission(plan, rule, "boot-7", []FrameEmission{
		{SeriesIdentity: "series-a", WindowStartUnixNano: 100, WindowEndUnixNano: 200, NowUnixMS: 1, EmittedFull: true},
		// A full-mode rule may never label emitted bytes as delta.
		{SeriesIdentity: "series-b", WindowStartUnixNano: 100, WindowEndUnixNano: 200, NowUnixMS: 1, EmittedFull: false},
	})
	if err == nil {
		t.Fatal("invalid second envelope did not reject the batch")
	}
	frames, err := sequencer.NextBatchForEmission(plan, rule, "boot-7", []FrameEmission{
		{SeriesIdentity: "series-a", WindowStartUnixNano: 100, WindowEndUnixNano: 200, NowUnixMS: 2, EmittedFull: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if frames[0].Sequence != 1 {
		t.Fatalf("failed batch advanced sequence: got %d, want 1", frames[0].Sequence)
	}
}

func TestFrameSequencerStartsFreshLineageForNewPlanGeneration(t *testing.T) {
	body := collectorPlanBody(t, "hll", map[string]float64{"precision": 14}, nil)
	var plan CollectorPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		t.Fatal(err)
	}
	rule := plan.TransmissionRules[0]
	var sequencer FrameSequencer
	first, err := sequencer.Next(plan, rule, "boot-7", "series-a", 100, 200, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	plan.Envelope.PlanVersion++
	second, err := sequencer.Next(plan, rule, "boot-7", "series-a", 200, 300, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 1 || second.Kind != "full" {
		t.Fatalf("new plan generation reused prior lineage: first=%+v second=%+v", first, second)
	}
}
