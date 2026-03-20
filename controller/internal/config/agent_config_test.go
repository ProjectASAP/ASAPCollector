package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/config"
	"github.com/ProjectASAP/controller/internal/types"
)

func ddsketchCfg() types.AgentCollectorConfig {
	return types.AgentCollectorConfig{
		OutputMode:   types.OutputModeSketch,
		SketchType:   types.SketchTypeDDSketch,
		SketchParams: types.SketchParams{RelativeAccuracy: 0.01, Quantiles: []float64{0.5, 0.9, 0.99}},
		AggregateBy:  []string{"host.name", "service"},
		LabelMatchers: []string{"env=prod"},
		WindowDuration: 5 * time.Minute,
		Mode:          types.ProcessorModeWindow,
		TransmitSketch: true,
		DropOriginal:   true,
	}
}

func TestGenerateAgentConfig_ContainsProcessorKey(t *testing.T) {
	yaml, err := config.GenerateAgentConfig(ddsketchCfg(), "ws://controller:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, "ddsketch:") {
		t.Errorf("YAML should contain 'ddsketch:' processor key, got:\n%s", yaml)
	}
}

func TestGenerateAgentConfig_ContainsOpAMPEndpoint(t *testing.T) {
	endpoint := "ws://my-controller:4320/v1/opamp"
	yaml, err := config.GenerateAgentConfig(ddsketchCfg(), endpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, endpoint) {
		t.Errorf("YAML should contain OpAMP endpoint %q, got:\n%s", endpoint, yaml)
	}
}

func TestGenerateAgentConfig_ContainsWindowDuration(t *testing.T) {
	yaml, err := config.GenerateAgentConfig(ddsketchCfg(), "ws://ctrl:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, "5m0s") {
		t.Errorf("YAML should contain window_duration '5m0s', got:\n%s", yaml)
	}
}

func TestGenerateAgentConfig_BatchModeNoWindowDuration(t *testing.T) {
	cfg := ddsketchCfg()
	cfg.Mode = types.ProcessorModeBatch
	cfg.WindowDuration = 0

	yaml, err := config.GenerateAgentConfig(cfg, "ws://ctrl:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(yaml, "window_duration") {
		t.Errorf("batch mode YAML should not contain window_duration, got:\n%s", yaml)
	}
}

func TestGenerateAgentConfig_ContainsAggregateBy(t *testing.T) {
	yaml, err := config.GenerateAgentConfig(ddsketchCfg(), "ws://ctrl:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, "host.name") {
		t.Errorf("YAML should contain aggregate_by labels, got:\n%s", yaml)
	}
}

func TestGenerateAgentConfig_HLLProcessor(t *testing.T) {
	cfg := types.AgentCollectorConfig{
		OutputMode:   types.OutputModeSketch,
		SketchType:   types.SketchTypeHLL,
		SketchParams: types.SketchParams{Precision: 14},
		Mode:         types.ProcessorModeBatch,
		TransmitSketch: true,
	}
	yaml, err := config.GenerateAgentConfig(cfg, "ws://ctrl:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, "hll:") {
		t.Errorf("YAML should contain 'hll:' processor key, got:\n%s", yaml)
	}
	if !strings.Contains(yaml, "precision:") {
		t.Errorf("YAML should contain 'precision:', got:\n%s", yaml)
	}
}

func TestGenerateAgentConfig_CountMinSketchProcessor(t *testing.T) {
	cfg := types.AgentCollectorConfig{
		OutputMode:   types.OutputModeSketch,
		SketchType:   types.SketchTypeCountMinSketch,
		SketchParams: types.SketchParams{Rows: 5, Cols: 2048},
		Mode:         types.ProcessorModeBatch,
		TransmitSketch: true,
	}
	yaml, err := config.GenerateAgentConfig(cfg, "ws://ctrl:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, "countminsketch:") {
		t.Errorf("YAML should contain 'countminsketch:' key, got:\n%s", yaml)
	}
}

func TestGenerateBackendConfig_ContainsMergeKey(t *testing.T) {
	cfg := types.BackendCollectorConfig{
		MergeSketchType: types.SketchTypeDDSketch,
		GroupBy:         []string{"host.name"},
	}
	yaml, err := config.GenerateBackendConfig(cfg, "ws://ctrl:4320/v1/opamp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(yaml, "ddsketch_merge:") {
		t.Errorf("YAML should contain 'ddsketch_merge:' key, got:\n%s", yaml)
	}
	if !strings.Contains(yaml, "host.name") {
		t.Errorf("YAML should contain group_by label, got:\n%s", yaml)
	}
}
