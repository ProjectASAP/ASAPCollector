// Package config generates OTel collector YAML configurations from a CollectionPlan.
package config

import (
	"fmt"

	"github.com/ProjectASAP/controller/internal/types"
	"gopkg.in/yaml.v3"
)

// AgentCollectorYAML is the YAML structure pushed to agent OTel collectors.
type AgentCollectorYAML struct {
	Extensions map[string]any `yaml:"extensions,omitempty"`
	Processors map[string]any `yaml:"processors"`
	Exporters  map[string]any `yaml:"exporters,omitempty"`
	Service    ServiceYAML    `yaml:"service"`
}

// ServiceYAML is the service pipeline section of the OTel collector config.
type ServiceYAML struct {
	Extensions []string         `yaml:"extensions,omitempty"`
	Pipelines  map[string]any   `yaml:"pipelines"`
}

// GenerateAgentConfig converts an AgentCollectorConfig into a YAML string that
// can be pushed to an OTel collector via OpAMP.
func GenerateAgentConfig(cfg types.AgentCollectorConfig, opampEndpoint string) (string, error) {
	processorKey, processorCfg := buildProcessorBlock(cfg)

	doc := AgentCollectorYAML{
		Extensions: map[string]any{
			"opamp": map[string]any{
				"server": map[string]any{
					"ws": map[string]any{
						"endpoint": opampEndpoint,
					},
				},
			},
		},
		Processors: map[string]any{
			processorKey: processorCfg,
		},
		Service: ServiceYAML{
			Extensions: []string{"opamp"},
			Pipelines: map[string]any{
				"metrics": map[string]any{
					"processors": []string{processorKey},
				},
			},
		},
	}

	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal agent config: %w", err)
	}
	return string(out), nil
}

// buildProcessorBlock returns the processor YAML key and its configuration map.
func buildProcessorBlock(cfg types.AgentCollectorConfig) (string, map[string]any) {
	key := cfg.SketchType.String()

	block := map[string]any{
		"mode":            cfg.Mode.String(),
		"transmit_sketch": cfg.TransmitSketch,
		"drop_original":   cfg.DropOriginal,
	}

	// Window duration only applies in window mode.
	if cfg.Mode == types.ProcessorModeWindow && cfg.WindowDuration > 0 {
		block["window_duration"] = cfg.WindowDuration.String()
	}

	if len(cfg.AggregateBy) > 0 {
		block["aggregate_by"] = cfg.AggregateBy
	}
	if len(cfg.LabelMatchers) > 0 {
		block["label_matchers"] = cfg.LabelMatchers
	}

	// Sketch-type-specific params.
	p := cfg.SketchParams
	switch cfg.SketchType {
	case types.SketchTypeDDSketch:
		block["relative_accuracy"] = p.RelativeAccuracy
		if len(p.Quantiles) > 0 {
			block["quantiles"] = p.Quantiles
		}
	case types.SketchTypeKLL:
		block["k"] = p.K
		if len(p.Quantiles) > 0 {
			block["quantiles"] = p.Quantiles
		}
	case types.SketchTypeHLL:
		block["precision"] = p.Precision
	case types.SketchTypeCountSketch, types.SketchTypeCountMinSketch:
		block["rows"] = p.Rows
		block["cols"] = p.Cols
	}

	return key, block
}
