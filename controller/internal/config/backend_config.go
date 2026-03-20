package config

import (
	"fmt"

	"github.com/ProjectASAP/controller/internal/types"
	"gopkg.in/yaml.v3"
)

// BackendCollectorYAML is the YAML structure pushed to backend OTel collectors.
type BackendCollectorYAML struct {
	Extensions map[string]any `yaml:"extensions,omitempty"`
	Processors map[string]any `yaml:"processors"`
	Service    ServiceYAML    `yaml:"service"`
}

// GenerateBackendConfig produces YAML for the backend collector that merges
// sketches arriving from the gateway.
func GenerateBackendConfig(cfg types.BackendCollectorConfig, opampEndpoint string) (string, error) {
	processorKey := cfg.MergeSketchType.String() + "_merge"

	mergeBlock := map[string]any{
		"mode": "merge",
	}
	if len(cfg.GroupBy) > 0 {
		mergeBlock["group_by"] = cfg.GroupBy
	}

	doc := BackendCollectorYAML{
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
			processorKey: mergeBlock,
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
		return "", fmt.Errorf("marshal backend config: %w", err)
	}
	return string(out), nil
}
