package countsketchprocessor

import "go.opentelemetry.io/collector/component"

var (
	Type = component.MustNewType("countsketch")
	MetricsStability = component.StabilityLevelDevelopment
)