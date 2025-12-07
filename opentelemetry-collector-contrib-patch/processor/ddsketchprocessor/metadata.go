// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import "go.opentelemetry.io/collector/component"

var (
	// Type is the component type for the processor factory.
	Type = component.MustNewType("ddsketch")
	// MetricsStability describes the maturity level for the processor.
	MetricsStability = component.StabilityLevelAlpha
)
