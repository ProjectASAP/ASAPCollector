// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import "go.opentelemetry.io/collector/component"

var (
	// Type is the component type for the processor factory.
	Type = component.MustNewType("countmin")

	// MetricsStability describes the maturity level for the processor.
	MetricsStability = component.StabilityLevelAlpha
)
