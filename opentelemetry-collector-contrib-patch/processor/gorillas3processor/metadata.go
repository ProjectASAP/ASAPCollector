// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import "go.opentelemetry.io/collector/component"

var (
	// Type is the unique identifier registered with the OTel collector
	// for this processor.
	Type = component.MustNewType("gorillas3")

	// MetricsStability is alpha — Phase 2 of the cold-engine integration.
	MetricsStability = component.StabilityLevelAlpha
)
