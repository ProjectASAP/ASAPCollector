// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import "go.opentelemetry.io/collector/component"

var (
	// Type is the unique identifier registered with the OTel collector
	// for this processor.
	Type = component.MustNewType("asap_edge")

	// MetricsStability is alpha — issue #46 edge-aggregation fusion.
	MetricsStability = component.StabilityLevelAlpha
)
