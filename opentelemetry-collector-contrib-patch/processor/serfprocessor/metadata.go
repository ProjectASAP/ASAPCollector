// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfprocessor

import "go.opentelemetry.io/collector/component"

var (
	Type             = component.MustNewType("serf")
	MetricsStability = component.StabilityLevelAlpha
)
