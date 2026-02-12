// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillaprocessor

import "go.opentelemetry.io/collector/component"

var (
	Type             = component.MustNewType("gorilla")
	MetricsStability = component.StabilityLevelAlpha
)
