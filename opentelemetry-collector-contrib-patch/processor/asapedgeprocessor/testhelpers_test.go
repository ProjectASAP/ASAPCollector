// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// capMetrics is the shared test sink: it captures every forwarded
// pmetric.Metrics batch so tests can assert on the processor's output.
// (Relocated here from the retired warm_sum_test.go so it stays available
// to the ~13 test files that use it.)
type capMetrics struct{ got []pmetric.Metrics }

func (c *capMetrics) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (c *capMetrics) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	c.got = append(c.got, md)
	return nil
}

// testSettings is the shared processor.Settings fixture (a no-op logger) used
// by every test that constructs a processor via newProcessor.
func testSettings() processor.Settings {
	return processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
}

// componenttestHost is a minimal component.Host for Start in tests.
type componenttestHost struct{}

func (componenttestHost) GetExtensions() map[component.ID]component.Component { return nil }
