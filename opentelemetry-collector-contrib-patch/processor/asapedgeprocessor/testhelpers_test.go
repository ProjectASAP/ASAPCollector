// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
