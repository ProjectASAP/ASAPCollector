// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
)

// enableSelfMonitoring wires the OTel-side runtime self-monitor.
// activeSeriesCount reads the live counter the runtime exposes via
// PrecomputeStats — single-Precompute model means we don't sum
// across a per-metric map (unlike the DDSketch / KLL / HLL / CMS
// shims).
func (p *countSketchProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, Type.String(), p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("countsketchprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *countSketchProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *countSketchProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *countSketchProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *countSketchProcessor) activeSeriesCount() int64 {
	if p.pc == nil {
		return 0
	}
	if s := p.pc.Stats(); s != nil {
		return s.ActiveSeries.Load()
	}
	return 0
}
