// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
)

// enableSelfMonitoring wires the OTel-side runtime self-monitor.
// activeSeriesCount sums per-Precompute ActiveSeries counters across
// every per-metric Precompute the shim is currently tracking.
func (p *ddsketchProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, Type.String(), p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("ddsketchprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *ddsketchProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *ddsketchProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *ddsketchProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *ddsketchProcessor) activeSeriesCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, pc := range p.precomputes {
		if s := pc.Stats(); s != nil {
			total += int64(s.ActiveSeries.Load())
		}
	}
	return total
}
