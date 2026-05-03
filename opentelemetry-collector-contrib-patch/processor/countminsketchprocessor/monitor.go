// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"
)

// Self-monitoring wiring is unchanged from the legacy processor: it
// publishes input / output / active-series counters via the OTel
// telemetry channel. Pulled into its own file so the shim's
// processor.go stays focused on the lifecycle + ConsumeMetrics path.

func (p *cmsProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, "countmin", p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("countminsketchprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor
}

func (p *cmsProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *cmsProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *cmsProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

// activeSeriesCount sums ActiveSeries across every per-metric
// Precompute the shim is currently driving. selfmonitor.New stores
// this as a callback so the OTel telemetry channel reads a fresh
// value on each scrape.
func (p *cmsProcessor) activeSeriesCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, pp := range p.pcByName {
		if s := pp.Stats(); s != nil {
			total += int64(s.ActiveSeries.Load())
		}
	}
	return total
}
