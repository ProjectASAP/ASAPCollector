package nopprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type nopProcessor struct {
	
}

func (p *nopProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *nopProcessor) Shutdown(context.Context) error {
	return nil
}

func (p *nopProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return md, nil
}