package nopprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type nopProcessor struct {
	next consumer.Traces
}

func (p *nopProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *nopProcessor) Shutdown(context.Context) error {
	return nil
}

func (p *nopProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *nopProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return p.next.ConsumeTraces(ctx, td)
}