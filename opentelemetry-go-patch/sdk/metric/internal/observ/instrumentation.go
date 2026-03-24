// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package observ provides experimental observability instrumentation for the
// metric reader.
package observ // import "go.opentelemetry.io/otel/sdk/metric/internal/observ"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk"
	"go.opentelemetry.io/otel/sdk/internal/x"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/semconv/v1.39.0/otelconv"
)

const (
	// ScopeName is the unique name of the meter used for instrumentation.
	ScopeName = "go.opentelemetry.io/otel/sdk/metric/internal/observ"

	// SchemaURL is the schema URL of the metrics produced by this
	// instrumentation.
	SchemaURL = semconv.SchemaURL
)

var (
	measureAttrsPool = &sync.Pool{
		New: func() any {
			const n = 1 + // component.name
				1 + // component.type
				1 // error.type
			s := make([]attribute.KeyValue, 0, n)
			// Return a pointer to a slice instead of a slice itself
			// to avoid allocations on every call.
			return &s
		},
	}

	recordOptPool = &sync.Pool{
		New: func() any {
			const n = 1 // WithAttributeSet
			o := make([]metric.RecordOption, 0, n)
			return &o
		},
	}
)

func get[T any](p *sync.Pool) *[]T { return p.Get().(*[]T) }

func put[T any](p *sync.Pool, s *[]T) {
	*s = (*s)[:0] // Reset.
	p.Put(s)
}

// ComponentName returns the component name for the metric reader with the
// provided ComponentType and ID.
func ComponentName(componentType string, id int64) string {
	return fmt.Sprintf("%s/%d", componentType, id)
}

// Instrumentation is experimental instrumentation for the metric reader.
type Instrumentation struct {
	colDuration metric.Float64Histogram
	expDuration metric.Float64Histogram
	expBytes    metric.Int64Counter
	expBps      metric.Float64Gauge
	cpuUser     metric.Float64Gauge
	cpuSys      metric.Float64Gauge
	rssMemory   metric.Int64Gauge
	heapAlloc   metric.Int64Gauge
	heapSys     metric.Int64Gauge
	goroutines  metric.Int64Gauge

	attrs   []attribute.KeyValue
	attrSet attribute.Set
	recOpt  metric.RecordOption
	addOpt  metric.AddOption
}

// NewInstrumentation returns instrumentation for metric reader with the provided component
// type (such as periodic and manual metric reader) and ID. It uses the global
// MeterProvider to create the instrumentation.
//
// The id should be the unique metric reader instance ID. It is used
// to set the "component.name" attribute.
//
// If the experimental observability is disabled, nil is returned.
func NewInstrumentation(componentType string, id int64) (*Instrumentation, error) {
	if !x.Observability.Enabled() {
		return nil, nil
	}

	return newInstrumentation(componentType, id)
}

// NewConfiguredInstrumentation returns instrumentation for a metric reader
// with an explicit enable switch controlled by caller configuration.
func NewConfiguredInstrumentation(enabled bool, componentType string, id int64) (*Instrumentation, error) {
	if !enabled {
		return nil, nil
	}

	return newInstrumentation(componentType, id)
}

func newInstrumentation(componentType string, id int64) (*Instrumentation, error) {
	i := &Instrumentation{
		attrs: []attribute.KeyValue{
			semconv.OTelComponentName(ComponentName(componentType, id)),
			semconv.OTelComponentTypeKey.String(componentType),
		},
	}

	r := attribute.NewSet(i.attrs...)
	i.attrSet = r
	i.recOpt = metric.WithAttributeSet(r)
	i.addOpt = metric.WithAttributeSet(r)

	meter := otel.GetMeterProvider().Meter(
		ScopeName,
		metric.WithInstrumentationVersion(sdk.Version()),
		metric.WithSchemaURL(SchemaURL),
	)

	colDuration, err := otelconv.NewSDKMetricReaderCollectionDuration(meter)
	if err != nil {
		err = fmt.Errorf("failed to create collection duration metric: %w", err)
	}
	i.colDuration = colDuration.Inst()

	expDuration, e := meter.Float64Histogram(
		"otel.sdk.metric.reader.export.duration",
		metric.WithDescription("The duration of metric reader export operations."),
		metric.WithUnit("s"),
	)
	err = errors.Join(err, wrapErr("export duration metric", e))
	i.expDuration = expDuration

	expBytes, e := meter.Int64Counter(
		"otel.sdk.metric.reader.export.bytes",
		metric.WithDescription("Estimated bytes exported by the metric reader."),
		metric.WithUnit("By"),
	)
	err = errors.Join(err, wrapErr("export bytes metric", e))
	i.expBytes = expBytes

	expBps, e := meter.Float64Gauge(
		"otel.sdk.metric.reader.export.bandwidth",
		metric.WithDescription("Estimated export bandwidth of the metric reader."),
		metric.WithUnit("By/s"),
	)
	err = errors.Join(err, wrapErr("export bandwidth metric", e))
	i.expBps = expBps

	cpuUser, e := meter.Float64Gauge(
		"otel.sdk.metric.reader.process.cpu.user_time",
		metric.WithDescription("Process user CPU time observed during export."),
		metric.WithUnit("s"),
	)
	err = errors.Join(err, wrapErr("process user cpu metric", e))
	i.cpuUser = cpuUser

	cpuSys, e := meter.Float64Gauge(
		"otel.sdk.metric.reader.process.cpu.system_time",
		metric.WithDescription("Process system CPU time observed during export."),
		metric.WithUnit("s"),
	)
	err = errors.Join(err, wrapErr("process system cpu metric", e))
	i.cpuSys = cpuSys

	rssMemory, e := meter.Int64Gauge(
		"otel.sdk.metric.reader.process.memory.rss",
		metric.WithDescription("Process resident memory observed during export."),
		metric.WithUnit("By"),
	)
	err = errors.Join(err, wrapErr("process rss metric", e))
	i.rssMemory = rssMemory

	heapAlloc, e := meter.Int64Gauge(
		"otel.sdk.metric.reader.process.memory.heap_alloc",
		metric.WithDescription("Heap allocation observed during export."),
		metric.WithUnit("By"),
	)
	err = errors.Join(err, wrapErr("heap alloc metric", e))
	i.heapAlloc = heapAlloc

	heapSys, e := meter.Int64Gauge(
		"otel.sdk.metric.reader.process.memory.heap_sys",
		metric.WithDescription("Heap memory obtained from the system observed during export."),
		metric.WithUnit("By"),
	)
	err = errors.Join(err, wrapErr("heap sys metric", e))
	i.heapSys = heapSys

	goroutines, e := meter.Int64Gauge(
		"otel.sdk.metric.reader.runtime.goroutines",
		metric.WithDescription("Goroutine count observed during export."),
		metric.WithUnit("{goroutine}"),
	)
	err = errors.Join(err, wrapErr("goroutines metric", e))
	i.goroutines = goroutines

	return i, err
}

// CollectMetrics instruments the collect method of metric reader. It returns an
// [CollectOp] that must have its [CollectOp.End] method called when the
// collection end.
func (i *Instrumentation) CollectMetrics(ctx context.Context) CollectOp {
	start := time.Now()

	return CollectOp{
		ctx:   ctx,
		start: start,
		inst:  i,
	}
}

// ExportMetrics instruments the export method of metric reader. It returns an
// [ExportOp] that must have its [ExportOp.End] method called when the export ends.
func (i *Instrumentation) ExportMetrics(ctx context.Context, estimatedBytes int64) ExportOp {
	return ExportOp{
		ctx:            ctx,
		start:          time.Now(),
		inst:           i,
		estimatedBytes: estimatedBytes,
	}
}

// CollectOp tracks the collect operation being observed by
// [Instrumentation.CollectMetrics].
type CollectOp struct {
	ctx   context.Context
	start time.Time

	inst *Instrumentation
}

// End completes the observation of the operation being observed by a call to
// [Instrumentation.CollectMetrics].
//
// Any error that is encountered is provided as err.
func (e CollectOp) End(err error) {
	if e.inst == nil || e.inst.colDuration == nil {
		return
	}

	recOpt := get[metric.RecordOption](recordOptPool)
	defer put(recordOptPool, recOpt)
	*recOpt = append(*recOpt, e.inst.recordOption(err))

	d := time.Since(e.start).Seconds()
	e.inst.colDuration.Record(e.ctx, d, *recOpt...)
}

// ExportOp tracks the export operation being observed by
// [Instrumentation.ExportMetrics].
type ExportOp struct {
	ctx   context.Context
	start time.Time

	inst           *Instrumentation
	estimatedBytes int64
}

// End completes the observation of the export operation.
func (e ExportOp) End(err error) {
	if e.inst == nil {
		return
	}

	recOpt := get[metric.RecordOption](recordOptPool)
	defer put(recordOptPool, recOpt)
	*recOpt = append(*recOpt, e.inst.recordOption(err))

	addOpt := e.inst.addOption(err)
	d := time.Since(e.start).Seconds()
	if d > 0 && e.inst.expDuration != nil {
		e.inst.expDuration.Record(e.ctx, d, *recOpt...)
	}
	if e.estimatedBytes > 0 && e.inst.expBytes != nil {
		e.inst.expBytes.Add(e.ctx, e.estimatedBytes, addOpt)
		if d > 0 && e.inst.expBps != nil {
			e.inst.expBps.Record(e.ctx, float64(e.estimatedBytes)/d, *recOpt...)
		}
	}

	sample := sampleRuntime()
	if e.inst.cpuUser != nil {
		e.inst.cpuUser.Record(e.ctx, sample.userCPUSeconds, *recOpt...)
	}
	if e.inst.cpuSys != nil {
		e.inst.cpuSys.Record(e.ctx, sample.sysCPUSeconds, *recOpt...)
	}
	if e.inst.rssMemory != nil {
		e.inst.rssMemory.Record(e.ctx, sample.rssBytes, *recOpt...)
	}
	if e.inst.heapAlloc != nil {
		e.inst.heapAlloc.Record(e.ctx, sample.heapAllocBytes, *recOpt...)
	}
	if e.inst.heapSys != nil {
		e.inst.heapSys.Record(e.ctx, sample.heapSysBytes, *recOpt...)
	}
	if e.inst.goroutines != nil {
		e.inst.goroutines.Record(e.ctx, sample.goroutines, *recOpt...)
	}
}

// recordOption returns a RecordOption with attributes representing the
// outcome of the collection being recorded.
//
// If err is nil, the default recOpt of the Instrumentation is returned.
//
// Otherwise, a new RecordOption is returned with the base attributes of the
// Instrumentation plus the error.type attribute set to the type of the error.
func (i *Instrumentation) recordOption(err error) metric.RecordOption {
	if err == nil {
		return i.recOpt
	}

	attrs := get[attribute.KeyValue](measureAttrsPool)
	defer put(measureAttrsPool, attrs)
	*attrs = append(*attrs, i.attrs...)
	*attrs = append(*attrs, semconv.ErrorType(err))

	// Do not inefficiently make a copy of attrs by using WithAttributes
	// instead of WithAttributeSet.
	return metric.WithAttributeSet(attribute.NewSet(*attrs...))
}

func (i *Instrumentation) addOption(err error) metric.AddOption {
	if err == nil {
		return i.addOpt
	}
	return metric.WithAttributeSet(i.attributeSet(err))
}

func (i *Instrumentation) attributeSet(err error) attribute.Set {
	if err == nil {
		return i.attrSet
	}

	attrs := get[attribute.KeyValue](measureAttrsPool)
	defer put(measureAttrsPool, attrs)
	*attrs = append(*attrs, i.attrs...)
	*attrs = append(*attrs, semconv.ErrorType(err))
	return attribute.NewSet(*attrs...)
}

func wrapErr(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to create %s: %w", name, err)
}
