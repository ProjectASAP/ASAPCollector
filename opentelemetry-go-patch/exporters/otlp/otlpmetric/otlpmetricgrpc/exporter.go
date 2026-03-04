// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlpmetricgrpc // import "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc/internal/oconf"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc/internal/series"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc/internal/transform"
	"go.opentelemetry.io/otel/internal/global"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Exporter is a OpenTelemetry metric Exporter using gRPC.
type Exporter struct {
	// Ensure synchronous access to the client across all functionality.
	clientMu sync.Mutex
	client   interface {
		UploadMetrics(context.Context, *metricpb.ResourceMetrics) (*colmetricpb.ExportMetricsServiceResponse, error)
		Shutdown(context.Context) error
	}

	temporalitySelector metric.TemporalitySelector
	aggregationSelector metric.AggregationSelector

	seriesState *series.Dictionary

	shutdownOnce sync.Once
}

func newExporter(c *client, cfg oconf.Config) (*Exporter, error) {
	ts := cfg.Metrics.TemporalitySelector
	if ts == nil {
		ts = func(metric.InstrumentKind) metricdata.Temporality {
			return metricdata.CumulativeTemporality
		}
	}

	as := cfg.Metrics.AggregationSelector
	if as == nil {
		as = metric.DefaultAggregationSelector
	}

	return &Exporter{
		client: c,

		temporalitySelector: ts,
		aggregationSelector: as,
		seriesState:         series.NewDictionary(),
	}, nil
}

// Temporality returns the Temporality to use for an instrument kind.
func (e *Exporter) Temporality(k metric.InstrumentKind) metricdata.Temporality {
	return e.temporalitySelector(k)
}

// Aggregation returns the Aggregation to use for an instrument kind.
func (e *Exporter) Aggregation(k metric.InstrumentKind) metric.Aggregation {
	return e.aggregationSelector(k)
}

// Export transforms and transmits metric data to an OTLP receiver.
//
// This method returns an error if called after Shutdown.
// This method returns an error if the method is canceled by the passed context.
func (e *Exporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	defer global.Debug("OTLP/gRPC exporter export", "Data", rm)

	e.seriesState.Annotate(rm)

	otlpRm, err := transform.ResourceMetrics(rm)
	// Best effort upload of transformable metrics.
	e.clientMu.Lock()
	resp, upErr := e.client.UploadMetrics(ctx, otlpRm)
	e.clientMu.Unlock()
	if resp != nil {
		applySeriesAssignments(e.seriesState, resp)
	}
	if upErr != nil {
		if err == nil {
			return fmt.Errorf("failed to upload metrics: %w", upErr)
		}
		// Merge the two errors.
		return fmt.Errorf("failed to upload incomplete metrics (%w): %w", err, upErr)
	}
	return err
}

// ForceFlush flushes any metric data held by an exporter.
//
// This method returns an error if called after Shutdown.
// This method returns an error if the method is canceled by the passed context.
//
// This method is safe to call concurrently.
func (*Exporter) ForceFlush(ctx context.Context) error {
	// The exporter and client hold no state, nothing to flush.
	return ctx.Err()
}

// Shutdown flushes all metric data held by an exporter and releases any held
// computational resources.
//
// This method returns an error if called after Shutdown.
// This method returns an error if the method is canceled by the passed context.
//
// This method is safe to call concurrently.
func (e *Exporter) Shutdown(ctx context.Context) error {
	err := errShutdown
	e.shutdownOnce.Do(func() {
		e.clientMu.Lock()
		client := e.client
		e.client = shutdownClient{}
		e.clientMu.Unlock()
		err = client.Shutdown(ctx)
	})
	return err
}

var errShutdown = errors.New("gRPC exporter is shutdown")

type shutdownClient struct{}

func (shutdownClient) err(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errShutdown
}

func (c shutdownClient) UploadMetrics(ctx context.Context, _ *metricpb.ResourceMetrics) (*colmetricpb.ExportMetricsServiceResponse, error) {
	return nil, c.err(ctx)
}

func (c shutdownClient) Shutdown(ctx context.Context) error {
	return c.err(ctx)
}

// MarshalLog returns logging data about the Exporter.
func (*Exporter) MarshalLog() any {
	return struct{ Type string }{Type: "OTLP/gRPC"}
}

// New returns an OpenTelemetry metric Exporter. The Exporter can be used with
// a PeriodicReader to export OpenTelemetry metric data to an OTLP receiving
// endpoint using gRPC.
//
// If an already established gRPC ClientConn is not passed in options using
// WithGRPCConn, a connection to the OTLP endpoint will be established based
// on options. If a connection cannot be establishes in the lifetime of ctx,
// an error will be returned.
func New(ctx context.Context, options ...Option) (*Exporter, error) {
	cfg := oconf.NewGRPCConfig(asGRPCOptions(options)...)
	c, err := newClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newExporter(c, cfg)
}

func applySeriesAssignments(dict *series.Dictionary, resp *colmetricpb.ExportMetricsServiceResponse) {
	if resp == nil {
		return
	}

	// Keep compatibility with both patched and upstream OTLP proto responses:
	// series_assignments only exists in patched proto builds.
	getSeries := reflect.ValueOf(resp).MethodByName("GetSeriesAssignments")
	if !getSeries.IsValid() {
		return
	}
	res := getSeries.Call(nil)
	if len(res) != 1 || res[0].Kind() != reflect.Slice || res[0].Len() == 0 {
		return
	}

	assignments := make([]series.Assignment, 0, res[0].Len())
	for i := 0; i < res[0].Len(); i++ {
		asg := res[0].Index(i)
		if asg.Kind() == reflect.Pointer && asg.IsNil() {
			continue
		}
		assignments = append(assignments, series.Assignment{
			ResourceKey:           callStringMethod(asg, "GetResourceKey"),
			ScopeKey:              callStringMethod(asg, "GetScopeKey"),
			MetricName:            callStringMethod(asg, "GetMetricName"),
			MetricType:            callStringMethod(asg, "GetMetricType"),
			AttributesFingerprint: string(callBytesMethod(asg, "GetAttributesFingerprint")),
			SeriesID:              callUint64Method(asg, "GetSeriesId"),
		})
	}
	dict.Apply(assignments)
}

func callStringMethod(v reflect.Value, name string) string {
	m := v.MethodByName(name)
	if !m.IsValid() {
		return ""
	}
	res := m.Call(nil)
	if len(res) != 1 || res[0].Kind() != reflect.String {
		return ""
	}
	return res[0].String()
}

func callBytesMethod(v reflect.Value, name string) []byte {
	m := v.MethodByName(name)
	if !m.IsValid() {
		return nil
	}
	res := m.Call(nil)
	if len(res) != 1 {
		return nil
	}
	b, ok := res[0].Interface().([]byte)
	if !ok {
		return nil
	}
	return b
}

func callUint64Method(v reflect.Value, name string) uint64 {
	m := v.MethodByName(name)
	if !m.IsValid() {
		return 0
	}
	res := m.Call(nil)
	if len(res) != 1 || res[0].Kind() != reflect.Uint64 {
		return 0
	}
	return res[0].Uint()
}
