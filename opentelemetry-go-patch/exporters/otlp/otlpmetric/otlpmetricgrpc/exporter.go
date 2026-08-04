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
	// seriesDictionaryEnabled gates seriesState.Annotate (and applying its
	// response-driven confirmations) entirely. Disabled via
	// WithSeriesDictionary(false) — intended for a "naive" comparison arm
	// that must pay full Attributes-on-every-DataPoint cost with none of
	// this ASAP-specific series-ID wire optimization, e.g. a
	// passthrough-all-raw-samples baseline being measured against a
	// sampled/sketched arm that DOES use it.
	seriesDictionaryEnabled bool

	shutdownOnce sync.Once
}

func newExporter(c *client, cfg oconf.Config, seriesDictionaryEnabled bool) (*Exporter, error) {
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

		temporalitySelector:     ts,
		aggregationSelector:     as,
		seriesState:             series.NewDictionary(),
		seriesDictionaryEnabled: seriesDictionaryEnabled,
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

	if e.seriesDictionaryEnabled {
		e.seriesState.Annotate(rm)
	}

	otlpRm, err := transform.ResourceMetrics(rm)
	// Best effort upload of transformable metrics.
	e.clientMu.Lock()
	resp, upErr := e.client.UploadMetrics(ctx, otlpRm)
	e.clientMu.Unlock()
	if resp != nil && e.seriesDictionaryEnabled {
		applySeriesAssignments(e.seriesState, resp)
		applyUnknownSeriesIds(e.seriesState, resp)
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
	seriesDictionaryEnabled := true
	filtered := make([]Option, 0, len(options))
	for _, o := range options {
		if sd, ok := o.(seriesDictionaryOption); ok {
			seriesDictionaryEnabled = bool(sd)
			continue
		}
		filtered = append(filtered, o)
	}

	cfg := oconf.NewGRPCConfig(asGRPCOptions(filtered)...)
	c, err := newClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newExporter(c, cfg, seriesDictionaryEnabled)
}

// seriesDictionaryOption is a standalone Option that never touches
// oconf.Config (an upstream gotmpl-generated file — see
// internal/oconf/options.go's header comment — that this exporter's ASAP
// patches otherwise avoid modifying). New() intercepts it directly.
type seriesDictionaryOption bool

func (seriesDictionaryOption) applyGRPCOption(cfg oconf.Config) oconf.Config { return cfg }

// WithSeriesDictionary controls whether the Exporter runs the ASAP-specific
// series-ID dictionary (internal/series.Dictionary): the mechanism that lets
// a repeatedly-exported series send only a collector-confirmed numeric ID
// instead of its full Attributes on the wire. Default true, matching prior
// behavior. Pass false for a "naive" comparison arm — e.g. a
// passthrough-all-raw-samples baseline — that should pay the full
// Attributes-on-every-DataPoint cost with none of this optimization, so a
// benchmark comparing it against a sampled/sketched arm attributes 100% of
// the difference to sampling/sketching rather than partly to this exporter
// wire-format trick.
func WithSeriesDictionary(enabled bool) Option {
	return seriesDictionaryOption(enabled)
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

// applyUnknownSeriesIds reads response.UnknownSeriesIds (refactor-2026-05
// addition; only present in patched proto builds) and evicts those sids
// from the local series dictionary. The next emission referencing those
// series will fall back to attribute-carrying mode and the receiver will
// re-resolve. This is the universal recovery primitive for sid-cache
// divergence (e.g., backend restart without persistence).
func applyUnknownSeriesIds(dict *series.Dictionary, resp *colmetricpb.ExportMetricsServiceResponse) {
	if resp == nil {
		return
	}
	// Reflective access keeps the patched-vs-upstream proto build switchable.
	getUnknown := reflect.ValueOf(resp).MethodByName("GetUnknownSeriesIds")
	if !getUnknown.IsValid() {
		return
	}
	res := getUnknown.Call(nil)
	if len(res) != 1 || res[0].Kind() != reflect.Slice || res[0].Len() == 0 {
		return
	}
	sids := make([]uint64, 0, res[0].Len())
	for i := 0; i < res[0].Len(); i++ {
		v := res[0].Index(i)
		if v.Kind() != reflect.Uint64 {
			continue
		}
		sids = append(sids, v.Uint())
	}
	dict.EvictByID(sids)
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
