//go:build asap_otlp_receiver_sketch
// +build asap_otlp_receiver_sketch

// This file is a SKETCH of the OTel `receiver.Metrics` component that wraps the
// wire-filter. It is excluded from normal builds (build tag
// `asap_otlp_receiver_sketch`) because it depends on the collector receiver SDK
// (go.opentelemetry.io/collector/receiver, .../consumer, .../component), which
// the asap-precompute-go module does NOT import — pulling it in would bloat this
// library module's dependency graph. The COMPILING + TESTED deliverable is the
// wire-filter library (state.go + filter.go); this shell documents how the
// receiver wires it and is meant to be lifted into the collector build (OCB)
// where those SDK modules are already present.
//
// To wire it for real: drop this file (minus the build tag) into a small
// receiver module that has the collector SDK in its go.mod, register the factory
// in builder-config.yaml, and swap `otlp` -> `asap_otlp` in the metrics
// pipeline (see README.md).
package otlpfilter

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

// Config mirrors the stock otlpreceiver Config (protocols block) and adds the
// hook to the shared SampleState. In a real build, embed the stock
// otlpreceiver.Config so all transport options (gRPC/HTTP, TLS, auth) carry over
// unchanged.
type Config struct {
	// Protocols would embed otlpreceiver.Protocols here.
	// Protocols otlpreceiver.Protocols `mapstructure:"protocols"`

	// sampleState is injected by the host (asap_edge) so OnGrant's writer and
	// this receiver's reader share one in-process map. Not part of YAML; set via
	// the extension/host wiring described in README.md.
	sampleState *SampleState
}

// NewFactory returns a receiver.Factory for the `asap_otlp` receiver. It is a
// thin wrapper over the stock otlpreceiver factory: same default config, same
// transports; only the metrics handler is replaced with one that wire-filters
// before decode. (Traces/logs delegate straight to the stock handlers.)
func NewFactory(state *SampleState) receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType("asap_otlp"),
		func() component.Config { return &Config{sampleState: state} },
		receiver.WithMetrics(createMetrics(state), component.StabilityLevelDevelopment),
	)
}

func createMetrics(state *SampleState) receiver.CreateMetricsFunc {
	return func(
		ctx context.Context,
		set receiver.Settings,
		cfg component.Config,
		next consumer.Metrics,
	) (receiver.Metrics, error) {
		return &asapOTLPReceiver{
			cfg:    cfg.(*Config),
			next:   next,
			state:  state,
			pbUnma: &pmetric.ProtoUnmarshaler{},
		}, nil
	}
}

// asapOTLPReceiver is the metrics receiver. It owns (or borrows) the stock OTLP
// transport and intercepts the raw protobuf body of each export request: it runs
// FilterRequest on the wire bytes, then UnmarshalMetrics on the (thinned) result,
// then forwards to the next consumer. The dropped fraction is never decoded.
type asapOTLPReceiver struct {
	cfg    *Config
	next   consumer.Metrics
	state  *SampleState
	pbUnma *pmetric.ProtoUnmarshaler
}

func (r *asapOTLPReceiver) Start(ctx context.Context, host component.Host) error {
	// Stand up the stock OTLP gRPC/HTTP servers, but register handlers whose
	// metrics path calls handleExportBytes below instead of decoding directly.
	// (Reuse otlpreceiver's server setup; only the metrics codec changes.)
	return nil
}

func (r *asapOTLPReceiver) Shutdown(ctx context.Context) error { return nil }

// handleExportBytes is the core hook, called with the RAW marshalled
// ExportMetricsServiceRequest body (from the gRPC frame or the HTTP request
// body, before any pdata decode).
//
//	wire bytes --FilterRequest--> thinned wire bytes --UnmarshalMetrics--> pmetric.Metrics --ConsumeMetrics-->
func (r *asapOTLPReceiver) handleExportBytes(ctx context.Context, body []byte) error {
	filtered := r.state.FilterRequest(body) // wire-skip dropped warm datapoints
	md, err := r.pbUnma.UnmarshalMetrics(filtered)
	if err != nil {
		return err
	}
	return r.next.ConsumeMetrics(ctx, md)
}

// ServeHTTP sketches the HTTP/protobuf path (Content-Type
// application/x-protobuf): read the body, filter, decode, consume. The gRPC path
// is analogous — intercept the unmarshalled-from-frame bytes before pdata decode
// via a custom codec.
func (r *asapOTLPReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// body, _ := io.ReadAll(req.Body)
	// if err := r.handleExportBytes(req.Context(), body); err != nil { ... }
	_ = req
	_ = w
}
