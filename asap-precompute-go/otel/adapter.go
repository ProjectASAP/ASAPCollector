package otel

import (
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// Adapter is the OTel implementation of precompute.Adapter. It wraps
// the package-level Decode / Encode helpers and owns the goroutine
// that drives Precompute.Tick on a fixed period.
//
// The zero-value Adapter is not usable; construct via New.
type Adapter struct {
	cfg *AdapterConfig

	// EmitTelemetryFn, when non-nil, is invoked by EmitTelemetry to
	// publish stats to the host's metric channel. The Adapter is
	// host-neutral about how telemetry leaves the process — the
	// OTel processor wrapper in step 2.5 wires this up to either
	// the obsreport-style scope metric or a plain log line.
	emitTelemetryFn func(stats *precompute.PrecomputeStats)

	mu      sync.Mutex
	tickers []*time.Ticker
	cancels []chan struct{}
}

// New constructs an Adapter with the supplied config and an optional
// telemetry-emit hook.
func New(cfg *AdapterConfig, emit func(stats *precompute.PrecomputeStats)) *Adapter {
	return &Adapter{
		cfg:             cfg,
		emitTelemetryFn: emit,
	}
}

// Decode implements precompute.Adapter.Decode. The expected event
// type is pmetric.Metrics; any other type is rejected.
func (a *Adapter) Decode(ev any) ([]precompute.Observation, error) {
	md, ok := ev.(pmetric.Metrics)
	if !ok {
		return nil, fmt.Errorf("otel.Adapter.Decode: expected pmetric.Metrics, got %T", ev)
	}
	return Decode(md, a.cfg)
}

// Encode implements precompute.Adapter.Encode. The returned `any`
// is a pmetric.Metrics by value; callers that need typed access
// can type-assert.
func (a *Adapter) Encode(envelopes []*precompute.SketchEnvelope) (any, error) {
	return Encode(envelopes, a.cfg)
}

// ScheduleTick implements precompute.Adapter.ScheduleTick. Spawns a
// goroutine that fires cb on the requested period until cancel is
// called. Multiple ScheduleTick calls are independent (each gets its
// own goroutine and its own cancel).
func (a *Adapter) ScheduleTick(period time.Duration, cb func()) (cancel func()) {
	t := time.NewTicker(period)
	stop := make(chan struct{})

	a.mu.Lock()
	a.tickers = append(a.tickers, t)
	a.cancels = append(a.cancels, stop)
	a.mu.Unlock()

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				cb()
			}
		}
	}()
	once := sync.Once{}
	return func() {
		once.Do(func() {
			t.Stop()
			close(stop)
		})
	}
}

// EmitTelemetry implements precompute.Adapter.EmitTelemetry by
// delegating to the configured hook (or no-op if unset).
func (a *Adapter) EmitTelemetry(stats *precompute.PrecomputeStats) {
	if a == nil || a.emitTelemetryFn == nil {
		return
	}
	a.emitTelemetryFn(stats)
}

// Compile-time assertion that *Adapter implements precompute.Adapter.
var _ precompute.Adapter = (*Adapter)(nil)
