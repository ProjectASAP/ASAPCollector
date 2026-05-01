// swappableFilter wraps an atomic.Pointer[attribute.Filter] so the
// controller can swap the SDK View's AttributeFilter at runtime
// without rebuilding the MeterProvider or restarting the process.
//
// Why this lives here, not in `opentelemetry-go-patch/sdk/metric/`:
//
// The OTel-Go SDK's `Stream.AttributeFilter` is a function value that
// the aggregate.Builder closes over when the per-instrument
// aggregator is constructed (see
// `opentelemetry-go/sdk/metric/internal/aggregate/aggregate.go` —
// `Builder.filter`). The closure is invoked per measurement; it does
// not cache the filter result. So if the captured `attribute.Filter`
// dispatches through atomic state, a runtime swap is observable on
// the very next measurement — no SDK patch required.
//
// Caveat: the post-filter attribute set is the aggregator key. After
// a swap, attribute sets that previously hashed to one bucket may
// hash differently. Old buckets keep their measurements; new
// measurements land in new buckets. The plan-transition driver
// records a boundary so the accuracy reducer can split before/after.
package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
)

// swappableFilter holds the currently-active attribute.Filter. A nil
// inner filter means "keep every attribute" — same semantics as
// passing `Stream.AttributeFilter = nil` upstream.
type swappableFilter struct {
	p atomic.Pointer[attribute.Filter]
}

// newSwappableFilter installs `initial` as the starting filter. Pass
// nil for the keep-all default.
func newSwappableFilter(initial attribute.Filter) *swappableFilter {
	sf := &swappableFilter{}
	if initial != nil {
		sf.p.Store(&initial)
	}
	return sf
}

// Filter returns the function value to wire into
// `Stream.AttributeFilter`. The returned closure dispatches through
// atomic state, so runtime swaps are observable on the next
// measurement.
func (s *swappableFilter) Filter() attribute.Filter {
	return func(kv attribute.KeyValue) bool {
		if f := s.p.Load(); f != nil {
			return (*f)(kv)
		}
		return true
	}
}

// Swap atomically replaces the inner filter. Pass nil to clear and
// fall back to keep-all behaviour.
func (s *swappableFilter) Swap(f attribute.Filter) {
	if f == nil {
		s.p.Store(nil)
		return
	}
	s.p.Store(&f)
}

// projectionRequest is the wire format for POST /control/projection.
// `Projection` follows the same grammar as the EXPORTER_SDK_PROJECTION
// env var: comma-separated keep-list, "" for keep-all, "-" for
// drop-all. Anything else is a malformed request.
type projectionRequest struct {
	Projection string `json:"projection"`
}

// projectionResponse echoes the applied projection so the controller
// can confirm the swap without re-reading state.
type projectionResponse struct {
	Applied string `json:"applied"`
}

// installControlServer mounts the control-plane endpoints on a new
// http.ServeMux and starts a server on `addr`. Returns the listener
// goroutine's error channel for callers that want to surface bind
// failures. Caller is expected to ignore the channel for the typical
// fire-and-forget setup in main.
func installControlServer(addr string, sf *swappableFilter) chan error {
	mux := http.NewServeMux()
	mux.HandleFunc("/control/projection", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req projectionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		sf.Swap(parseProjection(req.Projection))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(projectionResponse{Applied: req.Projection})
	})
	mux.HandleFunc("/control/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- http.ListenAndServe(addr, mux)
	}()
	return errCh
}
