package controlchannel

// HttpPollChannel is the controller-driven, pull-based ControlChannel
// implementation. The runtime polls a configurable HTTP endpoint at a
// fixed interval; the server returns a JSON-encoded PrecomputeConfigSet
// when the plan has changed and HTTP 304 (Not Modified) when it has
// not. Plan-version acknowledgement is forwarded to a separate (and
// optional) Ack endpoint via POST.
//
// # Wire format
//
// The plan body is the standard encoding/json marshalling of
// precompute.PrecomputeConfigSet. The struct has no explicit json tags
// today, so field names follow Go-export casing (Version, Configs,
// AggID, …); a future ADR may pin a more controller-stable schema, but
// the round-trip via encoding/json is the contract for now.
//
// Plan freshness is tracked via the HTTP ETag header. The first
// successful 200 response stores the ETag; subsequent polls send it
// back as If-None-Match and treat 304 as "no change". A 200 with an
// ETag that differs from the cached value (or no ETag at all) returns
// the freshly-decoded set.
//
// # Ack
//
// Ack(planVersion) POSTs a tiny JSON body of the shape
// `{"plan_version": N}` to cfg.AckURL. When AckURL is empty, Ack is a
// no-op. Ack errors are logged via cfg.Logger but do not surface
// through the (errorless) ControlChannel interface; callers can
// inspect HttpPollChannel.LastErr() for the most recent error.
//
// # Bearer token rotation
//
// cfg.BearerTokenFile, when set, is read on every poll and ack so that
// rotated credentials are picked up without restart. A read failure is
// treated as a poll error and the request is skipped that tick.
//
// # Concurrency
//
// A single HttpPollChannel is safe to share across goroutines: Poll,
// Ack, Close, and LastErr are all guarded by an internal mutex on
// the etag / closed / lastErr fields.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// ErrChannelClosed is returned (via LastErr) after Close has been
// called and another Poll/Ack is attempted.
var ErrChannelClosed = errors.New("controlchannel: channel closed")

// HttpPollConfig configures HttpPollChannel.
type HttpPollConfig struct {
	// URL is the GET endpoint that returns the JSON-encoded
	// PrecomputeConfigSet. Required.
	URL string

	// AckURL is the POST endpoint that receives plan-version
	// acknowledgements. Optional — when empty, Ack is a no-op.
	AckURL string

	// Interval is the suggested poll cadence. HttpPollChannel
	// itself does not run a goroutine; callers are responsible for
	// invoking Poll on a ticker. Interval is exposed here so that
	// platform adapters can reach for one canonical value.
	Interval time.Duration

	// Timeout is the per-request HTTP timeout. Used to construct
	// the default *http.Client when Client is nil.
	Timeout time.Duration

	// Headers are extra static headers attached to every Poll and
	// Ack request (e.g. tenant identifier, user-agent override).
	Headers map[string]string

	// BearerTokenFile, when set, is read on every request; the
	// trimmed contents become the value of an
	// `Authorization: Bearer <token>` header. Re-reading per
	// request is intentional — credential rotation should not
	// require a restart.
	BearerTokenFile string

	// Client overrides the default *http.Client. When nil, a
	// client with Timeout is constructed.
	Client *http.Client

	// Logger receives one-line log messages for non-fatal errors.
	// Defaults to log.Default().
	Logger *log.Logger
}

// HttpPollChannel implements ControlChannel by polling an HTTP
// endpoint. See package doc for wire-format details.
type HttpPollChannel struct {
	cfg    HttpPollConfig
	client *http.Client
	logger *log.Logger

	mu       sync.Mutex
	etag     string
	closed   bool
	lastErr  error
	closeCtx context.Context
	cancel   context.CancelFunc
}

// NewHttpPollChannel constructs a new HttpPollChannel. cfg.URL is
// required; everything else has reasonable defaults.
func NewHttpPollChannel(cfg HttpPollConfig) (*HttpPollChannel, error) {
	if cfg.URL == "" {
		return nil, errors.New("controlchannel: HttpPollConfig.URL is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &HttpPollChannel{
		cfg:      cfg,
		client:   client,
		logger:   logger,
		closeCtx: ctx,
		cancel:   cancel,
	}, nil
}

// Poll issues a GET to cfg.URL and returns the freshly-decoded
// PrecomputeConfigSet when the server reports a change. Returns nil
// for "no change" (HTTP 304), for errors (the error is stashed in
// LastErr and logged), and after Close.
//
// The ControlChannel interface is errorless; non-fatal errors do not
// stop polling. Callers that want stronger guarantees can read
// LastErr after each call.
func (h *HttpPollChannel) Poll() *precompute.PrecomputeConfigSet {
	h.mu.Lock()
	if h.closed {
		h.lastErr = ErrChannelClosed
		ctx := h.closeCtx
		h.mu.Unlock()
		_ = ctx
		return nil
	}
	etag := h.etag
	ctx := h.closeCtx
	h.mu.Unlock()

	reqCtx, cancel := context.WithTimeout(ctx, h.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, h.cfg.URL, nil)
	if err != nil {
		h.recordErr(fmt.Errorf("controlchannel: build request: %w", err))
		return nil
	}
	req.Header.Set("Accept", "application/json")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if err := h.applyAuth(req); err != nil {
		h.recordErr(err)
		return nil
	}
	for k, v := range h.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.recordErr(fmt.Errorf("controlchannel: GET %s: %w", h.cfg.URL, err))
		return nil
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotModified:
		h.clearErr()
		return nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			h.recordErr(fmt.Errorf("controlchannel: read body: %w", err))
			return nil
		}
		var set precompute.PrecomputeConfigSet
		if err := json.Unmarshal(body, &set); err != nil {
			h.recordErr(fmt.Errorf("controlchannel: decode body: %w", err))
			return nil
		}
		newEtag := resp.Header.Get("ETag")
		h.mu.Lock()
		h.etag = newEtag
		h.lastErr = nil
		h.mu.Unlock()
		return &set
	default:
		// Drain a small slice for diagnostics, then error out.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		h.recordErr(fmt.Errorf("controlchannel: GET %s: status %d: %s",
			h.cfg.URL, resp.StatusCode, strings.TrimSpace(string(snippet))))
		return nil
	}
}

// Ack POSTs the plan version to cfg.AckURL. When AckURL is empty Ack
// is a no-op. Errors are logged and recorded on LastErr; the (errorless)
// ControlChannel interface does not surface them directly.
func (h *HttpPollChannel) Ack(planVersion uint64) {
	h.mu.Lock()
	if h.closed {
		h.lastErr = ErrChannelClosed
		h.mu.Unlock()
		return
	}
	ctx := h.closeCtx
	h.mu.Unlock()

	if h.cfg.AckURL == "" {
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, h.cfg.Timeout)
	defer cancel()

	body, err := json.Marshal(struct {
		PlanVersion uint64 `json:"plan_version"`
	}{PlanVersion: planVersion})
	if err != nil {
		// Should be impossible for a uint64 wrapper, but be safe.
		h.recordErr(fmt.Errorf("controlchannel: marshal ack: %w", err))
		return
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, h.cfg.AckURL, bytes.NewReader(body))
	if err != nil {
		h.recordErr(fmt.Errorf("controlchannel: build ack: %w", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if err := h.applyAuth(req); err != nil {
		h.recordErr(err)
		return
	}
	for k, v := range h.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.recordErr(fmt.Errorf("controlchannel: POST %s: %w", h.cfg.AckURL, err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		h.recordErr(fmt.Errorf("controlchannel: POST %s: status %d: %s",
			h.cfg.AckURL, resp.StatusCode, strings.TrimSpace(string(snippet))))
		return
	}
	h.clearErr()
}

// Close stops accepting new Poll/Ack calls and cancels any in-flight
// request context. Subsequent Poll/Ack calls record ErrChannelClosed
// on LastErr and return without I/O. Close is idempotent.
func (h *HttpPollChannel) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	cancel := h.cancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// LastErr returns the most recent non-nil error observed by Poll/Ack.
// Reading LastErr does not clear it; a subsequent successful call
// resets it to nil.
func (h *HttpPollChannel) LastErr() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastErr
}

func (h *HttpPollChannel) recordErr(err error) {
	if err == nil {
		return
	}
	h.mu.Lock()
	h.lastErr = err
	h.mu.Unlock()
	h.logger.Printf("%v", err)
}

func (h *HttpPollChannel) clearErr() {
	h.mu.Lock()
	h.lastErr = nil
	h.mu.Unlock()
}

func (h *HttpPollChannel) applyAuth(req *http.Request) error {
	if h.cfg.BearerTokenFile == "" {
		return nil
	}
	raw, err := os.ReadFile(h.cfg.BearerTokenFile)
	if err != nil {
		return fmt.Errorf("controlchannel: read bearer token file %q: %w",
			h.cfg.BearerTokenFile, err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return fmt.Errorf("controlchannel: bearer token file %q is empty",
			h.cfg.BearerTokenFile)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

// Compile-time check.
var _ ControlChannel = (*HttpPollChannel)(nil)
