package controlchannel

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// makePlanBody returns the JSON-encoded form of a PrecomputeConfigSet
// with the given version and a single config — enough to exercise the
// happy-path round-trip.
func makePlanBody(t *testing.T, version uint64) []byte {
	t.Helper()
	set := precompute.PrecomputeConfigSet{
		Version: version,
		Configs: []precompute.PrecomputeConfig{{
			AggID: precompute.AggId(version * 100),
		}},
	}
	body, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}
	return body
}

// TestHttpPoll_ETag304Roundtrip covers the happy path: first Poll
// returns a fresh plan with a stored ETag; second Poll with the same
// ETag receives a 304 and returns nil; a third Poll after the server
// rotates the ETag returns the new plan.
func TestHttpPoll_ETag304Roundtrip(t *testing.T) {
	t.Parallel()

	const etagV1 = `"v1"`
	const etagV2 = `"v2"`
	var serverVersion atomic.Uint64
	serverVersion.Store(1)
	var requestCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		v := serverVersion.Load()
		var currentEtag string
		switch v {
		case 1:
			currentEtag = etagV1
		default:
			currentEtag = etagV2
		}
		if r.Header.Get("If-None-Match") == currentEtag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", currentEtag)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(makePlanBody(t, v))
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:      srv.URL,
		Timeout:  2 * time.Second,
		Interval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	// First poll: fresh plan v1.
	got := ch.Poll()
	if got == nil {
		t.Fatalf("first poll: want plan, got nil (lastErr=%v)", ch.LastErr())
	}
	if got.Version != 1 {
		t.Errorf("first poll version: want 1, got %d", got.Version)
	}
	if ch.LastErr() != nil {
		t.Errorf("first poll lastErr: want nil, got %v", ch.LastErr())
	}

	// Second poll with same server version: 304 → nil, no error.
	got = ch.Poll()
	if got != nil {
		t.Errorf("second poll: want nil (304), got %+v", got)
	}
	if ch.LastErr() != nil {
		t.Errorf("304 poll lastErr: want nil, got %v", ch.LastErr())
	}

	// Server rotates: third poll picks up plan v2.
	serverVersion.Store(2)
	got = ch.Poll()
	if got == nil {
		t.Fatalf("third poll: want plan v2, got nil (lastErr=%v)", ch.LastErr())
	}
	if got.Version != 2 {
		t.Errorf("third poll version: want 2, got %d", got.Version)
	}

	if requestCount.Load() != 3 {
		t.Errorf("server requests: want 3, got %d", requestCount.Load())
	}
}

// TestHttpPoll_ServerError exercises the 500-response path: Poll
// returns nil and stashes the error on LastErr.
func TestHttpPoll_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:     srv.URL,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	if got := ch.Poll(); got != nil {
		t.Fatalf("poll: want nil on 5xx, got %+v", got)
	}
	if ch.LastErr() == nil {
		t.Fatalf("LastErr: want non-nil on 5xx, got nil")
	}
	if !strings.Contains(ch.LastErr().Error(), "500") {
		t.Errorf("LastErr should mention status 500: %v", ch.LastErr())
	}
}

// TestHttpPoll_DecodeError covers malformed JSON: 200 OK but the body
// can't be parsed.
func TestHttpPoll_DecodeError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{URL: srv.URL, Timeout: time.Second})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	if got := ch.Poll(); got != nil {
		t.Fatalf("poll: want nil on decode error, got %+v", got)
	}
	if ch.LastErr() == nil || !strings.Contains(ch.LastErr().Error(), "decode") {
		t.Fatalf("LastErr: want decode error, got %v", ch.LastErr())
	}
}

// TestHttpPoll_BearerTokenRotation writes a token, polls, rewrites the
// token, polls again — the test handler asserts both tokens were
// observed (i.e. the file is re-read per request).
func TestHttpPoll_BearerTokenRotation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokFile, []byte("token-v1"), 0o600); err != nil {
		t.Fatalf("write token v1: %v", err)
	}

	var (
		mu   sync.Mutex
		seen = map[string]bool{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		seen[auth] = true
		mu.Unlock()
		w.Header().Set("ETag", auth) // rotate ETag with token so 304 doesn't kick in
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(makePlanBody(t, 1))
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:             srv.URL,
		Timeout:         2 * time.Second,
		BearerTokenFile: tokFile,
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	if got := ch.Poll(); got == nil {
		t.Fatalf("poll v1: want plan, got nil (lastErr=%v)", ch.LastErr())
	}
	if err := os.WriteFile(tokFile, []byte("token-v2"), 0o600); err != nil {
		t.Fatalf("write token v2: %v", err)
	}
	if got := ch.Poll(); got == nil {
		t.Fatalf("poll v2: want plan, got nil (lastErr=%v)", ch.LastErr())
	}

	mu.Lock()
	defer mu.Unlock()
	if !seen["Bearer token-v1"] {
		t.Errorf("server never saw token-v1; seen=%v", seen)
	}
	if !seen["Bearer token-v2"] {
		t.Errorf("server never saw token-v2; seen=%v", seen)
	}
}

// TestHttpPoll_BearerTokenFileMissing covers the failure path: the
// token file vanishes between polls. Poll should return nil and stash
// the error on LastErr.
func TestHttpPoll_BearerTokenFileMissing(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not be reached when token file is missing")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:             srv.URL,
		Timeout:         time.Second,
		BearerTokenFile: filepath.Join(t.TempDir(), "does-not-exist"),
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	if got := ch.Poll(); got != nil {
		t.Fatalf("poll: want nil on missing token, got %+v", got)
	}
	if ch.LastErr() == nil || !strings.Contains(ch.LastErr().Error(), "bearer token") {
		t.Fatalf("LastErr: want bearer-token error, got %v", ch.LastErr())
	}
}

// TestHttpPoll_ContextCancellation wires a slow server and calls Close
// from another goroutine while a Poll is in flight. The Poll must
// return nil promptly and surface a context-cancellation error.
func TestHttpPoll_ContextCancellation(t *testing.T) {
	t.Parallel()

	released := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-released:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	defer close(released)

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:     srv.URL,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}

	done := make(chan struct{})
	var got *precompute.PrecomputeConfigSet
	go func() {
		defer close(done)
		got = ch.Poll()
	}()

	// Give the request a moment to dispatch, then close.
	time.Sleep(50 * time.Millisecond)
	if err := ch.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Poll did not return after Close")
	}
	if got != nil {
		t.Errorf("Poll: want nil on cancel, got %+v", got)
	}
	if ch.LastErr() == nil ||
		!(strings.Contains(ch.LastErr().Error(), "context canceled") ||
			errors.Is(ch.LastErr(), context.Canceled)) {
		t.Errorf("LastErr: want context-canceled flavor, got %v", ch.LastErr())
	}
}

// TestHttpPoll_AckURL exercises the Ack path: a non-empty AckURL gets
// a POST with the plan_version JSON body; an empty AckURL is a no-op.
func TestHttpPoll_AckURL(t *testing.T) {
	t.Parallel()

	type ackBody struct {
		PlanVersion uint64 `json:"plan_version"`
	}
	gotBody := make(chan ackBody, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("ack: want POST, got %s", r.Method)
		}
		raw, _ := io.ReadAll(r.Body)
		var b ackBody
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Errorf("ack: decode body: %v", err)
		}
		gotBody <- b
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:     srv.URL + "/poll",
		AckURL:  srv.URL + "/ack",
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	ch.Ack(42)
	select {
	case b := <-gotBody:
		if b.PlanVersion != 42 {
			t.Errorf("ack body: want 42, got %d", b.PlanVersion)
		}
	case <-time.After(time.Second):
		t.Fatalf("ack: server never received POST")
	}
	if ch.LastErr() != nil {
		t.Errorf("ack lastErr: want nil, got %v", ch.LastErr())
	}

	// AckURL == "" → no-op (no panic, no error, no request).
	chNoAck, err := NewHttpPollChannel(HttpPollConfig{URL: srv.URL, Timeout: time.Second})
	if err != nil {
		t.Fatalf("NewHttpPollChannel no-ack: %v", err)
	}
	defer chNoAck.Close()
	chNoAck.Ack(7) // must not panic
	if chNoAck.LastErr() != nil {
		t.Errorf("no-ack lastErr: want nil, got %v", chNoAck.LastErr())
	}
}

// TestHttpPoll_AckErrorStatus exercises the 5xx-on-ack path: Ack
// records the error on LastErr but does not panic.
func TestHttpPoll_AckErrorStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusBadGateway)
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{
		URL:     srv.URL,
		AckURL:  srv.URL,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	defer ch.Close()

	ch.Ack(99)
	if ch.LastErr() == nil || !strings.Contains(ch.LastErr().Error(), "502") {
		t.Errorf("LastErr: want 502 error, got %v", ch.LastErr())
	}
}

// TestHttpPoll_AfterClose verifies that Poll/Ack after Close are
// inert and report ErrChannelClosed via LastErr.
func TestHttpPoll_AfterClose(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server should not be reached after Close")
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{URL: srv.URL, Timeout: time.Second})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := ch.Poll(); got != nil {
		t.Errorf("Poll after Close: want nil, got %+v", got)
	}
	if !errors.Is(ch.LastErr(), ErrChannelClosed) {
		t.Errorf("LastErr after Close: want ErrChannelClosed, got %v", ch.LastErr())
	}
	ch.Ack(1)
	if !errors.Is(ch.LastErr(), ErrChannelClosed) {
		t.Errorf("LastErr after Ack post-Close: want ErrChannelClosed, got %v", ch.LastErr())
	}
	// Close is idempotent.
	if err := ch.Close(); err != nil {
		t.Errorf("second Close: want nil, got %v", err)
	}
}

// TestHttpPoll_ConstructorValidation ensures a missing URL is rejected
// at construction time.
func TestHttpPoll_ConstructorValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewHttpPollChannel(HttpPollConfig{}); err == nil {
		t.Fatalf("NewHttpPollChannel(empty): want error, got nil")
	}
}

// TestHttpPoll_ConcurrentPollClose exercises the race detector: many
// concurrent Polls overlap with a Close. Nothing should crash, and
// after Close every Poll should return nil.
func TestHttpPoll_ConcurrentPollClose(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(makePlanBody(t, 1))
	}))
	defer srv.Close()

	ch, err := NewHttpPollChannel(HttpPollConfig{URL: srv.URL, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewHttpPollChannel: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				_ = ch.Poll()
				ch.Ack(uint64(j))
			}
		}()
	}
	// Race the Close against the goroutines.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = ch.Close()
	}()
	wg.Wait()
}
