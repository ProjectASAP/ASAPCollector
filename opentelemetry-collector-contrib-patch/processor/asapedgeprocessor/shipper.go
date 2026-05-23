// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
)

// fragmentShipper POSTs a batch of Gorilla XOR-chunk fragments to the backend
// merger /ingest endpoint. The body is the shared ASAPFRG1 binary frame
// (gorilla.EncodeFragmentBatch), gzip-compressed on the wire — no tar, no S3,
// no index build on the edge. An empty endpoint makes ship a no-op
// (build-only: fragments are drained but not shipped — used for measurement
// and warm-only deployments).
type fragmentShipper struct {
	endpoint   string
	client     *http.Client
	maxRetries int
	backoff    time.Duration
}

func newFragmentShipper(endpoint string) *fragmentShipper {
	return &fragmentShipper{
		endpoint:   endpoint,
		client:     &http.Client{Timeout: 30 * time.Second},
		maxRetries: 3,
		backoff:    time.Second,
	}
}

// noop reports whether shipping is disabled (nil shipper or empty endpoint =>
// build-only mode: fragments are drained but not shipped). Callers skip the
// spool entirely when shipping is a no-op.
func (s *fragmentShipper) noop() bool { return s == nil || s.endpoint == "" }

// encode serializes fragments to the ASAPFRG1 frame and gzips it — the exact
// on-disk spool body and wire body (re-ship is a plain POST of these bytes).
func (s *fragmentShipper) encode(frags []gorilla.Fragment) ([]byte, error) {
	return gzipBytes(gorilla.EncodeFragmentBatch(frags))
}

// ship encodes + ships a fragment batch in one call (the inline convenience
// used by tests / direct callers). Prefer encode + shipEncoded when the encoded
// body must also be spooled to disk on failure.
func (s *fragmentShipper) ship(ctx context.Context, frags []gorilla.Fragment) error {
	if s.noop() || len(frags) == 0 {
		return nil
	}
	body, err := s.encode(frags)
	if err != nil {
		return err
	}
	return s.shipEncoded(ctx, body)
}

// shipEncoded POSTs an already-gzipped ASAPFRG1 body to the merger, retrying
// with linear backoff. The body is the same bytes written to the spool, so a
// spooled file is re-shipped by reading it and calling shipEncoded directly.
func (s *fragmentShipper) shipEncoded(ctx context.Context, body []byte) error {
	if s.noop() || len(body) == 0 {
		return nil
	}
	var lastErr error
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.backoff * time.Duration(attempt)):
			}
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
		if reqErr != nil {
			return reqErr
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Content-Encoding", "gzip")
		resp, doErr := s.client.Do(req)
		if doErr != nil {
			lastErr = doErr
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("ship fragments: status %d", resp.StatusCode)
	}
	return lastErr
}

// gzipBytes gzip-compresses the ASAPFRG1 frame for the wire.
func gzipBytes(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(raw); err != nil {
		_ = gw.Close()
		return nil, fmt.Errorf("gzip fragment batch: %w", err)
	}
	if err := gw.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}
