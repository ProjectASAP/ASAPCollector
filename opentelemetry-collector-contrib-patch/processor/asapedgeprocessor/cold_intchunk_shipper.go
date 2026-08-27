// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
)

// coldPartShipper is the intchunk cold-part producer: the opt-in parallel to
// the gorilla-XOR fragmentShipper. It takes the SAME per-window drained
// fragments the fragment path would ship, decodes each fragment's XOR chunk
// back to raw samples, groups them by series, re-encodes each series losslessly
// with intchunk inside a coldpart.Part (WritePart), and POSTs the complete
// serialized part to the merger's POST /ingest/coldpart. The merger validates
// (OpenPart) and stores the bytes verbatim under a content-addressed key it
// derives itself, so the shipper sends only the bytes — no client-chosen name.
//
// Behavior parity: series labels are built with gorilla.FragmentLabels, the
// SAME mapping the fragment->TSDB finalizer uses, so a series cold-archived via
// either format is queried under one identity. An empty endpoint makes the
// shipper a no-op (the cold tier still drains fragments; they are just not
// shipped as a part).
type coldPartShipper struct {
	endpoint   string
	external   map[string]string
	client     *http.Client
	maxRetries int
	backoff    time.Duration
}

func newColdPartShipper(endpoint string, external map[string]string) *coldPartShipper {
	return &coldPartShipper{
		endpoint:   endpoint,
		external:   external,
		client:     &http.Client{Timeout: 30 * time.Second},
		maxRetries: 3,
		backoff:    time.Second,
	}
}

// noop reports whether cold-part shipping is disabled (nil shipper or empty
// endpoint => drain-only).
func (s *coldPartShipper) noop() bool { return s == nil || s.endpoint == "" }

// buildPart turns a drained fragment batch into a single serialized coldpart
// Part. It returns (nil, nil) when the batch carries no decodable samples (so
// the caller skips the POST). block_start_ms/block_end_ms bound the batch with
// ABSOLUTE-ms timestamps (the minimum and maximum sample timestamp across the
// batch); the merger's manifest overlap + per-series clipping rely on these,
// and the per-series intchunk timestamps are likewise absolute ms.
func (s *coldPartShipper) buildPart(frags []gorilla.Fragment) ([]byte, error) {
	series, blockStart, blockEnd, err := fragmentsToSeries(frags, s.external)
	if err != nil {
		return nil, err
	}
	if len(series) == 0 {
		return nil, nil
	}
	return encodePart(blockStart, blockEnd, series)
}

// encodePart serializes already-grouped series into one coldpart.Part with the
// given absolute-ms block bounds. Shared by the per-flush buildPart and the
// per-block accumulator so both produce a byte-identical wire format.
func encodePart(blockStart, blockEnd int64, series []coldpart.Series) ([]byte, error) {
	var buf bytes.Buffer
	if err := coldpart.WritePart(&buf, blockStart, blockEnd, series, coldpart.Options{}); err != nil {
		return nil, fmt.Errorf("coldpart: write part: %w", err)
	}
	return buf.Bytes(), nil
}

// ship builds + POSTs a cold part for one drained fragment batch. A no-op
// shipper, an empty batch, or a batch with no samples all return nil without a
// network call.
func (s *coldPartShipper) ship(ctx context.Context, frags []gorilla.Fragment) error {
	if s.noop() || len(frags) == 0 {
		return nil
	}
	body, err := s.buildPart(frags)
	if err != nil {
		return err
	}
	return s.shipEncoded(ctx, body)
}

// shipEncoded POSTs an already-serialized cold part to the merger's
// /ingest/coldpart, retrying with linear backoff. The body is the raw bytes
// coldpart.WritePart produced; the merger validates them (OpenPart) and derives
// the object key itself.
func (s *coldPartShipper) shipEncoded(ctx context.Context, body []byte) error {
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
		resp, doErr := s.client.Do(req)
		if doErr != nil {
			lastErr = doErr
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("ship cold part: status %d", resp.StatusCode)
	}
	return lastErr
}
