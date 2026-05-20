// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// mergerSink ships each TSDB block to the gorilla-head-merger's /ingest endpoint
// over HTTP instead of PUTting it to S3/MinIO. The Gorilla block-building is
// unchanged; only the delivery target differs. The merger durably WALs the block
// and later cuts+flushes the merged window block to S3 — moving the many small
// per-emit PUTs off the S3 request path.
//
// PutTSDBBlock returns nil only after the merger acks (HTTP 200), at which point
// the block is durable on the merger; the caller may then drop its copy.
type mergerSink struct {
	endpoint   string // full URL, e.g. http://merger:9099/ingest
	client     *http.Client
	maxRetries int
	backoff    time.Duration
}

func newMergerSink(cfg *Config) (*mergerSink, error) {
	if cfg.ShipEndpoint == "" {
		return nil, fmt.Errorf("gorillas3: ship_endpoint is empty")
	}
	timeout := cfg.UploadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	retries := cfg.MaxRetries
	if retries < 1 {
		retries = 1
	}
	backoff := cfg.RetryBackoff
	if backoff <= 0 {
		backoff = time.Second
	}
	return &mergerSink{
		endpoint:   cfg.ShipEndpoint,
		client:     &http.Client{Timeout: timeout},
		maxRetries: retries,
		backoff:    backoff,
	}, nil
}

// PutTSDBBlock tars the block files and POSTs them to the merger ingest endpoint.
func (m *mergerSink) PutTSDBBlock(ctx context.Context, blockULID string, files map[string][]byte) error {
	tarBytes, err := tarBlock(files)
	if err != nil {
		return fmt.Errorf("gorillas3: tar block %s: %w", blockULID, err)
	}

	url := m.endpoint
	if strings.Contains(url, "?") {
		url += "&ulid=" + blockULID
	} else {
		url += "?ulid=" + blockULID
	}

	var lastErr error
	for attempt := 0; attempt < m.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(m.backoff):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(tarBytes))
		if err != nil {
			return fmt.Errorf("gorillas3: new request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-tar")
		resp, err := m.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("merger returned status %d", resp.StatusCode)
	}
	return fmt.Errorf("gorillas3: ship block %s failed after %d attempts: %w", blockULID, m.maxRetries, lastErr)
}

func (m *mergerSink) Close() error { return nil }

// tarBlock serializes a TSDB block's files into a tar archive. Keys are the
// relative paths inside the block (e.g. "meta.json", "index", "chunks/000001").
func tarBlock(files map[string][]byte) ([]byte, error) {
	names := make([]string, 0, len(files))
	for k := range files {
		names = append(names, k)
	}
	sort.Strings(names)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range names {
		data := files[name]
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(data)),
			Typeflag: tar.TypeReg,
		}); err != nil {
			return nil, fmt.Errorf("tar header %s: %w", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			return nil, fmt.Errorf("tar write %s: %w", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
