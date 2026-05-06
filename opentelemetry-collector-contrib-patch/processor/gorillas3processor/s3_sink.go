// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// chunkSink is the S3 abstraction the processor calls into. Tests can
// substitute an in-memory implementation. Methods must be safe for
// concurrent use; the processor serializes per-window writes but the
// sink may also be invoked from Shutdown.
type chunkSink interface {
	// PutChunk writes a single GORILLA1 block under the supplied key.
	// keyHints carries the metric name + window bounds so the sink
	// can also (re-)write the per-hour index.json.
	PutChunk(ctx context.Context, key string, data []byte, hints chunkHints) error

	// Close releases any client resources.
	Close() error
}

// chunkHints carries the index metadata for a single chunk.
type chunkHints struct {
	Tenant      string
	MetricName  string
	StartTSNano int64
	EndTSNano   int64
	SeriesCount int
	PointCount  int
	SizeBytes   int
	IndexPrefix string // hour-bucket prefix used to derive "<prefix>index.json"
}

// indexEntry is one row inside a per-hour index.json.
type indexEntry struct {
	Object      string `json:"object"`
	StartTSNano int64  `json:"start_ts_nano"`
	EndTSNano   int64  `json:"end_ts_nano"`
	SeriesCount int    `json:"series_count"`
	PointCount  int    `json:"point_count"`
	SizeBytes   int    `json:"size_bytes"`
	WrittenAt   int64  `json:"written_at_unix_nano"`
}

type indexFile struct {
	Version int          `json:"version"`
	Tenant  string       `json:"tenant"`
	Metric  string       `json:"metric"`
	Entries []indexEntry `json:"entries"`
}

// s3Sink is the production aws-sdk-go-v1 implementation. We pick the
// v1 SDK to stay consistent with the existing telegraf gorilla_s3
// output and the gorillaprocessor sibling — same dependency surface,
// same retry/multipart logic patterns.
type s3Sink struct {
	cfg          *Config
	client       *s3.S3
	timeout      time.Duration
	backoff      time.Duration
	maxRetries   int
	indexMu      sync.Mutex
	indexCache   map[string]*indexFile
	spoolDir     string
	failureCount uint64
}

func newS3Sink(cfg *Config) (*s3Sink, error) {
	awsCfg := &aws.Config{
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(true), // MinIO requires path-style
	}
	if cfg.Endpoint != "" {
		awsCfg.Endpoint = aws.String(cfg.Endpoint)
		awsCfg.DisableSSL = aws.Bool(!cfg.UseSSL)
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey, "")
	}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, fmt.Errorf("gorillas3: create AWS session: %w", err)
	}
	return &s3Sink{
		cfg:        cfg,
		client:     s3.New(sess),
		timeout:    cfg.UploadTimeout,
		backoff:    cfg.RetryBackoff,
		maxRetries: cfg.MaxRetries,
		indexCache: make(map[string]*indexFile),
		spoolDir:   cfg.LocalSpoolDir,
	}, nil
}

// PutChunk uploads the chunk and updates the index.
func (s *s3Sink) PutChunk(ctx context.Context, key string, data []byte, hints chunkHints) error {
	if err := s.putWithRetry(ctx, key, data); err != nil {
		if s.spoolDir != "" {
			if spErr := writeLocalSpool(s.spoolDir, key, data); spErr != nil {
				return fmt.Errorf("s3 put failed (%w) and spool failed (%v)", err, spErr)
			}
			// Spool succeeded; surface the original error so the
			// caller can mark a failure but data is not lost.
			return fmt.Errorf("s3 put failed, spooled to %s: %w", s.spoolDir, err)
		}
		return err
	}
	return s.updateIndex(ctx, key, hints)
}

func (s *s3Sink) Close() error { return nil }

func (s *s3Sink) putWithRetry(ctx context.Context, key string, data []byte) error {
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, s.timeout)
		err := s.putOnce(cctx, key, data)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < s.maxRetries {
			time.Sleep(time.Duration(attempt+1) * s.backoff)
		}
	}
	return fmt.Errorf("s3 put failed after %d retries: %w", s.maxRetries, lastErr)
}

func (s *s3Sink) putOnce(ctx context.Context, key string, data []byte) error {
	in := &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	}
	_, err := s.client.PutObjectWithContext(ctx, in)
	if err != nil {
		return fmt.Errorf("PutObject: %w", err)
	}
	return nil
}

// updateIndex performs a read-modify-write on the per-(metric, hour)
// index.json. Concurrent writers from a single processor are serialized
// via indexMu; concurrent writers across processes are NOT a target
// guarantee for Phase 2 — the index is best-effort and a sidecar
// compactor regenerates it from the chunk listing.
func (s *s3Sink) updateIndex(ctx context.Context, chunkKey string, h chunkHints) error {
	indexKey := h.IndexPrefix + "index.json"
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	idx, ok := s.indexCache[indexKey]
	if !ok {
		// Try to fetch existing index. 404 => start fresh.
		idx = s.fetchIndex(ctx, indexKey, h)
		s.indexCache[indexKey] = idx
	}
	idx.Entries = append(idx.Entries, indexEntry{
		Object:      path.Base(chunkKey),
		StartTSNano: h.StartTSNano,
		EndTSNano:   h.EndTSNano,
		SeriesCount: h.SeriesCount,
		PointCount:  h.PointCount,
		SizeBytes:   h.SizeBytes,
		WrittenAt:   time.Now().UnixNano(),
	})
	body, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err = s.client.PutObjectWithContext(cctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(indexKey),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("put index: %w", err)
	}
	return nil
}

// fetchIndex reads any pre-existing index.json. Best-effort: returns an
// empty file if not found / unreadable.
func (s *s3Sink) fetchIndex(ctx context.Context, key string, h chunkHints) *indexFile {
	empty := &indexFile{Version: 1, Tenant: h.Tenant, Metric: h.MetricName}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	out, err := s.client.GetObjectWithContext(cctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return empty
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return empty
	}
	var idx indexFile
	if err := json.Unmarshal(body, &idx); err != nil {
		return empty
	}
	if idx.Version == 0 {
		idx.Version = 1
	}
	if idx.Tenant == "" {
		idx.Tenant = h.Tenant
	}
	if idx.Metric == "" {
		idx.Metric = h.MetricName
	}
	return &idx
}

// writeLocalSpool drops the chunk to a local directory mirroring the S3
// key layout. Callers use this only when S3 PutObject fails.
func writeLocalSpool(spoolDir, key string, data []byte) error {
	clean := filepath.Clean(key)
	localPath := filepath.Join(spoolDir, clean)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("mkdir spool: %w", err)
	}
	tmp := localPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write spool tmp: %w", err)
	}
	if err := os.Rename(tmp, localPath); err != nil {
		return fmt.Errorf("rename spool: %w", err)
	}
	return nil
}

// renderPrefix substitutes the {tenant} / {metric} / {YYYY..} tokens.
// blockTime is normalized to UTC.
func renderPrefix(template, tenant, metric string, blockTime time.Time) string {
	t := blockTime.UTC()
	repl := strings.NewReplacer(
		"{tenant}", tenant,
		"{metric}", sanitizeMetric(metric),
		"{YYYY}", t.Format("2006"),
		"{MM}", t.Format("01"),
		"{DD}", t.Format("02"),
		"{HH}", t.Format("15"),
	)
	out := repl.Replace(template)
	out = strings.ReplaceAll(out, "\\", "/")
	out = strings.TrimPrefix(out, "/")
	if !strings.HasSuffix(out, "/") {
		out += "/"
	}
	return out
}

// sanitizeMetric strips characters that are unfriendly to S3 keys.
func sanitizeMetric(m string) string {
	if m == "" {
		return "unknown"
	}
	repl := strings.NewReplacer(" ", "_", "/", "_", "\\", "_")
	return repl.Replace(m)
}

// buildObjectKey returns the full S3 key for a given chunk index.
func buildObjectKey(prefix string, blockTime time.Time, idx int) string {
	base := fmt.Sprintf("part-%d-%06d.gor", blockTime.UTC().Unix(), idx)
	return prefix + base
}
