// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	// PutTSDBBlock writes one Prometheus TSDB block to the
	// configured TSDB bucket. The map keys are the relative
	// object keys inside the block — typically `chunks/000001`,
	// `index`, `meta.json` — the sink prepends the block ULID
	// dir prefix. Implementations write all files atomically
	// from the caller's POV; readers SHOULD see a complete block
	// once the call returns.
	PutTSDBBlock(ctx context.Context, blockULID string, files map[string][]byte) error

	// Close releases any client resources.
	Close() error
}

// s3Sink is the production aws-sdk-go-v1 implementation. We pick the
// v1 SDK to stay consistent with the existing telegraf gorilla_s3
// output — same dependency surface, same retry/multipart logic patterns.
type s3Sink struct {
	cfg          *Config
	client       *s3.S3
	timeout      time.Duration
	backoff      time.Duration
	maxRetries   int
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
		spoolDir:   cfg.LocalSpoolDir,
	}, nil
}

// PutTSDBBlock uploads each file in the supplied block to the TSDB
// bucket, prefixing keys with `<blockULID>/`. mvp/step2.1: writes
// `meta.json` LAST so a Thanos store-gateway scanning the bucket
// while the upload is in flight does not pick up a half-written
// block (Thanos uses meta.json's existence as the readiness signal).
func (s *s3Sink) PutTSDBBlock(ctx context.Context, blockULID string, files map[string][]byte) error {
	if len(files) == 0 {
		return nil
	}
	// Config::Validate ensures TSDBBucket is non-empty for non-agent
	// roles; agent roles never enter this TSDB upload path.
	tsdbBucket := s.cfg.TSDBBucket
	// Order: everything except meta.json first, then meta.json. We
	// look for the meta.json key by suffix to be tolerant of either
	// the "<ulid>/meta.json" full key form or the "meta.json" relative
	// form callers might pass.
	var metaKey string
	otherKeys := make([]string, 0, len(files))
	for k := range files {
		if filepath.Base(k) == "meta.json" {
			metaKey = k
			continue
		}
		otherKeys = append(otherKeys, k)
	}
	for _, k := range otherKeys {
		if err := s.putTSDBObject(ctx, tsdbBucket, blockULID, k, files[k]); err != nil {
			return err
		}
	}
	if metaKey != "" {
		if err := s.putTSDBObject(ctx, tsdbBucket, blockULID, metaKey, files[metaKey]); err != nil {
			return err
		}
	}
	return nil
}

// putTSDBObject is the per-file upload path for TSDB blocks.
// `key` may already include the ulid prefix (callers reading from
// the artifact map use that form); if not, we add it. On upload
// failure the bytes are written to the local spool dir (when
// configured) and the original error is surfaced.
func (s *s3Sink) putTSDBObject(ctx context.Context, bucket, blockULID, key string, data []byte) error {
	if !strings.HasPrefix(key, blockULID+"/") {
		key = blockULID + "/" + key
	}
	contentType := "application/octet-stream"
	if filepath.Base(key) == "meta.json" {
		contentType = "application/json"
	}
	if err := s.putWithRetryToBucket(ctx, bucket, key, data, contentType); err != nil {
		if s.spoolDir != "" {
			if spErr := writeLocalSpool(s.spoolDir, key, data); spErr != nil {
				return fmt.Errorf("tsdb put failed (%w) and spool failed (%v)", err, spErr)
			}
			return fmt.Errorf("tsdb put failed, spooled to %s: %w", s.spoolDir, err)
		}
		return err
	}
	return nil
}

// putWithRetryToBucket runs the bounded retry loop around PutObject
// targeting the supplied bucket. All write paths in the sink go
// through this helper.
func (s *s3Sink) putWithRetryToBucket(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, s.timeout)
		_, err := s.client.PutObjectWithContext(cctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
		})
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

func (s *s3Sink) Close() error { return nil }

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
