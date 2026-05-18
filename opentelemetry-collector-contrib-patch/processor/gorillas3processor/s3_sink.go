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

	// PutPostings writes a single `postings-v1.json` sidecar object.
	// mvp/v5: the processor calls this once per flush; the
	// compactor (a separate binary) overrides + merges later.
	// `key` is the full S3 key; `data` is the raw POSTING1-framed
	// bytes from `buildPostings`.
	PutPostings(ctx context.Context, key string, data []byte) error

	// PutTSDBBlock writes one Prometheus TSDB block to the
	// configured TSDB bucket (or the primary bucket when
	// `tsdb_bucket` is empty). The map keys are the relative
	// object keys inside the block — typically `chunks/000001`,
	// `index`, `meta.json` — the sink prepends the block ULID
	// dir prefix. Implementations write all files atomically
	// from the caller's POV; readers SHOULD see a complete block
	// once the call returns.
	//
	// mvp/step2.1: only called when block_format ∈ {prometheus_tsdb, both}.
	PutTSDBBlock(ctx context.Context, blockULID string, files map[string][]byte) error

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
	// mvp/v5: a deterministic 64-bit canonical-label-set hash that
	// the postings sidecar joins on. Empty / 0 ⇒ producer didn't
	// compute one (older path).
	LabelHash uint64
}

// indexEntry is one row inside a per-hour index.json.
//
// **mvp/v5**: extended with `object_key`, `byte_offset`, `byte_length`
// for the compactor's merged-block layout. For pre-compactor flushes
// the agent emits `object_key = Object`, `byte_offset = 0`,
// `byte_length = SizeBytes` so backend partial-read code paths can
// be unconditional. The asap-gorilla Rust crate's `IndexEntry`
// `Option<…>` fields parse zero-valued JSON keys as "absent" via the
// `effective_*` accessors — we therefore omit them on the agent side
// (zero value === legacy semantic).
type indexEntry struct {
	Object      string `json:"object"`
	StartTSNano int64  `json:"start_ts_nano"`
	EndTSNano   int64  `json:"end_ts_nano"`
	SeriesCount int    `json:"series_count"`
	PointCount  int    `json:"point_count"`
	SizeBytes   int    `json:"size_bytes"`
	WrittenAt   int64  `json:"written_at_unix_nano"`
	// mvp/v5 extension. omitempty keeps the JSON byte-compatible
	// with pre-v5 readers, since our zero-value semantics
	// ("chunk lives at Object") match what readers infer from the
	// existing `Object` + `SizeBytes` pair.
	ObjectKey  string `json:"object_key,omitempty"`
	ByteOffset uint64 `json:"byte_offset,omitempty"`
	ByteLength uint64 `json:"byte_length,omitempty"`
	LabelHash  uint64 `json:"label_hash,omitempty"`
}

type indexFile struct {
	Version int          `json:"version"`
	Tenant  string       `json:"tenant"`
	Metric  string       `json:"metric"`
	Entries []indexEntry `json:"entries"`
	// mvp/v5: relative S3 path of the postings sidecar within the
	// same hour-bucket (`postings-v1.json`). Empty on pre-v5
	// blocks. The compactor rewrites this when it merges blocks.
	Postings string `json:"postings,omitempty"`
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

// PutPostings writes the `postings-v1.json` sidecar. mvp/v5: the
// processor calls this once per flush, after PutChunk. Failure is
// surfaced via the same retry/spool path as chunk uploads — postings
// missing on read becomes a backend `data_source_quirk: postings_missing`
// soft-fall-through rather than a hard error.
func (s *s3Sink) PutPostings(ctx context.Context, key string, data []byte) error {
	if err := s.putWithRetry(ctx, key, data); err != nil {
		if s.spoolDir != "" {
			if spErr := writeLocalSpool(s.spoolDir, key, data); spErr != nil {
				return fmt.Errorf("postings put failed (%w) and spool failed (%v)", err, spErr)
			}
			return fmt.Errorf("postings put failed, spooled to %s: %w", s.spoolDir, err)
		}
		return err
	}
	return nil
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
	// roles; agent roles never enter this TSDB upload path. The legacy
	// `tsdbBucket = cfg.Bucket` fallback was retired with the
	// gorillas3 Bucket-field retirement (B1 downstream).
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
// the artifact map use that form); if not, we add it. The same
// retry / spool fall-through path as chunk uploads applies.
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

// putWithRetryToBucket is the bucket-parameterised cousin of
// putWithRetry. The default chunk path goes through the
// `cfg.Bucket` shortcut; the TSDB path needs to target a different
// bucket so we factor the retry loop here.
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
	objectKey := path.Base(chunkKey)
	idx.Entries = append(idx.Entries, indexEntry{
		Object:      objectKey,
		StartTSNano: h.StartTSNano,
		EndTSNano:   h.EndTSNano,
		SeriesCount: h.SeriesCount,
		PointCount:  h.PointCount,
		SizeBytes:   h.SizeBytes,
		WrittenAt:   time.Now().UnixNano(),
		// mvp/v5: pre-compactor agent — the chunk IS its own S3
		// object so `object_key = Object` and the chunk slice
		// covers `[0, SizeBytes)` of that object. The compactor
		// later rewrites these for merged blocks.
		ObjectKey:  objectKey,
		ByteOffset: 0,
		ByteLength: uint64(h.SizeBytes),
		LabelHash:  h.LabelHash,
	})
	// mvp/v5: stamp the postings sidecar pointer so backends can
	// find it without a separate listing.
	idx.Postings = "postings-v1.json"
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
