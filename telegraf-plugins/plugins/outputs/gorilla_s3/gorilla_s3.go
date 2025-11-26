//go:generate ../../../tools/readme_config_includer/generator
package gorilla_s3

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type GorillaS3 struct {
	Bucket      string `toml:"bucket"`
	Region      string `toml:"region"`
	Prefix      string `toml:"prefix"`
	SSEKMSKeyID string `toml:"sse_kms_key_id"`
	ObjectName  string `toml:"object_name"`

	LocalDir string `toml:"local_dir"`

	MultipartThreshold int64         `toml:"multipart_threshold"`
	MultipartPartBytes int64         `toml:"multipart_part_bytes"`
	MaxRetries         int           `toml:"max_retries"`
	RetryBackoff       time.Duration `toml:"retry_backoff"`
	UploadTimeout      time.Duration `toml:"upload_timeout"`

	BlockMeasurement string `toml:"block_measurement"`
	PayloadField     string `toml:"payload_field"`

	Log telegraf.Logger `toml:"-"`

	s3 *s3.S3
}

func (*GorillaS3) SampleConfig() string {
	return sampleConfig
}

func (*GorillaS3) Description() string {
	return "Upload Gorilla aggregator blocks to Amazon S3 or the local filesystem"
}

func (g *GorillaS3) Init() error {
	if g.BlockMeasurement == "" {
		g.BlockMeasurement = "gorilla_block"
	}
	if g.PayloadField == "" {
		g.PayloadField = "payload"
	}
	return nil
}

func (g *GorillaS3) Connect() error {
	if g.LocalDir != "" {
		if err := os.MkdirAll(g.LocalDir, 0o755); err != nil {
			return fmt.Errorf("gorilla_s3: ensure local_dir: %w", err)
		}
	}

	if g.Bucket != "" || g.Region != "" {
		if g.Bucket == "" || g.Region == "" {
			return fmt.Errorf("gorilla_s3: both bucket and region must be set when using S3")
		}
		sess, err := session.NewSession(&aws.Config{Region: aws.String(g.Region)})
		if err != nil {
			return err
		}
		g.s3 = s3.New(sess)
	} else if g.LocalDir == "" {
		return fmt.Errorf("gorilla_s3: configure either S3 (bucket+region) or local_dir")
	}

	if g.MultipartThreshold <= 0 {
		g.MultipartThreshold = 8 * 1024 * 1024
	}
	if g.MultipartPartBytes <= 0 {
		g.MultipartPartBytes = 8 * 1024 * 1024
	}
	if g.MultipartPartBytes < 5*1024*1024 {
		g.MultipartPartBytes = 5 * 1024 * 1024
	}
	if g.MaxRetries <= 0 {
		g.MaxRetries = 3
	}
	if g.RetryBackoff <= 0 {
		g.RetryBackoff = time.Second
	}
	if g.UploadTimeout <= 0 {
		g.UploadTimeout = 30 * time.Second
	}
	return nil
}

func (g *GorillaS3) Close() error {
	return nil
}

func (g *GorillaS3) Write(metrics []telegraf.Metric) error {
	for idx, m := range metrics {
		if g.BlockMeasurement != "" && m.Name() != g.BlockMeasurement {
			continue
		}
		payload, ok := g.extractPayload(m)
		if !ok || len(payload) == 0 {
			continue
		}

		seriesCount := readIntField(m, "series_count")
		points := readIntField(m, "point_count")
		raw := readIntField(m, "estimated_raw_bytes")
		if raw <= 0 {
			raw = int64(len(payload))
		}
		ratio := float64(len(payload))
		if raw > 0 {
			ratio /= float64(raw)
		}

		key := g.buildObjectKey(m, idx)
		var blockStart, blockEnd string
		if v, ok := m.GetTag("block_start"); ok {
			blockStart = v
		}
		if v, ok := m.GetTag("block_end"); ok {
			blockEnd = v
		}

		if g.s3 != nil {
			start := time.Now()
			if err := g.uploadWithRetry(key, payload); err != nil {
				return err
			}
			dur := time.Since(start)
			g.Log.Infof("gorilla_s3 uploaded key=%s series=%d points=%d bytes=%d est_raw=%d ratio=%.4f block_start=%s block_end=%s latency_ms=%d",
				key, seriesCount, points, len(payload), raw, ratio, blockStart, blockEnd, dur.Milliseconds())
		}

		if g.LocalDir != "" {
			start := time.Now()
			localPath, err := g.writeLocalFile(key, payload)
			if err != nil {
				return err
			}
			dur := time.Since(start)
			g.Log.Infof("gorilla_s3 wrote local file=%s series=%d points=%d bytes=%d est_raw=%d ratio=%.4f block_start=%s block_end=%s latency_ms=%d",
				localPath, seriesCount, points, len(payload), raw, ratio, blockStart, blockEnd, dur.Milliseconds())
		}
	}
	return nil
}

func (g *GorillaS3) extractPayload(m telegraf.Metric) ([]byte, bool) {
	field, ok := m.GetField(g.PayloadField)
	if !ok {
		return nil, false
	}
	switch v := field.(type) {
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

func (g *GorillaS3) buildObjectKey(m telegraf.Metric, idx int) string {
	if g.ObjectName != "" {
		return g.ObjectName
	}

	blockTime := m.Time()
	if blockTime.IsZero() {
		blockTime = time.Now()
	}

	prefix := formatPrefix(g.Prefix, blockTime)
	base := fmt.Sprintf("block-%d-%02d-%08x.gorilla", blockTime.Unix(), idx, rand.Uint32())
	if prefix != "" {
		return path.Join(prefix, base)
	}
	return base
}

func (g *GorillaS3) writeLocalFile(key string, data []byte) (string, error) {
	if g.LocalDir == "" {
		return "", fmt.Errorf("gorilla_s3: local_dir not configured")
	}
	clean := filepath.Clean(key)
	localPath := filepath.Join(g.LocalDir, clean)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return "", fmt.Errorf("gorilla_s3: create local path: %w", err)
	}
	tmp := localPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", fmt.Errorf("gorilla_s3: write temp file: %w", err)
	}
	if err := os.Rename(tmp, localPath); err != nil {
		return "", fmt.Errorf("gorilla_s3: rename temp file: %w", err)
	}
	return localPath, nil
}

func (g *GorillaS3) uploadWithRetry(key string, data []byte) error {
	var lastErr error
	for attempt := 0; attempt <= g.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), g.UploadTimeout)
		err := g.uploadOnce(ctx, key, data)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return err
			}
		}
		if attempt < g.MaxRetries {
			time.Sleep(time.Duration(attempt+1) * g.RetryBackoff)
		}
	}
	return fmt.Errorf("upload failed after %d retries: %w", g.MaxRetries, lastErr)
}

func (g *GorillaS3) uploadOnce(ctx context.Context, key string, data []byte) error {
	if int64(len(data)) >= g.MultipartThreshold {
		return g.multipartUpload(ctx, key, data)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(g.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	}
	if g.SSEKMSKeyID != "" {
		input.ServerSideEncryption = aws.String("aws:kms")
		input.SSEKMSKeyId = aws.String(g.SSEKMSKeyID)
	}
	_, err := g.s3.PutObjectWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 putobject failed: %w", err)
	}
	return nil
}

func (g *GorillaS3) multipartUpload(ctx context.Context, key string, data []byte) error {
	create := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(g.Bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	}
	if g.SSEKMSKeyID != "" {
		create.ServerSideEncryption = aws.String("aws:kms")
		create.SSEKMSKeyId = aws.String(g.SSEKMSKeyID)
	}
	resp, err := g.s3.CreateMultipartUploadWithContext(ctx, create)
	if err != nil {
		return fmt.Errorf("create multipart upload failed: %w", err)
	}
	uploadID := aws.StringValue(resp.UploadId)

	var completed []*s3.CompletedPart
	rdr := bytes.NewReader(data)
	var offset int64
	partNum := int64(1)
	for offset < int64(len(data)) {
		size := g.MultipartPartBytes
		if offset+size > int64(len(data)) {
			size = int64(len(data)) - offset
		}
		section := io.NewSectionReader(rdr, offset, size)
		up := &s3.UploadPartInput{
			Bucket:     aws.String(g.Bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int64(partNum),
			UploadId:   aws.String(uploadID),
			Body:       section,
		}
		part, err := g.s3.UploadPartWithContext(ctx, up)
		if err != nil {
			_, _ = g.s3.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(g.Bucket),
				Key:      aws.String(key),
				UploadId: aws.String(uploadID),
			})
			return fmt.Errorf("upload part %d failed: %w", partNum, err)
		}
		completed = append(completed, &s3.CompletedPart{ETag: part.ETag, PartNumber: aws.Int64(partNum)})
		offset += size
		partNum++
	}

	_, err = g.s3.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(g.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload failed: %w", err)
	}
	return nil
}

func readIntField(m telegraf.Metric, key string) int64 {
	field, ok := m.GetField(key)
	if !ok {
		return 0
	}
	switch v := field.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case int:
		return int64(v)
	case uint64:
		return int64(v)
	case uint32:
		return int64(v)
	case uint16:
		return int64(v)
	case uint8:
		return int64(v)
	case uint:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

func formatPrefix(prefix string, t time.Time) string {
	if prefix == "" {
		return ""
	}
	repl := strings.NewReplacer(
		"%Y", t.Format("2006"),
		"%m", t.Format("01"),
		"%d", t.Format("02"),
		"%H", t.Format("15"),
		"%M", t.Format("04"),
		"%S", t.Format("05"),
	)
	s := repl.Replace(prefix)
	s = strings.ReplaceAll(s, "\\", "/")
	return strings.TrimPrefix(s, "/")
}

func init() {
	outputs.Add("gorilla_s3", func() telegraf.Output {
		return &GorillaS3{}
	})
}
