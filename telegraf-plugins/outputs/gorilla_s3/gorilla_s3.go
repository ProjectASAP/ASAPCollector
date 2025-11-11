package gorilla_s3

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

type GorillaS3 struct {
	Bucket      string `toml:"bucket"`
	Region      string `toml:"region"`
	Prefix      string `toml:"prefix"`
	SSEKMSKeyID string `toml:"sse_kms_key_id"`
	ObjectName  string `toml:"object_name"`

	// Optional: write Gorilla objects to a local directory.
	LocalDir string `toml:"local_dir"`

	// Object building and upload behavior
	MaxObjectBytes     int64         `toml:"max_object_bytes"`     // Split across objects when exceeded (0 = unlimited)
	MultipartThreshold int64         `toml:"multipart_threshold"`  // Use multipart when object >= this size (min 5MiB)
	MultipartPartBytes int64         `toml:"multipart_part_bytes"` // Size of each multipart part (min 5MiB)
	MaxRetries         int           `toml:"max_retries"`
	RetryBackoff       time.Duration `toml:"retry_backoff"`
	UploadTimeout      time.Duration `toml:"upload_timeout"`

	Log telegraf.Logger `toml:"-"`

	s3 *s3.S3
}

func (*GorillaS3) Description() string {
	return "Compress metrics with Gorilla encoding and upload to S3"
}

func (*GorillaS3) SampleConfig() string {
	return `
  ## Required: S3 bucket and region
  bucket = "my-metrics-bucket"
  region = "us-east-1"

  ## Optional: S3 key prefix with strftime tokens (%Y, %m, %d, %H, %M, %S)
  prefix = "metrics/date=%Y-%m-%d/hour=%H/"

  ## Optional: SSE-KMS key ID for encryption
  sse_kms_key_id = ""

  ## Optional: fixed object name; defaults to batch-<unix>-<rand>.gorilla
  # object_name = ""

  ## Optional: persist Gorilla objects to a local directory
  # local_dir = "/tmp/gorilla-objects"

  ## Optional: size and retry controls
  # max_object_bytes = 67108864     # Split batch into multiple objects when exceeded (0 = no split)
  # multipart_threshold = 8388608   # Use multipart upload when object >= this size (min 5MiB)
  # multipart_part_bytes = 8388608  # Size per multipart part (min 5MiB)
  # max_retries = 3                 # Number of upload retries on error
  # retry_backoff = "1s"            # Base backoff between retries
  # upload_timeout = "30s"          # Timeout for each upload attempt
`
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
	// Defaults
	if g.MultipartThreshold <= 0 {
		g.MultipartThreshold = 8 * 1024 * 1024 // 8 MiB
	}
	if g.MultipartPartBytes <= 0 {
		g.MultipartPartBytes = 8 * 1024 * 1024 // 8 MiB
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

func (g *GorillaS3) Close() error { return nil }

func (g *GorillaS3) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	// Group points by series key
	series := make(map[seriesKey][]point)
	metaTags := make(map[seriesKey]map[string]string)
	for _, m := range metrics {
		meas := m.Name()
		tags := m.Tags()
		// canonicalize tags into sorted key order
		keys := make([]string, 0, len(tags))
		for k := range tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for i, k := range keys {
			if i > 0 {
				b.WriteString("|")
			}
			b.WriteString(k)
			b.WriteString("=")
			b.WriteString(tags[k])
		}
		tkey := b.String()

		ts := m.Time().UnixNano()
		for _, f := range m.FieldList() {
			var fv float64
			switch v := f.Value.(type) {
			case float64:
				fv = v
			case float32:
				fv = float64(v)
			case int64:
				fv = float64(v)
			case int32:
				fv = float64(v)
			case int:
				fv = float64(v)
			case uint64:
				fv = float64(v)
			case uint32:
				fv = float64(v)
			default:
				continue // skip non-numeric fields
			}
			sk := seriesKey{measurement: meas, field: f.Key, tagsKey: tkey}
			series[sk] = append(series[sk], point{ts: ts, v: fv})
			if _, ok := metaTags[sk]; !ok {
				// copy tags map
				copyTags := make(map[string]string, len(tags))
				for k, v := range tags {
					copyTags[k] = v
				}
				metaTags[sk] = copyTags
			}
		}
	}

	// Iterate stable order for determinism
	keys := make([]seriesKey, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].measurement != keys[j].measurement {
			return keys[i].measurement < keys[j].measurement
		}
		if keys[i].field != keys[j].field {
			return keys[i].field < keys[j].field
		}
		return keys[i].tagsKey < keys[j].tagsKey
	})

	// Pre-encode each series into a chunk buffer to support size-based splitting
	type chunk struct {
		buf        []byte
		points     int
		meta       seriesMeta
		estRawSize int64
	}
	chunks := make([]chunk, 0, len(keys))
	var totalPoints int
	var totalRaw int64
	for _, k := range keys {
		pts := series[k]
		if len(pts) == 0 {
			continue
		}
		firstTS, firstValBits, tsBits, tsBitsLen, valBits, valBitsLen := sortAndEncode(pts)
		sort.Slice(pts, func(i, j int) bool { return pts[i].ts < pts[j].ts })
		meta := seriesMeta{
			Measurement: k.measurement,
			Field:       k.field,
			Tags:        metaTags[k],
			StartTS:     pts[0].ts,
			EndTS:       pts[len(pts)-1].ts,
			PointCount:  len(pts),
		}
		mb, _ := json.Marshal(meta)
		if len(mb) > math.MaxUint16 {
			return fmt.Errorf("metadata too large for series %s %s", k.measurement, k.field)
		}
		var sb bytes.Buffer
		_ = binary.Write(&sb, binary.LittleEndian, uint16(len(mb)))
		sb.Write(mb)
		_ = binary.Write(&sb, binary.LittleEndian, uint32(len(pts)))
		_ = binary.Write(&sb, binary.LittleEndian, uint64(firstTS))
		_ = binary.Write(&sb, binary.LittleEndian, uint64(firstValBits))
		_ = binary.Write(&sb, binary.LittleEndian, tsBitsLen)
		sb.Write(tsBits)
		_ = binary.Write(&sb, binary.LittleEndian, valBitsLen)
		sb.Write(valBits)
		chunks = append(chunks, chunk{
			buf:        sb.Bytes(),
			points:     len(pts),
			meta:       meta,
			estRawSize: int64(len(pts)) * 16, // naive 8-byte ts + 8-byte value
		})
		totalPoints += len(pts)
		totalRaw += int64(len(pts)) * 16
	}

	// Assemble one or more objects based on MaxObjectBytes
	objects := make([][]byte, 0, 1)
	var objSeriesCounts []int
	const headerOverhead = 8 + 1 + 4 // magic + endian + series count
	var cur bytes.Buffer
	var curCount int
	var curSize int64
	writeHeader := func() {
		cur.Reset()
		cur.WriteString("GORILLA1")
		cur.WriteByte(1)
		_ = binary.Write(&cur, binary.LittleEndian, uint32(0)) // placeholder, patch later
		curCount = 0
		curSize = headerOverhead
	}
	writeHeader()
	for _, c := range chunks {
		if g.MaxObjectBytes > 0 && curSize+int64(len(c.buf)) > g.MaxObjectBytes && curCount > 0 {
			// finalize current object
			// Patch series count
			b := cur.Bytes()
			binary.LittleEndian.PutUint32(b[9:13], uint32(curCount))
			objects = append(objects, b)
			objSeriesCounts = append(objSeriesCounts, curCount)
			writeHeader()
		}
		cur.Write(c.buf)
		curCount++
		curSize += int64(len(c.buf))
	}
	// finalize last object
	if curCount > 0 {
		b := cur.Bytes()
		binary.LittleEndian.PutUint32(b[9:13], uint32(curCount))
		objects = append(objects, b)
		objSeriesCounts = append(objSeriesCounts, curCount)
	}

	// Deliver each object to configured targets; measure latency and log compression
	now := time.Now().UTC()
	for idx, obj := range objects {
		key := g.ObjectName
		if key == "" {
			prefix := formatPrefix(g.Prefix, now)
			base := fmt.Sprintf("batch-%d-%02d-%08x.gorilla", now.Unix(), idx, rand.Uint32())
			if prefix != "" {
				key = path.Join(prefix, base)
			} else {
				key = base
			}
		}
		ratio := float64(len(obj)) / float64(totalRaw)
		if g.s3 != nil {
			start := time.Now()
			if err := g.uploadWithRetry(key, obj); err != nil {
				return err
			}
			dur := time.Since(start)
			g.Log.Infof("gorilla_s3 uploaded key=%s series=%d points=%d bytes=%d est_raw=%d ratio=%.4f latency_ms=%d", key, objSeriesCounts[idx], totalPoints, len(obj), totalRaw, ratio, dur.Milliseconds())
		}
		if g.LocalDir != "" {
			start := time.Now()
			localPath, err := g.writeLocalFile(key, obj)
			if err != nil {
				return err
			}
			dur := time.Since(start)
			g.Log.Infof("gorilla_s3 wrote local file=%s series=%d points=%d bytes=%d est_raw=%d ratio=%.4f latency_ms=%d", localPath, objSeriesCounts[idx], totalPoints, len(obj), totalRaw, ratio, dur.Milliseconds())
		}
	}

	return nil
}

func (g *GorillaS3) writeLocalFile(key string, data []byte) (string, error) {
	if g.LocalDir == "" {
		return "", fmt.Errorf("gorilla_s3: local_dir not configured")
	}
	clean := filepath.Clean(key)
	clean = strings.TrimPrefix(clean, "/")
	for strings.HasPrefix(clean, "../") {
		clean = strings.TrimPrefix(clean, "../")
	}
	dest := filepath.Join(g.LocalDir, clean)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return "", fmt.Errorf("gorilla_s3: create local path: %w", err)
	}
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", fmt.Errorf("gorilla_s3: write temp file: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return "", fmt.Errorf("gorilla_s3: rename temp file: %w", err)
	}
	return dest, nil
}

func formatPrefix(prefix string, t time.Time) string {
	if prefix == "" {
		return ""
	}
	// Support common strftime tokens
	repl := strings.NewReplacer(
		"%Y", t.Format("2006"),
		"%m", t.Format("01"),
		"%d", t.Format("02"),
		"%H", t.Format("15"),
		"%M", t.Format("04"),
		"%S", t.Format("05"),
	)
	s := repl.Replace(prefix)
	// Ensure no accidental Windows path separators etc.
	s = strings.ReplaceAll(s, "\\", "/")
	return strings.TrimPrefix(s, "/")
}

func init() {
	outputs.Add("gorilla_s3", func() telegraf.Output { return &GorillaS3{} })
}

// uploadWithRetry uploads data with either PutObject or Multipart depending on size/threshold.
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
		// Don't retry on client-side permanent errors from AWS if known
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
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

	// Prepare parts
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
			// Abort on error
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
