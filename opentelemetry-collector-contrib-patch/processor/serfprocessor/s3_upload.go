// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfprocessor

import (
	"bytes"
	"context"
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
	"github.com/aws/aws-sdk-go/service/s3"
)

// uploadWithRetry wraps uploadOnce with retry logic and exponential backoff.
func (p *serfProcessor) uploadWithRetry(key string, data []byte) error {
	timeout := p.cfg.S3.UploadTimeout
	backoff := p.cfg.S3.RetryBackoff
	maxRetries := p.cfg.S3.MaxRetries

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := p.uploadOnce(ctx, key, data)
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
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt+1) * backoff)
		}
	}
	return fmt.Errorf("upload failed after %d retries: %w", maxRetries, lastErr)
}

func (p *serfProcessor) uploadOnce(ctx context.Context, key string, data []byte) error {
	if int64(len(data)) >= p.cfg.S3.MultipartThreshold {
		return p.multipartUpload(ctx, key, data)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(p.cfg.S3.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	}
	if p.cfg.S3.SSEKMSKeyID != "" {
		input.ServerSideEncryption = aws.String("aws:kms")
		input.SSEKMSKeyId = aws.String(p.cfg.S3.SSEKMSKeyID)
	}
	_, err := p.s3Client.PutObjectWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 putobject failed: %w", err)
	}
	return nil
}

func (p *serfProcessor) multipartUpload(ctx context.Context, key string, data []byte) error {
	create := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(p.cfg.S3.Bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	}
	if p.cfg.S3.SSEKMSKeyID != "" {
		create.ServerSideEncryption = aws.String("aws:kms")
		create.SSEKMSKeyId = aws.String(p.cfg.S3.SSEKMSKeyID)
	}
	resp, err := p.s3Client.CreateMultipartUploadWithContext(ctx, create)
	if err != nil {
		return fmt.Errorf("create multipart upload failed: %w", err)
	}
	uploadID := aws.StringValue(resp.UploadId)

	var completed []*s3.CompletedPart
	rdr := bytes.NewReader(data)
	var offset int64
	partNum := int64(1)
	for offset < int64(len(data)) {
		size := p.cfg.S3.MultipartPartBytes
		if offset+size > int64(len(data)) {
			size = int64(len(data)) - offset
		}
		section := io.NewSectionReader(rdr, offset, size)
		up := &s3.UploadPartInput{
			Bucket:     aws.String(p.cfg.S3.Bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int64(partNum),
			UploadId:   aws.String(uploadID),
			Body:       section,
		}
		part, err := p.s3Client.UploadPartWithContext(ctx, up)
		if err != nil {
			_, _ = p.s3Client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(p.cfg.S3.Bucket),
				Key:      aws.String(key),
				UploadId: aws.String(uploadID),
			})
			return fmt.Errorf("upload part %d failed: %w", partNum, err)
		}
		completed = append(completed, &s3.CompletedPart{ETag: part.ETag, PartNumber: aws.Int64(partNum)})
		offset += size
		partNum++
	}

	_, err = p.s3Client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(p.cfg.S3.Bucket),
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

// writeLocalFile writes a compressed block to the local filesystem using
// atomic rename for crash safety.
func writeLocalFile(localDir string, key string, data []byte) (string, error) {
	clean := filepath.Clean(key)
	localPath := filepath.Join(localDir, clean)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return "", fmt.Errorf("serf: create local path: %w", err)
	}
	tmp := localPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", fmt.Errorf("serf: write temp file: %w", err)
	}
	if err := os.Rename(tmp, localPath); err != nil {
		return "", fmt.Errorf("serf: rename temp file: %w", err)
	}
	return localPath, nil
}

// buildObjectKey generates the S3/local key for a compressed block.
func buildObjectKey(prefix string, fixedName string, blockEnd time.Time, idx int) string {
	if fixedName != "" {
		return fixedName
	}
	if blockEnd.IsZero() {
		blockEnd = time.Now()
	}
	pfx := formatPrefix(prefix, blockEnd)
	base := fmt.Sprintf("block-%d-%02d-%08x.serf", blockEnd.Unix(), idx, rand.Uint32())
	if pfx != "" {
		return path.Join(pfx, base)
	}
	return base
}

// formatPrefix applies strftime-style token replacement to a prefix string.
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
