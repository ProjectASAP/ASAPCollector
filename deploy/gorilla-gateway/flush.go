package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
)

const upstreamBucket = "asap-gorilla-tsdb"

// runFlushLoop uploads all complete buffered blocks to MinIO on a fixed interval,
// then waits a grace period before deleting local copies to avoid a data dark
// period while the MinIO store-gateway syncs the newly uploaded block.
func runFlushLoop(ctx context.Context, db *DiskBuffer, mc *minio.Client, interval, grace time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			flushAll(ctx, db, mc, grace)
		}
	}
}

func flushAll(ctx context.Context, db *DiskBuffer, mc *minio.Client, grace time.Duration) {
	ids := db.CompleteBlocks()
	if len(ids) == 0 {
		log.Printf("flush: nothing to upload")
		return
	}
	log.Printf("flush: uploading %d complete block(s) to s3://%s", len(ids), upstreamBucket)

	for _, id := range ids {
		if err := uploadBlock(ctx, db, mc, id); err != nil {
			log.Printf("flush: upload %s FAILED: %v", id, err)
			continue
		}

		// Mark so the block is excluded from future flush passes.
		db.MarkFlushing(id)

		// Grace period: the MinIO store-gateway syncs on a timer (sync-block-duration=30s).
		// We must wait at least one full sync cycle before deleting the local copy, or
		// Thanos Query will briefly see neither store serving the block.
		// During the grace window both stores serve the block; Thanos Query
		// deduplicates via --query.replica-label=block_source.
		log.Printf("flush: block %s uploaded OK; waiting %s grace before deleting local", id, grace)
		select {
		case <-ctx.Done():
			return
		case <-time.After(grace):
		}

		if err := db.DeleteBlock(id); err != nil {
			log.Printf("flush: delete local %s FAILED: %v", id, err)
		} else {
			log.Printf("flush: deleted local block %s", id)
		}
	}
	log.Printf("flush done: processed %d block(s)", len(ids))
}

// uploadBlock uploads the three TSDB block files for id to MinIO.
//
// The meta.json is re-labeled with block_source=minio (distinct from the
// gateway-buffer label injected on disk-write) so Thanos Query can
// deduplicate both copies during the grace overlap window.
func uploadBlock(ctx context.Context, db *DiskBuffer, mc *minio.Client, id string) error {
	// meta.json first — label it block_source=minio for deduplication.
	metaData, err := db.MetaJSON(id, "minio")
	if err != nil {
		return fmt.Errorf("read meta.json: %w", err)
	}
	if err := putS3(ctx, mc, id+"/meta.json", metaData); err != nil {
		return fmt.Errorf("PUT meta.json: %w", err)
	}

	// index
	indexData, err := db.FileData(id, "index")
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	if err := putS3(ctx, mc, id+"/index", indexData); err != nil {
		return fmt.Errorf("PUT index: %w", err)
	}

	// chunks/000001
	chunkData, err := db.FileData(id, "chunks/000001")
	if err != nil {
		return fmt.Errorf("read chunks/000001: %w", err)
	}
	if err := putS3(ctx, mc, id+"/chunks/000001", chunkData); err != nil {
		return fmt.Errorf("PUT chunks/000001: %w", err)
	}

	return nil
}

func putS3(ctx context.Context, mc *minio.Client, key string, data []byte) error {
	_, err := mc.PutObject(ctx, upstreamBucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	log.Printf("flush OK  s3://%s/%s  %d B", upstreamBucket, key, len(data))
	return nil
}
