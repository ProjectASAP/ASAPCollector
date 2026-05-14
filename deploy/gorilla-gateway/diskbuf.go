package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// blockState tracks which files of a TSDB block have been received.
// A block is complete when all three files are present:
//   chunks/000001, index, meta.json
type blockState struct {
	hasChunks bool
	hasIndex  bool
	hasMeta   bool
	flushing  bool // being uploaded to MinIO; not re-submitted to flush queue
}

func (s *blockState) complete() bool {
	return s.hasChunks && s.hasIndex && s.hasMeta
}

// DiskBuffer persists received TSDB block files to disk and tracks which
// blocks are complete. It is safe for concurrent use.
type DiskBuffer struct {
	dir    string
	mu     sync.Mutex
	blocks map[string]*blockState // ULID → state
}

func newDiskBuffer(dir string) *DiskBuffer {
	db := &DiskBuffer{
		dir:    dir,
		blocks: make(map[string]*blockState),
	}
	db.scanExisting()
	return db
}

// Write persists one S3 PUT object to disk.
//
// key has the form "<ULID>/<rest>", e.g. "01KRK0X.../chunks/000001".
// For meta.json objects, the Thanos external label block_source=gateway-buffer
// is injected so the thanos-buffer-store sidecar can deduplicate correctly
// during the handoff overlap window (see flush.go).
func (db *DiskBuffer) Write(bucket, key string, data []byte) error {
	slash := strings.Index(key, "/")
	if slash < 0 {
		return fmt.Errorf("key has no slash: %q", key)
	}
	id, rest := key[:slash], key[slash+1:]

	if rest == "meta.json" {
		var err error
		data, err = injectThanosLabel(data, "gateway-buffer")
		if err != nil {
			log.Printf("diskbuf: meta.json label inject warn (using original): %v", err)
		}
	}

	dst := filepath.Join(db.dir, id, filepath.FromSlash(rest))
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("mkdirall: %w", err)
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		return fmt.Errorf("writefile: %w", err)
	}

	db.mu.Lock()
	s := db.getOrCreate(id)
	switch rest {
	case "chunks/000001":
		s.hasChunks = true
	case "index":
		s.hasIndex = true
	case "meta.json":
		s.hasMeta = true
	}
	complete := s.complete()
	n := len(db.blocks)
	db.mu.Unlock()

	log.Printf("recv  s3://%s/%s  %d B  buf=%d  complete=%v", bucket, key, len(data), n, complete)
	return nil
}

// CompleteBlocks returns ULIDs of blocks where all three files are present
// and which are not currently being flushed.
func (db *DiskBuffer) CompleteBlocks() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	var out []string
	for id, s := range db.blocks {
		if s.complete() && !s.flushing {
			out = append(out, id)
		}
	}
	return out
}

// MarkFlushing marks a block as in-progress so it is not re-queued.
func (db *DiskBuffer) MarkFlushing(id string) {
	db.mu.Lock()
	if s, ok := db.blocks[id]; ok {
		s.flushing = true
	}
	db.mu.Unlock()
}

// MetaJSON reads the on-disk meta.json and re-labels it with the given
// block_source value. Used by flush.go to stamp MinIO-bound blocks with
// block_source=minio (distinct from gateway-buffer) for Thanos deduplication.
func (db *DiskBuffer) MetaJSON(id, blockSource string) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(db.dir, id, "meta.json"))
	if err != nil {
		return nil, err
	}
	return injectThanosLabel(raw, blockSource)
}

// FileData returns the raw bytes of an arbitrary block file.
func (db *DiskBuffer) FileData(id, rel string) ([]byte, error) {
	return os.ReadFile(filepath.Join(db.dir, id, filepath.FromSlash(rel)))
}

// DeleteBlock removes the on-disk block directory and its in-memory entry.
func (db *DiskBuffer) DeleteBlock(id string) error {
	if err := os.RemoveAll(filepath.Join(db.dir, id)); err != nil {
		return err
	}
	db.mu.Lock()
	delete(db.blocks, id)
	db.mu.Unlock()
	return nil
}

// handleListBlocks serves GET /v1/blocks as a JSON array for observability.
func (db *DiskBuffer) handleListBlocks(w http.ResponseWriter, r *http.Request) {
	type entry struct {
		ULID     string `json:"ulid"`
		Complete bool   `json:"complete"`
		Flushing bool   `json:"flushing"`
	}
	db.mu.Lock()
	entries := make([]entry, 0, len(db.blocks))
	for id, s := range db.blocks {
		entries = append(entries, entry{
			ULID:     id,
			Complete: s.complete(),
			Flushing: s.flushing,
		})
	}
	db.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

func (db *DiskBuffer) getOrCreate(id string) *blockState {
	if s, ok := db.blocks[id]; ok {
		return s
	}
	s := &blockState{}
	db.blocks[id] = s
	return s
}

// scanExisting recovers block state from disk so that in-progress blocks
// from before a gateway restart are not lost.
func (db *DiskBuffer) scanExisting() {
	entries, _ := os.ReadDir(db.dir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		db.blocks[id] = &blockState{
			hasChunks: fileExists(filepath.Join(db.dir, id, "chunks", "000001")),
			hasIndex:  fileExists(filepath.Join(db.dir, id, "index")),
			hasMeta:   fileExists(filepath.Join(db.dir, id, "meta.json")),
		}
	}
	log.Printf("diskbuf: recovered %d block(s) from %s", len(db.blocks), db.dir)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// ── Thanos meta.json label injection ──────────────────────────────────────
//
// The Thanos-extended TSDB meta.json adds a "thanos" section with external
// labels. The thanos-buffer-store sidecar reads these labels via the StoreAPI,
// and Thanos Query uses --query.replica-label=block_source to deduplicate
// blocks that are served by both the buffer store and the MinIO store during
// the grace overlap window.

type rawMeta struct {
	ULID       string          `json:"ulid"`
	MinTime    int64           `json:"minTime"`
	MaxTime    int64           `json:"maxTime"`
	Stats      json.RawMessage `json:"stats"`
	Compaction json.RawMessage `json:"compaction"`
	Version    int             `json:"version"`
	Thanos     *thanosMeta     `json:"thanos,omitempty"`
}

type thanosMeta struct {
	Labels     map[string]string `json:"labels"`
	Downsample *downsampleMeta   `json:"downsample,omitempty"`
	Source     string            `json:"source,omitempty"`
}

type downsampleMeta struct {
	Resolution int64 `json:"resolution"`
}

func injectThanosLabel(data []byte, blockSource string) ([]byte, error) {
	var m rawMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse meta.json: %w", err)
	}
	if m.Thanos == nil {
		m.Thanos = &thanosMeta{}
	}
	if m.Thanos.Labels == nil {
		m.Thanos.Labels = make(map[string]string)
	}
	m.Thanos.Labels["block_source"] = blockSource
	if m.Thanos.Downsample == nil {
		m.Thanos.Downsample = &downsampleMeta{Resolution: 0}
	}
	if m.Thanos.Source == "" {
		m.Thanos.Source = "ingester"
	}
	return json.Marshal(&m)
}
