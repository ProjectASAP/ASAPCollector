// gorilla-head-merger — local head-block buffer + block-level WAL for gorilla-buffer-store.
//
// The merger is the recent, in-progress HEAD of the storage hierarchy (the role
// a Prometheus head block plays). gorillas3 agents keep computing Gorilla-XOR
// per-emit TSDB blocks (tsdb_block_duration, default 60s) but, instead of PUTting
// each one to S3/MinIO, they POST it to this merger's /ingest endpoint. The merger:
//
//   - Durably writes each received per-emit block into the served dir (atomic
//     temp+rename), ack'ing the agent only after the durable write. The served dir
//     IS the block-level WAL: on crash restart the blocks are still there and the
//     window is re-cut from them — no separate log to replay.
//   - Keeps the current (open) tumbling window's per-emit blocks in the served dir
//     so the hot-store (thanos store, FILESYSTEM objstore over the served dir) can
//     serve the freshest data immediately — no re-merge per poll.
//   - When a window completes (now ≥ window_end + grace), CUTS it once: concatenates
//     the per-series Gorilla chunks of that window's per-emit blocks into ONE block
//     (copying chunk bytes — no sample decode/re-encode), uploads that single block
//     to S3/MinIO, then prunes the window's per-emit blocks (replaced by the cut
//     block). ~window/emit fewer S3 PUTs than PUTting every per-emit block.
//
// Tumbling-window invariant:
//   - A block belongs to window w = floor(block.minTime / window).
//   - Window w covers the half-open interval [w*window, (w+1)*window).
//   - Per-emit (head) blocks carry Compaction.Level 1 (from gorillas3); the merger's
//     cut blocks carry Level 2 — that is how the two are told apart in the served dir.
//   - Each completed window is cut into exactly one Level-2 block. If a late per-emit
//     block arrives for an already-cut window, the window is re-cut (merging the prior
//     cut block + the late block) so no data is lost.
//   - The served dir retains the last N windows (N = ceil(retention/window), min 2),
//     dropping older windows once thanos-store-gateway has synced the cut blocks from S3.
//
// The window (-window / MERGE_WINDOW) is configurable, default 1h — the granularity at
// which the head is cut and flushed to S3/MinIO. The agent's per-emit interval
// (tsdb_block_duration) is configured independently.
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var (
	flagBucket      = flag.String("bucket", "asap-gorilla-tsdb", "S3/MinIO bucket for flushed (cut) window blocks")
	flagEndpoint    = flag.String("endpoint", "minio:9000", "S3/MinIO endpoint (host:port, no scheme)")
	flagAccessKey   = flag.String("access-key", "asap", "S3 access key")
	flagSecretKey   = flag.String("secret-key", "asap-local-only", "S3 secret key")
	flagWindow      = flag.Duration("window", time.Hour, "Tumbling window duration: fixed-size, non-overlapping; configurable, default 1h. Granularity at which the head is cut and flushed to S3/MinIO; the agent's per-emit interval (tsdb_block_duration) is configured independently.")
	flagRetention   = flag.Duration("retention", 0, "How far back to keep cut window blocks queryable from the local served dir (0 = same as -window). Retained windows = ceil(retention/window), min 2.")
	flagGrace       = flag.Duration("grace", time.Minute, "Grace period after a window's end before it is cut+flushed, to absorb late-arriving per-emit blocks.")
	flagCutInterval = flag.Duration("cut-interval", 15*time.Second, "How often to check for completed windows ready to cut+flush.")
	flagServedDir   = flag.String("served-dir", "/var/gorilla-buffer/served", "Hot dir served by thanos store (FILESYSTEM objstore): holds the current window's per-emit blocks (the head) + cut window blocks. Also the durable block-level WAL of received blocks.")
	flagIngestAddr  = flag.String("ingest-addr", ":9099", "HTTP address for the per-emit block ingest endpoint (POST /ingest)")
	flagMaxBlockMiB = flag.Int64("max-block-mib", 256, "Max accepted ingest body size (MiB)")
	flagMetricsAddr = flag.String("metrics-addr", ":9100", "Prometheus metrics endpoint address (empty to disable)")
)

// mu serializes served-dir mutations (ingest rename, cut swap+prune, retention
// expiry) so a window is never read mid-cut and a block never appears/disappears
// partially. Reads of already-written, immutable block dirs need no lock.
var mu sync.Mutex

var (
	metricBlocksIngested = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gorilla_head_blocks_ingested_total",
		Help: "Per-emit blocks received and durably written to the head/WAL.",
	})
	metricCutErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gorilla_head_cut_errors_total",
		Help: "Errors while cutting/flushing a completed window.",
	})
	metricHeadBlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gorilla_head_uncut_blocks",
		Help: "Per-emit (Level-1) blocks currently in the served dir (head, not yet cut).",
	})
	metricCutWindows = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gorilla_head_cut_windows",
		Help: "Cut (Level-2) window blocks retained in the served dir.",
	})
	metricBlocksUploaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gorilla_head_blocks_uploaded_total",
		Help: "Cut window blocks uploaded to S3/MinIO.",
	})
	metricLastCutTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gorilla_head_last_cut_timestamp_seconds",
		Help: "Unix time of the last successful cut+flush.",
	})
)

// blockInfo holds the meta.json fields we need to classify and window a block.
type blockInfo struct {
	ULID       string `json:"ulid"`
	MinTime    int64  `json:"minTime"`
	MaxTime    int64  `json:"maxTime"`
	Compaction struct {
		Level int `json:"level"`
	} `json:"compaction"`
}

// seriesEntry accumulates chunks for one time series across multiple source blocks.
type seriesEntry struct {
	lset labels.Labels
	chks []chunks.Meta // Chunk field populated; Ref updated by WriteChunks in-place
}

// windowOf returns the tumbling-window index for a millisecond timestamp:
// the half-open window [w*windowMs, (w+1)*windowMs) that contains ts.
func windowOf(tsMs, windowMs int64) int64 {
	return int64(math.Floor(float64(tsMs) / float64(windowMs)))
}

// numRetainedWindows derives how many tumbling windows to keep based on the
// retention duration. At minimum 2 (current in-progress + previous completed)
// so a query straddling a window boundary always finds a covering block until
// thanos-store-gateway has synced the archive.
func numRetainedWindows(retention, window time.Duration) int64 {
	if retention <= 0 {
		retention = window
	}
	n := int64(math.Ceil(float64(retention) / float64(window)))
	if n < 2 {
		n = 2
	}
	return n
}

func main() {
	flag.Parse()
	slog.Info("gorilla-head-merger starting (local head + block WAL)",
		"bucket", *flagBucket,
		"endpoint", *flagEndpoint,
		"window", *flagWindow,
		"retention", *flagRetention,
		"grace", *flagGrace,
		"cut_interval", *flagCutInterval,
		"served_dir", *flagServedDir,
		"ingest_addr", *flagIngestAddr,
		"retained_windows", numRetainedWindows(*flagRetention, *flagWindow),
	)

	if err := os.MkdirAll(*flagServedDir, 0o755); err != nil {
		slog.Error("create served dir", "path", *flagServedDir, "err", err)
		os.Exit(1)
	}
	// Drop any partial temp dirs left by a crash mid-ingest or mid-cut.
	cleanTempDirs(*flagServedDir)

	ctx := context.Background()
	s3c, err := newS3Client(ctx)
	if err != nil {
		slog.Error("create S3 client", "err", err)
		os.Exit(1)
	}

	if *flagMetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		go func() {
			slog.Info("metrics server listening", "addr", *flagMetricsAddr)
			if err := http.ListenAndServe(*flagMetricsAddr, mux); err != nil {
				slog.Error("metrics server failed", "err", err)
			}
		}()
	}

	// Ingest server: agents POST per-emit blocks here.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/ingest", handleIngest)
		slog.Info("ingest server listening", "addr", *flagIngestAddr)
		if err := http.ListenAndServe(*flagIngestAddr, mux); err != nil {
			slog.Error("ingest server failed", "err", err)
			os.Exit(1)
		}
	}()

	// Cut loop: run immediately (replays any past-grace windows present on disk
	// from before a restart), then on every tick.
	cutTick(ctx, s3c)
	ticker := time.NewTicker(*flagCutInterval)
	defer ticker.Stop()
	for range ticker.C {
		cutTick(ctx, s3c)
	}
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			*flagAccessKey, *flagSecretKey, "",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	endpoint := "http://" + *flagEndpoint
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}

// handleIngest receives one per-emit TSDB block as a tar stream and durably
// writes it into the served dir under its ULID. The agent should drop its local
// copy only after a 200 response (the block is then on the merger's WAL).
func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body := http.MaxBytesReader(w, r.Body, *flagMaxBlockMiB<<20)

	tmp, err := os.MkdirTemp(*flagServedDir, ".ingest-")
	if err != nil {
		http.Error(w, "mkdtemp: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(tmp)

	if err := extractTar(body, tmp); err != nil {
		http.Error(w, "extract: "+err.Error(), http.StatusBadRequest)
		return
	}

	// gorillas3 tars block files under a "<ulid>/" prefix (readTSDBBlockFiles),
	// so the extracted block dir is either tmp itself or a single tmp/<ulid>/
	// subdir. Locate the dir that actually holds meta.json.
	blockSrc, err := findBlockDir(tmp)
	if err != nil {
		http.Error(w, "locate block: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Derive the ULID from the query param, falling back to the block's meta.json.
	ulidStr := r.URL.Query().Get("ulid")
	if ulidStr == "" {
		if bi, err := readBlockMeta(blockSrc); err == nil {
			ulidStr = bi.ULID
		}
	}
	if ulidStr == "" || strings.ContainsAny(ulidStr, "/.") {
		http.Error(w, "missing or invalid block ulid", http.StatusBadRequest)
		return
	}

	syncDir(blockSrc)
	dst := filepath.Join(*flagServedDir, ulidStr)

	mu.Lock()
	if _, statErr := os.Stat(dst); statErr == nil {
		// Idempotent: a re-delivered block is already present.
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}
	renErr := os.Rename(blockSrc, dst)
	mu.Unlock()
	if renErr != nil {
		http.Error(w, "commit: "+renErr.Error(), http.StatusInternalServerError)
		return
	}
	syncDir(*flagServedDir)
	metricBlocksIngested.Inc()
	w.WriteHeader(http.StatusOK)
}

// findBlockDir returns the directory under root that holds meta.json — either
// root itself (bare block files) or its single immediate subdir (gorillas3's
// "<ulid>/"-prefixed layout).
func findBlockDir(root string) (string, error) {
	if _, err := os.Stat(filepath.Join(root, "meta.json")); err == nil {
		return root, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.IsDir() {
			sub := filepath.Join(root, e.Name())
			if _, err := os.Stat(filepath.Join(sub, "meta.json")); err == nil {
				return sub, nil
			}
		}
	}
	return "", fmt.Errorf("no meta.json found in uploaded block")
}

// extractTar unpacks a tar stream into destDir, rejecting unsafe paths and
// fsync'ing each regular file so the block survives a crash once committed.
func extractTar(r io.Reader, destDir string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(hdr.Name)
		if name == "." {
			continue
		}
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("unsafe tar path: %q", hdr.Name)
		}
		target := filepath.Join(destDir, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// cutTick scans the served dir, cuts any completed windows (past end+grace) that
// still hold per-emit blocks, and expires windows older than retention.
func cutTick(ctx context.Context, s3c *s3.Client) {
	windowMs := flagWindow.Milliseconds()
	if windowMs <= 0 {
		slog.Error("window must be positive", "window", *flagWindow)
		return
	}
	graceMs := flagGrace.Milliseconds()
	nowMs := time.Now().UnixMilli()
	curWindow := windowOf(nowMs, windowMs)
	oldestWindow := curWindow - (numRetainedWindows(*flagRetention, *flagWindow) - 1)

	type cutJob struct {
		w   int64
		src []string // per-emit block dirs (+ existing cut block) to merge
	}
	var jobs []cutJob

	mu.Lock()
	entries, err := os.ReadDir(*flagServedDir)
	if err != nil {
		mu.Unlock()
		slog.Error("read served dir", "err", err)
		return
	}
	headByWindow := map[int64][]string{} // Level-1 per-emit blocks
	cutByWindow := map[int64]string{}    // Level-2 cut block dir
	var headCount int
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) == 0 || e.Name()[0] == '.' {
			continue
		}
		dir := filepath.Join(*flagServedDir, e.Name())
		meta, err := readBlockMeta(dir)
		if err != nil {
			continue
		}
		w := windowOf(meta.MinTime, windowMs)
		if meta.Compaction.Level >= 2 {
			cutByWindow[w] = dir
		} else {
			headByWindow[w] = append(headByWindow[w], dir)
			headCount++
		}
	}

	// Expire windows older than retention (both cut blocks and any stray head blocks).
	for w, dir := range cutByWindow {
		if w < oldestWindow {
			slog.Info("expiring cut window block", "window", w)
			os.RemoveAll(dir)
			delete(cutByWindow, w)
		}
	}
	for w, dirs := range headByWindow {
		if w < oldestWindow {
			for _, d := range dirs {
				os.RemoveAll(d)
			}
			delete(headByWindow, w)
		}
	}

	// A completed window (past end+grace) that still has per-emit blocks must be
	// cut. Merge in the prior cut block too, so a late-arriving per-emit block is
	// folded in without losing the already-cut data.
	for w, dirs := range headByWindow {
		windowEnd := (w + 1) * windowMs
		if nowMs >= windowEnd+graceMs {
			src := append([]string{}, dirs...)
			if cut, ok := cutByWindow[w]; ok {
				src = append(src, cut)
			}
			jobs = append(jobs, cutJob{w: w, src: src})
		}
	}
	mu.Unlock()

	for _, j := range jobs {
		if err := cutWindow(ctx, s3c, j.w, windowMs, j.src); err != nil {
			slog.Error("cut window", "window", j.w, "err", err)
			metricCutErrors.Inc()
		}
	}

	metricHeadBlocks.Set(float64(headCount))
	metricCutWindows.Set(float64(len(cutByWindow)))
}

// cutWindow concatenates the per-series chunks of a completed window's source
// blocks into one Level-2 block, uploads it to S3, then atomically swaps it into
// the served dir and prunes the source blocks. Time bounds are clamped to the
// window's half-open interval so windows never overlap.
func cutWindow(ctx context.Context, s3c *s3.Client, w int64, windowMs int64, srcDirs []string) error {
	windowStart := w * windowMs
	windowEnd := (w + 1) * windowMs // exclusive

	tmpDir, err := os.MkdirTemp(*flagServedDir, ".cutting-")
	if err != nil {
		return fmt.Errorf("mkdtemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	newUID, nSeries, err := buildWindowBlock(ctx, srcDirs, windowStart, windowEnd, tmpDir)
	if err != nil {
		return fmt.Errorf("build cut block: %w", err)
	}

	// Empty window: nothing worth a block — just prune the source blocks so the
	// window stops being re-scanned.
	if nSeries == 0 {
		mu.Lock()
		for _, d := range srcDirs {
			os.RemoveAll(d)
		}
		mu.Unlock()
		slog.Info("cut window: no series, pruned sources", "window", w, "src_blocks", len(srcDirs))
		return nil
	}
	cutBlockDir := filepath.Join(tmpDir, newUID.String())

	// Upload to S3 FIRST so the archive has the window before the local per-emit
	// blocks (the only other copy) are pruned. If a crash happens after upload but
	// before the local swap, the next tick re-cuts (new ULID, duplicate object in
	// S3 — harmless, Thanos dedups identical chunks at query time).
	if err := uploadBlockToS3(ctx, s3c, *flagBucket, newUID.String(), cutBlockDir); err != nil {
		return fmt.Errorf("upload cut block: %w", err)
	}
	metricBlocksUploaded.Inc()

	dst := filepath.Join(*flagServedDir, newUID.String())
	mu.Lock()
	renErr := os.Rename(cutBlockDir, dst)
	if renErr == nil {
		// Prune the source blocks (per-emit blocks + any prior cut block for this
		// window); they are now represented by the single new cut block.
		for _, d := range srcDirs {
			os.RemoveAll(d)
		}
	}
	mu.Unlock()
	if renErr != nil {
		return fmt.Errorf("commit cut block: %w", renErr)
	}
	syncDir(*flagServedDir)

	metricLastCutTime.SetToCurrentTime()
	slog.Info("cut+flushed window",
		"window", w, "ulid", newUID, "series", nSeries, "src_blocks", len(srcDirs))
	return nil
}

// buildWindowBlock reads the in-window chunks of srcDirs, concatenates the
// per-series Gorilla chunks (copying chunk bytes — no sample decode/re-encode),
// deduplicates overlapping chunks, and writes ONE Level-2 block into destDir.
// Returns the new block's ULID and the number of series written (0 ⇒ no block
// written because the window had no in-range samples).
func buildWindowBlock(ctx context.Context, srcDirs []string, windowStart, windowEnd int64, destDir string) (ulid.ULID, int, error) {
	byKey := map[string]*seriesEntry{}
	for _, dir := range srcDirs {
		if err := readBlockSeries(ctx, dir, windowStart, windowEnd-1, byKey); err != nil {
			slog.Warn("read block series", "dir", dir, "err", err)
		}
	}

	// Sort series by labels; deduplicate overlapping chunks within each series.
	all := make([]*seriesEntry, 0, len(byKey))
	for _, se := range byKey {
		sort.Slice(se.chks, func(i, j int) bool { return se.chks[i].MinTime < se.chks[j].MinTime })
		deduped := se.chks[:0]
		lastMax := int64(math.MinInt64)
		for _, chk := range se.chks {
			if chk.MinTime <= lastMax {
				continue // overlapping chunk from same series in adjacent blocks
			}
			deduped = append(deduped, chk)
			lastMax = chk.MaxTime
		}
		se.chks = deduped
		if len(se.chks) > 0 {
			all = append(all, se)
		}
	}
	if len(all) == 0 {
		return ulid.ULID{}, 0, nil
	}

	sort.Slice(all, func(i, j int) bool {
		return labels.Compare(all[i].lset, all[j].lset) < 0
	})

	uid, err := writeMergedBlock(ctx, destDir, all, windowStart, windowEnd)
	if err != nil {
		return ulid.ULID{}, 0, err
	}
	return uid, len(all), nil
}

// uploadBlockToS3 uploads every file of a local block dir to the bucket under the
// `<ulid>/` prefix, writing meta.json LAST so readers see a complete block
// (Thanos uses meta.json's presence as the readiness signal).
func uploadBlockToS3(ctx context.Context, s3c *s3.Client, bucket, ulidStr, blockDir string) error {
	var files []string
	if err := filepath.WalkDir(blockDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool {
		mi := strings.HasSuffix(files[i], "meta.json")
		mj := strings.HasSuffix(files[j], "meta.json")
		if mi != mj {
			return mj // non-meta first; meta.json last
		}
		return files[i] < files[j]
	})
	for _, path := range files {
		rel, err := filepath.Rel(blockDir, path)
		if err != nil {
			return err
		}
		key := ulidStr + "/" + filepath.ToSlash(rel)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(data),
		}); err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}
	}
	return nil
}

// syncDir fsyncs a directory so a rename into/within it is durable.
func syncDir(dir string) {
	if f, err := os.Open(dir); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
}

// cleanTempDirs removes partial temp dirs (.ingest-*/.cutting-*) left by a crash.
func cleanTempDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) > 0 && e.Name()[0] == '.' {
			os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

func readBlockMeta(dir string) (blockInfo, error) {
	body, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return blockInfo{}, err
	}
	var bi blockInfo
	return bi, json.Unmarshal(body, &bi)
}

// readBlockSeries opens a TSDB block directory and appends its in-window chunks
// into the byKey accumulator map.
func readBlockSeries(ctx context.Context, dir string, mint, maxt int64, byKey map[string]*seriesEntry) error {
	ir, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return fmt.Errorf("open index: %w", err)
	}
	defer ir.Close()

	cr, err := chunks.NewDirReader(filepath.Join(dir, "chunks"), nil)
	if err != nil {
		return fmt.Errorf("open chunks dir: %w", err)
	}
	defer cr.Close()

	n, v := index.AllPostingsKey()
	p, err := ir.Postings(ctx, n, v)
	if err != nil {
		return fmt.Errorf("all postings: %w", err)
	}

	var builder labels.ScratchBuilder
	var chkMetas []chunks.Meta

	for p.Next() {
		chkMetas = chkMetas[:0]
		if err := ir.Series(p.At(), &builder, &chkMetas); err != nil {
			slog.Warn("read series from index", "ref", p.At(), "err", err)
			continue
		}
		lset := builder.Labels()

		var filtered []chunks.Meta
		for i := range chkMetas {
			chk := chkMetas[i]
			if chk.MaxTime < mint || chk.MinTime > maxt {
				continue
			}
			c, _, err := cr.ChunkOrIterable(chk)
			if err != nil {
				slog.Warn("read chunk data", "err", err)
				continue
			}
			// Deep-copy bytes: the DirReader is mmap-backed and closes on return.
			raw := make([]byte, len(c.Bytes()))
			copy(raw, c.Bytes())
			copied, err := chunkenc.FromData(c.Encoding(), raw)
			if err != nil {
				slog.Warn("copy chunk data", "err", err)
				continue
			}
			chk.Chunk = copied
			filtered = append(filtered, chk)
		}
		if len(filtered) == 0 {
			continue
		}

		key := lset.String()
		se := byKey[key]
		if se == nil {
			se = &seriesEntry{lset: lset.Copy()}
			byKey[key] = se
		}
		se.chks = append(se.chks, filtered...)
	}
	return p.Err()
}

// writeMergedBlock writes a Prometheus TSDB block from sorted series into dir.
// Returns the ULID of the written block.
// NOTE: chunks.Writer.WriteChunks updates the Ref field of each Meta in-place,
// so the same se.chks slice is reused for index.Writer.AddSeries.
func writeMergedBlock(ctx context.Context, dir string, series []*seriesEntry, mint, maxt int64) (ulid.ULID, error) {
	id := ulid.Make()
	blockDir := filepath.Join(dir, id.String())
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		return ulid.ULID{}, err
	}

	var numChunks uint64
	var actualMin, actualMax int64 = math.MaxInt64, math.MinInt64

	chunkw, err := chunks.NewWriter(filepath.Join(blockDir, "chunks"))
	if err != nil {
		return ulid.ULID{}, fmt.Errorf("create chunks writer: %w", err)
	}
	for _, se := range series {
		// WriteChunks updates se.chks[i].Ref in-place for the index writer.
		if err := chunkw.WriteChunks(se.chks...); err != nil {
			chunkw.Close()
			return ulid.ULID{}, fmt.Errorf("write chunks: %w", err)
		}
		numChunks += uint64(len(se.chks))
		for _, chk := range se.chks {
			if chk.MinTime < actualMin {
				actualMin = chk.MinTime
			}
			if chk.MaxTime > actualMax {
				actualMax = chk.MaxTime
			}
		}
	}
	if err := chunkw.Close(); err != nil {
		return ulid.ULID{}, fmt.Errorf("close chunks writer: %w", err)
	}

	idxw, err := index.NewWriter(ctx, filepath.Join(blockDir, "index"))
	if err != nil {
		return ulid.ULID{}, fmt.Errorf("create index writer: %w", err)
	}
	for _, sym := range collectSymbols(series) {
		if err := idxw.AddSymbol(sym); err != nil {
			idxw.Close()
			return ulid.ULID{}, fmt.Errorf("add symbol: %w", err)
		}
	}
	for i, se := range series {
		if err := idxw.AddSeries(storage.SeriesRef(i+1), se.lset, se.chks...); err != nil {
			idxw.Close()
			return ulid.ULID{}, fmt.Errorf("add series: %w", err)
		}
	}
	if err := idxw.Close(); err != nil {
		return ulid.ULID{}, fmt.Errorf("close index writer: %w", err)
	}

	minT, maxT := actualMin, actualMax+1
	if actualMin == math.MaxInt64 {
		minT, maxT = mint, maxt
	}
	meta := tsdb.BlockMeta{
		ULID:    id,
		MinTime: minT,
		MaxTime: maxT,
		Stats: tsdb.BlockStats{
			NumSeries: uint64(len(series)),
			NumChunks: numChunks,
		},
		Compaction: tsdb.BlockMetaCompaction{
			Level:   2,
			Sources: []ulid.ULID{id},
		},
		Version: 1,
	}
	body, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return ulid.ULID{}, err
	}
	if err := os.WriteFile(filepath.Join(blockDir, "meta.json"), body, 0o644); err != nil {
		return ulid.ULID{}, err
	}

	return id, nil
}

// collectSymbols gathers all label name/value strings from series (sorted, deduped).
func collectSymbols(series []*seriesEntry) []string {
	set := map[string]struct{}{}
	for _, se := range series {
		se.lset.Range(func(l labels.Label) {
			set[l.Name] = struct{}{}
			set[l.Value] = struct{}{}
		})
	}
	syms := make([]string, 0, len(set))
	for s := range set {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	return syms
}
