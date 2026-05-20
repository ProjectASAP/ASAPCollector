// gorilla-buffer-merger — tumbling-window TSDB block merger for gorilla-buffer-store.
//
// Polls MinIO every poll-interval for 60-second TSDB blocks written by gorillas3 agents.
// Buckets each block into a FIXED, NON-OVERLAPPING tumbling window of size -window
// (default 1h) by its start time, and merges all blocks in a window into ONE output
// block keyed by that window. The output directory holds one merged block per retained
// window, so gorilla-buffer-store can serve a continuous recent range.
//
// Tumbling-window invariant:
//   - A block belongs to window w = floor(block.minTime / window).
//   - Window w covers the half-open interval [w*window, (w+1)*window).
//   - Each window's blocks merge into exactly one output block (re-merged every poll
//     while the window is still in progress — i.e. while new 60s blocks keep landing).
//   - Once a window is complete (now ≥ window_end + grace), its merged block is
//     finalized and is NOT re-merged again.
//   - The output dir retains the last N windows (N derived from retention / window),
//     dropping windows older than retention so the hot range stays queryable until
//     thanos-store-gateway syncs the same blocks from MinIO.
//
// Example (window = 1h, blocks b1..b120 arriving over 2h):
//
//	b1..b60   (minTime in [0h, 1h)) → window 0 → merged block M0 = [b1+...+b60]
//	b61..b120 (minTime in [1h, 2h)) → window 1 → merged block M1 = [b61+...+b120]
//
// M0 and M1 are distinct, non-overlapping merged outputs (not a single rolling block).
//
// The output directory is served by a thanos store container via FILESYSTEM objstore.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
	flagBucket       = flag.String("bucket", "asap-gorilla-tsdb", "MinIO/S3 bucket name")
	flagEndpoint     = flag.String("endpoint", "minio:9000", "MinIO endpoint (host:port, no scheme)")
	flagAccessKey    = flag.String("access-key", "asap", "S3 access key")
	flagSecretKey    = flag.String("secret-key", "asap-local-only", "S3 secret key")
	flagWindow       = flag.Duration("window", time.Hour, "Tumbling window duration: fixed-size, non-overlapping; configurable, default 1h. This is the granularity at which merged state is flushed to the backend S3/MinIO archive; the agent's per-block emit interval (tsdb_block_duration) is configured independently.")
	flagRetention    = flag.Duration("retention", 0, "How far back to keep merged windows queryable (0 = same as -window). N retained windows = ceil(retention/window), min 2 so the current + previous window bridge boundaries.")
	flagGrace        = flag.Duration("grace", time.Minute, "Grace period after a window's end before it is finalized (no further re-merge), to absorb late-arriving 60s blocks.")
	flagPollInterval = flag.Duration("poll-interval", 15*time.Second, "MinIO poll interval")
	flagStagingDir   = flag.String("staging-dir", "/var/gorilla-buffer/staging", "Local staging dir for downloaded blocks")
	flagOutputDir    = flag.String("output-dir", "/var/gorilla-buffer/merged", "Output dir for merged blocks (served by thanos store), one block per retained window")
	flagMetricsAddr  = flag.String("metrics-addr", ":9100", "Prometheus metrics endpoint address (empty to disable)")
)

// Self-monitoring counters exposed on /metrics.
var (
	metricPollCycles = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gorilla_merger_poll_cycles_total",
		Help: "Total MinIO poll cycles.",
	})
	metricMergeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gorilla_merger_errors_total",
		Help: "Total errors during poll/merge cycles.",
	})
	metricStagingBlocks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gorilla_merger_staging_blocks",
		Help: "Number of retained (in-retention) staging blocks after the last poll.",
	})
	metricMergedWindows = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gorilla_merger_merged_windows",
		Help: "Number of merged output blocks (one per retained tumbling window) after the last poll.",
	})
	metricLastMergeTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gorilla_merger_last_merge_success_timestamp_seconds",
		Help: "Unix timestamp of the last successful merge cycle.",
	})
)

// blockInfo holds the time-range fields we need from a block's meta.json.
type blockInfo struct {
	ULID    string `json:"ulid"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
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
	slog.Info("gorilla-buffer-merger starting (tumbling window)",
		"bucket", *flagBucket,
		"endpoint", *flagEndpoint,
		"window", *flagWindow,
		"retention", *flagRetention,
		"grace", *flagGrace,
		"poll", *flagPollInterval,
		"retained_windows", numRetainedWindows(*flagRetention, *flagWindow),
	)

	for _, d := range []string{*flagStagingDir, *flagOutputDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			slog.Error("create dir", "path", d, "err", err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	s3c, err := newS3Client(ctx)
	if err != nil {
		slog.Error("create S3 client", "err", err)
		os.Exit(1)
	}

	// Start Prometheus metrics server.
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

	// Run immediately, then on every tick.
	metricPollCycles.Inc()
	if err := tick(ctx, s3c); err != nil {
		slog.Error("initial sync", "err", err)
		metricMergeErrors.Inc()
	} else {
		metricLastMergeTime.SetToCurrentTime()
	}
	ticker := time.NewTicker(*flagPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		metricPollCycles.Inc()
		if err := tick(ctx, s3c); err != nil {
			slog.Error("sync error", "err", err)
			metricMergeErrors.Inc()
		} else {
			metricLastMergeTime.SetToCurrentTime()
		}
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

func tick(ctx context.Context, s3c *s3.Client) error {
	now := time.Now()
	windowMs := flagWindow.Milliseconds()
	if windowMs <= 0 {
		return fmt.Errorf("window must be positive, got %s", *flagWindow)
	}
	graceMs := flagGrace.Milliseconds()
	nowMs := now.UnixMilli()

	// Tumbling-window bookkeeping.
	curWindow := windowOf(nowMs, windowMs)
	nWindows := numRetainedWindows(*flagRetention, *flagWindow)
	// Oldest window index we still retain. Windows < oldestWindow are expired.
	oldestWindow := curWindow - (nWindows - 1)

	// Step 1: list all TSDB block ULIDs in MinIO.
	minioBlocks, err := listMinIOBlocks(ctx, s3c, *flagBucket)
	if err != nil {
		return fmt.Errorf("list minio blocks: %w", err)
	}

	// Step 2: download any new in-retention blocks to staging. A block belongs
	// to the tumbling window of its start time (minTime); only blocks whose
	// window is still retained are downloaded.
	for _, mb := range minioBlocks {
		if windowOf(mb.MinTime, windowMs) < oldestWindow {
			continue
		}
		localDir := filepath.Join(*flagStagingDir, mb.ULID)
		if _, err := os.Stat(localDir); os.IsNotExist(err) {
			slog.Info("downloading block", "ulid", mb.ULID, "window", windowOf(mb.MinTime, windowMs))
			if dlErr := downloadBlock(ctx, s3c, *flagBucket, mb.ULID, localDir); dlErr != nil {
				slog.Error("download block", "ulid", mb.ULID, "err", dlErr)
				os.RemoveAll(localDir)
			}
		}
	}

	// Step 3: audit staging — bucket retained blocks by tumbling window, drop expired.
	entries, err := os.ReadDir(*flagStagingDir)
	if err != nil {
		return fmt.Errorf("read staging dir: %w", err)
	}
	byWindow := map[int64][]string{} // window index → staging block dirs
	var retained int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(*flagStagingDir, e.Name())
		meta, err := readBlockMeta(dir)
		if err != nil {
			slog.Warn("read meta.json", "dir", e.Name(), "err", err)
			continue
		}
		w := windowOf(meta.MinTime, windowMs)
		if w < oldestWindow {
			slog.Info("dropping expired staging block", "ulid", meta.ULID, "window", w)
			os.RemoveAll(dir)
			continue
		}
		byWindow[w] = append(byWindow[w], dir)
		retained++
	}
	metricStagingBlocks.Set(float64(retained))

	// Step 4: for each retained window, ensure a single merged output block exists.
	// A window is finalized (frozen, never re-merged) once now ≥ window_end + grace.
	// Until then it is re-merged every poll as new 60s blocks land.
	if err := os.MkdirAll(*flagOutputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	// existingMerged maps window index → output block dir name (ULID) already on disk.
	existingMerged := readMergedWindows(*flagOutputDir, windowMs)

	windows := make([]int64, 0, len(byWindow))
	for w := range byWindow {
		windows = append(windows, w)
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })

	var firstErr error
	for _, w := range windows {
		windowEnd := (w + 1) * windowMs
		finalized := nowMs >= windowEnd+graceMs
		_, alreadyMerged := existingMerged[w]

		// Skip re-merge of a window that is finalized AND already has its merged
		// block on disk — its output is frozen.
		if finalized && alreadyMerged {
			continue
		}
		if err := mergeWindow(ctx, w, byWindow[w], *flagOutputDir, existingMerged[w], windowMs, finalized); err != nil {
			slog.Warn("merge window", "window", w, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Step 5: expire merged output blocks for windows older than retention.
	expireMergedWindows(*flagOutputDir, windowMs, oldestWindow)

	// Report retained merged-window count.
	metricMergedWindows.Set(float64(len(readMergedWindows(*flagOutputDir, windowMs))))

	if retained == 0 {
		slog.Info("no in-retention blocks")
	}
	return firstErr
}

// listMinIOBlocks enumerates top-level TSDB block directories in the bucket.
// Each block is a "directory" prefix {ULID}/ containing meta.json, chunks/, index.
func listMinIOBlocks(ctx context.Context, s3c *s3.Client, bucket string) ([]blockInfo, error) {
	var blocks []blockInfo
	var token *string
	for {
		resp, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Delimiter:         aws.String("/"),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("ListObjectsV2: %w", err)
		}
		for _, cp := range resp.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			ulidStr := (*cp.Prefix)[:len(*cp.Prefix)-1]
			metaKey := ulidStr + "/meta.json"
			obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &metaKey})
			if err != nil {
				slog.Warn("fetch meta.json", "ulid", ulidStr, "err", err)
				continue
			}
			body, _ := io.ReadAll(obj.Body)
			obj.Body.Close()
			var bi blockInfo
			if json.Unmarshal(body, &bi) == nil {
				blocks = append(blocks, bi)
			}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	return blocks, nil
}

// downloadBlock fetches all S3 objects under the {ulidStr}/ prefix into destDir.
func downloadBlock(ctx context.Context, s3c *s3.Client, bucket, ulidStr, destDir string) error {
	prefix := ulidStr + "/"
	var token *string
	for {
		resp, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return fmt.Errorf("ListObjectsV2: %w", err)
		}
		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key
			rel := key[len(prefix):]
			if rel == "" {
				continue
			}
			if err := downloadFile(ctx, s3c, bucket, key, filepath.Join(destDir, filepath.FromSlash(rel))); err != nil {
				return fmt.Errorf("download %s: %w", key, err)
			}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	return nil
}

func downloadFile(ctx context.Context, s3c *s3.Client, bucket, key, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	obj, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return err
	}
	defer obj.Body.Close()
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, obj.Body)
	return err
}

func readBlockMeta(dir string) (blockInfo, error) {
	body, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return blockInfo{}, err
	}
	var bi blockInfo
	return bi, json.Unmarshal(body, &bi)
}

// readMergedWindows scans the output dir and maps each merged block's tumbling
// window index (derived from its meta.json minTime) → block dir name (ULID).
// Skips temp (".merging-*") dirs. If two blocks somehow share a window, the
// lexicographically-last ULID wins (newer ULIDs sort later).
func readMergedWindows(outputDir string, windowMs int64) map[int64]string {
	out := map[int64]string{}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return out
	}
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) == 0 || e.Name()[0] == '.' {
			continue
		}
		dir := filepath.Join(outputDir, e.Name())
		meta, err := readBlockMeta(dir)
		if err != nil {
			continue
		}
		w := windowOf(meta.MinTime, windowMs)
		if prev, ok := out[w]; !ok || e.Name() > prev {
			out[w] = e.Name()
		}
	}
	return out
}

// expireMergedWindows removes merged output blocks whose tumbling window is
// older than oldestWindow (i.e. fell out of the retention range).
func expireMergedWindows(outputDir string, windowMs, oldestWindow int64) {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) == 0 || e.Name()[0] == '.' {
			continue
		}
		dir := filepath.Join(outputDir, e.Name())
		meta, err := readBlockMeta(dir)
		if err != nil {
			continue
		}
		if windowOf(meta.MinTime, windowMs) < oldestWindow {
			slog.Info("expiring merged window block", "ulid", e.Name(), "window", windowOf(meta.MinTime, windowMs))
			os.RemoveAll(dir)
		}
	}
}

// mergeWindow merges all source blocks of a single tumbling window into one
// output block, then atomically swaps it in place of any prior merged block for
// the same window (prevReplace). The merged block's time bounds are clamped to
// the window's half-open interval [w*window, (w+1)*window) so windows never
// overlap, regardless of straggler chunks that bleed slightly past the boundary.
func mergeWindow(ctx context.Context, w int64, srcDirs []string, outputDir, prevReplace string, windowMs int64, finalized bool) error {
	windowStart := w * windowMs
	windowEnd := (w + 1) * windowMs // exclusive

	byKey := map[string]*seriesEntry{}
	for _, dir := range srcDirs {
		// Clamp to the window's interval so a single block's chunks can only ever
		// land in one window's merged output (chunks are 60s blocks, well inside).
		if err := readBlockSeries(ctx, dir, windowStart, windowEnd-1, byKey); err != nil {
			slog.Warn("read block series", "dir", dir, "err", err)
		}
	}
	if len(byKey) == 0 {
		slog.Info("no series after filter, skipping write", "window", w)
		return nil
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
	sort.Slice(all, func(i, j int) bool {
		return labels.Compare(all[i].lset, all[j].lset) < 0
	})

	// Write to a temp subdir inside outputDir, then rename atomically.
	tmpDir, err := os.MkdirTemp(outputDir, ".merging-")
	if err != nil {
		return fmt.Errorf("mkdtemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	newUID, err := writeMergedBlock(ctx, tmpDir, all, windowStart, windowEnd)
	if err != nil {
		return fmt.Errorf("write merged block: %w", err)
	}

	newDst := filepath.Join(outputDir, newUID.String())
	if err := os.Rename(filepath.Join(tmpDir, newUID.String()), newDst); err != nil {
		return fmt.Errorf("move merged block: %w", err)
	}
	os.RemoveAll(tmpDir)

	// Replace the prior merged block for THIS window only (if any), after the
	// new one is in place. Other windows' merged blocks are left untouched.
	if prevReplace != "" && prevReplace != newUID.String() {
		slog.Info("replacing merged window block", "window", w, "old", prevReplace, "new", newUID.String())
		os.RemoveAll(filepath.Join(outputDir, prevReplace))
	}

	slog.Info("merged window block written",
		"window", w, "ulid", newUID, "series", len(all), "src_blocks", len(srcDirs), "finalized", finalized)
	return nil
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
