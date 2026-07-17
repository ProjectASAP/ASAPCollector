// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.uber.org/zap"
)

// spoolFileExt is the suffix of a spooled (gzipped ASAPFRG1) batch on disk.
// The file body is exactly the wire body, so a re-ship is a plain POST of the
// file bytes — no re-encode.
const spoolFileExt = ".asapfrg.gz"

// shipWorker decouples the flush goroutine from the network. flushAll hands it
// an encoded (gzipped ASAPFRG1) batch over a buffered channel; the worker tries
// to ship it and, on failure, persists it to a bounded on-disk spool. A
// periodic loop re-ships spooled batches and deletes them on success, so a ship
// outage is durable across the window flush cadence (and process restarts —
// spooled files survive on disk).
type shipWorker struct {
	shipper *fragmentShipper
	logger  *zap.Logger

	spoolDir      string
	spoolMaxBytes int64
	retryEvery    time.Duration

	in     chan []byte // encoded (gzipped) batches from flushAll
	stopCh chan struct{}
	doneCh chan struct{}

	// shipTimeout bounds a single ship attempt's context (the worker uses its
	// own context, not the flush ctx, so a flush returning does not cancel an
	// in-flight ship). Derived from the shipper client timeout + retries.
	shipTimeout time.Duration

	// mu guards spool filesystem mutations across the worker loop, the retry
	// loop, and the cap enforcement so concurrent enqueue + retry stay
	// consistent. The actual ship I/O happens outside the lock.
	mu sync.Mutex

	// started records whether run() was launched. shutdown only waits on
	// doneCh when started, so a worker that was constructed but never start()ed
	// (e.g. a unit test that drives the processor directly) can be shut down
	// without blocking on a goroutine that never ran.
	started atomic.Bool
}

func newShipWorker(s *fragmentShipper, cold ColdConfig, logger *zap.Logger) *shipWorker {
	return &shipWorker{
		shipper:       s,
		logger:        logger,
		spoolDir:      cold.SpoolDir,
		spoolMaxBytes: cold.SpoolMaxBytes,
		retryEvery:    cold.SpoolRetryInterval,
		in:            make(chan []byte, cold.ShipQueueDepth),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		shipTimeout:   2 * time.Minute,
	}
}

// start launches the worker + retry loops. No-op shippers (build-only mode)
// still start so enqueue can drop cleanly, but they never touch disk.
func (w *shipWorker) start() {
	if w == nil || !w.started.CompareAndSwap(false, true) {
		return
	}
	if w.spoolDir != "" {
		if err := os.MkdirAll(w.spoolDir, 0o755); err != nil {
			w.logger.Warn("asap_edge: cannot create spool dir; ship failures will be dropped",
				zap.String("dir", w.spoolDir), zap.Error(err))
			w.spoolDir = ""
		}
	}
	go w.run()
}

// enqueue hands an encoded batch to the worker without blocking the flush
// goroutine. If the in-flight queue is full (worker stalled on a slow ship), the
// batch is spooled directly so the flush never blocks on the network. Returns
// immediately.
func (w *shipWorker) enqueue(body []byte) {
	if w == nil || w.shipper.noop() || len(body) == 0 {
		return
	}
	select {
	case w.in <- body:
	default:
		// Queue full: persist directly rather than block the flush goroutine.
		if err := w.spool(body); err != nil {
			w.logger.Warn("asap_edge: queue full and spool failed; dropping batch", zap.Error(err))
		}
	}
}

// run is the worker goroutine: drain the in-flight queue (ship-or-spool) and,
// on a ticker, re-ship spooled batches.
func (w *shipWorker) run() {
	defer close(w.doneCh)
	retry := w.retryEvery
	if retry <= 0 {
		retry = 30 * time.Second
	}
	t := time.NewTicker(retry)
	defer t.Stop()
	for {
		select {
		case body := <-w.in:
			w.shipOrSpool(context.Background(), body)
		case <-t.C:
			w.drainSpool(context.Background())
		case <-w.stopCh:
			return
		}
	}
}

// shipOrSpool attempts to ship a freshly-flushed batch; on failure it persists
// the body to the spool for the retry loop to pick up.
func (w *shipWorker) shipOrSpool(parent context.Context, body []byte) {
	ctx, cancel := context.WithTimeout(parent, w.shipTimeout)
	err := w.shipper.shipEncoded(ctx, body)
	cancel()
	if err == nil {
		return
	}
	w.logger.Warn("asap_edge: ship failed; spooling batch for retry", zap.Error(err))
	if serr := w.spool(body); serr != nil {
		w.logger.Warn("asap_edge: spool write failed; dropping batch", zap.Error(serr))
	}
}

// spool writes body to a uniquely-named file under spoolDir, then enforces the
// size cap (dropping the oldest files). No-op if spooling is disabled.
func (w *shipWorker) spool(body []byte) error {
	if w.spoolDir == "" {
		return fmt.Errorf("spool disabled (no spool dir)")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// Monotonic, sortable name (UnixNano + pid) so drain re-ships oldest-first
	// and the cap evicts oldest-first.
	name := fmt.Sprintf("%020d-%d%s", time.Now().UnixNano(), os.Getpid(), spoolFileExt)
	path := filepath.Join(w.spoolDir, name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return fmt.Errorf("write spool tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename spool file: %w", err)
	}
	w.enforceCapLocked()
	return nil
}

// enforceCapLocked drops oldest spooled files until total size is within
// spoolMaxBytes. Caller holds w.mu. A non-positive cap means unbounded.
func (w *shipWorker) enforceCapLocked() {
	if w.spoolMaxBytes <= 0 {
		return
	}
	files, total := w.listSpoolLocked()
	for total > w.spoolMaxBytes && len(files) > 0 {
		oldest := files[0]
		files = files[1:]
		if err := os.Remove(oldest.path); err != nil {
			w.logger.Warn("asap_edge: spool cap evict failed", zap.String("file", oldest.path), zap.Error(err))
			// Avoid a hot loop if a file is un-removable.
			break
		}
		total -= oldest.size
		w.logger.Warn("asap_edge: spool over cap; dropped oldest batch (permanent cold-archive hole)",
			zap.String("file", oldest.path), zap.Int64("bytes", oldest.size), zap.Int64("cap_bytes", w.spoolMaxBytes))
	}
}

type spoolEntry struct {
	path string
	size int64
}

// listSpoolLocked returns spooled batch files oldest-first (by name, which is
// time-ordered) plus their total size. Caller holds w.mu. Skips .tmp files.
func (w *shipWorker) listSpoolLocked() ([]spoolEntry, int64) {
	ents, err := os.ReadDir(w.spoolDir)
	if err != nil {
		return nil, 0
	}
	out := make([]spoolEntry, 0, len(ents))
	var total int64
	for _, e := range ents {
		if e.IsDir() || filepath.Ext(e.Name()) != ".gz" {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		out = append(out, spoolEntry{path: filepath.Join(w.spoolDir, e.Name()), size: info.Size()})
		total += info.Size()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, total
}

// drainSpool re-ships every spooled batch oldest-first, deleting each on a
// successful POST. Stops early if the context is cancelled (e.g. the Shutdown
// deadline) so the final drain respects the caller's budget.
func (w *shipWorker) drainSpool(ctx context.Context) {
	if w.spoolDir == "" || w.shipper.noop() {
		return
	}
	w.mu.Lock()
	files, _ := w.listSpoolLocked()
	w.mu.Unlock()
	for _, f := range files {
		select {
		case <-ctx.Done():
			return
		default:
		}
		body, err := os.ReadFile(f.path)
		if err != nil {
			w.logger.Warn("asap_edge: read spooled batch failed", zap.String("file", f.path), zap.Error(err))
			continue
		}
		if serr := w.shipper.shipEncoded(ctx, body); serr != nil {
			// Still failing — leave it on disk for the next tick.
			w.logger.Debug("asap_edge: spooled batch re-ship failed; will retry", zap.String("file", f.path), zap.Error(serr))
			return // network still down; don't hammer the rest this tick
		}
		w.mu.Lock()
		_ = os.Remove(f.path)
		w.mu.Unlock()
	}
}

// shutdown drains the in-flight queue (ship-or-spool) then makes one best-effort
// spool drain, all within ctx. It does NOT use context.Background() so the final
// ship honors the Shutdown deadline.
func (w *shipWorker) shutdown(ctx context.Context) {
	if w == nil {
		return
	}
	if !w.started.Load() {
		// Worker was never started (no run() goroutine to stop / doneCh to
		// wait on) — nothing in flight, nothing to drain.
		return
	}
	close(w.stopCh)
	select {
	case <-w.doneCh:
	case <-ctx.Done():
		return
	}
	// Worker loop has exited; drain whatever is still queued, then the spool,
	// all under the Shutdown deadline.
	for {
		select {
		case body := <-w.in:
			w.shipOrSpool(ctx, body)
		default:
			w.drainSpool(ctx)
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// shipBatch is the flush-side entry point: encode the drained fragments and
// hand the encoded body to the worker (non-blocking). Returns the encode error
// (if any) so the caller can log it; a nil error means the batch is queued (or
// dropped if shipping is a no-op).
func (w *shipWorker) shipBatch(frags []gorilla.Fragment) error {
	if w == nil || w.shipper.noop() || len(frags) == 0 {
		return nil
	}
	body, err := w.shipper.encode(frags)
	if err != nil {
		return err
	}
	w.enqueue(body)
	return nil
}
