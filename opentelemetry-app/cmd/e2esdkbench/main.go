// e2esdkbench – End-to-end OTel SDK (sdkSketch mode) → OTLP → Collector benchmark.
//
// Metrics captured from the SDK process:
//   - Bandwidth:  gRPC wire bytes via a stats.Handler on the SDK gRPC connection.
//   - Memory:     heap allocation + system heap from runtime.ReadMemStats, sampled every second.
//   - CPU:        process user+system time from syscall.Getrusage, delta over the run.
//
// Output
//   --output-dir/<sketch>_<rate>mps_timeseries.csv  – per-second samples
//   --output-dir/<sketch>_<rate>mps_summary.json    – final summary (also printed to stdout)
//
// Usage:
//
//	go run ./cmd/e2esdkbench \
//	  --sketch-type=ddsketch \
//	  --endpoint=localhost:4317 \
//	  --series=1000 --samples-per-sec-per-series=50 \
//	  --duration=60s \
//	  --rate-label=50000 \
//	  --output-dir=/tmp/e2e_bench
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

var (
	endpoint              = flag.String("endpoint", "localhost:4317", "OTLP gRPC endpoint of the collector")
	sketchArg             = flag.String("sketch-type", "ddsketch", "Sketch type: ddsketch|kll|hll")
	seriesCount           = flag.Int("series", 1000, "Total number of distinct time series")
	samplesPerSecPerSeries = flag.Float64("samples-per-sec-per-series", 50.0, "Samples per second per series (controls worker record rate)")
	workers               = flag.Int("workers", 10, "Number of worker goroutines (internal parallelism)")
	duration              = flag.Duration("duration", 60*time.Second, "Benchmark run duration")
	rateLabel             = flag.Int("rate-label", 0, "Nominal MPS rate for output file naming (0 = auto-calculated)")
	outputDir             = flag.String("output-dir", ".", "Directory to write CSV and JSON result files")
	sampleSec             = flag.Int("sample-interval-sec", 1, "Resource sampling interval in seconds")

	// DDSketch
	ddsketchAccuracy = flag.Float64("ddsketch-accuracy", 0.01, "DDSketch relative accuracy (0,1)")
	// KLL
	kllK = flag.Int("kll-k", 256, "KLL sketch parameter k (>= 2)")
	// CountSketch
	csEpsilon = flag.Float64("countsketch-epsilon", 0.01, "CountSketch epsilon (0,1)")
	csDelta   = flag.Float64("countsketch-delta", 0.99, "CountSketch delta (0,1)")
	// CountMinSketch
	cmsRows = flag.Int("countmin-rows", 5, "CountMinSketch rows")
	cmsCols = flag.Int("countmin-cols", 2000, "CountMinSketch columns")
	// Series aggregation
	seriesPerSketch = flag.Int("series-per-sketch", 1, "Number of series aggregated into one sketch (1=per-series, 0=all-in-one)")
	// Zipf
	zipfS    = flag.Float64("zipf-s", 1.1, "Zipf s parameter (> 1)")
	zipfV    = flag.Float64("zipf-v", 1.0, "Zipf v parameter (>= 1)")
	zipfMax  = flag.Uint64("zipf-max", 500, "Zipf imax")
	zipfMean = flag.Float64("zipf-mean", 250.0, "Target mean for Zipf scaling")
)

// ---------------------------------------------------------------------------
// Loopback bandwidth reader.
// Reads TX bytes for the loopback interface from /proc/net/dev.
// Bandwidth is measured externally (shell script) as a delta over the run.
// The Go benchmark emits the PID so the shell can track /proc/<pid>/status too.
// ---------------------------------------------------------------------------

func loLoTXBytes() int64 {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		// Format: "  lo:  rx_bytes ... tx_bytes ..."
		// Fields: iface rx_bytes rx_packets ... tx_bytes tx_packets ...
		// tx_bytes is field 9 (0-indexed after stripping "lo:")
		f := strings.Fields(line)
		if len(f) >= 10 && strings.TrimSuffix(f[0], ":") == "lo" {
			v, _ := strconv.ParseInt(f[9], 10, 64)
			return v
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Time-series sample
// ---------------------------------------------------------------------------

type sample struct {
	Timestamp    int64   `json:"ts"`             // Unix seconds
	BytesSentCum int64   `json:"bytes_sent_cum"` // cumulative bytes sent
	BandwidthBps float64 `json:"bandwidth_bps"`  // bytes/sec in this interval
	HeapAllocMB  float64 `json:"heap_alloc_mb"`
	HeapSysMB    float64 `json:"heap_sys_mb"`
	Goroutines   int     `json:"goroutines"`
}

// ---------------------------------------------------------------------------
// Summary result
// ---------------------------------------------------------------------------

type summary struct {
	SketchType       string  `json:"sketch_type"`
	SeriesPerSketch  int     `json:"series_per_sketch"`
	RateLabelMPS     int     `json:"rate_label_mps"`
	DurationSec      float64 `json:"duration_sec"`
	TotalBytesSent   int64   `json:"total_bytes_sent"`
	AvgBandwidthBps  float64 `json:"avg_bandwidth_bps"`
	PeakBandwidthBps float64 `json:"peak_bandwidth_bps"`
	AvgHeapAllocMB   float64 `json:"avg_heap_alloc_mb"`
	PeakHeapAllocMB  float64 `json:"peak_heap_alloc_mb"`
	SDKCPUUserMs     float64 `json:"sdk_cpu_user_ms"`
	SDKCPUSysMs      float64 `json:"sdk_cpu_sys_ms"`
	SDKCPUPercent    float64 `json:"sdk_cpu_percent"` // (user+sys) / wall * 100
}

// ---------------------------------------------------------------------------
// CPU helpers
// ---------------------------------------------------------------------------

func getRusage() (userMs, sysMs float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	userMs = float64(ru.Utime.Sec)*1000 + float64(ru.Utime.Usec)/1000
	sysMs = float64(ru.Stime.Sec)*1000 + float64(ru.Stime.Usec)/1000
	return
}

// ---------------------------------------------------------------------------
// Load workers
// ---------------------------------------------------------------------------

func generateZipfValue(zipf *rand.Zipf) float64 {
	scaleFactor := *zipfMean / (float64(*zipfMax) / 2.0)
	return float64(zipf.Uint64()+1) * scaleFactor
}

func runWorker(ctx context.Context, id, seriesStart, seriesEnd, seriesPerSketch, totalSeries int, interval time.Duration, inst metric.Float64Gauge, wg *sync.WaitGroup) {
	defer wg.Done()

	src := rand.NewSource(time.Now().UnixNano() + int64(id))
	rng := rand.New(src)
	zipf := rand.NewZipf(rng, *zipfS, *zipfV, *zipfMax)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for s := seriesStart; s < seriesEnd; s++ {
				v := generateZipfValue(zipf)
				switch {
				case seriesPerSketch <= 0 || seriesPerSketch >= totalSeries:
					// All series collapse into one sketch — no attribute.
					inst.Record(ctx, v)
				case seriesPerSketch == 1:
					// One sketch per series (fine-grained, existing behavior).
					inst.Record(ctx, v, metric.WithAttributes(attribute.String("series.id", fmt.Sprintf("%06d", s))))
				default:
					// Multiple series share a sketch, grouped by group.id.
					groupID := s / seriesPerSketch
					inst.Record(ctx, v, metric.WithAttributes(attribute.String("group.id", fmt.Sprintf("%06d", groupID))))
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Resource sampler – polls memory every sampleSec seconds.
// ---------------------------------------------------------------------------

// resourceSampler polls memory + loopback TX bytes every sampleSec seconds.
// Bandwidth is measured via /proc/net/dev loopback TX delta — this works for
// all metric types (sketch and non-sketch) without relying on gRPC internals.
func resourceSampler(ctx context.Context, samples *[]sample, mu *sync.Mutex) {
	ticker := time.NewTicker(time.Duration(*sampleSec) * time.Second)
	defer ticker.Stop()

	prevTX := loLoTXBytes()
	prevTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			curTX := loLoTXBytes()
			elapsed := now.Sub(prevTime).Seconds()
			bps := 0.0
			if elapsed > 0 {
				bps = float64(curTX-prevTX) / elapsed
			}
			prevTX = curTX
			prevTime = now

			s := sample{
				Timestamp:    now.Unix(),
				BytesSentCum: curTX,
				BandwidthBps: bps,
				HeapAllocMB:  float64(ms.HeapAlloc) / (1024 * 1024),
				HeapSysMB:    float64(ms.HeapSys) / (1024 * 1024),
				Goroutines:   runtime.NumGoroutine(),
			}

			mu.Lock()
			*samples = append(*samples, s)
			mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	mode := strings.ToLower(*sketchArg)
	switch mode {
	case "ddsketch", "kll", "hll", "baseline":
	default:
		log.Fatalf("invalid --sketch-type %q; valid: ddsketch|kll|hll", mode)
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("cannot create output-dir %s: %v", *outputDir, err)
	}

	totalSeries := *seriesCount
	// workerInterval controls how often each worker records a sample per series.
	workerInterval := time.Duration(float64(time.Second) / *samplesPerSecPerSeries)
	// readerInterval: sketch types export every 1s so each sketch aggregates a full second of samples.
	// Raw baseline exports at the worker rate so every sample is sent individually.
	readerInterval := time.Second
	if mode == "baseline" {
		readerInterval = workerInterval
	}
	nominalMPS := *rateLabel
	if nominalMPS == 0 {
		nominalMPS = int(float64(totalSeries) * *samplesPerSecPerSeries)
	}

	// Compute number of distinct sketches exported per interval.
	numSketches := totalSeries
	if *seriesPerSketch <= 0 || *seriesPerSketch >= totalSeries {
		numSketches = 1
	} else if *seriesPerSketch > 1 {
		numSketches = (totalSeries + *seriesPerSketch - 1) / *seriesPerSketch
	}

	fmt.Printf("=== e2e SDK Benchmark ===\n")
	fmt.Printf("Sketch:           %s\n", strings.ToUpper(mode))
	fmt.Printf("Endpoint:         %s\n", *endpoint)
	fmt.Printf("Series:           %d\n", totalSeries)
	fmt.Printf("Series-per-sketch:%d  → %d distinct sketch(es) per export\n", *seriesPerSketch, numSketches)
	fmt.Printf("Rate:             %.2f samples/s/series  → ~%.0f data-points/s to collector\n",
		*samplesPerSecPerSeries, float64(totalSeries)**samplesPerSecPerSeries)
	fmt.Printf("Worker interval:  %v\n", workerInterval)
	fmt.Printf("Reader interval:  %v\n", readerInterval)
	fmt.Printf("Duration:         %v\n", *duration)
	fmt.Printf("Output dir:       %s\n", *outputDir)
	fmt.Println()

	// --- Choose sketch aggregation ---
	// All modes use Float64Gauge. Sketch types attach a view to override the
	// default LastValue aggregation with the sketch aggregation. Baseline uses
	// no view — the gauge's natural LastValue aggregation sends raw samples.
	var agg sdkmetric.Aggregation
	switch mode {
	case "ddsketch":
		agg = sdkmetric.AggregationDDSketch{RelativeAccuracy: *ddsketchAccuracy}
	case "kll":
		agg = sdkmetric.AggregationKLLSketch{K: *kllK}
	case "hll":
		agg = sdkmetric.AggregationHLLSketch{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration+10*time.Second)
	defer cancel()

	// MaxCallSendMsgSize raised to 64 MiB: sketch payloads (CountSketch, CountMinSketch,
	// HLL) exceed the default 4 MiB gRPC limit at high series counts / export rates.
	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(*endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithDialOption(grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024))),
	)
	if err != nil {
		log.Fatalf("failed to create OTLP exporter: %v", err)
	}

	// View overrides aggregation for sketch types. Baseline skips the view so
	// the Float64Gauge uses its natural LastValue aggregation — no nil-aggregation
	// side-effect that would suppress reporting.
	providerOpts := []sdkmetric.Option{
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(readerInterval)),
		),
	}
	if agg != nil {
		providerOpts = append(providerOpts, sdkmetric.WithView(
			sdkmetric.NewView(
				sdkmetric.Instrument{Name: "benchmark.latency"},
				sdkmetric.Stream{Aggregation: agg},
			),
		))
	}

	provider := sdkmetric.NewMeterProvider(providerOpts...)
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := provider.Shutdown(shutCtx); err != nil {
			log.Printf("MeterProvider shutdown: %v", err)
		}
	}()

	meter := provider.Meter("e2esdkbench")

	// --- Instrument creation ---
	gaugeInst, err := meter.Float64Gauge("benchmark.latency",
		metric.WithDescription("Benchmark latency"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		log.Fatalf("failed to create gauge: %v", err)
	}

	// --- Resource sampling setup ---
	var samples []sample
	var mu sync.Mutex

	runCtx, runCancel := context.WithTimeout(context.Background(), *duration)
	defer runCancel()

	// Record loopback TX bytes before run starts (bandwidth baseline).
	loTXStart := loLoTXBytes()

	// Start sampler in background.
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		resourceSampler(runCtx, &samples, &mu)
	}()

	// --- Record CPU at start ---
	wallStart := time.Now()
	cpuUserStart, cpuSysStart := getRusage()

	// --- Spawn load workers ---
	// Distribute series evenly across workers.
	numWorkers := *workers
	if numWorkers > totalSeries {
		numWorkers = totalSeries
	}
	seriesPerWorker := (totalSeries + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		start := w * seriesPerWorker
		end := start + seriesPerWorker
		if end > totalSeries {
			end = totalSeries
		}
		wg.Add(1)
		go runWorker(runCtx, w, start, end, *seriesPerSketch, totalSeries, workerInterval, gaugeInst, &wg)
	}

	// Wait for run duration.
	<-runCtx.Done()
	wg.Wait()

	// --- Record CPU at end ---
	wallElapsed := time.Since(wallStart)
	cpuUserEnd, cpuSysEnd := getRusage()

	// Wait for sampler to finish.
	<-samplerDone

	// Flush final export.
	fmt.Println("Flushing final export...")
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	_ = flushCtx

	// --- Compute summary ---
	loTXEnd := loLoTXBytes()
	totalSent := loTXEnd - loTXStart // loopback TX delta = bytes sent during the run
	cpuUserDeltaMs := cpuUserEnd - cpuUserStart
	cpuSysDeltaMs := cpuSysEnd - cpuSysStart
	wallMs := wallElapsed.Seconds() * 1000

	var sumBW, peakBW float64
	var sumHeap, peakHeap float64
	mu.Lock()
	nSamples := len(samples)
	for _, s := range samples {
		sumBW += s.BandwidthBps
		if s.BandwidthBps > peakBW {
			peakBW = s.BandwidthBps
		}
		sumHeap += s.HeapAllocMB
		if s.HeapAllocMB > peakHeap {
			peakHeap = s.HeapAllocMB
		}
	}
	mu.Unlock()

	avgBW := 0.0
	avgHeap := 0.0
	if nSamples > 0 {
		avgBW = sumBW / float64(nSamples)
		avgHeap = sumHeap / float64(nSamples)
	}
	cpuPct := 0.0
	if wallMs > 0 {
		cpuPct = (cpuUserDeltaMs + cpuSysDeltaMs) / wallMs * 100
	}

	result := summary{
		SketchType:       mode,
		SeriesPerSketch:  *seriesPerSketch,
		RateLabelMPS:     nominalMPS,
		DurationSec:      wallElapsed.Seconds(),
		TotalBytesSent:   totalSent,
		AvgBandwidthBps:  avgBW,
		PeakBandwidthBps: peakBW,
		AvgHeapAllocMB:   avgHeap,
		PeakHeapAllocMB:  peakHeap,
		SDKCPUUserMs:     cpuUserDeltaMs,
		SDKCPUSysMs:      cpuSysDeltaMs,
		SDKCPUPercent:    cpuPct,
	}

	// --- Write time-series CSV ---
	// When series-per-sketch != 1, embed the group size in the filename so
	// parallel runs (grp1, grp10, grp100, …) don't overwrite each other.
	var fileBase string
	if *seriesPerSketch == 1 {
		fileBase = fmt.Sprintf("%s_%dmps", mode, nominalMPS)
	} else {
		fileBase = fmt.Sprintf("%s_grp%d_%dmps", mode, *seriesPerSketch, nominalMPS)
	}
	csvPath := filepath.Join(*outputDir, fileBase+"_timeseries.csv")
	if err := writeTimeseriesCSV(csvPath, samples); err != nil {
		log.Printf("warning: could not write CSV: %v", err)
	}

	// --- Write summary JSON ---
	jsonPath := filepath.Join(*outputDir, fileBase+"_summary.json")
	if err := writeSummaryJSON(jsonPath, result); err != nil {
		log.Printf("warning: could not write JSON: %v", err)
	}

	// --- Print summary to stdout ---
	fmt.Printf("\n=== SDK Benchmark Summary ===\n")
	fmt.Printf("Sketch type       : %s\n", strings.ToUpper(result.SketchType))
	fmt.Printf("Rate label        : %d MPS\n", result.RateLabelMPS)
	fmt.Printf("Wall time         : %.1f s\n", result.DurationSec)
	fmt.Printf("--- Bandwidth ---\n")
	fmt.Printf("Total bytes sent  : %d bytes (%.2f MB)\n",
		result.TotalBytesSent, float64(result.TotalBytesSent)/(1024*1024))
	fmt.Printf("Avg bandwidth     : %.1f B/s  (%.2f KB/s)\n",
		result.AvgBandwidthBps, result.AvgBandwidthBps/1024)
	fmt.Printf("Peak bandwidth    : %.1f B/s  (%.2f KB/s)\n",
		result.PeakBandwidthBps, result.PeakBandwidthBps/1024)
	fmt.Printf("--- SDK Memory ---\n")
	fmt.Printf("Avg heap alloc    : %.2f MB\n", result.AvgHeapAllocMB)
	fmt.Printf("Peak heap alloc   : %.2f MB\n", result.PeakHeapAllocMB)
	fmt.Printf("--- SDK CPU ---\n")
	fmt.Printf("User CPU time     : %.1f ms\n", result.SDKCPUUserMs)
	fmt.Printf("Sys CPU time      : %.1f ms\n", result.SDKCPUSysMs)
	fmt.Printf("CPU%%              : %.2f%%\n", result.SDKCPUPercent)
	fmt.Printf("--- Files ---\n")
	fmt.Printf("Timeseries CSV    : %s\n", csvPath)
	fmt.Printf("Summary JSON      : %s\n", jsonPath)

	// Emit machine-readable JSON to stdout for the orchestration script.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	fmt.Println("\n=== JSON_RESULT_BEGIN ===")
	_ = enc.Encode(result)
	fmt.Println("=== JSON_RESULT_END ===")
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

func writeTimeseriesCSV(path string, samples []sample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	_ = w.Write([]string{
		"timestamp", "bytes_sent_cum", "bandwidth_bps",
		"heap_alloc_mb", "heap_sys_mb", "goroutines",
	})
	for _, s := range samples {
		_ = w.Write([]string{
			strconv.FormatInt(s.Timestamp, 10),
			strconv.FormatInt(s.BytesSentCum, 10),
			strconv.FormatFloat(s.BandwidthBps, 'f', 2, 64),
			strconv.FormatFloat(s.HeapAllocMB, 'f', 3, 64),
			strconv.FormatFloat(s.HeapSysMB, 'f', 3, 64),
			strconv.Itoa(s.Goroutines),
		})
	}
	return nil
}

func writeSummaryJSON(path string, s summary) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
