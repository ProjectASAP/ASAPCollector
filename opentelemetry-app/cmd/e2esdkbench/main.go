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
//	  --workers=10 --hosts=10 --metrics=10 \
//	  --interval=20ms --duration=60s \
//	  --rate-label=10000 \
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
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	grpcstats "google.golang.org/grpc/stats"
)

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

var (
	endpoint   = flag.String("endpoint", "localhost:4317", "OTLP gRPC endpoint of the collector")
	sketchArg  = flag.String("sketch-type", "ddsketch", "Sketch type: ddsketch|kll|countsketch|countminsketch|hll|baseline")
	workers    = flag.Int("workers", 10, "Number of worker goroutines")
	hosts      = flag.Int("hosts", 10, "Simulated hosts per worker")
	metrics    = flag.Int("metrics", 10, "Metrics per host")
	interval   = flag.Duration("interval", 20*time.Millisecond, "SDK export interval (controls data-points/sec sent to collector)")
	duration   = flag.Duration("duration", 60*time.Second, "Benchmark run duration")
	rateLabel  = flag.Int("rate-label", 0, "Nominal MPS rate for output file naming (0 = auto-calculated)")
	outputDir  = flag.String("output-dir", ".", "Directory to write CSV and JSON result files")
	sampleSec  = flag.Int("sample-interval-sec", 1, "Resource sampling interval in seconds")

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
	// Zipf
	zipfS    = flag.Float64("zipf-s", 1.1, "Zipf s parameter (> 1)")
	zipfV    = flag.Float64("zipf-v", 1.0, "Zipf v parameter (>= 1)")
	zipfMax  = flag.Uint64("zipf-max", 500, "Zipf imax")
	zipfMean = flag.Float64("zipf-mean", 250.0, "Target mean for Zipf scaling")
)

// ---------------------------------------------------------------------------
// gRPC stats handler – counts wire bytes sent/received by the SDK.
// Implements google.golang.org/grpc/stats.Handler.
// ---------------------------------------------------------------------------

type grpcBytesCounter struct {
	sent atomic.Int64
	recv atomic.Int64
}

func (c *grpcBytesCounter) TagRPC(ctx context.Context, _ *grpcstats.RPCTagInfo) context.Context {
	return ctx
}

func (c *grpcBytesCounter) HandleRPC(_ context.Context, s grpcstats.RPCStats) {
	switch st := s.(type) {
	case *grpcstats.OutPayload:
		c.sent.Add(int64(st.WireLength))
	case *grpcstats.InPayload:
		c.recv.Add(int64(st.WireLength))
	}
}

func (c *grpcBytesCounter) TagConn(ctx context.Context, _ *grpcstats.ConnTagInfo) context.Context {
	return ctx
}

func (c *grpcBytesCounter) HandleConn(_ context.Context, _ grpcstats.ConnStats) {}

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

func runWorker(ctx context.Context, id int, inst interface{}, mode string, wg *sync.WaitGroup) {
	defer wg.Done()

	src := rand.NewSource(time.Now().UnixNano() + int64(id))
	rng := rand.New(src)
	zipf := rand.NewZipf(rng, *zipfS, *zipfV, *zipfMax)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for h := 0; h < *hosts; h++ {
				hostAttr := attribute.String("host.name", fmt.Sprintf("worker-%d-host-%02d", id, h))
				for m := 0; m < *metrics; m++ {
					metricAttr := attribute.String("metric.name", fmt.Sprintf("metric.%03d", m))
					attrs := metric.WithAttributes(hostAttr, metricAttr)
					v := generateZipfValue(zipf)
					switch instr := inst.(type) {
					case metric.Float64Histogram:
						instr.Record(ctx, v, attrs)
					case metric.Float64Gauge:
						instr.Record(ctx, v, attrs)
					}
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Resource sampler – polls memory every sampleSec seconds.
// ---------------------------------------------------------------------------

func resourceSampler(ctx context.Context, counter *grpcBytesCounter, samples *[]sample, mu *sync.Mutex) {
	ticker := time.NewTicker(time.Duration(*sampleSec) * time.Second)
	defer ticker.Stop()

	prevSent := int64(0)
	prevTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			curSent := counter.sent.Load()
			elapsed := now.Sub(prevTime).Seconds()
			bps := 0.0
			if elapsed > 0 {
				bps = float64(curSent-prevSent) / elapsed
			}
			prevSent = curSent
			prevTime = now

			s := sample{
				Timestamp:    now.Unix(),
				BytesSentCum: curSent,
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
	case "ddsketch", "kll", "countsketch", "countminsketch", "hll", "baseline":
	default:
		log.Fatalf("invalid --sketch-type %q; valid: ddsketch|kll|countsketch|countminsketch|hll|baseline", mode)
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("cannot create output-dir %s: %v", *outputDir, err)
	}

	totalSeries := *workers * *hosts * *metrics
	nominalMPS := *rateLabel
	if nominalMPS == 0 {
		nominalMPS = int(float64(totalSeries) / interval.Seconds())
	}

	fmt.Printf("=== e2e SDK Benchmark ===\n")
	fmt.Printf("Sketch:     %s\n", strings.ToUpper(mode))
	fmt.Printf("Endpoint:   %s\n", *endpoint)
	fmt.Printf("Series:     %d workers × %d hosts × %d metrics = %d total\n",
		*workers, *hosts, *metrics, totalSeries)
	fmt.Printf("Interval:   %v  → ~%.0f data-points/s to collector\n",
		*interval, float64(totalSeries)/interval.Seconds())
	fmt.Printf("Duration:   %v\n", *duration)
	fmt.Printf("Output dir: %s\n", *outputDir)
	fmt.Println()

	// --- gRPC byte counter ---
	counter := &grpcBytesCounter{}

	// --- Choose sketch aggregation ---
	// baseline: Float64Gauge with default LastValue aggregation — raw samples, no sketching.
	// All sketch types: Float64Histogram with the sketch aggregation view (sdkSketch mode).
	var agg sdkmetric.Aggregation
	useHistogram := true // true → Float64Histogram (sdkSketch); false → Float64Gauge (baseline)
	switch mode {
	case "ddsketch":
		agg = sdkmetric.AggregationDDSketch{RelativeAccuracy: *ddsketchAccuracy}
	case "kll":
		agg = sdkmetric.AggregationKLLSketch{K: *kllK}
	case "countsketch":
		agg = sdkmetric.AggregationCountSketch{Epsilon: *csEpsilon, Delta: *csDelta}
	case "countminsketch":
		agg = sdkmetric.AggregationCountMinSketch{Rows: *cmsRows, Cols: *cmsCols}
	case "hll":
		agg = sdkmetric.AggregationHLLSketch{}
	case "baseline":
		// Raw gauge: SDK emits one float64 per series per export window (LastValue).
		// Collector receives and drops raw data points — no sketch computation anywhere.
		useHistogram = false
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration+10*time.Second)
	defer cancel()

	// --- OTLP exporter with byte-counting stats handler ---
	// MaxCallSendMsgSize raised to 64 MiB: sketch payloads (CountSketch, CountMinSketch, HLL)
	// can exceed the default 4 MiB limit at high series counts / export rates.
	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(*endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithDialOption(grpc.WithStatsHandler(counter)),
		otlpmetricgrpc.WithDialOption(grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024))),
	)
	if err != nil {
		log.Fatalf("failed to create OTLP exporter: %v", err)
	}

	// View overrides aggregation for the benchmark instrument.
	view := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "benchmark.latency"},
		sdkmetric.Stream{Aggregation: agg},
	)

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(*interval)),
		),
		sdkmetric.WithView(view),
	)
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := provider.Shutdown(shutCtx); err != nil {
			log.Printf("MeterProvider shutdown: %v", err)
		}
	}()

	meter := provider.Meter("e2esdkbench")

	// --- Instrument creation ---
	var histInst metric.Float64Histogram
	var gaugeInst metric.Float64Gauge

	if useHistogram {
		histInst, err = meter.Float64Histogram("benchmark.latency",
			metric.WithDescription("Benchmark latency (sketch aggregation)"),
			metric.WithUnit("ms"),
		)
		if err != nil {
			log.Fatalf("failed to create histogram: %v", err)
		}
	} else {
		gaugeInst, err = meter.Float64Gauge("benchmark.latency",
			metric.WithDescription("Benchmark latency (gauge)"),
			metric.WithUnit("ms"),
		)
		if err != nil {
			log.Fatalf("failed to create gauge: %v", err)
		}
	}

	// --- Resource sampling setup ---
	var samples []sample
	var mu sync.Mutex

	runCtx, runCancel := context.WithTimeout(context.Background(), *duration)
	defer runCancel()

	// Start sampler in background.
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		resourceSampler(runCtx, counter, &samples, &mu)
	}()

	// --- Record CPU at start ---
	wallStart := time.Now()
	cpuUserStart, cpuSysStart := getRusage()

	// --- Spawn load workers ---
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		var inst interface{}
		if useHistogram {
			inst = histInst
		} else {
			inst = gaugeInst
		}
		go runWorker(runCtx, w, inst, mode, &wg)
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
	totalSent := counter.sent.Load()
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
	fileBase := fmt.Sprintf("%s_%dmps", mode, nominalMPS)
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
