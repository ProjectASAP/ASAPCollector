package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	endpoint       = flag.String("endpoint", "localhost:4317", "OTLP gRPC endpoint")
	workers        = flag.Int("workers", 1, "Number of worker threads")
	hostsPerWorker = flag.Int("hosts", 1, "Hosts simulated per worker")
	metricsCount   = flag.Int("metrics", 10, "Number of unique metrics per host")
	interval       = flag.Duration("interval", 1*time.Second, "Flush interval")
	duration       = flag.Duration("duration", 0, "Duration to run (e.g. 60s). 0s means forever")
	metricType     = flag.String("type", "mix", "Type of metrics: 'mix', 'gauge', or 'sum'")

	// Zipf distribution parameters (matching run_ddsketch.sh defaults)
	zipfS    = flag.Float64("zipf-s", 1.1, "Zipf distribution s parameter (exponent, must be > 1)")
	zipfV    = flag.Float64("zipf-v", 1.0, "Zipf distribution v parameter (must be >= 1)")
	zipfMax  = flag.Uint64("zipf-max", 500, "Zipf distribution maximum value (imax)")
	zipfMean = flag.Float64("zipf-mean", 250.0, "Target mean for scaling Zipf values")
)

func main() {
	flag.Parse()

	// Validate flag
	mode := strings.ToLower(*metricType)
	if mode != "mix" && mode != "gauge" && mode != "sum" {
		log.Fatalf("Invalid type '%s'. Use 'mix', 'gauge', or 'sum'.", mode)
	}

	// Validate Zipf parameters
	if *zipfS <= 1.0 {
		log.Fatalf("Invalid zipf-s '%.2f'. Must be > 1.0", *zipfS)
	}
	if *zipfV < 1.0 {
		log.Fatalf("Invalid zipf-v '%.2f'. Must be >= 1.0", *zipfV)
	}

	fmt.Printf("--- Generating Load ---\n")
	fmt.Printf("Target:   %s\n", *endpoint)
	fmt.Printf("Mode:     %s\n", strings.ToUpper(mode))
	fmt.Printf("Payload:  %d Hosts, %d Metrics per host\n", *hostsPerWorker, *metricsCount)
	fmt.Printf("Interval: %s\n", *interval)
	fmt.Printf("Zipf:     s=%.2f, v=%.2f, max=%d, mean=%.2f\n", *zipfS, *zipfV, *zipfMax, *zipfMean)

	var wg sync.WaitGroup
	var ctx context.Context
	var cancel context.CancelFunc

	// Handle Duration Logic
	if *duration > 0 {
		fmt.Printf("Duration: %v\n", *duration)
		ctx, cancel = context.WithTimeout(context.Background(), *duration)
	} else {
		fmt.Println("Duration: Until Ctrl+C")
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go runWorker(ctx, w, &wg, mode)
	}

	// Wait for context to finish (timeout or cancel)
	<-ctx.Done()
	fmt.Println("\nTime's up! Shutting down workers...")

	wg.Wait()
	fmt.Println("Done.")
}

// generateZipfValue generates a value using Zipf distribution and scales it
// to approximate the target mean. Returns a float64 value.
func generateZipfValue(zipf *rand.Zipf) float64 {
	// Get raw Zipf value (uint64)
	rawValue := zipf.Uint64()

	// Scale the value to approximate the target mean
	// Zipf generates values starting from 0, so we add 1 to avoid zero
	// Then scale by (targetMean / expectedMean) where expectedMean ≈ zipfMax/2 for typical distributions
	scaleFactor := *zipfMean / (float64(*zipfMax) / 2.0)
	scaledValue := float64(rawValue+1) * scaleFactor

	return scaledValue
}

func runWorker(ctx context.Context, id int, wg *sync.WaitGroup, mode string) {
	defer wg.Done()

	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Worker %d: Connection failed: %v", id, err)
		return
	}
	defer conn.Close()
	client := pmetricotlp.NewGRPCClient(conn)

	hostNames := make([]string, *hostsPerWorker)
	for i := 0; i < *hostsPerWorker; i++ {
		hostNames[i] = fmt.Sprintf("host-%d-%02d", id, i)
	}

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	counters := make(map[string]float64)

	// Create a Zipf generator for this worker with unique seed
	source := rand.NewSource(time.Now().UnixNano() + int64(id))
	rng := rand.New(source)
	zipf := rand.NewZipf(rng, *zipfS, *zipfV, *zipfMax)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req := pmetricotlp.NewExportRequest()
			md := req.Metrics()

			for _, hostname := range hostNames {
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("host.name", hostname)
				rm.Resource().Attributes().PutStr("service.name", "benchmark-app")

				scope := rm.ScopeMetrics().AppendEmpty()
				scope.Scope().SetName("load-generator")

				for m := 0; m < *metricsCount; m++ {
					isGauge := false

					switch mode {
					case "gauge":
						isGauge = true
					case "sum":
						isGauge = false
					case "mix":
						isGauge = (m%2 == 0)
					}

					metric := scope.Metrics().AppendEmpty()
					metric.SetUnit("1")

					// Generate Zipf-distributed value
					zipfValue := generateZipfValue(zipf)

					if isGauge {
						// --- GAUGE ---
						metric.SetName(fmt.Sprintf("system.metric.gauge.%d", m))
						gauge := metric.SetEmptyGauge()
						dp := gauge.DataPoints().AppendEmpty()
						dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
						dp.SetDoubleValue(zipfValue)
					} else {
						// --- SUM ---
						metricName := fmt.Sprintf("system.metric.sum.%d", m)
						metric.SetName(metricName)
						sum := metric.SetEmptySum()
						sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
						sum.SetIsMonotonic(true)

						dp := sum.DataPoints().AppendEmpty()
						dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

						// For cumulative sums, add Zipf increment (scaled down for realistic counter growth)
						counters[hostname+metricName] += zipfValue * 0.02 // ~5 increment on average
						dp.SetDoubleValue(counters[hostname+metricName])
					}
				}
			}

			reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Export(reqCtx, req)
			cancel()

			if err != nil {
				log.Printf("Worker %d: Export failed: %v", id, err)
			} else {
				fmt.Printf("Worker %d: Sent batch (%s)\n", id, mode)
			}
		}
	}
}
