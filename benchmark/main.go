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
)

func main() {
	flag.Parse()

	// Validate flag
	mode := strings.ToLower(*metricType)
	if mode != "mix" && mode != "gauge" && mode != "sum" {
		log.Fatalf("Invalid type '%s'. Use 'mix', 'gauge', or 'sum'.", mode)
	}

	fmt.Printf("--- Generating Load ---\n")
	fmt.Printf("Target:   %s\n", *endpoint)
	fmt.Printf("Mode:     %s\n", strings.ToUpper(mode))
	fmt.Printf("Payload:  %d Hosts, %d Metrics per host\n", *hostsPerWorker, *metricsCount)
	fmt.Printf("Interval: %s\n", *interval)

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

					if isGauge {
						// --- GAUGE ---
						metric.SetName(fmt.Sprintf("system.metric.gauge.%d", m))
						gauge := metric.SetEmptyGauge()
						dp := gauge.DataPoints().AppendEmpty()
						dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
						dp.SetDoubleValue(rand.Float64() * 100.0)
					} else {
						// --- SUM ---
						metricName := fmt.Sprintf("system.metric.sum.%d", m)
						metric.SetName(metricName)
						sum := metric.SetEmptySum()
						sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
						sum.SetIsMonotonic(true)

						dp := sum.DataPoints().AppendEmpty()
						dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

						counters[hostname+metricName] += rand.Float64() * 5
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