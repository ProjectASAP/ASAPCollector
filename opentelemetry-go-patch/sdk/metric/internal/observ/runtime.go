package observ // import "go.opentelemetry.io/otel/sdk/metric/internal/observ"

import (
	"encoding/json"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type runtimeSample struct {
	userCPUSeconds float64
	sysCPUSeconds  float64
	rssBytes       int64
	heapAllocBytes int64
	heapSysBytes   int64
	goroutines     int64
}

func sampleRuntime() runtimeSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	userCPU, sysCPU := processCPUTime()
	return runtimeSample{
		userCPUSeconds: userCPU,
		sysCPUSeconds:  sysCPU,
		rssBytes:       processRSSBytes(),
		heapAllocBytes: int64(ms.HeapAlloc),
		heapSysBytes:   int64(ms.HeapSys),
		goroutines:     int64(runtime.NumGoroutine()),
	}
}

func EstimateResourceMetricsBytes(rm *metricdata.ResourceMetrics) int64 {
	if rm == nil {
		return 0
	}
	buf, err := json.Marshal(rm)
	if err != nil {
		return 0
	}
	return int64(len(buf))
}

func processCPUTime() (float64, float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	user := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return user, sys
}

func processRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return value * 1024
	}
	return 0
}
