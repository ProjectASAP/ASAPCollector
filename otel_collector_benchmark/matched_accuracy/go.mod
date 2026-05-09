module matched_accuracy

go 1.25.3

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/ProjectASAP/sketchlib-go v0.0.0
	github.com/caio/go-tdigest v3.1.0+incompatible
)

// Local replace so the build does not require network access for the
// in-house sketch library. Adjust the path if your checkout lives elsewhere.
replace github.com/ProjectASAP/sketchlib-go => /home/zeying/repos/sketchlib-go

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	gonum.org/v1/gonum v0.8.2 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
