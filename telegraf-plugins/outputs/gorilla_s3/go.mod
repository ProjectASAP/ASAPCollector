module github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3

go 1.25.0

require (
	github.com/aws/aws-sdk-go v1.44.263
	github.com/influxdata/telegraf v1.37.0
)

require github.com/jmespath/go-jmespath v0.4.0 // indirect

replace github.com/influxdata/telegraf => ../../../telegraf
