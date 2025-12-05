//go:build !custom || outputs || outputs.gorilla_s3

package all

import _ "github.com/influxdata/telegraf/plugins/outputs/gorilla_s3" // register plugin
