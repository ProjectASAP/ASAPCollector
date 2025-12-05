//go:build !custom || aggregators || aggregators.gorilla

package all

import _ "github.com/influxdata/telegraf/plugins/aggregators/gorilla" // register plugin
