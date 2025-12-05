//go:build !custom || aggregators || aggregators.countmin

package all

import _ "github.com/influxdata/telegraf/plugins/aggregators/countmin" // register plugin
