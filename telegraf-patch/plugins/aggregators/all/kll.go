//go:build !custom || aggregators || aggregators.kll

package all

import _ "github.com/influxdata/telegraf/plugins/aggregators/kll" // register plugin
