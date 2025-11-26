//go:build !custom || aggregators || aggregators.ddsketch

package all

import _ "github.com/influxdata/telegraf/plugins/aggregators/ddsketch" // register plugin
