// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allsketches

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"

	telegrafcodec "github.com/ProjectASAP/asap-precompute-go/telegraf"
)

// init registers the allsketches plugin with Telegraf's processors
// registry under the name "allsketches". The factory returns a fresh
// AllSketches instance pre-populated with the package-level codec
// defaults so operators only need to set sketch_type and any
// sketch-specific tuning knobs in their TOML config.
func init() {
	processors.AddStreaming("allsketches", func() telegraf.StreamingProcessor {
		return &AllSketches{
			ValueField:       telegrafcodec.DefaultValueField,
			EnvelopeField:    telegrafcodec.DefaultEnvelopeField,
			OutputMetricName: telegrafcodec.DefaultOutputMetricName,
			WindowSize:       "10s",
		}
	})
}
