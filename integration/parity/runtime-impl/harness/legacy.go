package harness

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"

	cmsproc "github.com/open-telemetry/opentelemetry-collector-contrib/processor/countminsketchprocessor"
	csproc "github.com/open-telemetry/opentelemetry-collector-contrib/processor/countsketchprocessor"
	ddproc "github.com/open-telemetry/opentelemetry-collector-contrib/processor/ddsketchprocessor"
	hllproc "github.com/open-telemetry/opentelemetry-collector-contrib/processor/hllprocessor"
	kllproc "github.com/open-telemetry/opentelemetry-collector-contrib/processor/kllprocessor"
)

// LegacyOutput captures the pmetric.Metrics emitted by one legacy
// sketch processor in batch mode. We use batch mode (not window
// mode) because batch processes everything synchronously inside
// ConsumeMetrics — no goroutines, no timer, fully deterministic.
//
// Window mode would require waking a goroutine on a wall-clock
// ticker; the harness's "no flake tolerance" requirement forbids
// that. Both Path A and Path B's batch-mode output use the same
// SerializePortable bytes for the underlying sketch state, so the
// byte-identity invariant holds in batch mode just as well as in
// window mode (and the spec accepts batch as the ddsketch processor's
// original mode).
type LegacyOutput struct {
	MetricName string
	Sink       *consumertest.MetricsSink
	// AllOutputs is the slice of pmetric.Metrics the sink captured.
	// Each ConsumeMetrics call lands one entry; in batch mode we
	// run a single ConsumeMetrics so AllOutputs has length 1.
	AllOutputs []pmetric.Metrics
}

// RunLegacyPath runs the input through each of the 5 legacy
// processors in batch mode and captures their emitted metrics.
//
// Each processor gets its own sink so we can identify which
// envelopes came from which sketch type.
func RunLegacyPath(input pmetric.Metrics, cfg RuntimeConfig) (map[string]*LegacyOutput, error) {
	out := make(map[string]*LegacyOutput, 5)
	ctx := context.Background()

	// DDSketch
	{
		factory := ddproc.NewFactory()
		c := factory.CreateDefaultConfig().(*ddproc.Config)
		c.Mode = ddproc.ModeBatch
		c.RelativeAccuracy = cfg.DDSketchAlpha
		c.TransmitSketch = true
		c.MetricSuffix = ""
		c.EnableSelfMonitoring = false
		c.LabelMatchers = nil
		c.DeltaTransmission = false
		// Match only http_request_duration_ms by emitting raw
		// pmetric to the processor and stripping non-target
		// metrics ahead of ConsumeMetrics — the legacy processor
		// has no metric-name matcher field, so we filter the
		// input pmetric instead.
		sink := new(consumertest.MetricsSink)
		set := newProcessorSettings(component.MustNewType("ddsketch"))
		proc, err := factory.CreateMetrics(ctx, set, c, sink)
		if err != nil {
			return nil, fmt.Errorf("ddsketch CreateMetrics: %w", err)
		}
		filtered := filterByMetricName(input, MetricDDSketch)
		if err := proc.ConsumeMetrics(ctx, filtered); err != nil {
			return nil, fmt.Errorf("ddsketch ConsumeMetrics: %w", err)
		}
		_ = proc.Shutdown(ctx)
		out[MetricDDSketch] = &LegacyOutput{
			MetricName: MetricDDSketch, Sink: sink,
			AllOutputs: sink.AllMetrics(),
		}
	}

	// KLL
	{
		factory := kllproc.NewFactory()
		c := factory.CreateDefaultConfig().(*kllproc.Config)
		c.Mode = kllproc.ModeBatch
		c.K = cfg.KLLK
		c.TransmitSketch = true
		c.MetricSuffix = ""
		c.EnableSelfMonitoring = false
		c.LabelMatchers = nil
		c.DeltaTransmission = false
		// Pin the same RNG seed both pipelines use so KLL compaction is
		// byte-deterministic. Production deployments leave Seed nil,
		// preserving today's time-seeded behavior.
		seed := HarnessKLLSeed
		c.Seed = &seed
		sink := new(consumertest.MetricsSink)
		set := newProcessorSettings(component.MustNewType("KLL"))
		proc, err := factory.CreateMetrics(ctx, set, c, sink)
		if err != nil {
			return nil, fmt.Errorf("kll CreateMetrics: %w", err)
		}
		filtered := filterByMetricName(input, MetricKLL)
		if err := proc.ConsumeMetrics(ctx, filtered); err != nil {
			return nil, fmt.Errorf("kll ConsumeMetrics: %w", err)
		}
		_ = proc.Shutdown(ctx)
		out[MetricKLL] = &LegacyOutput{
			MetricName: MetricKLL, Sink: sink,
			AllOutputs: sink.AllMetrics(),
		}
	}

	// HLL
	{
		factory := hllproc.NewFactory()
		c := factory.CreateDefaultConfig().(*hllproc.Config)
		c.Mode = hllproc.ModeBatch
		c.TransmitSketch = true
		c.MetricSuffix = ""
		c.EnableSelfMonitoring = false
		c.LabelMatchers = nil
		c.DeltaTransmission = false
		c.Encoding = hllproc.EncodingProto
		sink := new(consumertest.MetricsSink)
		set := newProcessorSettings(component.MustNewType("HLL"))
		proc, err := factory.CreateMetrics(ctx, set, c, sink)
		if err != nil {
			return nil, fmt.Errorf("hll CreateMetrics: %w", err)
		}
		filtered := filterByMetricName(input, MetricHLL)
		if err := proc.ConsumeMetrics(ctx, filtered); err != nil {
			return nil, fmt.Errorf("hll ConsumeMetrics: %w", err)
		}
		_ = proc.Shutdown(ctx)
		out[MetricHLL] = &LegacyOutput{
			MetricName: MetricHLL, Sink: sink,
			AllOutputs: sink.AllMetrics(),
		}
	}

	// CountSketch — runs in window mode because the legacy
	// processor has no batch path that produces typed
	// CountSketch envelopes. We rely on Shutdown to drive the
	// final flush; the WindowDuration is set to match
	// RuntimeConfig.WindowSize so both pipelines stamp the
	// same window_duration_seconds attr (now part of the byte-
	// parity comparison). Test execution is sub-second, so the
	// internal ticker never fires before Shutdown.
	{
		factory := csproc.NewFactory()
		c := factory.CreateDefaultConfig().(*csproc.Config)
		c.Mode = csproc.ModeWindow
		c.WindowDuration = cfg.WindowSize
		c.Epsilon = cfg.CountSketchEps
		c.Delta = cfg.CountSketchDelta
		c.TransmitSketch = true
		c.EnableSelfMonitoring = false
		c.LabelMatchers = nil
		c.DeltaTransmission = false
		c.Encoding = csproc.EncodingProto
		sink := new(consumertest.MetricsSink)
		set := newProcessorSettings(component.MustNewType("countsketch"))
		proc, err := factory.CreateMetrics(ctx, set, c, sink)
		if err != nil {
			return nil, fmt.Errorf("countsketch CreateMetrics: %w", err)
		}
		if err := proc.Start(ctx, componenttest.NewNopHost()); err != nil {
			return nil, fmt.Errorf("countsketch Start: %w", err)
		}
		filtered := filterByMetricName(input, MetricCountSketch)
		if err := proc.ConsumeMetrics(ctx, filtered); err != nil {
			return nil, fmt.Errorf("countsketch ConsumeMetrics: %w", err)
		}
		// Shutdown triggers a final flush in window mode.
		if err := proc.Shutdown(ctx); err != nil {
			return nil, fmt.Errorf("countsketch Shutdown: %w", err)
		}
		out[MetricCountSketch] = &LegacyOutput{
			MetricName: MetricCountSketch, Sink: sink,
			AllOutputs: sink.AllMetrics(),
		}
	}

	// CountMinSketch — same window-mode reasoning as CountSketch.
	{
		factory := cmsproc.NewFactory()
		c := factory.CreateDefaultConfig().(*cmsproc.Config)
		c.Mode = cmsproc.ModeWindow
		c.WindowDuration = cfg.WindowSize
		c.MetricName = MetricCMS
		c.Rows = cfg.CMSRows
		c.Columns = cfg.CMSCols
		c.TransmitSketch = true
		c.EnableSelfMonitoring = false
		c.LabelMatchers = nil
		c.DeltaTransmission = false
		c.Encoding = cmsproc.EncodingProto
		sink := new(consumertest.MetricsSink)
		set := newProcessorSettings(component.MustNewType("countmin"))
		proc, err := factory.CreateMetrics(ctx, set, c, sink)
		if err != nil {
			return nil, fmt.Errorf("cms CreateMetrics: %w", err)
		}
		if err := proc.Start(ctx, componenttest.NewNopHost()); err != nil {
			return nil, fmt.Errorf("cms Start: %w", err)
		}
		filtered := filterByMetricName(input, MetricCMS)
		if err := proc.ConsumeMetrics(ctx, filtered); err != nil {
			return nil, fmt.Errorf("cms ConsumeMetrics: %w", err)
		}
		if err := proc.Shutdown(ctx); err != nil {
			return nil, fmt.Errorf("cms Shutdown: %w", err)
		}
		out[MetricCMS] = &LegacyOutput{
			MetricName: MetricCMS, Sink: sink,
			AllOutputs: sink.AllMetrics(),
		}
	}

	return out, nil
}

// newProcessorSettings constructs the minimal processor.Settings the
// legacy factories require. The harness uses Nop telemetry settings
// so no observability hooks fire — the sink is the only authoritative
// output.
func newProcessorSettings(t component.Type) processor.Settings {
	return processor.Settings{
		ID:                component.NewID(t),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}
}

// filterByMetricName returns a deep-copied pmetric.Metrics containing
// only metrics whose name equals `name`. The legacy processors apply
// their logic to ALL metrics they see; the harness scopes each
// processor to its own metric by pre-filtering the input. Equivalent
// to the runtime path's per-Precompute metric-name LabelMatcher.
//
// Empty ScopeMetrics / ResourceMetrics blocks are removed so the
// downstream sink doesn't see "phantom" empty resources that would
// confuse envelope-counting.
func filterByMetricName(src pmetric.Metrics, name string) pmetric.Metrics {
	dst := pmetric.NewMetrics()
	rms := src.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rmSrc := rms.At(i)
		var rmDst pmetric.ResourceMetrics
		var rmDstInit bool
		sms := rmSrc.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			smSrc := sms.At(j)
			var smDst pmetric.ScopeMetrics
			var smDstInit bool
			ms := smSrc.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() != name {
					continue
				}
				if !rmDstInit {
					rmDst = dst.ResourceMetrics().AppendEmpty()
					rmSrc.Resource().CopyTo(rmDst.Resource())
					rmDstInit = true
				}
				if !smDstInit {
					smDst = rmDst.ScopeMetrics().AppendEmpty()
					smSrc.Scope().CopyTo(smDst.Scope())
					smDstInit = true
				}
				m.CopyTo(smDst.Metrics().AppendEmpty())
			}
		}
	}
	return dst
}
