// sample_controller.go — CDM coordinated warm-sampling for the producer.
//
// Priority 2: turn otel-app into a CDM *edge*. Instead of a static
// -warm-sample-p, the producer's admitted fraction p is driven by the
// coordinator's grant (Grant.SampleP), computed by the data-plane monitor
// coordinator from each edge's reported per-window rate
// (AllocateSampleRates: p_i ∝ √(f_i/rate_i), so the hot edge gets a smaller
// p). The grant is applied at the NEXT window boundary, never mid-window.
//
// The wire contract reuses the existing nested transport module
// asap-precompute-go/monitor/grpcclient (grpcclient.New(coordURL, engine))
// driving a monitor.Engine (NewEngine(edgeID, windowMs, reporter)). On each
// window the edge calls Engine.Observe(aggID, key, value, windowStart) with
// its running window value; the engine reports the rate (obsCount) and
// crossings to the coordinator, and stores the granted p, which we read via
// Engine.GrantedSampleP(aggID) at each boundary.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient"
)

// monitorValuePerObs is the small per-candidate increment of the monitored
// additive value (see observe). Kept tiny so per-window values stay far below
// a modest tau (grant regime) while still crossing the per-round slack so a
// Report — carrying this edge's rate — fires every window.
const monitorValuePerObs = 0.5

// sampleController holds the current warm-sample probability and, when
// coordinated, the CDM engine + transport. currentP/observe are called on the
// replay hot path; they are cheap and lock-guarded.
type sampleController struct {
	mu sync.Mutex

	// p is the live admitted fraction. Bootstrapped to cfg.WarmSampleP; when
	// coordinated, replaced at each window boundary by the granted p.
	p float64

	coordinated bool
	aggID       uint64
	edgeID      string
	windowMs    uint64
	// monitorKey, when non-empty, is the cms_point heavy-hitter series whose
	// per-window frequency is reported as f_i (the value), decoupled from the
	// total rate. Empty ⇒ legacy sum-monitor (value ∝ rate).
	monitorKey string

	engine *monitor.Engine
	client *grpcclient.Client

	// window bookkeeping
	windowStartMs uint64 // current epoch's aligned start (ms since unix epoch)
	windowCount   uint64 // admitted-candidate count this window (the reported rate)
	windowValue   float64
}

// newSampleController builds the controller. When cfg.CoordinatorURL is empty
// it is a static holder returning cfg.WarmSampleP. Otherwise it dials the
// coordinator and starts reporting/observing under cfg.MonitorAggID.
func newSampleController(cfg Config, metricName string) *sampleController {
	sc := &sampleController{p: cfg.WarmSampleP}
	if cfg.CoordinatorURL == "" {
		return sc
	}
	sc.coordinated = true
	sc.aggID = cfg.MonitorAggID
	// The cms_point key normally flows from the control plane: the controller
	// pushes its monitors: config to the data-plane streaming-config, and the
	// edge learns which key it is monitoring from the entry matching its agg_id.
	// A non-empty -monitor-key flag is a manual override (tests / no config URL).
	sc.monitorKey = cfg.MonitorKey
	if sc.monitorKey == "" && cfg.MonitorConfigURL != "" {
		if k, err := fetchMonitorKey(cfg.MonitorConfigURL, cfg.MonitorAggID); err != nil {
			log.Printf("otel-app CDM edge: could not learn monitor key from %s (agg_id=%d): %v — counting all events as f_i (sum-monitor)",
				cfg.MonitorConfigURL, cfg.MonitorAggID, err)
		} else if k != "" {
			sc.monitorKey = k
			log.Printf("otel-app CDM edge: learned cms_point key %q from controller config (%s, agg_id=%d)",
				k, cfg.MonitorConfigURL, cfg.MonitorAggID)
		} else {
			log.Printf("otel-app CDM edge: no monitor entry for agg_id=%d at %s — sum-monitor (value ∝ rate)",
				cfg.MonitorAggID, cfg.MonitorConfigURL)
		}
	}
	sc.edgeID = cfg.EdgeID
	if sc.edgeID == "" {
		sc.edgeID = cfg.ProducerID
	}
	if sc.edgeID == "" {
		sc.edgeID = "edge-" + metricName
	}
	sc.windowMs = uint64(cfg.SDKWindow / time.Millisecond)
	if sc.windowMs == 0 {
		sc.windowMs = 1000
	}
	sc.engine = monitor.NewEngine(sc.edgeID, sc.windowMs, nil)
	sc.client = grpcclient.New(cfg.CoordinatorURL, sc.engine)
	sc.engine.SetReporter(sc.client)
	sc.windowStartMs = sc.alignedNow()
	log.Printf("otel-app CDM edge: coordinator=%s edge_id=%s agg_id=%d window_ms=%d bootstrap_p=%.3f",
		cfg.CoordinatorURL, sc.edgeID, sc.aggID, sc.windowMs, sc.p)
	return sc
}

func (sc *sampleController) alignedNow() uint64 {
	now := uint64(time.Now().UnixMilli())
	if sc.windowMs == 0 {
		return now
	}
	return now - (now % sc.windowMs)
}

// currentP returns the live admitted fraction. On a coordinated edge it first
// rolls the window if the wall clock crossed a boundary: it reports the closed
// window's value (which carries the observed rate to the coordinator), then
// re-reads the granted p for the new window. The grant therefore takes effect
// only at boundaries — both SDK-aggregation merge operands share one p.
func (sc *sampleController) currentP() float64 {
	if !sc.coordinated {
		return sc.p
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	now := sc.alignedNow()
	if now != sc.windowStartMs {
		// Close the prior window: a final Observe flushes its value+rate so the
		// coordinator sees this edge's per-window rate (obsCount) and can size p.
		sc.engine.Observe(sc.aggID, nil, sc.windowValue, sc.windowStartMs)
		// Roll to the new epoch.
		sc.engine.EpochReset(now)
		sc.windowStartMs = now
		sc.windowCount = 0
		sc.windowValue = 0
		// Apply the coordinator's granted p for the new window (1.0 if none).
		newP := sc.engine.GrantedSampleP(sc.aggID)
		if newP != sc.p {
			log.Printf("otel-app CDM edge %s: applied granted warm-sample-p %.4f (was %.4f) at window %d",
				sc.edgeID, newP, sc.p, sc.windowStartMs)
		}
		sc.p = newP
	}
	return sc.p
}

// observe counts one candidate point toward the current window's rate and
// advances the window value, then feeds the engine so the reported rate
// (obsCount) and value climb within the epoch.
func (sc *sampleController) observe(seriesID string) {
	if !sc.coordinated {
		return
	}
	sc.mu.Lock()
	// EVERY candidate counts toward the reported RATE (rate_i = total updates).
	sc.windowCount++
	// The reported VALUE is the monitored functional's per-window f_i:
	//   - cms_point (MonitorKey set): f_i = the monitored key's FREQUENCY — only
	//     events matching MonitorKey add to the value. This decouples f_i from
	//     rate_i, so an edge where the key is a fixed-frequency needle in a
	//     higher-rate haystack reports a SMALLER f_i/rate_i and the coordinator's
	//     p_i ∝ √(f_i/rate_i) samples it harder (smaller p). This is the signal
	//     coordinated sampling actually coordinates over.
	//   - sum monitor (MonitorKey empty): legacy behaviour — every candidate adds
	//     a small fixed increment, so the value climbs to cross the per-round
	//     slack (gating emission) while staying below a modest tau. Here value ∝
	//     rate, so the allocation is uniform (correct for a sum: every update is
	//     equal-weight) and differentiation comes only from the ε-floor.
	if sc.monitorKey != "" {
		if seriesID == sc.monitorKey {
			sc.windowValue += 1.0
		}
	} else {
		sc.windowValue += monitorValuePerObs
	}
	sc.engine.Observe(sc.aggID, nil, sc.windowValue, sc.windowStartMs)
	sc.mu.Unlock()
}

func (sc *sampleController) close() {
	if sc.client != nil {
		sc.client.Close()
	}
}

// fetchMonitorKey reads the data-plane streaming-config (the document the
// controller POSTs its monitors: config to) and returns the cms_point key of the
// monitors[] entry whose agg_id matches aggID. ("", nil) means there is no
// matching monitor entry ⇒ the edge falls back to sum-monitor behaviour.
func fetchMonitorKey(url string, aggID uint64) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	// agg_id is a full uint64 (often > 2^53); encoding/json parses an integer
	// JSON number straight into a uint64 field, so no float64 precision loss.
	var doc struct {
		StreamingConfig struct {
			Monitors []struct {
				AggID uint64 `json:"agg_id"`
				Key   string `json:"key"`
			} `json:"monitors"`
		} `json:"streaming_config"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", err
	}
	for _, m := range doc.StreamingConfig.Monitors {
		if m.AggID == aggID {
			return m.Key, nil
		}
	}
	return "", nil
}
