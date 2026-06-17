// sample_controller.go — CDM coordinated warm-sampling for the producer.
//
// Priority 2: turn otel-app into a CDM *edge*. Instead of a static
// -warm-sample-p, the producer's admitted fraction p is driven by the
// coordinator's grant (Grant.SampleP), computed by the data-plane monitor
// coordinator from each edge's reported per-window (rate, f_i)
// (AllocateSampleRates: p_i ∝ √(f_i/rate_i), so the hot edge gets a smaller p).
// The grant is applied at the NEXT window boundary, never mid-window.
//
// Multi-monitor: an edge's stream can feed SEVERAL cms_point monitors at once
// (different keys / different series). The edge learns ALL of them from the
// controller-pushed config, maintains a per-monitor f_m (the frequency of that
// monitor's key) plus a shared rate, reports each monitor under its own agg_id,
// and — since the warm-sample p is a SINGLE admission for the whole warm stream —
// applies p = max_m p_m (the least-aggressive grant), which keeps every monitor's
// variance budget satisfied.
//
// The wire contract reuses asap-precompute-go/monitor/grpcclient
// (grpcclient.New(coordURL, engine)) driving one monitor.Engine that multiplexes
// all agg_ids: Engine.Observe(aggID, key, value, windowStart) per monitor,
// Engine.GrantedSampleP(aggID) per monitor at each boundary.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient"
)

// monitorValuePerObs is the small per-candidate increment of a sum-monitor's
// value (see observe). Kept tiny so per-window values stay far below a modest
// tau (grant regime) while still crossing the per-round slack so a Report fires.
const monitorValuePerObs = 0.5

// monitorEntry is one cms_point/sum monitor this edge coordinates on. `key` is
// the heavy-hitter series whose per-window frequency is reported as this
// monitor's f_m; an empty key means a sum monitor (value ∝ rate). windowValue is
// this monitor's running f_m for the current window.
type monitorEntry struct {
	aggID uint64
	key   string
	// keyBytes is key as []byte, precomputed once: the coordinator identifies a
	// monitor by (agg_id, key), so the engine MUST register/report under this
	// exact key (not nil) or the coordinator rejects it as an unconfigured
	// monitor. Cached to avoid a per-observe allocation on the replay hot path.
	keyBytes []byte
	// functional selects what windowValue means: "cms_point" = count of `key`;
	// "f2" = whole-sketch L2 mass Σ_x f(x)² (no key, identity (agg_id,"")); "" /
	// "sum" = sum monitor (value ∝ rate). The coordinator allocation is the same
	// p_i ∝ √(value/rate) regardless — only the reported scalar differs.
	functional  string
	windowValue float64
	// counts is the per-series window count, F2 only: windowValue tracks the
	// running Σ f(x)² incrementally (an event on x with old count c adds
	// (c+1)²−c² = 2c+1), so the coordinator sees F2 grow monotonically within the
	// epoch (it drives the slack countdown exactly like cms_point's count).
	counts map[string]float64
}

// newMonitorEntry resolves the effective functional and initializes per-mode
// state. An empty functional is inferred from the key (key set ⇒ cms_point, else
// sum), matching pre-functional configs. F2 (whole-sketch L2) carries no key —
// its identity is (agg_id, "") — and needs the per-series count map.
func newMonitorEntry(aggID uint64, key, functional string) monitorEntry {
	fn := functional
	if fn == "l2" {
		fn = "f2"
	}
	if fn == "" {
		if key != "" {
			fn = "cms_point"
		} else {
			fn = "sum"
		}
	}
	if fn == "f2" {
		key = "" // whole-sketch: no per-point key
	}
	e := monitorEntry{aggID: aggID, key: key, keyBytes: []byte(key), functional: fn}
	if fn == "f2" {
		e.counts = make(map[string]float64)
	}
	return e
}

// sampleController holds the current warm-sample probability and, when
// coordinated, the CDM engine + transport. currentP/observe are called on the
// replay hot path; they are cheap and lock-guarded.
type sampleController struct {
	mu sync.Mutex

	// p is the live admitted fraction. Bootstrapped to cfg.WarmSampleP; when
	// coordinated, replaced at each window boundary by max_m(granted p_m).
	p float64

	coordinated bool
	edgeID      string
	windowMs    uint64

	// monitors this edge reports f_m for (one or more); the admission p is the
	// max of their grants.
	monitors []monitorEntry

	engine *monitor.Engine
	client *grpcclient.Client

	// window bookkeeping (rate is shared across monitors; f_m is per-monitor)
	windowStartMs uint64 // current epoch's aligned start (ms since unix epoch)
	windowCount   uint64 // admitted-candidate count this window (the reported rate)
}

// newSampleController builds the controller. When cfg.CoordinatorURL is empty it
// is a static holder returning cfg.WarmSampleP. Otherwise it dials the
// coordinator and learns the monitor set it coordinates on.
func newSampleController(cfg Config, metricName string) *sampleController {
	sc := &sampleController{p: cfg.WarmSampleP}
	if cfg.CoordinatorURL == "" {
		return sc
	}
	sc.coordinated = true

	// Monitor set: learn ALL monitors from the controller-pushed config; an edge
	// can feed several cms_point monitors (different keys/series) at once.
	if cfg.MonitorConfigURL != "" {
		if ms, err := fetchMonitors(cfg.MonitorConfigURL); err != nil {
			log.Printf("otel-app CDM edge: could not learn monitors from %s: %v", cfg.MonitorConfigURL, err)
		} else {
			for _, m := range ms {
				sc.monitors = append(sc.monitors, newMonitorEntry(m.AggID, m.Key, m.Functional))
			}
			descs := make([]string, len(ms))
			for i, m := range ms {
				descs[i] = fmt.Sprintf("agg_id=%d func=%q key=%q", m.AggID, sc.monitors[i].functional, m.Key)
			}
			log.Printf("otel-app CDM edge: learned %d monitor(s) from controller config (%s): [%s]",
				len(ms), cfg.MonitorConfigURL, strings.Join(descs, "; "))
		}
	}
	// Fallback / manual override: a single monitor from the flags.
	if len(sc.monitors) == 0 {
		sc.monitors = []monitorEntry{newMonitorEntry(cfg.MonitorAggID, cfg.MonitorKey, cfg.MonitorFunctional)}
	}
	// -monitor-functional OVERRIDES the learned/inferred functional on every
	// monitor, so the flag forces the reporting mode even when the controller
	// config omits/disagrees on `functional` (e.g. an older control plane). With
	// the flag unset, the auto-learned functional stands.
	if cfg.MonitorFunctional != "" {
		for i := range sc.monitors {
			sc.monitors[i] = newMonitorEntry(sc.monitors[i].aggID, sc.monitors[i].key, cfg.MonitorFunctional)
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
	log.Printf("otel-app CDM edge: coordinator=%s edge_id=%s monitors=%d window_ms=%d bootstrap_p=%.3f",
		cfg.CoordinatorURL, sc.edgeID, len(sc.monitors), sc.windowMs, sc.p)
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
// rolls the window if the wall clock crossed a boundary: it flushes each
// monitor's (f_m, rate) to the coordinator, then re-reads the grant for each and
// applies p = max_m p_m for the new window (a single admission for the whole warm
// stream must satisfy EVERY monitor's variance budget, so it takes the least
// aggressive grant).
func (sc *sampleController) currentP() float64 {
	if !sc.coordinated {
		return sc.p
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	now := sc.alignedNow()
	if now != sc.windowStartMs {
		// Close the prior window: flush each monitor's value+rate.
		for i := range sc.monitors {
			sc.engine.Observe(sc.monitors[i].aggID, sc.monitors[i].keyBytes, sc.monitors[i].windowValue, sc.windowStartMs)
		}
		sc.engine.EpochReset(now)
		sc.windowStartMs = now
		sc.windowCount = 0
		// Apply the new grants: p = max over monitors (1.0 if none granted).
		maxP := 0.0
		for i := range sc.monitors {
			sc.monitors[i].windowValue = 0
			if sc.monitors[i].counts != nil { // f2: clear per-series counts for the new epoch
				clear(sc.monitors[i].counts)
			}
			pm := sc.engine.GrantedSampleP(sc.monitors[i].aggID)
			if pm > maxP {
				maxP = pm
			}
		}
		if maxP <= 0.0 {
			maxP = 1.0
		}
		if maxP != sc.p {
			log.Printf("otel-app CDM edge %s: applied warm-sample-p %.4f (was %.4f) = max over %d monitors at window %d",
				sc.edgeID, maxP, sc.p, len(sc.monitors), sc.windowStartMs)
		}
		sc.p = maxP
	}
	return sc.p
}

// observe counts one candidate toward the shared rate and toward each monitor's
// f_m (only when seriesID matches that monitor's key; sum monitors add a fixed
// increment), then feeds the engine per monitor so the reported (rate, f_m)
// climb within the epoch.
func (sc *sampleController) observe(seriesID string) {
	if !sc.coordinated {
		return
	}
	sc.mu.Lock()
	// EVERY candidate counts toward the shared RATE (rate_i = total updates).
	sc.windowCount++
	for i := range sc.monitors {
		m := &sc.monitors[i]
		switch m.functional {
		case "f2":
			// whole-sketch L2: windowValue = running Σ_x f(x)². Incremental:
			// counts[x] c→c+1 raises the square by (c+1)²−c² = 2c+1.
			c := m.counts[seriesID]
			m.windowValue += 2*c + 1
			m.counts[seriesID] = c + 1
		case "cms_point":
			if seriesID == m.key {
				m.windowValue += 1.0
			}
		default: // sum / whole-stream: value ∝ rate
			m.windowValue += monitorValuePerObs
		}
		// Observe under each agg_id every event so its rate (obsCount) is the
		// TOTAL and its value is this monitor's reported scalar (f_m / F2 / sum).
		sc.engine.Observe(m.aggID, m.keyBytes, m.windowValue, sc.windowStartMs)
	}
	sc.mu.Unlock()
}

func (sc *sampleController) close() {
	if sc.client != nil {
		sc.client.Close()
	}
}

// monitorDecl is one entry of the data-plane streaming-config `monitors:` list.
type monitorDecl struct {
	AggID      uint64 `json:"agg_id"`
	Functional string `json:"functional"` // "", "sum", "cms_point", "f2"
	Key        string `json:"key"`
}

// fetchMonitors reads the data-plane streaming-config (the document the
// controller POSTs its monitors: config to) and returns ALL monitor entries.
// agg_id is a full uint64 (often > 2^53); encoding/json parses an integer JSON
// number straight into a uint64 field, so there is no float64 precision loss.
func fetchMonitors(url string) ([]monitorDecl, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var doc struct {
		StreamingConfig struct {
			Monitors []monitorDecl `json:"monitors"`
		} `json:"streaming_config"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	return doc.StreamingConfig.Monitors, nil
}
