// controller is the DataCollector control plane.
// It accepts QueryWorkload submissions via HTTP, plans a CollectionPlan using
// the rule-based planner, generates OTel collector YAML, and pushes it to
// connected agents via the OpAMP WebSocket server.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ProjectASAP/controller/internal/analyzer"
	"github.com/ProjectASAP/controller/internal/config"
	"github.com/ProjectASAP/controller/internal/opamp"
	"github.com/ProjectASAP/controller/internal/planner"
	"github.com/ProjectASAP/controller/internal/store"
)

const (
	defaultListenAddr  = ":8080"
	defaultOpAMPAddr   = ":4320"
	defaultOpAMPEndpoint = "ws://controller:4320/v1/opamp"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	listenAddr := envOr("CONTROLLER_ADDR", defaultListenAddr)
	opampAddr := envOr("CONTROLLER_OPAMP_ADDR", defaultOpAMPAddr)
	opampEndpoint := envOr("CONTROLLER_OPAMP_ENDPOINT", defaultOpAMPEndpoint)

	// Wire up components.
	an := analyzer.New()
	pl := planner.NewRulesPlanner()
	ps := store.New()
	opampSrv := opamp.New(logger)

	// Start re-planning loop for expired plans.
	go replanLoop(logger, ps, pl, opampSrv, opampEndpoint)

	// ── HTTP API server ───────────────────────────────────────────────────────

	mux := http.NewServeMux()

	// POST /api/v1/plan — submit a QueryWorkload, receive a CollectionPlan.
	mux.HandleFunc("POST /api/v1/plan", func(w http.ResponseWriter, r *http.Request) {
		var spec analyzer.QuerySpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		workload, err := an.Analyze(spec)
		if err != nil {
			http.Error(w, "invalid workload: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}

		plan := pl.Plan(workload)
		ps.Set(workload.MetricName, plan)

		// Generate and push agent config to all connected agents.
		agentYAML, err := config.GenerateAgentConfig(plan.AgentConfig, opampEndpoint)
		if err != nil {
			logger.Error("generate agent config", "err", err)
		} else {
			hash := yamlHash(agentYAML)
			opampSrv.PushAll(opamp.RemoteConfig{
				ConfigHash: hash,
				YAML:       agentYAML,
				IssuedAt:   time.Now(),
			})
			logger.Info("plan applied and pushed",
				"metric", workload.MetricName,
				"sketch", plan.AgentConfig.SketchType,
				"mode", plan.AgentConfig.Mode,
				"agents", len(opampSrv.ConnectedAgents()),
			)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	// GET /api/v1/plan/{metric} — get the current plan for a metric.
	mux.HandleFunc("GET /api/v1/plan/{metric}", func(w http.ResponseWriter, r *http.Request) {
		metric := r.PathValue("metric")
		plan, err := ps.Get(metric)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	// POST /api/v1/plan/{metric}/rollback — rollback to the previous plan.
	mux.HandleFunc("POST /api/v1/plan/{metric}/rollback", func(w http.ResponseWriter, r *http.Request) {
		metric := r.PathValue("metric")
		plan, err := ps.Rollback(metric)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	// GET /api/v1/agents — list connected agents.
	mux.HandleFunc("GET /api/v1/agents", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(opampSrv.ConnectedAgents())
	})

	// ── OpAMP WebSocket server ────────────────────────────────────────────────

	opampMux := http.NewServeMux()
	opampMux.HandleFunc("/v1/opamp", opampSrv.Handler())

	go func() {
		logger.Info("OpAMP server listening", "addr", opampAddr)
		if err := http.ListenAndServe(opampAddr, opampMux); err != nil {
			logger.Error("OpAMP server error", "err", err)
			os.Exit(1)
		}
	}()

	logger.Info("controller API listening", "addr", listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		logger.Error("controller error", "err", err)
		os.Exit(1)
	}
}

// replanLoop periodically checks for expired plans and re-plans them.
func replanLoop(
	logger *slog.Logger,
	ps *store.PlanStore,
	pl *planner.RulesPlanner,
	_ *opamp.Server,
	_ string,
) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		expired := ps.Expired(time.Now())
		for _, metric := range expired {
			logger.Info("plan expired, will re-plan on next workload submission", "metric", metric)
		}
	}
}

func yamlHash(yaml string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(yaml)))[:16]
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
