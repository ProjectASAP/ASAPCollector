# Runtime accuracy feedback and replanning

> Status: proposed control-loop extension

The runtime evidence loop compares materialized-summary answers with eligible exact
or higher-fidelity references and reports empirical error, coverage, freshness,
latency, and sample count. Every observation is keyed by tenant, plan version,
materialization/SID, query class, time interval, algorithm parameters, and evaluator
version so evidence cannot leak between incompatible policies.

```text
Collector/backend measurements -> evidence aggregation -> Planner profile update
                              -> threshold/hysteresis -> new plan generation
```

Theoretical sketch guarantees remain legality constraints. Empirical evidence may
improve cost/risk estimates but cannot certify an algorithm for an unsupported query
capability. Replanning uses minimum sample sizes, confidence bounds, cooldown, and
change thresholds to avoid oscillation. Raw comparison data follows tenant and
retention policy; the Planner receives bounded aggregate profiles.

