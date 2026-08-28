# Operations and troubleshooting

Use this guide after the collector has been built or while running the MVP
deployment. It distinguishes process health from end-to-end readiness.

## Quick checks

1. Confirm that the expected collector process or container is running.
2. Check the collector health endpoint configured by the active YAML (the
   production examples use the OpenTelemetry health-check extension).
3. Confirm that the agent appears in the control plane:

   ```bash
   curl -fsS http://CONTROL_PLANE:8080/api/v1/agents
   ```

4. Fetch the controller-generated bootstrap configuration for a known agent:

   ```bash
   curl -fsS -H 'X-Agent-ID: AGENT_ID' \
     http://CONTROL_PLANE:8080/api/v1/collector-config/agent
   ```

5. Query the backend and inspect response provenance and accuracy annotations.
   A healthy process with no applied plan or no backend-visible data is not
   ready.

Replace `CONTROL_PLANE` and `AGENT_ID` with deployment values. Do not publish
bootstrap output without checking it for environment-specific endpoints.

## Common failures

| Symptom | Likely boundary | Check |
| --- | --- | --- |
| Collector does not start | Bootstrap/build | YAML parsing, missing component, port collision, and builder version |
| Agent absent from control plane | OpAMP connectivity | Controller endpoint, DNS/TLS, agent identity, and controller logs |
| Plan delivered but not active | Plan validation | Target instance, schema version, plan ordering, and referenced exporter |
| Metrics arrive but queries miss | Processing/identity | Metric name, retained labels, window, summary family, and plan identity on both sides |
| Query is unexpectedly exact | Routing | Response provenance and whether the query is in the supported catalog |
| Results are stale | Source-to-query path | Source timestamps, active-window updates, exporter retries, and backend ingest |
| Build ignores a local change | Patch workflow | Whether the edit exists in the tracked overlay and local module replacement |

## Collect useful evidence

Capture the collector commit, active configuration hash or plan version,
container logs, controller agent status, backend query response, and timestamps
from the same interval. Keep secrets and credentials out of issue attachments.

For the four-node harness, prefer its generated run artifacts over manually
assembled snippets:

```bash
bash deploy/mvp-multinode/scripts/run_demo.sh all
```

The run directory contains the manifest, query evidence, measurements, and final
verdict. See the [MVP demo runbook](mvp-demo-runbook.md) for acceptance rules and
[distributed setup](otel-distributed-setup.md) for topology configuration.
