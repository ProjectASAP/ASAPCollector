# Deploy TODO

Scaffold landed in this PR. Three image supply-chain gaps +
Helm templates are the remaining work before a real N=100 run.

## Image supply chain

- [ ] `deploy/docker/Dockerfile.sketchcol` — build the edge
      agent by layering ASAP's sketch processors onto
      `otel-collector-contrib`. `build_sketchcollector.sh` at
      the repo root has the patch-apply logic; wrap it in a
      multi-stage Dockerfile.
- [ ] `deploy/docker/Dockerfile.backend` — should be a one-file
      Rust multi-stage build on the `ASAPQuery-backend` side.
      Tracked in that repo's TODO.md.
- [ ] `deploy/docker/Dockerfile.fake-exporter` — replay
      `datasets_eval/google-cluster-2019` trace at configurable
      rate/cardinality. Go binary, small image.

## Helm manifests

The `values.yaml` + `Chart.yaml` land here. Templates don't —
writing them properly wants an initial pass on a real cluster
to validate the readiness probes, resource requests, and
network policies. Do one at a time:

- [ ] `templates/controller.yaml` — Deployment + Service
- [ ] `templates/backend.yaml` — Deployment + Service +
      PVC for sketch-DB disk
- [ ] `templates/gateway.yaml` — Deployment + Service
- [ ] `templates/agents.yaml` — Deployment (replicas =
      `{{ .Values.agents.count }}`) + headless Service for
      Prometheus DNS SD
- [ ] `templates/minio.yaml` — StatefulSet + PVC + Service
      (gate on `.Values.minio.enabled`)
- [ ] `templates/prometheus.yaml` + `templates/grafana.yaml`
- [ ] `templates/_helpers.tpl` — label selectors, name prefix

## Missing polish

- [ ] Per-agent `AGENT_ID` label in compose — the static
      enumeration works for N ≤ 100 but makes Prometheus target
      lists verbose. Once `asap/sketchcol` is built, the agent
      can emit its own hostname as a label and the scrape
      config can go back to a single DNS-SD rule.
- [ ] `deploy/k8s/` manifests as an alternative to Helm for
      operators who don't want Helm.
- [ ] CI check that compose files parse clean and base.yml +
      agents-N<K>.yml merge produces a valid combined config.

## Not in scope here

Post-image-build, the next DC blockers are:

- DC #2 Instrumentation — Prometheus scrape config for each
  node + Grafana dashboards. Partly covered by
  `configs/prometheus.yml` + `configs/grafana-datasources.yml`
  here, but the actual dashboards don't exist yet.
- DC #3 Baselines B0-B3 — one compose overlay per baseline.
  Cleaner to land when sketchcol image is buildable.
- DC #4 Workload replay harness — needs the
  `asap/fake-exporter` image first.
- DC #5 Controller feedback loop e2e on real workload — needs
  all of the above.
