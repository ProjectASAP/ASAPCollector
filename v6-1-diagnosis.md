# MVP v6.1 — three-gap diagnosis

This file records the actual root cause of each integration gap the
v6 demo (PR #300) surfaced as `not-exercised` / `v5-merge-pending` /
empty-CSV. Findings here drive the minimal fixes that the v6.1 PR
applies.

## Gap 1 — controller's typed-stage-split didn't fire (§8 STATUS=`not-exercised`)

### What the v6 demo saw

`controller-emitted-configs/STATUS` = `not-exercised`. The driver's
detector grepped controller stderr for the
`[USE_TYPED_STAGE_SPLIT] pushing typed …` info-level event and
found nothing.

### Actual cause

The typed-stage-split → emitter → push pipeline lives **inside
`handle_plan` in `controller/src/main.rs`** (line 465+). `handle_plan`
runs only on `POST /api/v1/plan`.

The startup workload-registry pre-population path (line 220–258)
calls `analyzer.analyze` + `planner.plan` and stores the result in
`PlanStore`/`WorkloadStore`, but it never invokes the typed-stage-split
block — that block is part of `handle_plan`'s body, not a shared
function.

With the v6 multi-stage overlay, **no client posts to `/api/v1/plan`**
during the demo. The driver only mounts `mvp-v6-workload.yaml` as
`CONTROLLER_WORKLOADS`, which exercises only the pre-population path.
`USE_TYPED_STAGE_SPLIT=1` is set inside the container but the gate it
guards is never reached.

### Confirmation evidence

- `controller/src/main.rs:215-258` — pre-pop loop calls
  `analyzer.analyze` + `planner.plan` only.
- `controller/src/main.rs:398-563` — `handle_plan` is the only
  caller of `planner::stage_split::split_typed_three_stage` +
  `emit_edge_yaml` / `emit_gateway_yaml` / `emit_backend_config_json`.
- v6 driver `run_mvp_demo_v6.sh` never POSTs `/api/v1/plan`.

### Minimal fix

Have the v6.1 driver POST a `QuerySpec` to `/api/v1/plan` per
workload entry once the stack settles. This exercises the
typed-stage-split path on the actual controller process the test
runs against. The driver already knows the workload list (it's just
copying the entries from `mvp-v6-workload.yaml`).

Also wire the controller's `CONTROLLER_BACKEND_ENDPOINT` env in the
v6 overlay so the typed-backend JSON push has a real backend to
target (Phase C plumbing — without this, the Backend stage emits
the JSON but no client pushes it).

## Gap 2 — backend image cache (v5 features absent)

### What the v6 demo saw

§4 postings filtering: `v5-merge-pending` for both queries.
§6 S3-ops cost: all zeros.
Criterion ⑤ cold-fallback: no `gorilla_archive` marker.

### Actual cause — three layered problems

1. **Stale image content.** The v5 features (postings index, S3
   cost tracker, GorillaQueryEngine route gating) live on
   `ASAPQuery-backend` `main` (#90 already merged on origin). But
   the local `asap/query-backend:dev` image (sha `1821e16d2583`)
   was built from a tree that didn't have those commits. Confirmed
   by inspecting the binary:

       docker run --rm --entrypoint sh asap/query-backend:dev \
         -c 'strings /usr/local/bin/asap-query-backend | grep -E "postings_filtered|gorilla_archive|s3_cost"'

   returned **nothing** — the binary contains none of the v5 strings.

2. **No backend-storage-routing wiring.** The v6 multi-stage overlay
   does NOT mount `backend-storage-routing.yaml` or set
   `ASAP_BACKEND_STORAGE_ROUTING`. So even with a v5 image, every
   query lands on the warm tier; the cold-archive route is never
   taken; criterion ⑤ marker can't fire.

3. **No `ASAP_GORILLA_S3_*` env on the backend.** Same overlay
   omits the env block that `baseline-b6-asap-single-sketch.yml`
   carries. Without it, the GorillaQueryEngine isn't even
   registered.

### Minimal fix

1. Force-rebuild backend with `--no-cache` from the v5-merged
   ASAPQuery-backend tree.
2. Add a `backend:` block to `mvp-v6-multi-stage.yml` that mounts
   `backend-storage-routing.yaml` and sets the `ASAP_GORILLA_S3_*`
   + `ASAP_BACKEND_STORAGE_ROUTING` env (mirror what
   `baseline-b6-asap-single-sketch.yml` does).
3. Also seed the `asap-gorilla` MinIO bucket — the gorillas3
   processor needs it to exist (mirror `minio-setup-gorilla` from
   the b6 overlay).

## Gap 3 — freshness probe routing (empty CSVs)

### What the v6 demo saw

`freshness/{raw,warm,archive}.csv` all zero rows / empty.

### Actual cause

1. **Producer side is OK.** `EXPORTER_FRESHNESS_PROBES` defaults
   to `on` (`probes.go:64`). Driver script also exports `on`. The
   fake-exporter does emit the three probes.

2. **Routing config missing.** The v6 overlay's backend has no
   `backend-storage-routing.yaml`, so `http_freshness_probe_warm`
   and `http_freshness_probe_archive` BOTH fall through to the
   default warm tier — they're not dispatched to their tier-specific
   engines. Worse: with no v5 image and no Gorilla engine wired,
   the archive probe has no path to be served.

3. **Driver hits backend for "raw" path too.** The v6 driver
   passes `http://localhost:19091` as the raw endpoint (since B0
   Prometheus isn't in the same compose stack). The "raw" CSV
   captures the warm-tier latency — that's a script-level decision,
   honest given the §7 caveat that B0 isn't co-deployed.

### Minimal fix

Same as Gap 2 fix #2: mount `backend-storage-routing.yaml`. The
file already declares routes for `http_freshness_probe_warm` →
`sketch_warm_tier` and `http_freshness_probe_archive` →
`gorilla_s3_archive`. With the routing wired and a v5 image, both
warm + archive freshness paths produce real data.

For the "raw" CSV: the v6.1 driver continues to hit the backend
warm tier (that's the v6 caveat, repeated honestly here). Two of
three paths is the v6.1 success criterion.
