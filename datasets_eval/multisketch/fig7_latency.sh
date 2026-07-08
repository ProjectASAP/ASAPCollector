#!/usr/bin/env bash
# fig7_latency.sh — Fig 7 cold-OFF warm-tier query latency CDF.
#
# Brings up the cold-OFF (warm-only) all-families stack on node0, replays the
# aliased gct trace to populate the DDSketch warm window, then replays warm
# latency queries (queries-latency-warm.json) against the warm tier :9091 and
# reports p50/p99. cold OFF => no Thanos archive failover, so range quantiles
# resolve warm (the design's ~20ms target) unlike the cold-ON multinode sweep.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT=/mydata/ASAPCollector
GCT="${ROOT}/datasets_eval/google_cluster"
TRACE="${TRACE:-/tmp/perfam-ddsketch.jsonl}"   # aliased trace (contains _q_ddsketch)
QUERIES="${ROOT}/deploy/mvp-singlenode/scripts/queries-latency-warm.json"
OUT="${OUT:-/mydata/eval/results/fig7}"; mkdir -p "${OUT}"
log(){ printf '[%s] [fig7] %s\n' "$(date +%H:%M:%S)" "$*"; }

bash "${HERE}/stack-coldoff.sh" down >/dev/null 2>&1 || true
log "stack up (all-families cold-OFF)"
bash "${HERE}/stack-coldoff.sh" up "${HERE}/workloads/all-families.yaml" \
     "${HERE}/agent-allfamilies-coldoff.yaml" >"${OUT}/stack.log" 2>&1
sleep 5

log "replay aliased trace (wall-clock anchored) to populate warm DDSketch"
python3 "${GCT}/run.py" replay --jsonl "${TRACE}" --endpoint 127.0.0.1:4317 \
    --pace-factor 0 --wall-clock-anchor >"${OUT}/replay.log" 2>&1

# wait until the warm quantile resolves (sketch sealed+queryable)
log "waiting for warm window to seal..."
for i in $(seq 1 30); do
  r=$(curl -s -G "http://127.0.0.1:9091/api/v1/query" \
       --data-urlencode "query=quantile_over_time(0.50, google_cluster_2019_cpu_rate_q_ddsketch[300s])" 2>/dev/null)
  echo "$r" | grep -q '"result":\[{' && { log "warm queryable"; break; }
  sleep 4
done

log "replay latency queries (60s @ 15 qps)"
python3 "${ROOT}/deploy/mvp-singlenode/scripts/metricsql_replay.py" \
    --target http://127.0.0.1:9091 --queries "${QUERIES}" \
    --qps 15 --duration 60 --no-plan-poll --out "${OUT}/fig7_latency.jsonl" \
    >"${OUT}/replay_lat.log" 2>&1 || true

log "per-query latency stats"
python3 - "${OUT}/fig7_latency.jsonl" <<'PY'
import json, sys
from collections import defaultdict
by=defaultdict(list); allv=[]
for ln in open(sys.argv[1]):
    try:
        d=json.loads(ln); v=d.get('duration_ms') or d.get('latency_ms')
        if v is None: continue
        k=d.get('kind') or d.get('id') or 'all'; by[k].append(float(v)); allv.append(float(v))
    except: pass
def stat(v):
    v=sorted(v); n=len(v); return (n, v[n//2], v[min(n-1,int(n*0.99))]) if n else (0,0,0)
for k in list(by)+['all']:
    v=allv if k=='all' else by[k]; n,p50,p99=stat(v)
    print(f"{k}: n={n} p50={p50:.2f} p99={p99:.2f} ms")
PY
bash "${HERE}/stack-coldoff.sh" down >/dev/null 2>&1 || true
log "done -> ${OUT}/fig7_latency.jsonl"
