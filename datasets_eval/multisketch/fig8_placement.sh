#!/usr/bin/env bash
# fig8_placement.sh — Fig 8 cross-layer placement: same DDSketch agg_type computed
# at the SDK vs the agent, measuring where the CPU/RSS lands across the
# producer / agent / backend layers. Single cold-OFF stack on node0; only the
# producer's -agg changes between arms:
#   agent placement : producer emits raw-buffer -> the asap_edge agent sketches
#   sdk placement   : producer emits AggregationDDSketch -> agent forwards
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT=/mydata/ASAPCollector
OUT="${OUT:-/mydata/eval/results/fig8}"; mkdir -p "${OUT}"
CSV="${OUT}/fig8.csv"; echo "placement,layer,container,cpu_perc,rss_mib" > "${CSV}"
log(){ printf '[%s] [fig8] %s\n' "$(date +%H:%M:%S)" "$*"; }

bash "${HERE}/stack-coldoff.sh" down >/dev/null 2>&1 || true
log "stack up (ddsketch cold-OFF)"
bash "${HERE}/stack-coldoff.sh" up "${HERE}/workloads/ddsketch.yaml" \
     "${HERE}/agent-ddsketch-coldoff.yaml" >"${OUT}/stack.log" 2>&1
sleep 6

# sample mean CPU/RSS of a container over ~24s (6 snapshots)
sample(){ # sample CONTAINER
  python3 - "$1" <<'PY'
import subprocess,sys,re,time
c=sys.argv[1]; cpu=[]; mem=[]
for _ in range(6):
    try:
        o=subprocess.run(["docker","stats","--no-stream","--format","{{.CPUPerc}}|{{.MemUsage}}",c],
                         capture_output=True,text=True,timeout=8).stdout.strip()
        if "|" in o:
            cp,mm=o.split("|"); cpu.append(float(cp.strip("% ")))
            m=re.search(r'([\d.]+)([KMG]i?B)',mm);
            if m: mem.append(float(m.group(1))*{"KiB":1/1024,"MiB":1,"GiB":1024,"KB":1/1024,"MB":1,"GB":1024}.get(m.group(2),1))
    except: pass
    time.sleep(4)
print(f"{(sum(cpu)/len(cpu) if cpu else 0):.1f},{(sum(mem)/len(mem) if mem else 0):.0f}")
PY
}

run_arm(){ # run_arm PLACEMENT AGG
  local placement=$1 agg=$2
  docker rm -f asap-prod-f8 >/dev/null 2>&1 || true
  log "${placement}: producer -agg=${agg}"
  docker run -d --network host --name asap-prod-f8 asap/otel-app:dev \
    -target=127.0.0.1:4317 -producer-id=f8 -metric=http_requests_total \
    -cardinality=500 -freq-hz=100 -sdk-window=1s -agg="${agg}" \
    -five-sketch=false -freshness-probes=false >/dev/null
  sleep 35   # warm + steady state
  for pair in "producer:asap-prod-f8" "agent:asap-agent-a" "backend:asap-data-plane"; do
    layer=${pair%%:*}; c=${pair#*:}
    stat=$(sample "$c")
    echo "${placement},${layer},${c},${stat}" >> "${CSV}"
    log "  ${layer} (${c}): cpu%,rss=${stat}"
  done
  docker rm -f asap-prod-f8 >/dev/null 2>&1 || true
  sleep 3
}

run_arm agent   raw-buffer
run_arm sdk     ddsketch

log "done -> ${CSV}"; column -t -s, "${CSV}"
bash "${HERE}/stack-coldoff.sh" down >/dev/null 2>&1 || true
