#!/usr/bin/env bash
# soak_rss.sh — Fig 6b: bring up the asap arm and sample agent RSS over a soak
# window to check for a memory leak (slope of RSS vs time). A 24h soak is the
# paper target; this runs SOAK_MIN minutes (default 30) as the in-session proxy
# and reports the MiB/hour slope (extrapolated to 24h).
#
# Usage: TOPOLOGY_ENV=... SKIP_BUILD=1 SKIP_LOAD=1 soak_rss.sh [SOAK_MIN]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPOLOGY_ENV="${TOPOLOGY_ENV:-${SCRIPT_DIR}/../topology.8node.env}"
source "${TOPOLOGY_ENV}"
SOAK_MIN="${1:-30}"
RUN_ID="${RUN_ID:-soak-$(date +%H%M%S)}"
OUT="${RUN_BASE}/${RUN_ID}"; mkdir -p "${OUT}"
CSV="${OUT}/soak_rss.csv"; echo "elapsed_s,node,container,rss_mib,cpu_perc" > "${CSV}"
log(){ printf '[%s] [soak] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "${OUT}/soak.log"; }

# bring up the asap arm (reuses run_demo.sh up)
log "bringing up asap arm for ${SOAK_MIN}min soak"
SKIP_BUILD=1 SKIP_LOAD=1 bash "${SCRIPT_DIR}/run_demo.sh" up asap >>"${OUT}/soak.log" 2>&1

AGENT_NODES=("${NODE0_HOST}" "${NODE3_HOST}")
start=$(date +%s); end=$((start + SOAK_MIN*60))
log "sampling RSS every 30s until +${SOAK_MIN}min"
while [ "$(date +%s)" -lt "${end}" ]; do
    el=$(( $(date +%s) - start ))
    for n in "${AGENT_NODES[@]}"; do
        ssh -o BatchMode=yes "$n" "docker stats --no-stream --format '{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}' 2>/dev/null | grep '^asap-agent'" 2>/dev/null \
        | while IFS='|' read -r name mem cpu; do
            mib=$(echo "$mem" | awk -F'/' '{print $1}' | awk '{v=$1; sub(/[A-Za-z]+/,"",v); u=$1; sub(/^[0-9.]+/,"",u); m=1; if(u=="GiB")m=1024; else if(u=="KiB")m=1/1024; printf "%.1f", v*m}')
            echo "${el},${n},${name},${mib},$(echo "$cpu" | tr -d '%')" >> "${CSV}"
        done
    done
    sleep 30
done

log "soak done; computing RSS slope"
python3 - "${CSV}" <<'PY'
import csv, sys
from collections import defaultdict
rows=defaultdict(list)
for r in csv.DictReader(open(sys.argv[1])):
    try: rows[(r['node'],r['container'])].append((float(r['elapsed_s']), float(r['rss_mib'])))
    except: pass
print("container,n,first_mib,last_mib,slope_mib_per_hr,proj_24h_mib")
for k,v in sorted(rows.items()):
    if len(v)<3: continue
    v.sort()
    n=len(v); xs=[p[0] for p in v]; ys=[p[1] for p in v]
    mx=sum(xs)/n; my=sum(ys)/n
    den=sum((x-mx)**2 for x in xs) or 1
    slope=sum((x-mx)*(y-my) for x,y in zip(xs,ys))/den  # MiB/sec
    sph=slope*3600
    print(f"{k[1]},{n},{ys[0]:.0f},{ys[-1]:.0f},{sph:.2f},{ys[-1]+sph*24:.0f}")
PY

SKIP_BUILD=1 SKIP_LOAD=1 bash "${SCRIPT_DIR}/run_demo.sh" down >>"${OUT}/soak.log" 2>&1
log "torn down -> ${CSV}"
