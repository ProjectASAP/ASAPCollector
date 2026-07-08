#!/usr/bin/env bash
# snapshot_resources.sh — capture per-container CPU/mem and per-node
# NIC tx/rx for a running arm over a measurement window. Stdlib bash;
# fans out via ssh and dumps CSV per node + a combined summary.
#
# Usage: snapshot_resources.sh <arm_label> <duration_s> <out_dir>
set -euo pipefail
ARM=${1:?arm}
DUR=${2:-60}
OUT=${3:-/mydata/mvp-multinode/results/snap}
mkdir -p "$OUT"

# Node set is configurable so the same snapshotter works for the 4-node MVP
# and the 8-node scaling cluster. Defaults to the original 4 nodes.
read -ra NODES <<< "${SNAP_NODES:-node0 node1 node2 node3}"

# Resolve the experiment-LAN NIC by IP subnet instead of a hard-coded name —
# CloudLab hardware varies (enp130s0f0 on the original 4-node profile, eno2 on
# the 8-node profile). Mirrors the auto-detect already in measure_nic_bw.sh.
IFACE_PROBE='iface=$(ip -4 -o addr show | awk '"'"'$4 ~ /^10\.10\.1\./ {print $2; exit}'"'"'); iface=${iface:-eth0}'

echo "[snap] arm=${ARM} duration=${DUR}s out=${OUT}"

# 1. Capture START NIC bytes on each node and START container stats.
declare -A start_rx start_tx start_t end_rx end_tx end_t
for n in "${NODES[@]}"; do
  start=$(ssh "$n" "${IFACE_PROBE}"'; echo "$(cat /sys/class/net/$iface/statistics/rx_bytes) $(cat /sys/class/net/$iface/statistics/tx_bytes) $(date +%s.%N)"')
  start_rx[$n]=$(echo "$start" | awk '{print $1}')
  start_tx[$n]=$(echo "$start" | awk '{print $2}')
  start_t[$n]=$(echo "$start" | awk '{print $3}')
done

# 2. Sample container stats periodically — captures the time-averaged CPU/mem
#    for each container on each node into a per-node CSV.
for n in "${NODES[@]}"; do
  ssh "$n" "rm -f /tmp/snap-${n}.csv; \
     end=\$((\$(date +%s)+${DUR})); \
     echo 'sample_ts_ms,name,cpu_perc,mem_usage_bytes,mem_limit_bytes' > /tmp/snap-${n}.csv; \
     while [ \$(date +%s) -lt \$end ]; do \
       ts=\$(($(date +%s%N)/1000000)); \
       docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null | grep '^asap-' | \
         while IFS='|' read -r name cpu mem; do \
           cpu_v=\$(echo \"\$cpu\" | tr -d '%'); \
           mem_used=\$(echo \"\$mem\" | awk -F'/' '{print \$1}' | tr -d ' '); \
           mem_lim=\$(echo \"\$mem\" | awk -F'/' '{print \$2}' | tr -d ' '); \
           # convert sizes like 1.234GiB / 567MiB to bytes
           mem_used_b=\$(echo \"\$mem_used\" | awk '{
             v=\$1; sub(/[A-Za-z]+/,\"\",v);
             u=\$1; sub(/^[0-9.]+/,\"\",u);
             m=1; if(u==\"KiB\") m=1024; else if(u==\"MiB\") m=1024*1024;
             else if(u==\"GiB\") m=1024*1024*1024; else if(u==\"TiB\") m=1024^4;
             printf(\"%d\", v*m)}'); \
           mem_lim_b=\$(echo \"\$mem_lim\" | awk '{
             v=\$1; sub(/[A-Za-z]+/,\"\",v);
             u=\$1; sub(/^[0-9.]+/,\"\",u);
             m=1; if(u==\"KiB\") m=1024; else if(u==\"MiB\") m=1024*1024;
             else if(u==\"GiB\") m=1024*1024*1024; else if(u==\"TiB\") m=1024^4;
             printf(\"%d\", v*m)}'); \
           echo \"\$ts,\$name,\$cpu_v,\$mem_used_b,\$mem_lim_b\" >> /tmp/snap-${n}.csv; \
         done; \
       sleep 5; \
     done" &
done
wait

# 3. Capture END NIC bytes on each node.
for n in "${NODES[@]}"; do
  ev=$(ssh "$n" "${IFACE_PROBE}"'; echo "$(cat /sys/class/net/$iface/statistics/rx_bytes) $(cat /sys/class/net/$iface/statistics/tx_bytes) $(date +%s.%N)"')
  end_rx[$n]=$(echo "$ev" | awk '{print $1}')
  end_tx[$n]=$(echo "$ev" | awk '{print $2}')
  end_t[$n]=$(echo "$ev" | awk '{print $3}')
done

# 4. Per-node NIC summary
{
  echo "arm,node,window_s,rx_bytes_total,tx_bytes_total,rx_bytes_per_s,tx_bytes_per_s"
  for n in "${NODES[@]}"; do
    win=$(awk -v a="${start_t[$n]}" -v b="${end_t[$n]}" 'BEGIN{printf "%.3f", b-a}')
    drx=$((end_rx[$n] - start_rx[$n]))
    dtx=$((end_tx[$n] - start_tx[$n]))
    rxps=$(awk -v d="$drx" -v w="$win" 'BEGIN{printf "%.0f", d/w}')
    txps=$(awk -v d="$dtx" -v w="$win" 'BEGIN{printf "%.0f", d/w}')
    echo "${ARM},${n},${win},${drx},${dtx},${rxps},${txps}"
  done
} > "${OUT}/nic-${ARM}.csv"

# 5. Pull container-stats csv from each node, merge, summarize per container.
for n in "${NODES[@]}"; do
  scp -q "$n:/tmp/snap-${n}.csv" "${OUT}/container-${ARM}-${n}.csv" 2>&1 || true
done

# 6. Per-container summary across all nodes (mean cpu%, max mem)
python3 - <<EOF > "${OUT}/container-summary-${ARM}.csv"
import csv, glob, os
arms_cont = {}  # (name, host) -> [cpu_samples], [mem_samples]
for f in glob.glob("${OUT}/container-${ARM}-*.csv"):
    host = os.path.basename(f).replace("container-${ARM}-","").replace(".csv","")
    with open(f) as fh:
        r = csv.DictReader(fh)
        for row in r:
            try:
                cpu = float(row["cpu_perc"])
                mem = int(row["mem_usage_bytes"])
            except:
                continue
            k = (row["name"], host)
            arms_cont.setdefault(k, ([],[]))
            arms_cont[k][0].append(cpu)
            arms_cont[k][1].append(mem)
print("arm,host,container,cpu_mean_perc,cpu_max_perc,mem_mean_mib,mem_max_mib,n_samples")
for (name, host), (cpus, mems) in sorted(arms_cont.items()):
    if not cpus: continue
    cpu_mean = sum(cpus)/len(cpus); cpu_max = max(cpus)
    mem_mean = (sum(mems)/len(mems))/1024/1024; mem_max = max(mems)/1024/1024
    print(f"${ARM},{host},{name},{cpu_mean:.1f},{cpu_max:.1f},{mem_mean:.1f},{mem_max:.1f},{len(cpus)}")
EOF

echo "[snap] arm=${ARM} done; outputs:"
ls -la "${OUT}"/*${ARM}* 2>&1 | head
