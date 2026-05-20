#!/usr/bin/env bash
# measure_backend_rx.sh — backend-ingress RX probe for the bandwidth sweep.
#
# Runs ON node2. Samples, over a steady-state window:
#   1. Total NIC RX on enp130s0f0 (rx_bytes counter delta) — the headline
#      "backend-ingress" metric the sweep table reports.
#   2. Per-destination-port byte accounting via an iptables counting chain
#      so the OTLP/PRW ingest (ports 4317 backend, 8428 VM) can be SEPARATED
#      from the gorillas3 cold-tier MinIO archive PUTs (port 9000). The
#      asap arms colocate MinIO on node2 and write the full-cardinality raw
#      stream to S3 (gorillas3 drop_original:false), so total NIC RX
#      double-counts the cold tier for asap/asap-gzip but NOT for the
#      b0/b1/b2/b3 baselines (VM only, no MinIO). The per-port split makes
#      the apples-to-apples backend-OTLP-only comparison explicit.
#
# Usage:  ssh node2 'bash measure_backend_rx.sh <arm-label> <window_s>'
# Output: one summary block to stdout; parse the `RXSUMMARY` lines.
set -euo pipefail
ARM="${1:?arm label}"
WINDOW="${2:-60}"
NIC="${NIC:-enp130s0f0}"

# Set up an iptables counting chain on INPUT for the relevant dst ports.
sudo iptables -N ASAPBW 2>/dev/null || true
sudo iptables -F ASAPBW
for p in 4317 8428 9000 9091; do
    sudo iptables -A ASAPBW -p tcp --dport "${p}"
done
sudo iptables -C INPUT -j ASAPBW 2>/dev/null || sudo iptables -I INPUT -j ASAPBW
sudo iptables -Z ASAPBW

r1=$(cat "/sys/class/net/${NIC}/statistics/rx_bytes")
t1=$(date +%s.%N)
sleep "${WINDOW}"
r2=$(cat "/sys/class/net/${NIC}/statistics/rx_bytes")
t2=$(date +%s.%N)

# Pull per-port byte counters.
counters=$(sudo iptables -L ASAPBW -v -n -x)

python3 - "$ARM" "$r1" "$r2" "$t1" "$t2" <<PY
import sys, subprocess, re
arm, r1, r2, t1, t2 = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])
dt = t2 - t1
nic_db = r2 - r1
ports = {}
out = subprocess.run(["sudo","iptables","-L","ASAPBW","-v","-n","-x"], capture_output=True, text=True).stdout
for line in out.splitlines():
    m = re.search(r'^\s*\d+\s+(\d+)\s+.*dpt:(\d+)', line)
    if m:
        ports[int(m.group(2))] = int(m.group(1))
def mbps(b): return b*8/dt/1e6
print(f"RXSUMMARY arm={arm} window_s={dt:.1f}")
print(f"RXSUMMARY arm={arm} total_nic_rx_mbps={mbps(nic_db):.3f} bytes={nic_db}")
for p,label in [(4317,'backend_otlp'),(8428,'vm_ingest'),(9000,'minio_archive'),(9091,'backend_query')]:
    b = ports.get(p,0)
    print(f"RXSUMMARY arm={arm} port_{p}_{label}_mbps={mbps(b):.3f} bytes={b}")
PY

sudo iptables -D INPUT -j ASAPBW 2>/dev/null || true
sudo iptables -F ASAPBW 2>/dev/null || true
sudo iptables -X ASAPBW 2>/dev/null || true
