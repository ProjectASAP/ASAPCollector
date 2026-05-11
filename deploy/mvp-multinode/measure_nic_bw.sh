#!/usr/bin/env bash
# measure_nic_bw.sh — sample 10.10.1.x interface tx/rx bytes at 1Hz on the
# local node and write a CSV. With containers using --network host,
# `docker stats` net.io is always 0 — NIC-level counters are the truth.
#
# Usage: measure_nic_bw.sh <duration_s> <out_csv>
set -euo pipefail
DUR=${1:-60}
OUT=${2:-nic.csv}
HOST=$(hostname -s)
IFACE=$(ip -4 -o addr show | awk '$4 ~ /^10\.10\.1\./ {print $2; exit}')
[ -z "$IFACE" ] && { echo "no 10.10.1.x interface found" >&2; exit 1; }

echo "host,iface,sample_ts_ms,window_s,rx_bytes_total,tx_bytes_total,rx_bytes_per_s,tx_bytes_per_s" > "$OUT"

prev_rx=$(cat /sys/class/net/$IFACE/statistics/rx_bytes)
prev_tx=$(cat /sys/class/net/$IFACE/statistics/tx_bytes)
prev_t=$(date +%s.%N)

end_t=$(awk -v t="$prev_t" -v d="$DUR" 'BEGIN{printf "%.6f", t+d}')

while :; do
  sleep 1
  cur_rx=$(cat /sys/class/net/$IFACE/statistics/rx_bytes)
  cur_tx=$(cat /sys/class/net/$IFACE/statistics/tx_bytes)
  cur_t=$(date +%s.%N)
  ts_ms=$(awk -v t="$cur_t" 'BEGIN{printf "%d", t*1000}')
  win=$(awk -v a="$prev_t" -v b="$cur_t" 'BEGIN{printf "%.3f", b-a}')
  d_rx=$((cur_rx - prev_rx))
  d_tx=$((cur_tx - prev_tx))
  rx_ps=$(awk -v d="$d_rx" -v w="$win" 'BEGIN{printf "%.1f", d/w}')
  tx_ps=$(awk -v d="$d_tx" -v w="$win" 'BEGIN{printf "%.1f", d/w}')
  echo "$HOST,$IFACE,$ts_ms,$win,$cur_rx,$cur_tx,$rx_ps,$tx_ps" >> "$OUT"
  prev_rx=$cur_rx; prev_tx=$cur_tx; prev_t=$cur_t
  awk -v c="$cur_t" -v e="$end_t" 'BEGIN{exit !(c>=e)}' && break
done
