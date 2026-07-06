#!/usr/bin/env bash
# GOS per-cell threshold cluster eval — a REAL measurement of the
# anisotropic-vs-isotropic delta-communication ratio ρ on live agents (replacing
# the closed-form-only TestGosAnisoSavingsRatio comparison):
#
#   producer (otel-app five-sketch, Zipf(s) endpoint labels)
#     → asap-otel agent (countsketch + gos_delta_epsilon, iso vs aniso arms)
#       → 10 GbE → WARM-node OTLP sink, bytes counted by an iptables
#         dport-4317 counter on the sink node (kernel-side, exact).
#
# Matrix: zipf_s ∈ {1.1, 1.5, 2.0} × {iso, aniso}  (Go rand.Zipf needs s>1).
# Each arm: WARMUP_S to let the first full frame + sketch fill pass, then the
# counter is zeroed and MEASURE_S of steady-state delta traffic is counted —
# so the ratio reflects the DELTA gate, not the shared full-frame cost.
#
# Usage: gos_aniso_cluster.sh
set -uo pipefail

SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MV="$(cd "${SD}/.." && pwd)"
# shellcheck disable=SC1090
source "${TOPOLOGY_ENV:-${MV}/topology.8node.env}"
read -r EDGE_HOST _ <<< "${SRC_HOSTS}"
CFG="${MV}/configs/asap"

WARMUP_S="${WARMUP_S:-20}"
MEASURE_S="${MEASURE_S:-70}"
FREQ_HZ="${FREQ_HZ:-200}"
ENDPOINTS="${ENDPOINTS:-2048}"
SWEEP="${SWEEP:-1.1 1.5 2.0}"
CSV="${GOS_ANISO_OUT:-${MV}/eval-8node/gos_aniso_cluster.csv}"

cleanup() {
  ssh -o BatchMode=yes "${EDGE_HOST}" 'docker rm -f gos-agent gos-producer' >/dev/null 2>&1
  ssh -o BatchMode=yes "${WARM_HOST}" 'docker rm -f gos-sink; sudo iptables -D INPUT -j ASAPGOS 2>/dev/null; sudo iptables -F ASAPGOS 2>/dev/null; sudo iptables -X ASAPGOS 2>/dev/null' >/dev/null 2>&1
}
trap cleanup EXIT
cleanup

echo "== sink up on ${WARM_HOST} (OTLP :4317 → nop; iptables byte counter) =="
scp -q -o BatchMode=yes "${CFG}/gos-eval-sink.yaml" "${WARM_HOST}:/tmp/gos-eval-sink.yaml"
ssh -o BatchMode=yes "${WARM_HOST}" 'docker run -d --network host --name gos-sink \
  -v /tmp/gos-eval-sink.yaml:/etc/otel/config.yaml:ro \
  asap/asap-otel:dev --config=/etc/otel/config.yaml' >/dev/null
ssh -o BatchMode=yes "${WARM_HOST}" 'sudo iptables -N ASAPGOS 2>/dev/null; sudo iptables -F ASAPGOS; \
  sudo iptables -A ASAPGOS -p tcp --dport 4317; \
  sudo iptables -C INPUT -j ASAPGOS 2>/dev/null || sudo iptables -I INPUT -j ASAPGOS'
sleep 3

read_bytes() { # → bytes hitting dport 4317 on the sink node since last zero
  ssh -o BatchMode=yes "${WARM_HOST}" \
    "sudo iptables -L ASAPGOS -v -n -x | awk '/dpt:4317/ {print \$2}'"
}

arm() { # zipf_s aniso(true|false) label
  local s="$1" aniso="$2" label="$3"
  sed -e "s/__ANISO__/${aniso}/" -e "s/__WARM_IP__/${WARM_IP}/" -e "s/__EDGE_ID__/gos-${label}/" \
    "${CFG}/gos-aniso-agent.yaml.tmpl" > /tmp/gos-agent.yaml
  scp -q -o BatchMode=yes /tmp/gos-agent.yaml "${EDGE_HOST}:/tmp/gos-agent.yaml"
  ssh -o BatchMode=yes "${EDGE_HOST}" 'docker rm -f gos-agent gos-producer >/dev/null 2>&1; \
    docker run -d --network host --name gos-agent \
      -v /tmp/gos-agent.yaml:/etc/otel/config.yaml:ro \
      asap/asap-otel:dev --config=/etc/otel/config.yaml' >/dev/null
  sleep 3
  ssh -o BatchMode=yes "${EDGE_HOST}" "docker run -d --network host --name gos-producer \
      asap/otel-app:dev \
      -target=127.0.0.1:4317 -producer-id=gos-p -cardinality=1 -freq-hz=${FREQ_HZ} \
      -five-sketch -five-sketch-endpoints=${ENDPOINTS} -zipf-s=${s} -seed=42" >/dev/null
  sleep "${WARMUP_S}"
  ssh -o BatchMode=yes "${WARM_HOST}" 'sudo iptables -Z ASAPGOS'
  sleep "${MEASURE_S}"
  local bytes
  bytes=$(read_bytes)
  printf "  zipf_s=%-4s %-5s bytes_%ss=%s\n" "$s" "$label" "${MEASURE_S}" "${bytes:-0}"
  echo "$s,$label,${bytes:-0}" >> "$CSV"
  ssh -o BatchMode=yes "${EDGE_HOST}" 'docker rm -f gos-agent gos-producer' >/dev/null 2>&1
}

echo "zipf_s,arm,delta_bytes_${MEASURE_S}s" > "$CSV"
echo "== per-cell GOS: agent on ${EDGE_HOST} → sink on ${WARM_HOST} (${WARM_IP}), ${MEASURE_S}s steady-state per arm =="
for s in ${SWEEP}; do
  arm "$s" false iso
  arm "$s" true  aniso
done

echo
echo "== ρ (aniso/iso bytes) =="
python3 - "$CSV" <<'EOF'
import csv, sys
rows = list(csv.reader(open(sys.argv[1])))[1:]
d = {}
for s, arm, b in rows:
    d.setdefault(s, {})[arm] = int(b)
print(f"{'zipf_s':>7} {'iso':>12} {'aniso':>12} {'rho':>7}")
for s, v in d.items():
    iso, an = v.get('iso', 0), v.get('aniso', 0)
    rho = an / iso if iso else float('nan')
    print(f"{s:>7} {iso:>12} {an:>12} {rho:>7.4f}")
EOF
echo "recorded → $CSV"
