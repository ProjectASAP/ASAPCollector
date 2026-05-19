#!/usr/bin/env bash
# run_demo_sweep.sh — fully unattended sweep over B0 / B1 / ASAP, capturing
# per-container resource snapshots + per-node NIC bandwidth + PromQL
# query latency for each arm, then aggregating into a single report.
#
# Each arm:
#   1. tear down whatever is running cluster-wide
#   2. bring the arm up (per topology.env workload sizing)
#   3. wait warmup
#   4. snapshot for SOAK seconds (per-container CPU/mem on every node,
#      per-node NIC tx/rx, PromQL replay on the right query endpoint)
#   5. tear down
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "${SCRIPT_DIR}")"
source "${PKG_DIR}/topology.env"

RUN_ID="${RUN_ID:-mvp-multinode-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${RUN_BASE}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
SOAK="${SOAK_S:-60}"

echo "[sweep] RUN_ID=${RUN_ID}  SOAK=${SOAK}s  out=${RUN_DIR}" | tee -a "${RUN_DIR}/run.log"
echo "[sweep] workload: PER_AGENT_CARDINALITY=${PER_AGENT_CARDINALITY}  EXPORTER_FREQ_HZ=${EXPORTER_FREQ_HZ}  EXPORTER_SDK_AGG=${EXPORTER_SDK_AGG}  EXPORTER_SDK_WINDOW=${EXPORTER_SDK_WINDOW}  N_PRODUCERS_PER_NODE=${N_PRODUCERS_PER_NODE}" | tee -a "${RUN_DIR}/run.log"

# ── ssh sync configs once ──
echo "[sweep] sync configs to all 4 nodes" | tee -a "${RUN_DIR}/run.log"
bash "${SCRIPT_DIR}/run_demo.sh" sync >> "${RUN_DIR}/run.log" 2>&1

# ── replay queries ──
QUERIES_JSON="${ROOT}/deploy/mvp-singlenode/scripts/queries-e2e.json"

run_arm() {
    local arm=$1
    local out="${RUN_DIR}/${arm}"
    mkdir -p "${out}"
    echo "[sweep] === arm=${arm} starting ==="| tee -a "${RUN_DIR}/run.log"

    # tear down anything residual
    bash "${SCRIPT_DIR}/run_demo.sh" down >> "${RUN_DIR}/run.log" 2>&1 || true
    sleep 3

    # bring up
    bash "${SCRIPT_DIR}/run_demo.sh" up "${arm}" >> "${out}/up.log" 2>&1
    echo "[sweep] arm=${arm} brought up; warmup already happened in up()"  | tee -a "${RUN_DIR}/run.log"
    sleep 5

    # determine query endpoint per arm (b0/b1 → prometheus, asap → backend)
    local q_endpoint
    if [ "${arm}" = "asap" ]; then q_endpoint="http://${NODE2_IP}:9091"
    else                            q_endpoint="http://${NODE2_IP}:9090"
    fi

    # background: MetricsQL replay
    if [ -f "${QUERIES_JSON}" ]; then
        timeout $((SOAK+10)) python3 "${ROOT}/deploy/mvp-singlenode/scripts/metricsql_replay.py" \
            --target "${q_endpoint}" \
            --queries "${QUERIES_JSON}" \
            --duration "${SOAK}" \
            --out "${out}/replay.jsonl" \
            > "${out}/replay.log" 2>&1 &
        local REPLAY_PID=$!
    fi

    # foreground: resource snapshot for SOAK seconds (this blocks)
    bash "${SCRIPT_DIR}/snapshot_resources.sh" "${arm}" "${SOAK}" "${out}" \
        > "${out}/snap.log" 2>&1

    # wait for replay to finish (or timeout)
    [ -n "${REPLAY_PID:-}" ] && wait ${REPLAY_PID} 2>/dev/null || true

    # save container-summary for the user
    cp -f "${out}/container-summary-${arm}.csv" "${RUN_DIR}/" 2>/dev/null || true
    cp -f "${out}/nic-${arm}.csv"               "${RUN_DIR}/" 2>/dev/null || true

    # snapshot of prometheus / backend ingest counts (legacy quick checks)
    curl -s "${q_endpoint}/api/v1/query?query=count(http_requests_total)" \
        > "${out}/ingest-count.json" 2>&1 || true
    curl -s "${q_endpoint}/api/v1/query?query=sum(rate(http_requests_total[1m]))" \
        > "${out}/ingest-rate.json" 2>&1 || true

    # Full per-arm ingest validation: distinct series, distinct producers,
    # per-series sample rate, samples-in-1m. Writes validate-${arm}.{json,md}.
    ARM=${arm} OUT=${out} \
        N_PRODUCERS_PER_NODE=${N_PRODUCERS_PER_NODE} \
        PER_AGENT_CARDINALITY=${PER_AGENT_CARDINALITY} \
        EXPORTER_FREQ_HZ=${EXPORTER_FREQ_HZ} \
        NODE2_IP=${NODE2_IP} \
        bash "${SCRIPT_DIR}/validate_arm.sh" \
        > "${out}/validate.log" 2>&1 || \
        echo "[sweep] validate ${arm} non-fatal err — see ${out}/validate.log" \
            | tee -a "${RUN_DIR}/run.log"
    cp -f "${out}/validate-${arm}.json" "${RUN_DIR}/" 2>/dev/null || true
    cp -f "${out}/validate-${arm}.md"   "${RUN_DIR}/" 2>/dev/null || true

    # Data-freshness probe: 60 polls × 100ms × per-tier endpoint, computes
    # Δ = poll_ts_ms - probe_value_ms (probe value = source emission ts_ms).
    ARM=${arm} OUT=${out} NODE2_IP=${NODE2_IP} N_SAMPLES=60 POLL_MS=100 \
        bash "${SCRIPT_DIR}/measure_freshness.sh" \
        > "${out}/freshness.log" 2>&1 || \
        echo "[sweep] freshness ${arm} non-fatal err — see ${out}/freshness.log" \
            | tee -a "${RUN_DIR}/run.log"
    cp -f "${out}/freshness-${arm}.csv" "${RUN_DIR}/" 2>/dev/null || true

    # tear down
    bash "${SCRIPT_DIR}/run_demo.sh" down >> "${RUN_DIR}/run.log" 2>&1 || true
    echo "[sweep] === arm=${arm} done ===" | tee -a "${RUN_DIR}/run.log"
}

for arm in b0 b1 asap; do
    run_arm "${arm}" || echo "[sweep] arm=${arm} FAILED but continuing" | tee -a "${RUN_DIR}/run.log"
done

# ── aggregate report ──
python3 - <<EOF > "${RUN_DIR}/MVP_REPORT.md"
import csv, glob, json, os
RUN_DIR = "${RUN_DIR}"
print("# MVP multi-node sweep report")
print(f"\\nRun: \`${RUN_ID}\`  |  Soak: \`${SOAK}s\`  |  Workload: PER_AGENT_CARDINALITY=${PER_AGENT_CARDINALITY}, FREQ_HZ=${EXPORTER_FREQ_HZ}, SDK_AGG=${EXPORTER_SDK_AGG}, SDK_WINDOW=${EXPORTER_SDK_WINDOW}, N_PRODUCERS_PER_NODE=${N_PRODUCERS_PER_NODE}\\n")
print("## §1 Per-arm NIC bandwidth (cluster-wide, /sys/class/net/enp130s0f0)\\n")
print("| arm | node | role | rx_MB/s | tx_MB/s | rx_total_MB | tx_total_MB |")
print("|---|---|---|---|---|---|---|")
roles = {"node0":"producer+agent-a","node1":"(unused since #400)","node2":"backend","node3":"producer+agent-b"}
for f in sorted(glob.glob(os.path.join(RUN_DIR,"nic-*.csv"))):
    arm = os.path.basename(f).replace("nic-","").replace(".csv","")
    with open(f) as fh:
        r = csv.DictReader(fh)
        for row in r:
            print(f"| {arm} | {row['node']} | {roles.get(row['node'],'')} | {float(row['rx_bytes_per_s'])/1e6:.2f} | {float(row['tx_bytes_per_s'])/1e6:.2f} | {float(row['rx_bytes_total'])/1e6:.1f} | {float(row['tx_bytes_total'])/1e6:.1f} |")
print()
print("## §2 Per-container resource usage\\n")
print("| arm | host | container | cpu_mean_% | cpu_max_% | mem_mean_MiB | mem_max_MiB | n |")
print("|---|---|---|---|---|---|---|---|")
for f in sorted(glob.glob(os.path.join(RUN_DIR,"container-summary-*.csv"))):
    with open(f) as fh:
        r = csv.DictReader(fh)
        for row in r:
            print(f"| {row['arm']} | {row['host']} | {row['container']} | {row['cpu_mean_perc']} | {row['cpu_max_perc']} | {row['mem_mean_mib']} | {row['mem_max_mib']} | {row['n_samples']} |")
print()
print("## §3 Ingest rate observed at query backend\\n")
print("| arm | ingest count | event rate (/s) |")
print("|---|---|---|")
for arm in ("b0","b1","asap"):
    cd = os.path.join(RUN_DIR, arm)
    cnt = "?"; rate = "?"
    try:
        with open(os.path.join(cd,"ingest-count.json")) as fh:
            d = json.load(fh)
            r = d.get("data",{}).get("result",[])
            if r: cnt = r[0]["value"][1]
    except: pass
    try:
        with open(os.path.join(cd,"ingest-rate.json")) as fh:
            d = json.load(fh)
            r = d.get("data",{}).get("result",[])
            if r: rate = r[0]["value"][1]
    except: pass
    print(f"| {arm} | {cnt} | {rate} |")
print()
print("## §4 Ingest validation (series count, producer count, per-series rate)\\n")
print("| arm | series_expected | series_observed | series_OK | producers_expected | producers_observed | producers_OK | per-series rate (events/s) | rate_OK |")
print("|---|---|---|---|---|---|---|---|---|")
for arm in ("b0","b1","asap"):
    f = os.path.join(RUN_DIR, f"validate-{arm}.json")
    if not os.path.isfile(f):
        print(f"| {arm} | — | NOT CAPTURED | — | — | — | — | — | — |")
        continue
    try:
        with open(f) as fh: v = json.load(fh)
        e = v["expected"]; o = v["observed"]; vd = v["verdict"]
        print(f"| {arm} | {e['series']} | {o['series_count']} | {vd['series_count']} | {e['producers']} | {o['producer_count']} | {vd['producer_count']} | observed={o['per_series_rate_events_per_s']}, expected={e['per_series_rate_events_per_s']} | {vd['per_series_rate']} |")
    except Exception as ex:
        print(f"| {arm} | — | parse error: {ex} | — | — | — | — | — | — |")
print()
print("## §5 PromQL replay latency per query class (p50/p99 ms)\\n")
print("| arm | query_id | n | p50_ms | p90_ms | p99_ms |")
print("|---|---|---|---|---|---|")
for arm in ("b0","b1","asap"):
    f = os.path.join(RUN_DIR, arm, "replay.jsonl")
    if not os.path.isfile(f):
        print(f"| {arm} | (no replay.jsonl) | — | — | — | — |")
        continue
    by_q = {}
    with open(f) as fh:
        for line in fh:
            try:
                d = json.loads(line)
                qid = d.get("query_id") or d.get("name") or d.get("query","?")[:24]
                lat = d.get("latency_ms") or d.get("elapsed_ms") or d.get("dur_ms")
                if lat is not None:
                    by_q.setdefault(qid, []).append(float(lat))
            except: pass
    if not by_q:
        print(f"| {arm} | (no parseable lines) | — | — | — | — |")
        continue
    for qid in sorted(by_q):
        vs = sorted(by_q[qid]); n = len(vs)
        def pct(p): return vs[max(0,min(n-1,int(p*n)))]
        print(f"| {arm} | {qid} | {n} | {pct(0.50):.1f} | {pct(0.90):.1f} | {pct(0.99):.1f} |")
print()
print("## §6.5 Data freshness — sample-generation-to-backend-write delay (ms)\\n")
print("| arm | tier | n | p50_ms | p90_ms | p99_ms | min_ms | max_ms |")
print("|---|---|---|---|---|---|---|---|")
for arm in ("b0","b1","asap"):
    f = os.path.join(RUN_DIR, f"freshness-{arm}.csv")
    if not os.path.isfile(f):
        print(f"| {arm} | (no freshness csv) | — | — | — | — | — | — |")
        continue
    by_tier = {}
    with open(f) as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            d = row.get("delta_ms","").strip()
            if not d: continue
            try: by_tier.setdefault(row["tier"], []).append(int(d))
            except: pass
    if not by_tier:
        print(f"| {arm} | (no successful polls) | — | — | — | — | — | — |")
        continue
    for tier in sorted(by_tier):
        v = sorted(by_tier[tier]); n = len(v)
        def pct(p): return v[max(0,min(n-1,int(p*n)))]
        print(f"| {arm} | {tier} | {n} | {pct(0.5)} | {pct(0.9)} | {pct(0.99)} | {v[0]} | {v[-1]} |")
print()
print("## §6 Cross-arm query-value accuracy (B0 = ground truth)\\n")
print("(Computed from the per-arm replay.jsonl response values for the same query.)\\n")
print("| query_id | b0_value | b1_value | asap_value | b1_rel_err_% | asap_rel_err_% |")
print("|---|---|---|---|---|---|")
def load_q_values(arm):
    f = os.path.join(RUN_DIR, arm, "replay.jsonl")
    out = {}
    if not os.path.isfile(f): return out
    with open(f) as fh:
        for line in fh:
            try:
                d = json.loads(line)
                qid = d.get("query_id") or d.get("name") or d.get("query","?")[:24]
                resp = d.get("response") or d.get("body") or {}
                if isinstance(resp, str):
                    try: resp = json.loads(resp)
                    except: continue
                r = resp.get("data",{}).get("result",[])
                if r:
                    val = r[0].get("value") or r[0].get("values",[[None,None]])[0]
                    if isinstance(val, list) and len(val) >= 2:
                        try:
                            v = float(val[1])
                            out.setdefault(qid, []).append(v)
                        except: pass
            except: pass
    return {k: sum(v)/len(v) for k,v in out.items() if v}
b0v = load_q_values("b0")
b1v = load_q_values("b1")
asv = load_q_values("asap")
all_q = sorted(set(b0v) | set(b1v) | set(asv))
for q in all_q:
    bv = b0v.get(q); b1V = b1v.get(q); av = asv.get(q)
    def fmt(x): return f"{x:.4g}" if x is not None else "—"
    def rerr(t,e): return "—" if t is None or e is None or t == 0 else f"{abs(e-t)/abs(t)*100:.2f}"
    print(f"| {q} | {fmt(bv)} | {fmt(b1V)} | {fmt(av)} | {rerr(bv,b1V)} | {rerr(bv,av)} |")
EOF

echo "[sweep] FINAL report at: ${RUN_DIR}/MVP_REPORT.md"
echo "[sweep] all artefacts under: ${RUN_DIR}/"
