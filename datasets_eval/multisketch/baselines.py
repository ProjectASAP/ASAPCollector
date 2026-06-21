# LIMITATION FOUND: --network host makes lo carry BOTH the replay→agent leg
# (constant raw ~35MB) AND the agent→data-plane leg, so lo cannot isolate the
# wire — all arms measured ~35MB. The clean compression measurement needs the
# CLUSTER (per-node NIC isolates agent→backend). Encoding factor was instead
# measured offline via gzip of the payload (12.3x). See evaluation-plan (c‴).
#!/usr/bin/env python3
"""C1 encoding-factor baselines: ASAP-sketch vs raw-OTLP vs raw+gzip vs raw+zstd,
on the SAME real-gct family slice.

The cold-off stack is `--network host`, so per-container RX isn't isolatable;
instead we read the loopback interface byte counter (`/proc/net/dev` lo) around
each replay. The replay (tens of MB) dominates the small control/query overhead,
so the delta is the actual on-the-wire bytes — and it reflects exporter
compression (gzip/zstd), which the serialized `:8890` metric does not.

Decomposition the paper wants:
  aggregation_factor  = W_raw / W_sketch          (sketching, no compression)
  encoding_factor     = W_raw / W_raw_gzip        (compression alone)
  net ASAP vs raw+gzip = W_raw_gzip / W_sketch     (the honest baseline)
"""
import json, math, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
GCT = HERE.parent / "google_cluster"
STACK = str(HERE / "stack-coldoff.sh")
SLICE = "/tmp/perfam-ddsketch.jsonl"  # set by c1_wire.slice_family before this runs

ARMS = {
    "raw":      "agent-raw-coldoff.yaml",
    "raw_gzip": "agent-raw-gzip.yaml",
    "raw_zstd": "agent-raw-zstd.yaml",
    "sketch":   "agent-ddsketch-coldoff.yaml",
}


def lo_rx_bytes():
    for line in Path("/proc/net/dev").read_text().splitlines():
        if line.strip().startswith("lo:"):
            return int(line.split(":")[1].split()[0])
    return 0


def replay(jsonl):
    subprocess.call([sys.executable, str(GCT / "run.py"), "replay",
                     "--jsonl", jsonl, "--endpoint", "127.0.0.1:4317",
                     "--pace-factor", "0", "--wall-clock-anchor"])


def run_arm(agent, settle=35):
    subprocess.run([STACK, "down"], cwd=str(ROOT), timeout=60)
    subprocess.run([STACK, "up", str(HERE / "workloads" / "ddsketch.yaml"),
                    str(HERE / agent)], cwd=str(ROOT), check=True, timeout=180)
    time.sleep(3)
    b0 = lo_rx_bytes()
    replay(SLICE)
    time.sleep(settle)  # let the export queue + sketch ship fully
    wire = lo_rx_bytes() - b0
    subprocess.run([STACK, "down"], cwd=str(ROOT), timeout=60)
    return wire


def ci95(xs):
    xs = [x for x in xs if x]
    if len(xs) < 2:
        return (xs[0] if xs else float("nan")), 0.0
    m = sum(xs) / len(xs)
    sd = (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5
    return m, 1.96 * sd / math.sqrt(len(xs))


def main():
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    data = {a: [] for a in ARMS}
    for t in range(trials):
        for arm, agent in ARMS.items():
            w = run_arm(agent)
            data[arm].append(w)
            print(f"  trial {t+1} {arm:9} lo-wire {w/1e6:8.2f} MB", file=sys.stderr)
    means = {a: ci95(v) for a, v in data.items()}
    out = {a: dict(wire_MB=m / 1e6, ci95_MB=c / 1e6, n=len([x for x in data[a] if x]))
           for a, (m, c) in means.items()}
    # factors
    wr, ws = means["raw"][0], means["sketch"][0]
    wg = means["raw_gzip"][0]
    out["_factors"] = dict(
        aggregation_raw_over_sketch=wr / ws if ws else None,
        encoding_raw_over_gzip=wr / wg if wg else None,
        net_asap_vs_rawgzip=wg / ws if ws else None,
    )
    (HERE / "results" / "baselines.json").write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
