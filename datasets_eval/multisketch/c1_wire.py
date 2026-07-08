#!/usr/bin/env python3
"""Clean C1 bandwidth (sketch vs raw) on real gct, with trials + 95% CI.

Fixes the two harness gaps that polluted the per-family wire:
  1. TRUE per-family data slice — keep only {cpu_rate (Sum anchor for the
     ship-wait), memory_usage (Sum), <family sketch metric>}; drop the other
     6 sketch aliases that were forwarded raw and dominated the 57 MB.
  2. A RAW-forward baseline arm (agent-raw-coldoff.yaml, no asap_edge) replays
     the SAME slice so the comparison is apples-to-apples.

Per trial: sketch arm (via run_perfamily) → W_sketch + accuracy; raw arm →
W_raw. Reduction = W_raw / W_sketch. Aggregated mean ± 95% CI over N trials.
"""
import json, math, subprocess, sys, time, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
GCT = HERE.parent / "google_cluster"
STACK = str(HERE / "stack-coldoff.sh")
SOURCE = "/tmp/perfam-source.jsonl"
AGENT_METRICS = "http://127.0.0.1:8890/metrics"

# family → its sketch metric; the slice always also keeps the two Sum anchors.
FAM_METRIC = {
    "ddsketch": "google_cluster_2019_cpu_rate_q_ddsketch",
    "kll": "google_cluster_2019_cpu_rate_q_kll",
    "hll": "google_cluster_2019_cpu_rate_card_hll",
}
ANCHORS = {"google_cluster_2019_cpu_rate", "google_cluster_2019_memory_usage"}


def slice_family(fam):
    keep = ANCHORS | {FAM_METRIC[fam]}
    dst = f"/tmp/perfam-{fam}.jsonl"
    n = 0
    with open(SOURCE) as f, open(dst, "w") as o:
        for line in f:
            try:
                m = json.loads(line)
            except Exception:
                continue
            if m.get("metric") in keep:
                o.write(line)
                n += 1
    return dst, n


def agent_wire():
    """(bytes, points) shipped agent→backend, from the agent's otel telemetry."""
    wire = pts = None
    try:
        text = urllib.request.urlopen(AGENT_METRICS, timeout=10).read().decode()
        for line in text.splitlines():
            if "otlp/backend" not in line:
                continue
            if line.startswith("otelcol_exporter_sent_metric_points_total"):
                pts = float(line.rsplit(" ", 1)[1])
            elif line.startswith("otelcol_exporter_queue_batch_send_size_bytes_sum"):
                wire = float(line.rsplit(" ", 1)[1])
    except Exception:
        pass
    return wire, pts


def run_sketch(fam):
    """Sketch arm via run_perfamily; returns (W_sketch_bytes, accuracy_dict)."""
    subprocess.run([sys.executable, str(HERE / "run_perfamily.py"),
                    "--arms", fam, "--window", "120s"],
                   cwd=str(HERE), check=False, timeout=600)
    rec = json.loads((HERE / "results" / "perfamily-all.json").read_text()).get(fam, {})
    return rec.get("agent_wire_bytes_to_backend"), rec.get("score", {})


def run_raw(fam, sliced):
    """Raw-forward arm: replay the same slice through a no-asap_edge agent."""
    subprocess.run([STACK, "down"], cwd=str(ROOT), timeout=60)
    subprocess.run([STACK, "up", str(HERE / "workloads" / f"{fam}.yaml"),
                    str(HERE / "agent-raw-coldoff.yaml")],
                   cwd=str(ROOT), check=True, timeout=180)
    subprocess.call([sys.executable, str(GCT / "run.py"), "replay",
                     "--jsonl", sliced, "--endpoint", "127.0.0.1:4317",
                     "--pace-factor", "0", "--wall-clock-anchor"])
    time.sleep(20)  # let the export queue drain
    wire, _ = agent_wire()
    subprocess.run([STACK, "down"], cwd=str(ROOT), timeout=60)
    return wire


def ci95(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return (xs[0] if xs else float("nan")), 0.0
    m = sum(xs) / len(xs)
    sd = (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5
    return m, 1.96 * sd / math.sqrt(len(xs))


def main():
    fams = (sys.argv[1].split(",") if len(sys.argv) > 1 else ["ddsketch", "hll"])
    trials = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    out = {}
    for fam in fams:
        sliced, n = slice_family(fam)
        print(f"\n##### {fam}: sliced {n} pts → {sliced}", file=sys.stderr)
        reds, ws, wr, accs = [], [], [], []
        for t in range(trials):
            print(f"  [{fam}] trial {t+1}/{trials} sketch arm...", file=sys.stderr)
            w_s, acc = run_sketch(fam)
            print(f"  [{fam}] trial {t+1}/{trials} raw arm...", file=sys.stderr)
            w_r = run_raw(fam, sliced)
            if w_s and w_r and w_s > 0:
                reds.append(w_r / w_s); ws.append(w_s); wr.append(w_r); accs.append(acc)
            print(f"    W_sketch={w_s} W_raw={w_r} reduction={(w_r/w_s if w_s else 0):.1f}x",
                  file=sys.stderr)
        rm, rci = ci95(reds)
        out[fam] = dict(trials=len(reds), reduction_mean=rm, reduction_ci95=rci,
                        W_sketch_mean=ci95(ws)[0], W_raw_mean=ci95(wr)[0],
                        accuracy_last=accs[-1] if accs else None)
        print(f"##### {fam}: reduction {rm:.1f}× ±{rci:.1f} (n={len(reds)})", file=sys.stderr)
    (HERE / "results" / "c1-wire.json").write_text(json.dumps(out, indent=2, default=str) + "\n")
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
