#!/usr/bin/env python3
"""Generate per-family isolated workload+agent-config+queries from the
all-families set, so each family can run on its OWN fresh stack and the
agent's total wire bytes == that family's wire (clean per-family size).

Each family arm keeps the Sum metrics too (cheap, gives a lossless anchor +
a sum-sized baseline), plus exactly ONE sketch family.
"""
from __future__ import annotations
import json, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# family-id -> (alias metric, family, extra agent keys, query, gt)
FAMILIES = {
    "ddsketch": dict(
        metric="google_cluster_2019_cpu_rate_q_ddsketch", family="ddsketch",
        agent={"tier": "warm", "delta_transmission": True},
        wl={"sketch_family_override": "DDSketch", "target_path": "warm"},
        queries=[("ddsketch-p99", "quantile_over_time(0.99, {m}[60s])",
                  {"op": "quantile", "q": 0.99, "by": []}),
                 ("ddsketch-p50", "quantile_over_time(0.50, {m}[60s])",
                  {"op": "quantile", "q": 0.50, "by": []})]),
    "kll": dict(
        metric="google_cluster_2019_cpu_rate_q_kll", family="kll",
        agent={"tier": "warm", "k": 200},
        wl={"sketch_family_override": "KLL", "target_path": "warm"},
        queries=[("kll-p99", "quantile_over_time(0.99, {m}[60s])",
                  {"op": "quantile", "q": 0.99, "by": []}),
                 ("kll-p50", "quantile_over_time(0.50, {m}[60s])",
                  {"op": "quantile", "q": 0.50, "by": []})]),
    "countsketch": dict(
        metric="google_cluster_2019_cpu_rate_topk_cs", family="countsketch",
        agent={"tier": "warm", "rows": 5, "cols": 2048, "delta_transmission": True,
               "emit_heap": True, "heap_size": 100, "item_label": "host"},
        wl={"sketch_family_override": "CountSketch", "target_path": "warm", "item_label": "host"},
        queries=[("countsketch-topk-host", "topk(10, sum by (host) ({m}))",
                  {"op": "topk_sum", "k": 10, "key_label": "host"})]),
    "countsketch_noheap": dict(
        metric="google_cluster_2019_cpu_rate_topk_cms", family="countsketch",
        agent={"tier": "warm", "rows": 5, "cols": 2048, "delta_transmission": True,
               "item_label": "host"},
        wl={"sketch_family_override": "CountSketch", "target_path": "warm", "item_label": "host"},
        queries=[("countsketch-noheap-topk-host", "topk(10, sum by (host) ({m}))",
                  {"op": "topk_sum", "k": 10, "key_label": "host"})]),
    "hll": dict(
        metric="google_cluster_2019_cpu_rate_card_hll", family="hll",
        agent={"tier": "warm", "delta_transmission": True, "item_label": "service"},
        wl={"sketch_family_override": "HLL", "target_path": "warm", "item_label": "service"},
        queries=[("hll-card-service", "count({m})",
                  {"op": "count_distinct", "key_label": "service"})]),
    "countminsketch": dict(
        metric="google_cluster_2019_cpu_rate_freq_cms", family="countminsketch",
        agent={"tier": "warm", "rows": 5, "cols": 2048, "delta_transmission": True,
               "item_label": "service"},
        wl={"sketch_family_override": "CountMinSketch", "target_path": "warm", "item_label": "service"},
        queries=[("cms-freq-service", 'count_over_time({m}{{service="svc-000003"}}[60s])',
                  {"op": "frequency", "item_label": "service", "item_value": "svc-000003"})]),
}

AGENT_HEADER = """# Auto-generated per-family agent config (multisketch eval). See make_perfamily.py.
receivers:
  otlp:
    protocols:
      grpc: {endpoint: 0.0.0.0:4317, max_recv_msg_size_mib: 4096}
      http: {endpoint: 0.0.0.0:4318}
processors:
  memory_limiter: {check_interval: 1s, limit_mib: 8192, spike_limit_mib: 1024}
  asap_edge:
    shard_count: 12
    window_duration: 60s
    drop_original: true
    max_series: 200000
    delta_transmission: false
    metrics:
      - {metric: google_cluster_2019_cpu_rate, family: sum, tier: both}
      - {metric: google_cluster_2019_memory_usage, family: sum, aggregate_by: [zone], tier: both}
"""
AGENT_FOOTER = """    cold: {enabled: true, ship_endpoint: 'http://gorilla-merger:10908/ingest/gorilla', block_duration: 60s, reorder_grace: 2s, external_labels: {cluster: asap-mvp}}
    control_channel: {enabled: false}
exporters:
  otlp/backend:
    endpoint: data-plane:14317
    tls: {insecure: true}
    timeout: 120s
    sending_queue: {enabled: true, num_consumers: 4, queue_size: 5000}
service:
  pipelines:
    metrics: {receivers: [otlp], processors: [memory_limiter, asap_edge], exporters: [otlp/backend]}
  telemetry:
    metrics:
      level: detailed
      readers: [{pull: {exporter: {prometheus: {host: 0.0.0.0, port: 8890}}}}]
"""


def gen(fid, sample_p=None, outdir=HERE):
    f = FAMILIES[fid]
    m = f["metric"]
    # agent config
    entry = {"metric": m, "family": f["family"], **f["agent"]}
    if sample_p is not None:
        entry["sample_p"] = sample_p
    lines = [AGENT_HEADER.rstrip("\n")]
    def emit(d, indent="      "):
        out = [f"{indent}- metric: {d['metric']}"]
        for k, v in d.items():
            if k == "metric":
                continue
            out.append(f"{indent}  {k}: {json.dumps(v)}")
        return "\n".join(out)
    lines.append(emit(entry))
    lines.append(AGENT_FOOTER.rstrip("\n"))
    suffix = f"-p{int(sample_p*100):03d}" if sample_p is not None else ""
    agent_path = outdir / f"agent-{fid}{suffix}.yaml"
    agent_path.write_text("\n".join(lines) + "\n")

    # workload
    wl = [{"metric_name": "google_cluster_2019_cpu_rate",
           "query_string": "sum(google_cluster_2019_cpu_rate)",
           "accuracy_sla": 0.0, "assign_to_role": "agent"},
          {"metric_name": "google_cluster_2019_memory_usage",
           "query_string": "sum by (zone) (google_cluster_2019_memory_usage)",
           "accuracy_sla": 0.0, "assign_to_role": "agent", "grouping_labels": ["zone"]}]
    q0 = f["queries"][0][1].format(m=m)
    wl.append({"metric_name": m, "query_string": q0, "accuracy_sla": 0.05,
               "assign_to_role": "agent", **f["wl"]})
    import yaml  # noqa
    wl_path = outdir / "workloads" / f"{fid}.yaml"
    wl_path.write_text(yaml.safe_dump(wl, sort_keys=False))

    # queries
    qs = []
    for qid, qtmpl, gt in f["queries"]:
        gt = dict(gt, metric=m)
        qs.append({"id": qid, "kind": gt["op"], "metricsql": qtmpl.format(m=m), "gt": gt})
    # always include the sum queries
    qs += [{"id": "sum-cpu", "kind": "sum",
            "metricsql": "sum(google_cluster_2019_cpu_rate)",
            "gt": {"op": "sum", "metric": "google_cluster_2019_cpu_rate", "by": []}},
           {"id": "sum-mem-by-zone", "kind": "sum",
            "metricsql": "sum by (zone) (google_cluster_2019_memory_usage)",
            "gt": {"op": "sum", "metric": "google_cluster_2019_memory_usage", "by": ["zone"]}}]
    q_path = outdir / f"queries-{fid}.json"
    q_path.write_text(json.dumps(qs, indent=2) + "\n")
    print(f"  {fid}{suffix}: agent={agent_path.name} wl={wl_path.name} q={q_path.name}", file=sys.stderr)


if __name__ == "__main__":
    (HERE / "workloads").mkdir(exist_ok=True)
    for fid in FAMILIES:
        gen(fid)
    # sampling variants for the sampling-capable families
    for fid in ("ddsketch", "countsketch", "countminsketch", "hll", "kll"):
        gen(fid, sample_p=0.25)
    print("done", file=sys.stderr)
