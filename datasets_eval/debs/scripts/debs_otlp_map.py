#!/usr/bin/env python3
"""Map real DEBS 2022 trading events → an OTLP-replay JSONL that exercises the
asap agent's four sketch families, and compute the EXACT ground truth for each
query so the backend answer can be scored:

  metric (agent family)              query           GT
  ---------------------------------  --------------  --------------------------
  top_endpoint_qps    (CountSketch)  topk symbols    exact top-10 by trade count
  endpoint_request_freq (CountMin)   freq of a key   exact count of one symbol
  unique_users_per_min (HLL)         distinct count  exact distinct symbols
  http_requests_total_latency_ms(KLL) quantile       exact p50/p90/p99 of Ask price

JSONL line: {"timestamp_ms","series_id","metric_name","value","attributes":{}}
Usage: debs_otlp_map.py <csv> <n_events> <out.jsonl> <gt.json>
"""
import sys, json, time
from collections import Counter

csv, n, out_jsonl, gt_path = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

counts = Counter()      # symbol -> trade count (topk / frequency GT)
prices = []             # Ask prices (quantile GT)
distinct = set()
now_ms = int(time.time() * 1000)

read = 0
with open(csv, errors="replace") as f, open(out_jsonl, "w") as w:
    for line in f:
        if line[0] == "#" or line.startswith("ID,"):
            continue
        p = line.split(",")
        sym = p[0]
        if not sym:
            continue
        ts = now_ms + read  # monotone, wall-clock-anchored downstream
        # Metric names + labels match agent-allfamilies-coldoff.yaml so the
        # EXISTING all-families stack serves them (topk item_label=host,
        # cms/hll item_label=service).
        # topk (CountSketch) + frequency (CountMin): one point per event, symbol.
        w.write(json.dumps({"timestamp_ms": ts, "series_id": f"tk:{sym}",
                            "metric": "google_cluster_2019_cpu_rate_topk_cs", "value": 1.0,
                            "attributes": {"host": sym}}) + "\n")
        w.write(json.dumps({"timestamp_ms": ts, "series_id": f"fq:{sym}",
                            "metric": "google_cluster_2019_cpu_rate_freq_cms", "value": 1.0,
                            "attributes": {"service": sym}}) + "\n")
        # distinct (HLL): distinct label = symbol.
        w.write(json.dumps({"timestamp_ms": ts, "series_id": f"hll:{sym}",
                            "metric": "google_cluster_2019_cpu_rate_card_hll", "value": 1.0,
                            "attributes": {"service": sym}}) + "\n")
        counts[sym] += 1
        distinct.add(sym)
        # quantile (DDSketch + KLL): Ask price (col 5) when present.
        # ALL prices fold into ONE global sketch (constant item) so the backend
        # quantile query returns a single global p99 comparable to the global GT.
        # (Per-symbol DDSketch would need per-symbol GT; a single global quantile
        # is the clean headline number the user asked for.)
        if len(p) > 4 and p[4]:
            try:
                v = float(p[4])
                for m in ("google_cluster_2019_cpu_rate_q_ddsketch",
                          "google_cluster_2019_cpu_rate_q_kll"):
                    w.write(json.dumps({"timestamp_ms": ts, "series_id": "px:ALL",
                                        "metric": m, "value": v,
                                        "attributes": {"host": "ALL"}}) + "\n")
                prices.append(v)
            except ValueError:
                pass
        read += 1
        if read >= n:
            break

prices.sort()
def pq(q):
    return prices[min(len(prices) - 1, int(q * len(prices)))] if prices else None
top10 = counts.most_common(10)
# Point-frequency query is answered from the CountSketch heap (topk_cs), which
# retains the top heap_size=100 keys with their estimated counts — so pick a key
# comfortably INSIDE the heap (rank ~15) rather than at its edge. (The CountMin
# keyed frequency path is a warm-only safe-miss by design — Phase 2b unfinished —
# so in the cold-OFF stack a CMS point query returns no result; the CountSketch
# is itself a point-frequency sketch and answers the same question.)
freq_key, freq_true = counts.most_common(20)[-1] if len(counts) > 20 else top10[0]
gt = {
    "events": read,
    "topk10": [{"symbol": s, "count": c} for s, c in top10],
    "freq_query": {"symbol": freq_key, "true_count": freq_true},
    "distinct": len(distinct),
    "quantile": {"p50": pq(0.50), "p90": pq(0.90), "p99": pq(0.99), "n": len(prices)},
}
json.dump(gt, open(gt_path, "w"), indent=1)
print(f"# DEBS OTLP map: events={read} distinct={len(distinct)} prices={len(prices)}", file=sys.stderr)
print(f"# top symbol: {top10[0]}  freq_query symbol: {freq_key}(true={freq_true})", file=sys.stderr)
print(f"# price quantiles p50={pq(0.5)} p90={pq(0.9)} p99={pq(0.99)}", file=sys.stderr)
