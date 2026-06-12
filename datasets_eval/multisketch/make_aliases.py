#!/usr/bin/env python3
"""Emit family-aliased metric copies from a mapped google_cluster OTLP JSONL.

The fused asap_edge processor assigns exactly ONE warm family per metric
NAME, and the base mapper (otlp_mapper.py) emits only two metric names
(google_cluster_2019_cpu_rate, google_cluster_2019_memory_usage). To run
ALL six sketch families simultaneously on one collector — and to put TWO
families on the SAME underlying series (the "multiple sketches per metric"
test) — we duplicate each cpu_rate row under several family-specific
metric NAME aliases that share identical value + attributes.

Because the duplicated rows carry the SAME value and SAME label set, the
exact offline ground truth (gt_eval) computed over an alias is identical
to the ground truth over the base cpu_rate metric — so each family is
still scored against the real trace's true answer.

Aliases (all derived from google_cluster_2019_cpu_rate, the densest metric):
  *_q_ddsketch   quantile family routed to DDSketch
  *_q_kll        quantile family routed to KLL          (head-to-head vs ddsketch)
  *_topk_cs      topk     family routed to CountSketch
  *_topk_cms     topk     family routed to CMS-with-heap (head-to-head vs CS)
  *_card_hll     cardinality family routed to HLL
  *_freq_cms     frequency/point family routed to CountMinSketch
  *_sum          (kept as the raw cpu_rate name) sum / exact-agg

memory_usage is passed through unchanged for the by-zone quantile + sum.

Usage:
  make_aliases.py --in /tmp/gct-otlp.jsonl --out /tmp/gct-otlp-aliased.jsonl \
      [--families ddsketch,kll,cs,cms,hll,cmsfreq] [--sample-p 1.0] [--seed 1]
"""
from __future__ import annotations
import argparse
import json
import random
import sys
from pathlib import Path

PREFIX = "google_cluster_2019_cpu_rate"

ALL_ALIASES = {
    "ddsketch": PREFIX + "_q_ddsketch",
    "kll":      PREFIX + "_q_kll",
    "cs":       PREFIX + "_topk_cs",
    "cms":      PREFIX + "_topk_cms",
    "hll":      PREFIX + "_card_hll",
    "cmsfreq":  PREFIX + "_freq_cms",
}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--families", default="ddsketch,kll,cs,cms,hll,cmsfreq",
                    help="comma list of alias families to emit")
    ap.add_argument("--keep-base", action="store_true", default=True,
                    help="also keep the original cpu_rate/memory_usage rows (sum/exact)")
    ap.add_argument("--sample-p", type=float, default=1.0,
                    help="Bernoulli keep-probability applied ONLY to the alias rows "
                         "(simulates edge sampling); base rows always kept.")
    ap.add_argument("--sample-families", default="",
                    help="comma list of alias families to subsample; empty = all aliases")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args(argv)

    want = [f.strip() for f in args.families.split(",") if f.strip()]
    for f in want:
        if f not in ALL_ALIASES:
            print(f"unknown family {f}; valid={list(ALL_ALIASES)}", file=sys.stderr)
            return 2
    sample_fams = set(f.strip() for f in args.sample_families.split(",") if f.strip()) or set(want)
    rng = random.Random(args.seed)

    n_in = n_out = n_dropped = 0
    with open(args.inp) as fin, open(args.out, "w") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            n_in += 1
            if args.keep_base:
                fout.write(json.dumps(r) + "\n")
                n_out += 1
            if r["metric"] != PREFIX:
                continue  # only cpu_rate gets aliased
            for fam in want:
                if args.sample_p < 1.0 and fam in sample_fams:
                    if rng.random() > args.sample_p:
                        n_dropped += 1
                        continue
                a = dict(r)
                a["metric"] = ALL_ALIASES[fam]
                fout.write(json.dumps(a) + "\n")
                n_out += 1

    print(f"make_aliases: in={n_in} out={n_out} dropped_by_sampling={n_dropped} "
          f"families={want} sample_p={args.sample_p} -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
