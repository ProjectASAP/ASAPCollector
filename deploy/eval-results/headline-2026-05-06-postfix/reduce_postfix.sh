#!/usr/bin/env bash
# Reduce postfix sweep cells through fast_reduce.py and stash
# the per-cell accuracy CSVs under per-cell/ so build_stats_postfix.py
# picks them up.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SWEEP="/home/zeying/repos/sweep-eval/sketchcol-sweep-postfix-20260506-113446"
HEADLINE="/home/zeying/repos/sweep-eval/headline-2026-05-06"

mkdir -p "${HERE}/per-cell"

for cell_dir in "${SWEEP}"/*; do
    [[ -d "$cell_dir" ]] || continue
    cell="$(basename "$cell_dir")"
    if [[ ! -f "${cell_dir}/replay.jsonl" ]]; then
        continue
    fi
    if [[ ! -d "${cell_dir}/cold-truth" ]]; then
        echo "[skip] $cell — no cold-truth"
        continue
    fi
    out="${HERE}/per-cell/accuracy-${cell}.csv"
    echo "[reduce] $cell → $out"
    python3 "${HEADLINE}/fast_reduce.py" \
        --cell-dir "$cell_dir" \
        --out "$out" 2>&1 | tail -5 || true
done

echo "Done. Per-cell CSVs under ${HERE}/per-cell/"
