#!/usr/bin/env bash
# After the 5-family / N=1 / W=1s / c=1k sweep completes, run the
# 4 HLL N=1 cells that had agent_*=NaN in the original sweep:
#   hll_N1_w100ms_c10000, hll_N1_w100ms_c100000,
#   hll_N1_w1000ms_c10000, hll_N1_w1000ms_c100000.
# These verify Fix 3 (bytes-sample-window 5s → 15s + warmup)
# closes the agent docker-stats NaN gap.
set -euo pipefail
SCRIPT_DIR=/home/zeying/repos/ASAPCollector/deploy/scripts
OUT=/home/zeying/repos/sweep-eval/sketchcol-sweep-postfix-20260506-113446

# 100 ms / 1 s × 10 000 / 100 000 cells, HLL only.
bash "$SCRIPT_DIR/run_e2e_sweep.sh" \
    --out-dir "$OUT" \
    --soak-secs 60 \
    --pre-transition-secs 15 \
    --skip-cells "ddsketch,kll,cs,cms" \
    --ns "1" \
    --scrapes-ms "100,1000" \
    --cardinalities "10000,100000"
