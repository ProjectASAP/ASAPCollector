#!/usr/bin/env bash
# Extract real per-symbol trade frequencies from DEBS-2022 day-1 → /tmp/debs_symbol_counts.csv
# (the skewed per-key rate distribution used by epsilon_floor_vs_nitro_test.go).
CSV="${1:-/mydata/ASAPCollector/datasets_eval/debs/data/debs2022-gc-trading-day-08-11-21.csv}"
grep -vE '^#|^ID,' "$CSV" | awk -F, '{c[$1]++} END{for(s in c) print s","c[s]}' \
  | sort -t, -k2 -nr > /tmp/debs_symbol_counts.csv
echo "wrote /tmp/debs_symbol_counts.csv ($(wc -l </tmp/debs_symbol_counts.csv) symbols)"
