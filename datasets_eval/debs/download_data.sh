#!/usr/bin/env bash
# Download DEBS 2022 Grand Challenge daily CSVs into datasets_eval/debs/data/
# Source: https://zenodo.org/records/6382482
# After download, run: cd analysis/code && python3 filter_data.py  (builds data_filtered/)

set -euo pipefail
ZENODO="https://zenodo.org/records/6382482/files"
DEST="$(cd "$(dirname "$0")" && pwd)/data"
mkdir -p "$DEST"
for day in 08-11-21 09-11-21 10-11-21 11-11-21 12-11-21 13-11-21 14-11-21; do
    f="debs2022-gc-trading-day-${day}.csv"
    if [[ -f "$DEST/$f" ]]; then
        echo "skip existing $f"
        continue
    fi
    echo "fetch $f"
    curl -fsSL -o "$DEST/$f" "$ZENODO/$f"
done
echo "done -> $DEST"
