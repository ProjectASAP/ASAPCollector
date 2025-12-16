#!/usr/bin/env bash

set -euo pipefail

go run ./cmd/fakemetricload \
  --enable-ddsketch=true \
  --enable-histogram=false \
  --enable-raw=false \
  --distribution zipf \
  --distribution-mean 250 \
  --distribution-std 40 \
  --distribution-zipf-s 1.1 \
  --series 4 \
  --rate-per-series 25000 \
  --enable-counter=false \
  --export-interval 10s \
  --raw-export-interval 10ms \
  --endpoint localhost:4317 \
  --insecure \
  --ddsketch-accuracy 0.01 \
  --workers 4
