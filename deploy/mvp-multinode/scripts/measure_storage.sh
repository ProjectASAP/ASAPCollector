#!/usr/bin/env bash
# Measure persistent host volumes. Missing expected volumes fail the harness.
set -euo pipefail
ARM=${1:?arm}
NODE=${2:?node label}
OUT=${3:?output csv}
ROOT=/mydata/mvp-multinode/data
echo "arm,node,component,bytes" > "${OUT}"
emit() {
  local component=$1 path=$2
  [ -d "${path}" ] || { echo "missing storage path: ${path}" >&2; exit 1; }
  printf '%s,%s,%s,%s\n' "${ARM}" "${NODE}" "${component}" "$(du -sb "${path}" | awk '{print $1}')" >> "${OUT}"
}
case "${ARM}:${NODE}" in
  b*:node1) emit victoriametrics "${ROOT}/victoriametrics" ;;
  asap*:node1)
    emit minio "${ROOT}/minio"
    emit gorilla-merger "${ROOT}/gorilla-merger"
    ;;
  asap*:node2) emit sketch-persistence "${ROOT}/sketch-persistence" ;;
esac
