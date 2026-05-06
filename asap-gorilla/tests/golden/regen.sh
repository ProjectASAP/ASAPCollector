#!/usr/bin/env bash
# Regenerate `single_series.gor` against the canonical Go encoder.
#
# This produces a fixture byte sequence that the Rust byte-compat
# tests under `tests/byte_compat.rs` consume to assert byte-parity
# with the Go gorillaprocessor.
#
# Inputs (must stay in sync with the Rust `fixture_samples()` helper):
#
#   metric: asap_gorilla_fixture_metric
#   labels: env=test, instance=i-fixture, region=us-east-1
#   samples: 64 points at base=1_700_000_000_000_000_000 ns,
#            ts step = 1s, value = i * 0.5
#
# The Go gorillaprocessor encoder is in
# `opentelemetry-collector-contrib-patch/processor/gorillaprocessor/`.
# This script invokes a small Go program in that package that:
#
#   1. constructs `[]point{...}` matching the inputs above
#   2. wraps them into a `series` map keyed by the same metric/labels
#   3. calls `buildObjects(series, 0)` (no max-bytes split)
#   4. asserts exactly one object is returned
#   5. writes that object's bytes to single_series.gor
#
# Until the helper main lands, this script is a placeholder that
# documents the contract.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PROCESSOR_DIR="${REPO_ROOT}/opentelemetry-collector-contrib-patch/processor/gorillaprocessor"

if [[ ! -d "${PROCESSOR_DIR}" ]]; then
  echo "error: ${PROCESSOR_DIR} not found — clone with submodules?" >&2
  exit 1
fi

# Sketch of the eventual workflow:
#
#   pushd "${PROCESSOR_DIR}" >/dev/null
#   go run ./internal/fixturegen \
#       -metric asap_gorilla_fixture_metric \
#       -label env=test -label instance=i-fixture -label region=us-east-1 \
#       -base 1700000000000000000 \
#       -count 64 \
#       -interval 1s \
#       -value-step 0.5 \
#       -out "${SCRIPT_DIR}/single_series.gor"
#   popd >/dev/null
#
# Until `internal/fixturegen` exists, this script exits non-zero so
# the byte-compat test continues to be skipped (per its skip-on-
# missing-file branch) rather than asserting against stale data.

echo "regen.sh: cross-language fixture generator not yet implemented." >&2
echo "          The Rust byte-compat test will skip until the Go-side" >&2
echo "          fixturegen helper is added under ${PROCESSOR_DIR}." >&2
exit 1
